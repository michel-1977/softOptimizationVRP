from datetime import datetime, timedelta, timezone
import json
import math
import os
import time
import unicodedata
import difflib
from typing import Any, Dict, List, Optional, Set, Tuple
import urllib.error
import urllib.parse
import urllib.request

from solve_vrp.here_emulator import HerePlatformEmulator
from solve_vrp.here_platform import HerePlatformClient

EARTH_RADIUS_KM = 6371.0

DEFAULT_SEMANTIC_RADIUS_KM = 1.2
DEFAULT_TOP_K = 8
DEFAULT_AVG_SPEED_KMH = 40.0
DEFAULT_HERE_TIMEOUT_SEC = 12
DEFAULT_HERE_TRAFFIC_RADIUS_M = 300
DEFAULT_HERE_FORECAST_WINDOW_HOURS = 24
DEFAULT_HERE_FORECAST_INTERVAL_MIN = 120
DEFAULT_MUNICIPALITY_STEP_KM = 40.0 # was 20
DEFAULT_MUNICIPALITY_RADIUS_KM = 5.0
DEFAULT_OSM_TIMEOUT_SEC = 8
DEFAULT_OSRM_ROUTE_TIMEOUT_SEC = 10
DEFAULT_MUNICIPALITY_MAX_SAMPLES_PER_SEGMENT = 6 # was 12
DEFAULT_MUNICIPALITY_REVERSE_MIN_INTERVAL_MS = 1100
DEFAULT_AZURE_MAPS_REVERSE_MIN_INTERVAL_MS = 100
DEFAULT_MUNICIPALITY_REVERSE_SOURCE = "nominatim_reverse"
DEFAULT_AZURE_OPENAI_API_VERSION = "2024-10-21"
DEFAULT_AZURE_MAPS_REVERSE_API_VERSION = "2025-01-01"
DEFAULT_AZURE_MAPS_REVERSE_ENDPOINT = "https://atlas.microsoft.com/reverseGeocode"
DEFAULT_MUNICIPALITY_LLM_TIMEOUT_SEC = 90
DEFAULT_MUNICIPALITY_LLM_RETRIES = 2
DEFAULT_MUNICIPALITY_LLM_MAX_TOKENS = 4000
MUNICIPALITY_LLM_TEMPERATURE = 1
DEFAULT_OVERPASS_ENDPOINTS = (
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
)
DEFAULT_REVERSE_GEOCODER_ENDPOINTS = (
    "https://nominatim.openstreetmap.org/reverse",
)

KNOWN_CATEGORY_MAP = {
    ("amenity", "fuel"): "fuel",
    ("amenity", "charging_station"): "charging",
    ("amenity", "parking"): "parking",
    ("amenity", "parking_entrance"): "parking",
    ("amenity", "restaurant"): "food",
    ("amenity", "fast_food"): "food",
    ("amenity", "cafe"): "food",
    ("amenity", "bar"): "food",
    ("amenity", "pub"): "food",
    ("amenity", "hospital"): "healthcare",
    ("amenity", "clinic"): "healthcare",
    ("amenity", "pharmacy"): "healthcare",
    ("amenity", "car_repair"): "vehicle_service",
    ("amenity", "car_wash"): "vehicle_service",
    ("tourism", "hotel"): "lodging",
    ("tourism", "motel"): "lodging",
    ("shop", "supermarket"): "grocery",
    ("shop", "convenience"): "grocery",
    ("highway", "rest_area"): "rest_area",
    ("highway", "services"): "rest_area",
}

MUNICIPALITY_PLACE_WEIGHTS = {
    "city": 5,
    "town": 4,
    "municipality": 4,
    "village": 3,
    "borough": 3,
    "suburb": 2,
    "quarter": 2,
    "hamlet": 1,
    "neighbourhood": 1,
}

MUNICIPALITY_ADDRESS_PRIORITY = (
    "municipality",
    "city",
    "town",
    "village",
    "city_district",
    "district",
    "borough",
    "suburb",
    "quarter",
    "hamlet",
    "neighbourhood",
    "locality",
)

NON_MUNICIPALITY_ADMIN_FIELDS = {
    "country",
    "country_code",
    "state",
    "state_district",
    "province",
    "region",
    "county",
}

PROVINCE_ADDRESS_PRIORITY = (
    "province",
    "state",
    "state_district",
    "region",
    "county",
)

PROVINCE_CAPITAL_MEMBER_ROLES = (
    "admin_centre",
    "capital",
    "label",
)


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _safe_int_str(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return int(value)
    raw = str(value).strip()
    if not raw:
        return default
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return default
    try:
        return int(digits)
    except ValueError:
        return default


def _resolve_here_data_source(value: Any) -> str:
    raw = str(value or "here").strip().lower()
    if raw in {"emulator", "mock", "simulated", "synthetic"}:
        return "emulator"
    return "here"


def _resolve_municipality_reverse_source(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"azure", "azure_maps", "azure_maps_reverse", "azmaps"}:
        return "azure_maps_reverse"
    if raw in {"nominatim", "nominatim_reverse", "osm"}:
        return "nominatim_reverse"
    return DEFAULT_MUNICIPALITY_REVERSE_SOURCE


def _to_iso_z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_utc_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_console_print(*values: Any) -> None:
    text = " ".join(str(value) for value in values)
    try:
        print(text)
        return
    except UnicodeEncodeError:
        pass
    except Exception:
        return

    try:
        fallback = text.encode("ascii", errors="backslashreplace").decode("ascii")
        print(fallback)
    except Exception:
        try:
            print(repr(text))
        except Exception:
            return


def _normalize_categories(raw: Any) -> Set[str]:
    if not isinstance(raw, list):
        return set()
    normalized = set()
    for item in raw:
        if not isinstance(item, str):
            continue
        label = item.strip().lower()
        if label:
            normalized.add(label)
    return normalized


def _haversine_km(
    point_a: Tuple[float, float],
    point_b: Tuple[float, float],
) -> float:
    lat1, lon1 = map(math.radians, point_a)
    lat2, lon2 = map(math.radians, point_b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return EARTH_RADIUS_KM * 2 * math.asin(math.sqrt(h))


def _coordinate_key(lat: float, lng: float, precision: int = 6) -> str:
    return f"{round(float(lat), precision):.{precision}f},{round(float(lng), precision):.{precision}f}"


def _extract_municipality_from_reverse_payload(
    payload: Dict[str, Any],
) -> Tuple[Optional[str], Optional[str]]:
    address = payload.get("address", {})
    if not isinstance(address, dict):
        address = {}
    for key in MUNICIPALITY_ADDRESS_PRIORITY:
        value = str(address.get(key) or "").strip()
        if value:
            return value, key
    return None, None


def _extract_province_from_address(address: Any) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(address, dict):
        return None, None
    for key in PROVINCE_ADDRESS_PRIORITY:
        value = str(address.get(key) or "").strip()
        if value:
            return value, key
    return None, None


def _extract_country_code_from_address(address: Any) -> Optional[str]:
    if not isinstance(address, dict):
        return None
    value = str(address.get("country_code") or "").strip().upper()
    return value or None


def _escape_overpass_literal(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def _query_overpass_json(query: str, timeout_sec: int) -> Dict[str, Any]:
    body = urllib.parse.urlencode({"data": query}).encode("utf-8")
    payload = None
    last_error: Optional[str] = None
    for endpoint in DEFAULT_OVERPASS_ENDPOINTS:
        try:
            request = urllib.request.Request(
                endpoint,
                data=body,
                headers={
                    "User-Agent": "softOptimizationVRP/province-capital-resolver",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=max(2, timeout_sec)) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise RuntimeError("Unexpected Overpass payload.")
            remark = str(payload.get("remark") or "").strip()
            if remark:
                raise RuntimeError(f"Overpass remark: {remark}")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = f"{endpoint}: {exc}"
            time.sleep(0.15)
    raise RuntimeError(last_error or "No Overpass endpoint available.")


def _province_name_match_score(candidate: str, target: str) -> int:
    cand = str(candidate or "").strip().casefold()
    tgt = str(target or "").strip().casefold()
    if not cand or not tgt:
        return 99
    if cand == tgt:
        return 0
    if cand.startswith(tgt) or cand.endswith(tgt) or tgt in cand:
        return 1
    if tgt.startswith(cand) or tgt.endswith(cand) or cand in tgt:
        return 2
    return 99


def _pick_province_relation(
    elements: List[Dict[str, Any]],
    province_name: str,
    country_code: Optional[str],
) -> Optional[Dict[str, Any]]:
    if not province_name:
        return None
    candidates: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
    for element in elements:
        if not isinstance(element, dict) or element.get("type") != "relation":
            continue
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue
        relation_name = str(tags.get("name") or "").strip()
        name_score = _province_name_match_score(relation_name, province_name)
        if name_score >= 90:
            continue
        iso_code = str(tags.get("ISO3166-2") or "").strip().upper()
        cc = str(country_code or "").strip().upper()
        country_score = 0 if (cc and iso_code.startswith(f"{cc}-")) else (1 if cc else 0)
        admin_level = _safe_int(tags.get("admin_level"), 99)
        level_score = abs(admin_level - 6)
        members = element.get("members", [])
        has_capital_member = (
            isinstance(members, list)
            and any(
                str(member.get("role") or "").strip().lower()
                in PROVINCE_CAPITAL_MEMBER_ROLES
                for member in members
                if isinstance(member, dict)
            )
        )
        candidates.append(
            (
                (
                    name_score,
                    country_score,
                    level_score,
                    0 if has_capital_member else 1,
                    relation_name.casefold(),
                ),
                element,
            )
        )
    if not candidates:
        return None
    candidates.sort(key=lambda row: row[0])
    return candidates[0][1]


def _extract_capital_from_relation(
    relation: Dict[str, Any],
    elements: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    by_ref: Dict[Tuple[str, int], Dict[str, Any]] = {}
    for element in elements:
        if not isinstance(element, dict):
            continue
        ref_type = str(element.get("type") or "").strip().lower()
        ref_id = element.get("id")
        if ref_type and isinstance(ref_id, (int, float)):
            by_ref[(ref_type, int(ref_id))] = element

    members = relation.get("members", [])
    if not isinstance(members, list):
        members = []
    for role in PROVINCE_CAPITAL_MEMBER_ROLES:
        for member in members:
            if not isinstance(member, dict):
                continue
            member_role = str(member.get("role") or "").strip().lower()
            if member_role != role:
                continue
            member_type = str(member.get("type") or "").strip().lower()
            member_ref = member.get("ref")
            if not member_type or not isinstance(member_ref, (int, float)):
                continue
            element = by_ref.get((member_type, int(member_ref)))
            if not isinstance(element, dict):
                continue
            tags = element.get("tags", {})
            if not isinstance(tags, dict):
                tags = {}
            name = str(tags.get("name") or "").strip()
            if not name:
                continue
            lat = _safe_float(element.get("lat"))
            lng = _safe_float(element.get("lon"))
            if lat is None or lng is None:
                center = element.get("center")
                if isinstance(center, dict):
                    lat = _safe_float(center.get("lat"))
                    lng = _safe_float(center.get("lon"))
            osm_ref = f"{member_type}/{int(member_ref)}"
            return {
                "name": name,
                "lat": round(float(lat), 6) if lat is not None else None,
                "lng": round(float(lng), 6) if lng is not None else None,
                "osm_ref": osm_ref,
                "member_role": role,
                "source": "overpass_relation_member",
            }
    return None


def _resolve_province_capital(
    province_name: Optional[str],
    country_code: Optional[str],
    cache: Dict[str, Dict[str, Any]],
    errors: Optional[List[str]],
    timeout_sec: int,
) -> Dict[str, Any]:
    normalized_name = str(province_name or "").strip()
    cc = str(country_code or "").strip().upper()
    if not normalized_name:
        return {
            "status": "unknown",
            "province_name": None,
            "country_code": cc or None,
            "capital_name": None,
            "capital_osm_ref": None,
            "capital_lat": None,
            "capital_lng": None,
            "source": None,
            "error": None,
        }

    key = f"{cc}|{normalized_name.casefold()}"
    cached = cache.get(key)
    if isinstance(cached, dict):
        return cached

    escaped_name = _escape_overpass_literal(normalized_name)
    query = (
        f"[out:json][timeout:{max(5, timeout_sec)}];\n(\n"
        f'  relation["boundary"="administrative"]["name"="{escaped_name}"]["admin_level"~"4|5|6|7|8"];\n'
        f'  relation["type"="boundary"]["name"="{escaped_name}"]["admin_level"~"4|5|6|7|8"];\n'
        ");\nout body;\n>;\nout body;"
    )
    try:
        payload = _query_overpass_json(query, timeout_sec=max(3, timeout_sec))
        elements = payload.get("elements", [])
        if not isinstance(elements, list):
            raise RuntimeError("Overpass relation payload missing elements.")
        relation = _pick_province_relation(elements, normalized_name, cc)
        if not isinstance(relation, dict):
            result = {
                "status": "unknown",
                "province_name": normalized_name,
                "country_code": cc or None,
                "capital_name": None,
                "capital_osm_ref": None,
                "capital_lat": None,
                "capital_lng": None,
                "source": "overpass_relation_member",
                "error": "province_relation_not_found",
            }
            cache[key] = result
            return result

        capital = _extract_capital_from_relation(relation, elements)
        if not isinstance(capital, dict):
            result = {
                "status": "unknown",
                "province_name": normalized_name,
                "country_code": cc or None,
                "capital_name": None,
                "capital_osm_ref": None,
                "capital_lat": None,
                "capital_lng": None,
                "source": "overpass_relation_member",
                "error": "province_capital_not_found",
            }
            cache[key] = result
            return result

        result = {
            "status": "resolved",
            "province_name": normalized_name,
            "country_code": cc or None,
            "capital_name": capital.get("name"),
            "capital_osm_ref": capital.get("osm_ref"),
            "capital_lat": capital.get("lat"),
            "capital_lng": capital.get("lng"),
            "source": capital.get("source"),
            "member_role": capital.get("member_role"),
            "error": None,
        }
        cache[key] = result
        return result
    except Exception as exc:  # noqa: BLE001
        result = {
            "status": "error",
            "province_name": normalized_name,
            "country_code": cc or None,
            "capital_name": None,
            "capital_osm_ref": None,
            "capital_lat": None,
            "capital_lng": None,
            "source": "overpass_relation_member",
            "error": str(exc),
        }
        cache[key] = result
        if errors is not None:
            errors.append(
                f"province capital lookup failed for '{normalized_name}' ({cc or 'n/a'}): {exc}"
            )
        return result


def _append_unique_in_order(items: List[str], value: Any) -> None:
    text = _normalize_llm_text(value)
    if not text:
        return
    if items and items[-1].casefold() == text.casefold():
        return
    items.append(text)


def _mojibake_penalty(text: str) -> int:
    raw = str(text or "")
    return (
        raw.count("\uFFFD")
        + raw.count("Ã")
        + raw.count("Â")
        + raw.count("â")
    )


def _normalize_llm_text(value: Any) -> str:
    text = unicodedata.normalize("NFC", str(value or "").strip())
    if not text:
        return ""
    repaired = text
    if any(marker in text for marker in ("Ã", "Â", "â")):
        try:
            candidate = text.encode("latin-1").decode("utf-8")
            if _mojibake_penalty(candidate) < _mojibake_penalty(repaired):
                repaired = candidate
        except Exception:  # noqa: BLE001
            pass
    return unicodedata.normalize("NFC", repaired).strip()


def _normalize_municipality_vector(items: Any) -> List[str]:
    if not isinstance(items, list):
        return []
    normalized: List[str] = []
    for value in items:
        _append_unique_in_order(normalized, value)
    return normalized


def _normalize_road_vector(items: Any) -> List[str]:
    if not isinstance(items, list):
        return []
    normalized: List[str] = []
    seen: Set[str] = set()
    for value in items:
        text = _normalize_llm_text(value)
        if not text:
            continue
        lowered = text.casefold()
        if lowered in {"unnamed road", "road", "unknown road", "n/a"}:
            continue
        key = _canonical_name_key(text) or lowered
        if key in seen:
            continue
        seen.add(key)
        normalized.append(text)
    return normalized


def _sample_vector_items(items: Any, max_items: int) -> List[str]:
    if max_items <= 0:
        return []
    if not isinstance(items, list):
        return []
    cleaned: List[str] = []
    for value in items:
        text = _normalize_llm_text(value)
        if not text:
            continue
        if cleaned and cleaned[-1].casefold() == text.casefold():
            continue
        cleaned.append(text)
    if len(cleaned) <= max_items:
        return cleaned
    if max_items == 1:
        return [cleaned[0]]

    last_index = len(cleaned) - 1
    step = last_index / float(max_items - 1)
    sampled: List[str] = []
    for idx in range(max_items):
        source_index = int(round(idx * step))
        source_index = max(0, min(last_index, source_index))
        _append_unique_in_order(sampled, cleaned[source_index])
    return sampled


def _canonical_name_key(text: Any) -> str:
    value = _normalize_llm_text(text)
    if not value:
        return ""
    folded = unicodedata.normalize("NFKD", value)
    ascii_like = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return "".join(ch for ch in ascii_like.casefold() if ch.isalnum())


def _is_suspect_municipality_name(text: Any) -> bool:
    value = str(text or "")
    return (
        "\uFFFD" in value
        or "Ã" in value
        or "Â" in value
        or "â" in value
        or "�" in value
        or "?" in value
        or "\\u" in value
    )


def _repair_name_from_candidates(
    raw_name: Any,
    canonical_by_key: Dict[str, str],
) -> str:
    normalized_name = _normalize_llm_text(raw_name)
    if not normalized_name:
        return ""
    direct_key = _canonical_name_key(normalized_name)
    if direct_key and direct_key in canonical_by_key:
        return canonical_by_key[direct_key]
    if not _is_suspect_municipality_name(raw_name):
        return normalized_name
    if not direct_key:
        return normalized_name

    candidate_keys = list(canonical_by_key.keys())
    if not candidate_keys:
        return normalized_name
    best_key = ""
    best_ratio = 0.0
    for candidate_key in candidate_keys:
        ratio = difflib.SequenceMatcher(None, direct_key, candidate_key).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_key = candidate_key
    if best_key and best_ratio >= 0.78:
        return canonical_by_key[best_key]
    return normalized_name


def _municipality_chain_label(items: Any) -> str:
    names = _normalize_municipality_vector(items)
    if not names:
        return "n/a"
    return " -> ".join(names)


def _road_chain_label(items: Any) -> str:
    names = _normalize_road_vector(items)
    if not names:
        return "n/a"
    return " -> ".join(names)


def _extract_json_object(raw_text: str) -> Optional[Dict[str, Any]]:
    if not isinstance(raw_text, str):
        return None
    text = raw_text.strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        pass

    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            candidate, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate
    return None


def _extract_chat_completion_content(message: Any) -> str:
    def _collect_text(value: Any) -> List[str]:
        parts: List[str] = []
        if isinstance(value, str):
            text = value.strip()
            if text:
                parts.append(text)
            return parts
        if isinstance(value, list):
            for item in value:
                parts.extend(_collect_text(item))
            return parts
        if isinstance(value, dict):
            for key in ("text", "content", "value", "output_text"):
                if key in value:
                    parts.extend(_collect_text(value.get(key)))
            return parts
        return parts

    if isinstance(message, str):
        return message.strip()
    if isinstance(message, list):
        return "\n".join(_collect_text(message)).strip()
    if isinstance(message, dict):
        return "\n".join(_collect_text(message)).strip()
    return str(message or "").strip()


def _call_azure_openai_chat_completion(
    *,
    endpoint: str,
    api_key: str,
    deployment: str,
    api_version: str,
    messages: List[Dict[str, str]],
    temperature: float,
    timeout_sec: int,
    enforce_json_response: bool,
    max_tokens: Optional[int] = None,
) -> str:
    base = str(endpoint or "").strip().rstrip("/")
    if base.lower().endswith("/openai"):
        base = base[: -len("/openai")]
    try:
        parsed_base = urllib.parse.urlparse(base)
    except Exception:
        parsed_base = None
    if parsed_base and parsed_base.scheme and parsed_base.netloc:
        endpoint_host = parsed_base.netloc
        # Accept endpoint values copied from OpenAI-style docs (e.g. /openai/v1)
        # but normalize to Azure resource root for deployment-scoped URL construction.
        base = f"{parsed_base.scheme}://{parsed_base.netloc}"
    else:
        endpoint_host = ""
    deployment_name = str(deployment or "").strip()
    version = str(api_version or "").strip()
    if not base or not deployment_name or not version:
        raise RuntimeError("Azure OpenAI endpoint/deployment/api_version is required.")
    if not api_key:
        raise RuntimeError("Azure OpenAI API key is required.")

    encoded_deployment = urllib.parse.quote(deployment_name, safe="")
    encoded_version = urllib.parse.quote(version, safe="")
    url = (
        f"{base}/openai/deployments/{encoded_deployment}/chat/completions"
        f"?api-version={encoded_version}"
    )

    def _send_request(token_key: Optional[str]) -> Dict[str, Any]:
        body = {
            "messages": messages,
            "temperature": float(temperature),
        }
        if isinstance(max_tokens, int) and max_tokens > 0 and token_key:
            body[token_key] = int(max_tokens)
        if enforce_json_response:
            body["response_format"] = {"type": "json_object"}
        request = urllib.request.Request(
            url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "api-key": api_key,
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=max(3, timeout_sec)) as response:
            return json.loads(response.read().decode("utf-8"))

    token_key = "max_completion_tokens"
    try:
        payload = _send_request(token_key=token_key)
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            detail = ""
        detail_text = detail.strip() or str(exc)
        lowered_detail = detail_text.lower()
        should_retry_with_max_tokens = (
            exc.code == 400
            and "unsupported parameter" in lowered_detail
            and "max_completion_tokens" in lowered_detail
            and "max_tokens" in lowered_detail
        )
        if should_retry_with_max_tokens:
            try:
                payload = _send_request(token_key="max_tokens")
            except urllib.error.HTTPError as retry_exc:
                retry_detail = ""
                try:
                    retry_detail = retry_exc.read().decode("utf-8", errors="replace")
                except Exception:  # noqa: BLE001
                    retry_detail = ""
                retry_detail_text = retry_detail.strip() or str(retry_exc)
                raise RuntimeError(
                    f"Azure OpenAI HTTP {retry_exc.code}: {retry_detail_text}"
                ) from retry_exc
            except Exception as retry_exc:  # noqa: BLE001
                raise RuntimeError(f"Azure OpenAI request failed: {retry_exc}") from retry_exc
        else:
            if exc.code == 404:
                raise RuntimeError(
                    "Azure OpenAI HTTP 404: Resource not found. "
                    "Check endpoint root, deployment name, and API version. "
                    f"endpoint_host={endpoint_host or 'unknown'}, deployment={deployment_name}, api_version={version}."
                ) from exc
            raise RuntimeError(f"Azure OpenAI HTTP {exc.code}: {detail_text}") from exc
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Azure OpenAI request failed: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Azure OpenAI response is not a JSON object.")
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise RuntimeError("Azure OpenAI response has no choices.")
    first_choice = choices[0]
    if not isinstance(first_choice, dict):
        raise RuntimeError("Azure OpenAI response choice is invalid.")
    message = first_choice.get("message", {})
    if not isinstance(message, dict):
        raise RuntimeError("Azure OpenAI response message is invalid.")
    content = _extract_chat_completion_content(message.get("content"))
    if not content:
        content = _extract_chat_completion_content(message.get("output_text"))
    if not content:
        content = _extract_chat_completion_content(first_choice.get("text"))
    if not content:
        refusal = _extract_chat_completion_content(message.get("refusal"))
        finish_reason = _normalize_llm_text(first_choice.get("finish_reason"))
        if refusal:
            raise RuntimeError(f"Azure OpenAI refusal: {refusal}")
        if finish_reason:
            raise RuntimeError(
                f"Azure OpenAI response content is empty (finish_reason={finish_reason})."
            )
    if not content:
        raise RuntimeError("Azure OpenAI response content is empty.")
    return content


def _route_has_zero_distance_segment(segment_context: List[Dict[str, Any]]) -> bool:
    for segment in segment_context:
        if not isinstance(segment, dict):
            continue
        distance_km = _safe_float(segment.get("distance_km"), 0.0) or 0.0
        if distance_km <= 0.0:
            return True
    return False


def _is_timeout_like_error(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    return "timed out" in text or "timeout" in text or "time out" in text


def _is_non_retryable_llm_error(error_text: str) -> bool:
    text = str(error_text or "").strip().lower()
    return (
        "http 404" in text
        or "resource not found" in text
        or "http 401" in text
        or "http 403" in text
        or "deploymentnotfound" in text
        or "invalid api key" in text
    )


def _prepare_segment_payload_for_llm(
    *,
    route: Dict[str, Any],
    route_stop_municipality_links: List[Dict[str, Any]],
    segment_context: List[Dict[str, Any]],
    segment: Dict[str, Any],
) -> Dict[str, Any]:
    route_stop_names: List[str] = []
    for stop in route_stop_municipality_links:
        if not isinstance(stop, dict):
            continue
        _append_unique_in_order(route_stop_names, stop.get("municipality_name"))

    full_route_chain: List[str] = []
    for row in segment_context:
        if not isinstance(row, dict):
            continue
        for name in _normalize_municipality_vector(row.get("municipality_names")):
            _append_unique_in_order(full_route_chain, name)

    segment_index = (
        int(segment.get("segment_index"))
        if isinstance(segment.get("segment_index"), int)
        else None
    )
    current_names = _normalize_municipality_vector(segment.get("municipality_names"))
    current_roads = _sample_vector_items(
        _normalize_road_vector(segment.get("road_names")), max_items=10
    )

    previous_tail = ""
    next_head = ""
    if segment_index is not None:
        previous_segment = next(
            (
                row
                for row in segment_context
                if isinstance(row, dict) and row.get("segment_index") == segment_index - 1
            ),
            None,
        )
        next_segment = next(
            (
                row
                for row in segment_context
                if isinstance(row, dict) and row.get("segment_index") == segment_index + 1
            ),
            None,
        )
        if isinstance(previous_segment, dict):
            prev_names = _normalize_municipality_vector(
                previous_segment.get("municipality_names")
            )
            if prev_names:
                previous_tail = prev_names[-1]
        if isinstance(next_segment, dict):
            next_names = _normalize_municipality_vector(next_segment.get("municipality_names"))
            if next_names:
                next_head = next_names[0]

    return {
        "vehicle": route.get("vehicle"),
        "served_customer_ids": (
            list(route.get("served_customer_ids", []))
            if isinstance(route.get("served_customer_ids"), list)
            else []
        ),
        "route_distance_km": route.get("distance_km"),
        "route_stop_chain": _municipality_chain_label(route_stop_names),
        "route_municipality_chain_compact": _municipality_chain_label(
            _sample_vector_items(full_route_chain, max_items=14)
        ),
        "segment_index": segment_index,
        "from_stop_id": segment.get("from_stop_id"),
        "to_stop_id": segment.get("to_stop_id"),
        "distance_km": segment.get("distance_km"),
        "current_municipality_names": current_names,
        "current_municipality_chain": _municipality_chain_label(current_names),
        "current_road_names": current_roads,
        "current_road_chain": _road_chain_label(current_roads),
        "previous_tail": previous_tail or None,
        "next_head": next_head or None,
    }


def _build_municipality_segment_llm_messages(
    prompt_payload: Dict[str, Any], compact: bool
) -> List[Dict[str, str]]:
    segment_index = prompt_payload.get("segment_index")
    segment_label = (
        f"Segment {int(segment_index) + 1}"
        if isinstance(segment_index, int)
        else "Segment ?"
    )
    current_names = _normalize_municipality_vector(
        prompt_payload.get("current_municipality_names")
    )
    if compact:
        sampled_names: List[str] = []
        if current_names:
            sampled_names.append(current_names[0])
            if len(current_names) > 2:
                sampled_names.append(current_names[len(current_names) // 2])
            if len(current_names) > 1:
                sampled_names.append(current_names[-1])
        current_chain = _municipality_chain_label(sampled_names)
        roads = _sample_vector_items(prompt_payload.get("current_road_names"), max_items=3)
    else:
        current_chain = _municipality_chain_label(current_names)
        roads = _sample_vector_items(prompt_payload.get("current_road_names"), max_items=6)
    road_chain = _road_chain_label(roads)
    mode_line = "compact" if compact else "standard"
    system_prompt = (
        "Return compact JSON only. Do not explain. "
        "Keep municipality order and only insert plausible no-detour additions."
    )
    user_prompt = (
        "Task: update one segment municipality sequence.\n"
        "Constraints:\n"
        "- Keep first and last municipality unchanged.\n"
        "- Keep existing order; only insert missing municipalities.\n"
        "- Use road hints and adjacent segment boundary hints.\n"
        "- Spanish municipality names, concise output.\n"
        "Output JSON schema:\n"
        '{"segment_index":0,"municipality_names":["A","B"],'
        '"added_municipalities":[{"name":"X","reason":"short"}]}\n'
        f"mode={mode_line}\n"
        f"segment_index={segment_index}\n"
        f"from_stop_id={prompt_payload.get('from_stop_id')}\n"
        f"to_stop_id={prompt_payload.get('to_stop_id')}\n"
        f"distance_km={prompt_payload.get('distance_km')}\n"
        f"current_municipalities={current_chain}\n"
        f"current_roads={road_chain}\n"
        f"prev_tail={prompt_payload.get('previous_tail') or 'n/a'}\n"
        f"next_head={prompt_payload.get('next_head') or 'n/a'}\n"
        f"route_stops={prompt_payload.get('route_stop_chain')}\n"
        f"segment_label={segment_label}"
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def _parse_segment_llm_response(
    llm_payload: Dict[str, Any],
    *,
    segment_index: int,
    canonical_by_key: Dict[str, str],
    original_municipality_names: List[str],
) -> Dict[str, Any]:
    raw_names = _normalize_municipality_vector(llm_payload.get("municipality_names"))
    if not raw_names:
        raw_segments = llm_payload.get("segment_context")
        if isinstance(raw_segments, list):
            for row in raw_segments:
                if not isinstance(row, dict):
                    continue
                if row.get("segment_index") != segment_index:
                    continue
                raw_names = _normalize_municipality_vector(row.get("municipality_names"))
                if raw_names:
                    break
    if not raw_names:
        raise ValueError("LLM response does not contain municipality_names.")

    repaired_names = [
        _repair_name_from_candidates(name, canonical_by_key) for name in raw_names
    ]
    repaired_names = _normalize_municipality_vector(repaired_names)
    if not repaired_names:
        raise ValueError("LLM response municipality_names are empty after normalization.")

    if original_municipality_names:
        start_name = original_municipality_names[0]
        end_name = original_municipality_names[-1]
        start_key = _canonical_name_key(start_name)
        end_key = _canonical_name_key(end_name)
        if start_key and (
            not repaired_names or _canonical_name_key(repaired_names[0]) != start_key
        ):
            repaired_names = [
                start_name,
                *[
                    name
                    for name in repaired_names
                    if _canonical_name_key(name) != start_key
                ],
            ]
        if end_key and (
            not repaired_names or _canonical_name_key(repaired_names[-1]) != end_key
        ):
            repaired_names = [
                *[
                    name
                    for name in repaired_names
                    if _canonical_name_key(name) != end_key
                ],
                end_name,
            ]
        repaired_names = _normalize_municipality_vector(repaired_names)

    original_keys = {
        _canonical_name_key(name)
        for name in original_municipality_names
        if _canonical_name_key(name)
    }
    added: List[Dict[str, Any]] = []
    raw_added = llm_payload.get("added_municipalities")
    if isinstance(raw_added, list):
        for item in raw_added[:20]:
            if not isinstance(item, dict):
                continue
            raw_name = _normalize_llm_text(item.get("name"))
            if not raw_name:
                continue
            fixed_name = _repair_name_from_candidates(raw_name, canonical_by_key)
            fixed_key = _canonical_name_key(fixed_name)
            if fixed_key and fixed_key in original_keys:
                continue
            added.append(
                {
                    "name": fixed_name,
                    "segment_index": segment_index,
                    "reason": _normalize_llm_text(item.get("reason")) or None,
                }
            )

    repaired_names = _inject_added_names_into_segment_sequence(repaired_names, added)

    return {
        "segment_index": segment_index,
        "municipality_names": repaired_names,
        "added_municipalities": added,
    }


def _inject_added_names_into_segment_sequence(
    municipality_names: List[str], added_rows: List[Dict[str, Any]]
) -> List[str]:
    """Ensure explicit LLM additions appear in the segment sequence.

    If the model reports names in ``added_municipalities`` but omits them from
    ``municipality_names``, inject them in-order right before the segment tail
    municipality. This preserves start/end anchors while surfacing additions in
    downstream vectors and UI.
    """
    names = _normalize_municipality_vector(municipality_names)
    if len(names) < 2 or not isinstance(added_rows, list):
        return names

    seen_keys = {
        _canonical_name_key(name)
        for name in names
        if _canonical_name_key(name)
    }
    insert_index = max(1, len(names) - 1)
    for row in added_rows:
        if not isinstance(row, dict):
            continue
        candidate = _normalize_llm_text(row.get("name"))
        if not candidate:
            continue
        candidate_key = _canonical_name_key(candidate)
        if not candidate_key or candidate_key in seen_keys:
            continue
        names.insert(insert_index, candidate)
        insert_index += 1
        seen_keys.add(candidate_key)
    return _normalize_municipality_vector(names)


def _enrich_route_municipality_vectors_with_llm(
    *,
    route: Dict[str, Any],
    route_stop_municipality_links: List[Dict[str, Any]],
    segment_context: List[Dict[str, Any]],
    endpoint: str,
    api_key: str,
    deployment: str,
    api_version: str,
    timeout_sec: int,
    retries: int,
    max_tokens: int,
    debug_first_route: bool,
) -> Dict[str, Any]:
    canonical_by_key: Dict[str, str] = {}
    route_fatal_error: Optional[str] = None
    for segment in segment_context:
        if not isinstance(segment, dict):
            continue
        for name in _normalize_municipality_vector(segment.get("municipality_names")):
            key = _canonical_name_key(name)
            if key and key not in canonical_by_key:
                canonical_by_key[key] = name

    segment_vectors: Dict[int, List[str]] = {}
    added_municipalities: List[Dict[str, Any]] = []
    segment_errors: List[str] = []
    segment_max_tokens_base = max(1200, min(int(max_tokens), 6000))
    attempted_segments = 0
    enriched_segments = 0
    failed_segments = 0

    for segment in segment_context:
        if not isinstance(segment, dict):
            continue
        segment_index = segment.get("segment_index")
        if not isinstance(segment_index, int):
            continue
        original_names = _normalize_municipality_vector(segment.get("municipality_names"))
        if len(original_names) < 2:
            continue
        attempted_segments += 1

        prompt_payload = _prepare_segment_payload_for_llm(
            route=route,
            route_stop_municipality_links=route_stop_municipality_links,
            segment_context=segment_context,
            segment=segment,
        )
        messages = _build_municipality_segment_llm_messages(prompt_payload, compact=False)
        debug_prefix = f"[municipality-llm][route-1][segment-{segment_index + 1}]"

        if debug_first_route:
            _safe_console_print(f"{debug_prefix}[prompt]")
            _safe_console_print(
                json.dumps(
                    {
                        "messages": messages,
                        "temperature": MUNICIPALITY_LLM_TEMPERATURE,
                        "max_tokens": segment_max_tokens_base,
                    },
                    ensure_ascii=False,
                )
            )

        last_error: Optional[str] = None
        response_text: Optional[str] = None
        enforce_json_response = True
        max_attempts = 1 + max(0, int(retries))
        attempt_max_tokens = segment_max_tokens_base
        for attempt in range(max_attempts):
            try:
                response_text = _call_azure_openai_chat_completion(
                    endpoint=endpoint,
                    api_key=api_key,
                    deployment=deployment,
                    api_version=api_version,
                    messages=messages,
                    temperature=MUNICIPALITY_LLM_TEMPERATURE,
                    timeout_sec=timeout_sec,
                    enforce_json_response=enforce_json_response,
                    max_tokens=attempt_max_tokens,
                )
                break
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                lowered_error = last_error.lower()
                if enforce_json_response and (
                    "response_format" in lowered_error
                    or "json_object" in lowered_error
                ):
                    enforce_json_response = False
                if "finish_reason=length" in lowered_error and attempt_max_tokens < 6000:
                    attempt_max_tokens = min(6000, max(attempt_max_tokens + 800, int(attempt_max_tokens * 1.8)))
                if debug_first_route:
                    _safe_console_print(
                        f"{debug_prefix}[attempt-{attempt + 1}-error][max_tokens={attempt_max_tokens}] {last_error}"
                    )
                if _is_non_retryable_llm_error(last_error):
                    break
                if attempt + 1 < max_attempts:
                    time.sleep(0.25 * (attempt + 1))

        compact_error: Optional[str] = None
        empty_content_error = "content is empty" in str(last_error or "").strip().lower()
        timeout_like_error = _is_timeout_like_error(last_error or "")
        should_try_compact_fallback = timeout_like_error or empty_content_error
        if response_text is None and should_try_compact_fallback:
            compact_messages = _build_municipality_segment_llm_messages(
                prompt_payload, compact=True
            )
            if timeout_like_error:
                compact_max_tokens = max(500, min(1500, int(attempt_max_tokens * 0.5)))
            else:
                compact_max_tokens = max(900, min(3000, int(attempt_max_tokens)))
            compact_force_json = not empty_content_error
            if debug_first_route:
                _safe_console_print(f"{debug_prefix}[fallback-compact][prompt]")
                _safe_console_print(
                    json.dumps(
                        {
                            "messages": compact_messages,
                            "temperature": MUNICIPALITY_LLM_TEMPERATURE,
                            "max_tokens": compact_max_tokens,
                            "enforce_json_response": compact_force_json,
                        },
                        ensure_ascii=False,
                    )
                )
            try:
                response_text = _call_azure_openai_chat_completion(
                    endpoint=endpoint,
                    api_key=api_key,
                    deployment=deployment,
                    api_version=api_version,
                    messages=compact_messages,
                    temperature=MUNICIPALITY_LLM_TEMPERATURE,
                    timeout_sec=max(20, timeout_sec),
                    enforce_json_response=compact_force_json,
                    max_tokens=compact_max_tokens,
                )
            except Exception as exc:  # noqa: BLE001
                compact_error = str(exc)
                lowered_compact_error = compact_error.lower()
                if (
                    "finish_reason=length" in lowered_compact_error
                    and compact_max_tokens < 6000
                ):
                    compact_retry_tokens = min(6000, max(compact_max_tokens + 1200, int(compact_max_tokens * 1.8)))
                    try:
                        response_text = _call_azure_openai_chat_completion(
                            endpoint=endpoint,
                            api_key=api_key,
                            deployment=deployment,
                            api_version=api_version,
                            messages=compact_messages,
                            temperature=MUNICIPALITY_LLM_TEMPERATURE,
                            timeout_sec=max(20, timeout_sec),
                            enforce_json_response=compact_force_json,
                            max_tokens=compact_retry_tokens,
                        )
                    except Exception as retry_exc:  # noqa: BLE001
                        compact_error = str(retry_exc)
                        if debug_first_route:
                            _safe_console_print(
                                f"{debug_prefix}[fallback-compact][retry-error][max_tokens={compact_retry_tokens}] {compact_error}"
                            )
                if response_text is None and debug_first_route:
                    _safe_console_print(
                        f"{debug_prefix}[fallback-compact][error] {compact_error}"
                    )

        if response_text is None:
            error_text = (
                f"{last_error or 'Azure OpenAI returned no response.'} | "
                f"compact_fallback: {compact_error}"
                if compact_error
                else (last_error or "Azure OpenAI returned no response.")
            )
            segment_errors.append(f"segment={segment_index + 1}: {error_text}")
            failed_segments += 1
            if _is_non_retryable_llm_error(error_text):
                route_fatal_error = error_text
                break
            continue

        if debug_first_route:
            _safe_console_print(f"{debug_prefix}[response]")
            _safe_console_print(response_text)

        parsed_payload = _extract_json_object(response_text)
        if not isinstance(parsed_payload, dict):
            segment_errors.append(
                f"segment={segment_index + 1}: LLM response is not valid JSON object."
            )
            failed_segments += 1
            continue
        try:
            parsed_segment = _parse_segment_llm_response(
                parsed_payload,
                segment_index=segment_index,
                canonical_by_key=canonical_by_key,
                original_municipality_names=original_names,
            )
        except Exception as exc:  # noqa: BLE001
            segment_errors.append(f"segment={segment_index + 1}: {exc}")
            failed_segments += 1
            continue

        names = _normalize_municipality_vector(parsed_segment.get("municipality_names"))
        if names:
            segment_vectors[segment_index] = names
            for municipality_name in names:
                key = _canonical_name_key(municipality_name)
                if key and key not in canonical_by_key:
                    canonical_by_key[key] = municipality_name
            enriched_segments += 1
        for item in parsed_segment.get("added_municipalities", []):
            if not isinstance(item, dict):
                continue
            if len(added_municipalities) >= 40:
                break
            added_municipalities.append(item)

    if route_fatal_error and not segment_vectors:
        return {
            "status": "error",
            "error": route_fatal_error,
            "attempted_segments": attempted_segments,
            "enriched_segments": enriched_segments,
            "failed_segments": failed_segments,
        }
    if not segment_vectors:
        return {
            "status": "error",
            "error": (
                "; ".join(segment_errors[:6])
                if segment_errors
                else "LLM response does not contain segment updates."
            ),
            "attempted_segments": attempted_segments,
            "enriched_segments": enriched_segments,
            "failed_segments": failed_segments,
        }

    route_vector: List[str] = []
    for segment in segment_context:
        if not isinstance(segment, dict):
            continue
        segment_index = segment.get("segment_index")
        if not isinstance(segment_index, int):
            continue
        names = segment_vectors.get(
            segment_index,
            _normalize_municipality_vector(segment.get("municipality_names")),
        )
        for municipality_name in names:
            _append_unique_in_order(route_vector, municipality_name)

    result: Dict[str, Any] = {
        "status": "ok",
        "route_municipality_vector": route_vector,
        "segment_vectors": segment_vectors,
        "added_municipalities": added_municipalities[:40],
        "attempted_segments": attempted_segments,
        "enriched_segments": enriched_segments,
        "failed_segments": failed_segments,
    }
    if segment_errors:
        result["warnings"] = segment_errors[:20]
    return result


def _new_point_registry_entry(lat: float, lng: float, coord_key: str) -> Dict[str, Any]:
    return {
        "coord_key": coord_key,
        "lat": round(float(lat), 6),
        "lng": round(float(lng), 6),
        "stop_ids": [],
        "customer_ids": [],
        "source_tags": [],
    }


def _append_unique(items: List[Any], value: Any) -> None:
    if value is None:
        return
    if value not in items:
        items.append(value)


def _merge_point_metadata(target: Dict[str, Any], point: Dict[str, Any]) -> None:
    for key in ("stop_ids", "customer_ids", "source_tags"):
        target_list = target.get(key)
        if not isinstance(target_list, list):
            target_list = []
            target[key] = target_list
        source_list = point.get(key)
        if not isinstance(source_list, list):
            continue
        for value in source_list:
            _append_unique(target_list, value)


def _register_point(
    registry: Dict[str, Dict[str, Any]],
    lat: float,
    lng: float,
    *,
    source_tag: Optional[str] = None,
    stop_id: Any = None,
    customer_id: Any = None,
) -> Dict[str, Any]:
    key = _coordinate_key(lat, lng)
    point = registry.get(key)
    if point is None:
        point = _new_point_registry_entry(lat, lng, key)
        registry[key] = point
    if source_tag:
        _append_unique(point["source_tags"], source_tag)
    _append_unique(point["stop_ids"], stop_id)
    _append_unique(point["customer_ids"], customer_id)
    return point


def _empty_municipality_entry(
    lat: float,
    lng: float,
    *,
    error: Optional[str] = None,
    source_endpoint: Optional[str] = None,
    source: str = "nominatim_reverse",
) -> Dict[str, Any]:
    status = "error" if error else "unknown"
    return {
        "status": status,
        "source": source,
        "source_endpoint": source_endpoint,
        "lat": round(float(lat), 6),
        "lng": round(float(lng), 6),
        "municipality_name": None,
        "municipality_source_field": None,
        "display_name": None,
        "address": {},
        "osm_type": None,
        "osm_id": None,
        "osm_ref": None,
        "place_id": None,
        "category": None,
        "type": None,
        "resolution_note": "request_failed" if error else "municipality_not_found",
        "stop_ids": [],
        "customer_ids": [],
        "source_tags": [],
        "error": error,
    }


def _normalize_azure_maps_reverse_address(address: Any) -> Dict[str, Any]:
    if not isinstance(address, dict):
        return {}

    normalized: Dict[str, Any] = {}

    def put(key: str, value: Any) -> None:
        text = str(value or "").strip()
        if text and key not in normalized:
            normalized[key] = text

    put("municipality", address.get("municipality"))
    put("city", address.get("locality"))
    put("town", address.get("municipalitySubdivision"))
    put("village", address.get("localName"))
    put("suburb", address.get("neighborhood"))
    put("county", address.get("countrySecondarySubdivision"))
    put("state", address.get("countrySubdivision"))
    put("province", address.get("countrySubdivisionName") or address.get("countrySubdivision"))
    put("country", address.get("country"))

    admin_districts = address.get("adminDistricts")
    if isinstance(admin_districts, list):
        district_names: List[str] = []
        for row in admin_districts:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("shortName") or "").strip()
            if name:
                district_names.append(name)
        if district_names:
            put("state", district_names[0])
            put("province", district_names[0])
            if len(district_names) >= 2:
                put("county", district_names[1])

    country_region = address.get("countryRegion")
    if isinstance(country_region, dict):
        put("country", country_region.get("name"))
        iso_value = str(
            country_region.get("iso")
            or country_region.get("ISO")
            or country_region.get("code")
            or ""
        ).strip()
        if len(iso_value) == 2:
            normalized["country_code"] = iso_value.lower()

    for candidate in (
        address.get("countryCode"),
        address.get("countryCodeISO2"),
        address.get("countryIsoCode"),
    ):
        value = str(candidate or "").strip()
        if len(value) == 2:
            normalized["country_code"] = value.lower()
            break

    return normalized


def _reverse_geocode_stop_address(
    lat: float,
    lng: float,
    timeout_sec: int,
    reverse_source: str,
    azure_maps_subscription_key: str,
    azure_maps_reverse_endpoint: str,
    azure_maps_api_version: str,
) -> Dict[str, Any]:
    selected_source = _resolve_municipality_reverse_source(reverse_source)

    if selected_source == "nominatim_reverse":
        params = urllib.parse.urlencode(
            {
                "format": "jsonv2",
                "lat": round(float(lat), 6),
                "lon": round(float(lng), 6),
                "addressdetails": 1,
                "zoom": 10,
                "namedetails": 1,
            }
        )
        payload = None
        source_endpoint = None
        last_error: Optional[str] = None
        for endpoint in DEFAULT_REVERSE_GEOCODER_ENDPOINTS:
            url = f"{endpoint}?{params}"
            try:
                request = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "softOptimizationVRP/municipality-reverse-geocoder",
                        "Accept": "application/json",
                    },
                    method="GET",
                )
                with urllib.request.urlopen(request, timeout=max(2, timeout_sec)) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                    source_endpoint = endpoint
                    break
            except Exception as exc:  # noqa: BLE001
                last_error = f"{endpoint}: {exc}"
                time.sleep(0.15)

        if payload is None:
            raise RuntimeError(last_error or "No reverse geocoder endpoint available.")
        if not isinstance(payload, dict):
            raise RuntimeError("Unexpected reverse geocoder payload.")

        error_msg = str(payload.get("error") or "").strip()
        if error_msg:
            raise RuntimeError(f"{source_endpoint or 'reverse_geocoder'}: {error_msg}")

        municipality_name, municipality_source_field = _extract_municipality_from_reverse_payload(
            payload
        )
        address = payload.get("address", {})
        if not isinstance(address, dict):
            address = {}
        osm_type = str(payload.get("osm_type") or "").strip() or None
        osm_id = payload.get("osm_id")
        osm_ref = None
        if osm_type is not None and osm_id is not None:
            osm_ref = f"{osm_type}/{osm_id}"

        if municipality_name is None:
            lower_keys = {str(key).strip().lower() for key in address.keys()}
            if lower_keys and lower_keys.issubset(NON_MUNICIPALITY_ADMIN_FIELDS):
                resolution_note = "non_municipality_admin_only"
            else:
                resolution_note = "municipality_not_found"
        else:
            resolution_note = "resolved"

        return {
            "status": "resolved" if municipality_name else "unknown",
            "source": "nominatim_reverse",
            "source_endpoint": source_endpoint,
            "lat": round(float(lat), 6),
            "lng": round(float(lng), 6),
            "municipality_name": municipality_name,
            "municipality_source_field": municipality_source_field,
            "display_name": payload.get("display_name"),
            "address": address,
            "osm_type": osm_type,
            "osm_id": osm_id,
            "osm_ref": osm_ref,
            "place_id": payload.get("place_id"),
            "category": payload.get("category"),
            "type": payload.get("type"),
            "resolution_note": resolution_note,
            "stop_ids": [],
            "customer_ids": [],
            "source_tags": [],
        }

    subscription_key = str(azure_maps_subscription_key or "").strip()
    if not subscription_key:
        raise RuntimeError("AZURE_MAPS_SUBSCRIPTION_KEY is not configured.")

    endpoint = (
        str(azure_maps_reverse_endpoint or "").strip().rstrip("/")
        or DEFAULT_AZURE_MAPS_REVERSE_ENDPOINT
    )
    api_version = (
        str(azure_maps_api_version or "").strip() or DEFAULT_AZURE_MAPS_REVERSE_API_VERSION
    )

    params = urllib.parse.urlencode(
        {
            "api-version": api_version,
            "subscription-key": subscription_key,
            "coordinates": f"{round(float(lng), 6)},{round(float(lat), 6)}",
            "resultTypes": "Address",
            "view": "Auto",
        }
    )
    url = f"{endpoint}?{params}"
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "softOptimizationVRP/municipality-reverse-geocoder",
            "Accept": "application/geo+json, application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=max(2, timeout_sec)) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"{endpoint}: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected Azure Maps reverse geocoder payload.")

    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        code = str(error_payload.get("code") or "").strip()
        message = str(error_payload.get("message") or "").strip()
        reason = f"{code}: {message}" if code and message else (message or code)
        raise RuntimeError(f"{endpoint}: {reason or 'Azure Maps reverse geocoder error.'}")

    features = payload.get("features")
    if not isinstance(features, list) or not features:
        raise RuntimeError("Azure Maps reverse geocoder payload does not contain features.")
    first_feature = features[0] if isinstance(features[0], dict) else {}
    properties = first_feature.get("properties", {})
    if not isinstance(properties, dict):
        properties = {}
    raw_address = properties.get("address", {})
    normalized_address = _normalize_azure_maps_reverse_address(raw_address)
    municipality_name, municipality_source_field = _extract_municipality_from_reverse_payload(
        {"address": normalized_address}
    )

    if municipality_name is None:
        lower_keys = {str(key).strip().lower() for key in normalized_address.keys()}
        if lower_keys and lower_keys.issubset(NON_MUNICIPALITY_ADMIN_FIELDS):
            resolution_note = "non_municipality_admin_only"
        else:
            resolution_note = "municipality_not_found"
    else:
        resolution_note = "resolved"

    formatted_address = (
        properties.get("formattedAddress")
        if isinstance(properties.get("formattedAddress"), str)
        else None
    )

    return {
        "status": "resolved" if municipality_name else "unknown",
        "source": "azure_maps_reverse",
        "source_endpoint": endpoint,
        "lat": round(float(lat), 6),
        "lng": round(float(lng), 6),
        "municipality_name": municipality_name,
        "municipality_source_field": municipality_source_field,
        "display_name": formatted_address,
        "address": normalized_address,
        "osm_type": None,
        "osm_id": None,
        "osm_ref": None,
        "place_id": first_feature.get("id"),
        "category": properties.get("type"),
        "type": first_feature.get("type"),
        "resolution_note": resolution_note,
        "stop_ids": [],
        "customer_ids": [],
        "source_tags": [],
    }


def _build_municipality_lookup(
    initial_book: Optional[Dict[str, Dict[str, Any]]],
    timeout_sec: int,
    min_interval_ms: int,
    reverse_source: str,
    azure_maps_subscription_key: str,
    azure_maps_reverse_endpoint: str,
    azure_maps_api_version: str,
) -> Dict[str, Any]:
    return {
        "book": dict(initial_book) if isinstance(initial_book, dict) else {},
        "timeout_sec": max(2, int(timeout_sec)),
        "min_interval_ms": max(0, int(min_interval_ms)),
        "reverse_source": _resolve_municipality_reverse_source(reverse_source),
        "azure_maps_subscription_key": str(azure_maps_subscription_key or "").strip(),
        "azure_maps_reverse_endpoint": (
            str(azure_maps_reverse_endpoint or "").strip()
            or DEFAULT_AZURE_MAPS_REVERSE_ENDPOINT
        ),
        "azure_maps_api_version": (
            str(azure_maps_api_version or "").strip()
            or DEFAULT_AZURE_MAPS_REVERSE_API_VERSION
        ),
        "last_request_ts": None,
        "http_requests": 0,
        "cache_hits": 0,
    }


def _lookup_snapshot(lookup: Dict[str, Any]) -> Dict[str, int]:
    return {
        "http_requests": int(lookup.get("http_requests", 0)),
        "cache_hits": int(lookup.get("cache_hits", 0)),
    }


def _lookup_delta(before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
    return {
        "http_requests": max(0, after.get("http_requests", 0) - before.get("http_requests", 0)),
        "cache_hits": max(0, after.get("cache_hits", 0) - before.get("cache_hits", 0)),
    }


def _resolve_municipality_point(
    point: Dict[str, Any],
    lookup: Dict[str, Any],
    errors: Optional[List[str]],
    *,
    context_label: str,
    record_unknown: bool,
) -> Tuple[str, Dict[str, Any]]:
    lat = float(point["lat"])
    lng = float(point["lng"])
    key = _coordinate_key(lat, lng)
    book = lookup["book"]
    cached = book.get(key)
    if isinstance(cached, dict):
        lookup["cache_hits"] = int(lookup.get("cache_hits", 0)) + 1
        _merge_point_metadata(cached, point)
        return key, cached

    last_request_ts = lookup.get("last_request_ts")
    min_interval_ms = int(lookup.get("min_interval_ms", 0))
    if last_request_ts is not None and min_interval_ms > 0:
        elapsed_ms = (time.monotonic() - float(last_request_ts)) * 1000.0
        if elapsed_ms < min_interval_ms:
            time.sleep((min_interval_ms - elapsed_ms) / 1000.0)

    try:
        reverse_source = _resolve_municipality_reverse_source(
            lookup.get("reverse_source")
        )
        row = _reverse_geocode_stop_address(
            lat=lat,
            lng=lng,
            timeout_sec=int(lookup.get("timeout_sec", DEFAULT_OSM_TIMEOUT_SEC)),
            reverse_source=reverse_source,
            azure_maps_subscription_key=str(
                lookup.get("azure_maps_subscription_key") or ""
            ),
            azure_maps_reverse_endpoint=str(
                lookup.get("azure_maps_reverse_endpoint")
                or DEFAULT_AZURE_MAPS_REVERSE_ENDPOINT
            ),
            azure_maps_api_version=str(
                lookup.get("azure_maps_api_version")
                or DEFAULT_AZURE_MAPS_REVERSE_API_VERSION
            ),
        )
    except Exception as exc:  # noqa: BLE001
        lookup["http_requests"] = int(lookup.get("http_requests", 0)) + 1
        lookup["last_request_ts"] = time.monotonic()
        if errors is not None:
            errors.append(f"{context_label} failed at {round(lat, 6)},{round(lng, 6)}: {exc}")
        row = _empty_municipality_entry(
            lat,
            lng,
            error=str(exc),
            source=reverse_source,
        )
        _merge_point_metadata(row, point)
        book[key] = row
        return key, row

    lookup["http_requests"] = int(lookup.get("http_requests", 0)) + 1
    lookup["last_request_ts"] = time.monotonic()
    _merge_point_metadata(row, point)
    book[key] = row

    if record_unknown and row.get("status") != "resolved" and errors is not None:
        errors.append(
            f"{context_label} unresolved at {round(lat, 6)},{round(lng, 6)}: "
            f"{row.get('resolution_note') or 'municipality_not_found'}"
        )
    return key, row


def _summarize_points(points: Dict[str, Dict[str, Any]], book: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    resolved = 0
    unknown = 0
    failed = 0
    for key in points.keys():
        row = book.get(key, {})
        status = str(row.get("status") or "unknown").strip().lower()
        if status == "resolved":
            resolved += 1
        elif status == "error":
            failed += 1
        else:
            unknown += 1
    return {
        "total": len(points),
        "resolved": resolved,
        "unknown": unknown,
        "failed": failed,
    }


def _collect_problem_coordinates(
    vrp_result: Dict[str, Any],
    raw_payload: Dict[str, Any],
) -> Dict[str, Dict[str, Any]]:
    points: Dict[str, Dict[str, Any]] = {}

    depot = raw_payload.get("depot")
    if isinstance(depot, dict):
        lat = _safe_float(depot.get("lat"))
        lng = _safe_float(depot.get("lng"))
        if lat is not None and lng is not None:
            _register_point(
                points,
                lat,
                lng,
                source_tag="depot_input",
                stop_id=depot.get("id", "depot"),
            )

    customers = raw_payload.get("customers")
    if isinstance(customers, list):
        for customer in customers:
            if not isinstance(customer, dict):
                continue
            lat = _safe_float(customer.get("lat"))
            lng = _safe_float(customer.get("lng"))
            if lat is None or lng is None:
                continue
            _register_point(
                points,
                lat,
                lng,
                source_tag="customer_input",
                customer_id=customer.get("id"),
            )

    for route in vrp_result.get("routes", []):
        if not isinstance(route, dict):
            continue
        for stop in route.get("stops", []):
            if not isinstance(stop, dict):
                continue
            lat = _safe_float(stop.get("lat"))
            lng = _safe_float(stop.get("lng"))
            if lat is None or lng is None:
                continue
            _register_point(
                points,
                lat,
                lng,
                source_tag="route_stop",
                stop_id=stop.get("id"),
            )
    return points


def _build_municipality_trace_from_segment_samples(
    segment: Dict[str, Any],
    lookup: Dict[str, Any],
    errors: Optional[List[str]],
    phase2_points: Dict[str, Dict[str, Any]],
    step_km: float,
    max_samples: int,
    route_shape_points: Optional[List[Dict[str, float]]] = None,
) -> List[Dict[str, Any]]:
    if isinstance(route_shape_points, list) and len(route_shape_points) >= 2:
        samples = _sample_polyline_points(
            polyline=route_shape_points,
            step_km=step_km,
        )
    else:
        distance_km = float(segment.get("distance_km", 0.0) or 0.0)
        samples = _sample_segment_points(
            start=segment.get("start", {}),
            end=segment.get("end", {}),
            distance_km=distance_km,
            step_km=step_km,
        )
    samples = _limit_samples(samples, max_samples)
    output: List[Dict[str, Any]] = []
    previous_name_key: Optional[str] = None
    for sample in samples:
        lat = _safe_float(sample.get("lat"))
        lng = _safe_float(sample.get("lng"))
        if lat is None or lng is None:
            continue
        point = _register_point(
            phase2_points,
            lat,
            lng,
            source_tag="segment_sample",
        )
        coord_key, resolved = _resolve_municipality_point(
            point=point,
            lookup=lookup,
            errors=errors,
            context_label="municipality reverse geocode sample",
            record_unknown=False,
        )
        municipality_name = str(resolved.get("municipality_name") or "").strip()
        if not municipality_name:
            continue
        name_key = municipality_name.casefold()
        if name_key == previous_name_key:
            continue
        previous_name_key = name_key
        output.append(
            {
                "sample_index": int(sample.get("sample_index", 0)),
                "position": sample.get("position", "along"),
                "distance_from_start_km": round(float(sample.get("distance_from_start_km", 0.0) or 0.0), 3),
                "query_point": {
                    "lat": round(float(lat), 6),
                    "lng": round(float(lng), 6),
                },
                "municipality": {
                    "name": municipality_name,
                    "place": resolved.get("municipality_source_field"),
                    "population": None,
                    "osm_ref": resolved.get("osm_ref"),
                    "lat": resolved.get("lat"),
                    "lng": resolved.get("lng"),
                    "distance_to_query_km": 0.0,
                    "address_ref": coord_key,
                },
            }
        )
    return output


def _build_route_stop_municipality_links(
    stops: List[Dict[str, Any]],
    municipality_book: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    links: List[Dict[str, Any]] = []
    for index, stop in enumerate(stops):
        if not isinstance(stop, dict):
            continue
        lat = _safe_float(stop.get("lat"))
        lng = _safe_float(stop.get("lng"))
        if lat is None or lng is None:
            continue
        coord_key = _coordinate_key(lat, lng)
        resolved = municipality_book.get(coord_key, {})
        links.append(
            {
                "stop_index": index,
                "stop_id": stop.get("id"),
                "lat": round(float(lat), 6),
                "lng": round(float(lng), 6),
                "municipality_name": resolved.get("municipality_name"),
                "address_ref": coord_key,
                "status": resolved.get("status", "unknown"),
            }
        )
    return links


def _build_phase1_input_points(
    phase1_points: Dict[str, Dict[str, Any]],
    municipality_book: Dict[str, Dict[str, Any]],
    province_capital_cache: Dict[str, Dict[str, Any]],
    province_capital_errors: Optional[List[str]],
    province_capital_lookup_enabled: bool,
    province_capital_timeout_sec: int,
) -> List[Dict[str, Any]]:
    points_output: List[Dict[str, Any]] = []
    for coord_key in sorted(phase1_points.keys()):
        point = phase1_points.get(coord_key, {})
        if not isinstance(point, dict):
            continue
        source_tags = point.get("source_tags", [])
        if not isinstance(source_tags, list):
            source_tags = []
        if "depot_input" in source_tags:
            point_role = "depot"
        elif "customer_input" in source_tags:
            point_role = "customer"
        else:
            continue

        resolved = municipality_book.get(coord_key, {})
        if not isinstance(resolved, dict):
            resolved = {}
        address = resolved.get("address", {})
        province_name, province_source_field = _extract_province_from_address(address)
        country_code = _extract_country_code_from_address(address)
        province_capital = (
            _resolve_province_capital(
                province_name=province_name,
                country_code=country_code,
                cache=province_capital_cache,
                errors=province_capital_errors,
                timeout_sec=province_capital_timeout_sec,
            )
            if province_capital_lookup_enabled and province_name
            else {}
        )

        entry = {
            "coord_key": coord_key,
            "role": point_role,
            "lat": round(float(point.get("lat", 0.0)), 6),
            "lng": round(float(point.get("lng", 0.0)), 6),
            "stop_ids": list(point.get("stop_ids", []))
            if isinstance(point.get("stop_ids"), list)
            else [],
            "customer_ids": list(point.get("customer_ids", []))
            if isinstance(point.get("customer_ids"), list)
            else [],
            "status": resolved.get("status", "unknown"),
            "resolution_note": resolved.get("resolution_note"),
            "municipality_name": resolved.get("municipality_name"),
            "municipality_source_field": resolved.get("municipality_source_field"),
            "province_name": province_name,
            "province_source_field": province_source_field,
            "province_capital_name": (
                province_capital.get("capital_name")
                if isinstance(province_capital, dict)
                else None
            ),
            "province_capital_status": (
                province_capital.get("status")
                if isinstance(province_capital, dict)
                else None
            ),
            "country_code": country_code,
            "address_ref": coord_key,
        }
        points_output.append(entry)
    return points_output


def _build_segment_admin_vectors(
    municipality_trace: List[Dict[str, Any]],
    municipality_book: Dict[str, Dict[str, Any]],
    province_capital_cache: Dict[str, Dict[str, Any]],
    province_capital_errors: Optional[List[str]],
    province_capital_lookup_enabled: bool,
    province_capital_timeout_sec: int,
) -> Tuple[List[str], List[str], List[str]]:
    segment_municipality_vector: List[str] = []
    segment_province_vector: List[str] = []
    segment_province_capital_vector: List[str] = []
    for row in municipality_trace:
        if not isinstance(row, dict):
            continue
        municipality = row.get("municipality", {})
        if not isinstance(municipality, dict):
            municipality = {}
        municipality_name = municipality.get("name")
        _append_unique_in_order(segment_municipality_vector, municipality_name)

        address_ref = str(municipality.get("address_ref") or "").strip()
        if not address_ref:
            continue
        resolved = municipality_book.get(address_ref, {})
        if not isinstance(resolved, dict):
            continue
        address = resolved.get("address", {})
        province_name, _ = _extract_province_from_address(address)
        _append_unique_in_order(segment_province_vector, province_name)
        if not (province_capital_lookup_enabled and province_name):
            continue
        capital = _resolve_province_capital(
            province_name=province_name,
            country_code=_extract_country_code_from_address(address),
            cache=province_capital_cache,
            errors=province_capital_errors,
            timeout_sec=province_capital_timeout_sec,
        )
        if isinstance(capital, dict):
            _append_unique_in_order(
                segment_province_capital_vector, capital.get("capital_name")
            )
    return (
        segment_municipality_vector,
        segment_province_vector,
        segment_province_capital_vector,
    )


def _interpolate_point(
    start: Tuple[float, float], end: Tuple[float, float], fraction: float
) -> Tuple[float, float]:
    clamped = max(0.0, min(1.0, fraction))
    lat = start[0] + (end[0] - start[0]) * clamped
    lng = start[1] + (end[1] - start[1]) * clamped
    return lat, lng


def _sample_segment_points(
    start: Dict[str, float], end: Dict[str, float], distance_km: float, step_km: float
) -> List[Dict[str, Any]]:
    segment_steps = max(1, int(math.ceil(max(distance_km, 0.0) / max(step_km, 1.0))))
    start_point = (float(start["lat"]), float(start["lng"]))
    end_point = (float(end["lat"]), float(end["lng"]))
    samples: List[Dict[str, Any]] = []
    for idx in range(segment_steps + 1):
        ratio = idx / float(segment_steps)
        lat, lng = _interpolate_point(start_point, end_point, ratio)
        if idx == 0:
            position = "start"
        elif idx == segment_steps:
            position = "end"
        else:
            position = "along"
        samples.append(
            {
                "sample_index": idx,
                "position": position,
                "distance_from_start_km": round(distance_km * ratio, 3),
                "lat": lat,
                "lng": lng,
            }
        )
    return samples


def _extract_osrm_step_road_names(route_row: Dict[str, Any]) -> List[str]:
    if not isinstance(route_row, dict):
        return []
    roads: List[str] = []
    seen_keys: Set[str] = set()
    legs = route_row.get("legs")
    if not isinstance(legs, list):
        return roads
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        steps = leg.get("steps")
        if not isinstance(steps, list):
            continue
        for step in steps:
            if not isinstance(step, dict):
                continue
            road_name = _normalize_llm_text(step.get("name"))
            road_ref = _normalize_llm_text(step.get("ref"))
            if road_ref and road_name:
                if _canonical_name_key(road_ref) == _canonical_name_key(road_name):
                    label = road_ref
                else:
                    label = f"{road_ref} ({road_name})"
            else:
                label = road_ref or road_name
            label = _normalize_llm_text(label)
            if not label:
                continue
            lowered = label.casefold()
            if lowered in {"unnamed road", "road", "unknown road", "n/a"}:
                continue
            key = _canonical_name_key(label) or lowered
            if key in seen_keys:
                continue
            seen_keys.add(key)
            roads.append(label)
            if len(roads) >= 30:
                return roads
    return roads


def _fetch_osrm_segment_geometry(
    start: Dict[str, float],
    end: Dict[str, float],
    osrm_base_url: str,
    timeout_sec: int,
) -> Dict[str, Any]:
    start_lat = _safe_float(start.get("lat"))
    start_lng = _safe_float(start.get("lng"))
    end_lat = _safe_float(end.get("lat"))
    end_lng = _safe_float(end.get("lng"))
    if (
        start_lat is None
        or start_lng is None
        or end_lat is None
        or end_lng is None
    ):
        raise RuntimeError("Invalid coordinates for OSRM route geometry.")
    if abs(start_lat - end_lat) < 1e-9 and abs(start_lng - end_lng) < 1e-9:
        return {
            "points": [
                {"lat": float(start_lat), "lng": float(start_lng)},
                {"lat": float(end_lat), "lng": float(end_lng)},
            ],
            "road_names": [],
        }

    coords = f"{start_lng},{start_lat};{end_lng},{end_lat}"
    encoded_coords = urllib.parse.quote(coords, safe=";,")
    base = str(osrm_base_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("OSRM base URL is empty.")
    url = (
        f"{base}/route/v1/driving/{encoded_coords}"
        "?overview=full&geometries=geojson&steps=true"
    )

    payload = None
    last_error: Optional[str] = None
    for attempt in range(2):
        try:
            with urllib.request.urlopen(url, timeout=max(2, timeout_sec)) as response:
                payload = json.loads(response.read().decode("utf-8"))
            break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            if attempt < 1:
                time.sleep(0.2)

    if payload is None:
        raise RuntimeError(last_error or "OSRM geometry request failed.")
    if not isinstance(payload, dict):
        raise RuntimeError("Unexpected OSRM geometry payload.")
    if payload.get("code") != "Ok":
        raise RuntimeError(f"OSRM geometry code={payload.get('code')}")
    routes = payload.get("routes")
    if not isinstance(routes, list) or not routes:
        raise RuntimeError("OSRM geometry missing routes.")
    first_route = routes[0]
    if not isinstance(first_route, dict):
        raise RuntimeError("OSRM geometry first route invalid.")
    geometry = first_route.get("geometry", {})
    if not isinstance(geometry, dict):
        raise RuntimeError("OSRM geometry object missing.")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise RuntimeError("OSRM geometry coordinates unavailable.")

    points: List[Dict[str, float]] = []
    for row in coordinates:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        lng = _safe_float(row[0])
        lat = _safe_float(row[1])
        if lat is None or lng is None:
            continue
        if points and abs(points[-1]["lat"] - lat) < 1e-9 and abs(points[-1]["lng"] - lng) < 1e-9:
            continue
        points.append({"lat": lat, "lng": lng})
    if len(points) < 2:
        raise RuntimeError("OSRM geometry has insufficient valid points.")
    road_names = _extract_osrm_step_road_names(first_route)
    return {
        "points": points,
        "road_names": road_names,
    }


def _sample_polyline_points(
    polyline: List[Dict[str, float]],
    step_km: float,
) -> List[Dict[str, Any]]:
    if len(polyline) < 2:
        return []

    cumulative: List[float] = [0.0]
    for idx in range(1, len(polyline)):
        prev = polyline[idx - 1]
        curr = polyline[idx]
        segment_km = _haversine_km(
            (float(prev["lat"]), float(prev["lng"])),
            (float(curr["lat"]), float(curr["lng"])),
        )
        cumulative.append(cumulative[-1] + segment_km)

    total_km = cumulative[-1]
    if total_km <= 0:
        return [
            {
                "sample_index": 0,
                "position": "start",
                "distance_from_start_km": 0.0,
                "lat": float(polyline[0]["lat"]),
                "lng": float(polyline[0]["lng"]),
            },
            {
                "sample_index": 1,
                "position": "end",
                "distance_from_start_km": 0.0,
                "lat": float(polyline[-1]["lat"]),
                "lng": float(polyline[-1]["lng"]),
            },
        ]

    steps = max(1, int(math.ceil(total_km / max(step_km, 1.0))))
    samples: List[Dict[str, Any]] = []
    edge_idx = 0
    for sample_index in range(steps + 1):
        target_km = total_km * (sample_index / float(steps))
        while edge_idx < len(cumulative) - 2 and cumulative[edge_idx + 1] < target_km:
            edge_idx += 1
        edge_start_km = cumulative[edge_idx]
        edge_end_km = cumulative[edge_idx + 1]
        if edge_end_km <= edge_start_km:
            fraction = 0.0
        else:
            fraction = (target_km - edge_start_km) / (edge_end_km - edge_start_km)

        start_point = polyline[edge_idx]
        end_point = polyline[edge_idx + 1]
        lat, lng = _interpolate_point(
            (float(start_point["lat"]), float(start_point["lng"])),
            (float(end_point["lat"]), float(end_point["lng"])),
            fraction,
        )

        if sample_index == 0:
            position = "start"
        elif sample_index == steps:
            position = "end"
        else:
            position = "along"

        samples.append(
            {
                "sample_index": sample_index,
                "position": position,
                "distance_from_start_km": round(target_km, 3),
                "lat": lat,
                "lng": lng,
            }
        )
    return samples


def _limit_samples(samples: List[Dict[str, Any]], max_samples: int) -> List[Dict[str, Any]]:
    if max_samples <= 0 or len(samples) <= max_samples:
        return samples
    if max_samples == 1:
        return [samples[0]]
    last_index = len(samples) - 1
    picked: List[Dict[str, Any]] = []
    used_indexes: Set[int] = set()
    for slot in range(max_samples):
        idx = int(round((slot * last_index) / float(max_samples - 1)))
        if idx in used_indexes:
            continue
        used_indexes.add(idx)
        picked.append(samples[idx])
    picked.sort(key=lambda row: int(row.get("sample_index", 0)))
    return picked


def _extract_municipality_candidates(elements: Any) -> List[Dict[str, Any]]:
    if not isinstance(elements, list):
        return []
    by_ref: Dict[str, Dict[str, Any]] = {}
    for element in elements:
        if not isinstance(element, dict):
            continue
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue
        name = str(tags.get("name") or "").strip()
        if not name:
            continue
        place = str(tags.get("place") or "").strip().lower()
        if place not in MUNICIPALITY_PLACE_WEIGHTS:
            continue

        point_lat = _safe_float(element.get("lat"))
        point_lng = _safe_float(element.get("lon"))
        if point_lat is None or point_lng is None:
            center = element.get("center")
            if isinstance(center, dict):
                point_lat = _safe_float(center.get("lat"))
                point_lng = _safe_float(center.get("lon"))
        if point_lat is None or point_lng is None:
            continue

        osm_ref = f"{element.get('type', 'element')}/{element.get('id')}"
        candidate = {
            "osm_ref": osm_ref,
            "name": name,
            "place": place,
            "population": _safe_int_str(tags.get("population"), 0),
            "lat": point_lat,
            "lng": point_lng,
        }
        previous = by_ref.get(osm_ref)
        if previous is None:
            by_ref[osm_ref] = candidate
            continue
        prev_rank = (
            previous["population"],
            MUNICIPALITY_PLACE_WEIGHTS.get(previous["place"], 0),
        )
        new_rank = (
            candidate["population"],
            MUNICIPALITY_PLACE_WEIGHTS.get(candidate["place"], 0),
        )
        if new_rank > prev_rank:
            by_ref[osm_ref] = candidate
    return list(by_ref.values())


def _query_osm_municipality_candidates_batch(
    samples: List[Dict[str, Any]], radius_km: float, timeout_sec: int
) -> List[Dict[str, Any]]:
    if not samples:
        return []
    radius_m = int(max(1000.0, radius_km * 1000.0))
    around_clauses: List[str] = []
    for sample in samples:
        lat = float(sample["lat"])
        lng = float(sample["lng"])
        around_clauses.append(
            f'node(around:{radius_m},{lat},{lng})["place"~"city|town|municipality|village|borough|suburb|quarter|hamlet|neighbourhood"];'
        )
        around_clauses.append(
            f'way(around:{radius_m},{lat},{lng})["place"~"city|town|municipality|village|borough|suburb|quarter|hamlet|neighbourhood"];'
        )
        around_clauses.append(
            f'relation(around:{radius_m},{lat},{lng})["place"~"city|town|municipality|village|borough|suburb|quarter|hamlet|neighbourhood"];'
        )
    query = (
        f"[out:json][timeout:{max(5, timeout_sec)}];\n(\n"
        + "\n".join(around_clauses)
        + "\n);\nout tags center;"
    )
    body = urllib.parse.urlencode({"data": query}).encode("utf-8")
    last_error: Optional[str] = None
    payload = None
    for endpoint in DEFAULT_OVERPASS_ENDPOINTS:
        try:
            request = urllib.request.Request(
                endpoint,
                data=body,
                headers={
                    "User-Agent": "softOptimizationVRP/municipality-enricher",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=max(2, timeout_sec)) as response:
                payload = json.loads(response.read().decode("utf-8"))
                break
        except Exception as exc:  # noqa: BLE001
            last_error = f"{endpoint}: {exc}"
            time.sleep(0.15)
    if payload is None:
        raise RuntimeError(last_error or "No Overpass endpoint available.")
    if isinstance(payload, dict):
        remark = str(payload.get("remark") or "").strip()
        if remark:
            raise RuntimeError(f"Overpass remark: {remark}")
    candidates = _extract_municipality_candidates(payload.get("elements", []))
    candidates.sort(
        key=lambda item: (
            -item["population"],
            -MUNICIPALITY_PLACE_WEIGHTS.get(item["place"], 0),
            item["name"].lower(),
        )
    )
    return candidates

def _pick_best_municipality_for_sample(
    sample: Dict[str, Any], candidates: List[Dict[str, Any]], radius_km: float
) -> Optional[Dict[str, Any]]:
    sample_point = (float(sample["lat"]), float(sample["lng"]))
    ranked: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
    for candidate in candidates:
        distance_km = _haversine_km(
            sample_point, (float(candidate["lat"]), float(candidate["lng"]))
        )
        if distance_km > radius_km:
            continue
        rank_key = (
            distance_km,
            -MUNICIPALITY_PLACE_WEIGHTS.get(str(candidate["place"]), 0),
            -int(candidate["population"]),
            str(candidate["name"]).lower(),
        )
        ranked.append((rank_key, {**candidate, "distance_km": round(distance_km, 3)}))
    if not ranked:
        return None
    ranked.sort(key=lambda row: row[0])
    return ranked[0][1]


def _query_osm_municipality_candidates_single(
    sample: Dict[str, Any], radius_km: float, timeout_sec: int
) -> List[Dict[str, Any]]:
    lat = float(sample["lat"])
    lng = float(sample["lng"])
    return _query_osm_municipality_candidates_batch(
        samples=[{"lat": lat, "lng": lng}],
        radius_km=radius_km,
        timeout_sec=timeout_sec,
    )


def _build_municipality_trace_for_segment(
    segment: Dict[str, Any],
    step_km: float,
    radius_km: float,
    timeout_sec: int,
    max_samples: int,
    allow_sample_fallback: bool,
    errors: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    distance_km = float(segment.get("distance_km", 0.0) or 0.0)
    samples = _sample_segment_points(
        start=segment["start"],
        end=segment["end"],
        distance_km=distance_km,
        step_km=step_km,
    )
    samples = _limit_samples(samples, max_samples)
    try:
        candidates = _query_osm_municipality_candidates_batch(
            samples=samples, radius_km=radius_km, timeout_sec=timeout_sec
        )
    except Exception as exc:
        candidates = []
        if errors is not None:
            errors.append(f"municipality batch query failed: {exc}")

    output: List[Dict[str, Any]] = []
    seen_refs: Set[str] = set()
    for sample in samples:
        best = _pick_best_municipality_for_sample(sample, candidates, radius_km)
        if best is None and allow_sample_fallback:
            # Fallback: query only this sample point (helps when batch query is partial/empty).
            try:
                local_candidates = _query_osm_municipality_candidates_single(
                    sample=sample,
                    radius_km=radius_km,
                    timeout_sec=timeout_sec,
                )
            except Exception as exc:
                local_candidates = []
                if errors is not None:
                    errors.append(f"municipality sample query failed: {exc}")
            best = _pick_best_municipality_for_sample(sample, local_candidates, radius_km)
        if best is None:
            continue
        unique_ref = str(best.get("osm_ref") or "").strip() or best["name"].lower()
        if unique_ref in seen_refs:
            continue
        seen_refs.add(unique_ref)
        output.append(
            {
                "sample_index": sample["sample_index"],
                "position": sample["position"],
                "distance_from_start_km": sample["distance_from_start_km"],
                "query_point": {
                    "lat": round(float(sample["lat"]), 6),
                    "lng": round(float(sample["lng"]), 6),
                },
                "municipality": {
                    "name": best["name"],
                    "place": best["place"],
                    "population": best["population"] if best["population"] > 0 else None,
                    "osm_ref": best["osm_ref"],
                    "lat": best["lat"],
                    "lng": best["lng"],
                    "distance_to_query_km": best["distance_km"],
                },
            }
        )
    return output


def _lat_lng_to_xy_km(lat: float, lng: float, ref_lat: float) -> Tuple[float, float]:
    x = math.radians(lng) * EARTH_RADIUS_KM * math.cos(math.radians(ref_lat))
    y = math.radians(lat) * EARTH_RADIUS_KM
    return x, y


def _point_to_segment_distance_km(
    point: Tuple[float, float],
    start: Tuple[float, float],
    end: Tuple[float, float],
) -> float:
    ref_lat = (point[0] + start[0] + end[0]) / 3.0
    px, py = _lat_lng_to_xy_km(point[0], point[1], ref_lat)
    sx, sy = _lat_lng_to_xy_km(start[0], start[1], ref_lat)
    ex, ey = _lat_lng_to_xy_km(end[0], end[1], ref_lat)

    vx = ex - sx
    vy = ey - sy
    seg_len_sq = vx * vx + vy * vy
    if seg_len_sq == 0.0:
        return math.hypot(px - sx, py - sy)

    t = ((px - sx) * vx + (py - sy) * vy) / seg_len_sq
    t = max(0.0, min(1.0, t))
    closest_x = sx + t * vx
    closest_y = sy + t * vy
    return math.hypot(px - closest_x, py - closest_y)


def _infer_category(location: Dict[str, Any]) -> str:
    explicit = location.get("semantic_category") or location.get("category")
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip().lower()

    tags = location.get("tags")
    if not isinstance(tags, dict):
        return "other"

    for key, value in tags.items():
        mapped = KNOWN_CATEGORY_MAP.get((str(key).strip(), str(value).strip()))
        if mapped:
            return mapped
    return "other"


def _normalize_locations(raw_locations: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_locations, list):
        return []

    normalized = []
    for index, raw in enumerate(raw_locations, start=1):
        if not isinstance(raw, dict):
            continue
        lat = _safe_float(raw.get("lat"))
        lng = _safe_float(raw.get("lng"))
        if lat is None or lng is None:
            continue

        tags = raw.get("tags", {})
        if not isinstance(tags, dict):
            tags = {}

        entry = {
            "id": raw.get("id", f"loc_{index}"),
            "name": raw.get("name"),
            "lat": lat,
            "lng": lng,
            "tags": tags,
            "source": raw.get("source", "candidate_locations"),
        }
        entry["semantic_category"] = _infer_category(entry)
        normalized.append(entry)
    return normalized


def _normalize_observations(raw_observations: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_observations, list):
        return []

    normalized = []
    for raw in raw_observations:
        if not isinstance(raw, dict):
            continue
        lat = _safe_float(raw.get("lat"))
        lng = _safe_float(raw.get("lng"))
        if lat is None or lng is None:
            continue

        row = dict(raw)
        row["lat"] = lat
        row["lng"] = lng
        row["_parsed_time"] = _parse_utc_datetime(raw.get("time_utc"))
        normalized.append(row)
    return normalized


def _distance_to_route_km(
    location: Dict[str, Any],
    stops: List[Dict[str, Any]],
) -> Tuple[float, Optional[int]]:
    if len(stops) < 2:
        return float("inf"), None

    point = (location["lat"], location["lng"])
    best_distance = float("inf")
    best_segment_index = None
    for index in range(len(stops) - 1):
        start = (float(stops[index]["lat"]), float(stops[index]["lng"]))
        end = (float(stops[index + 1]["lat"]), float(stops[index + 1]["lng"]))
        distance = _point_to_segment_distance_km(point, start, end)
        if distance < best_distance:
            best_distance = distance
            best_segment_index = index

    return best_distance, best_segment_index


def _match_observation(
    segment_midpoint: Dict[str, float],
    target_time_utc: Optional[datetime],
    observations: List[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], Optional[float], Optional[float]]:
    if not observations:
        return None, None, None

    best = None
    best_distance = None
    best_time_offset_min = None
    best_score = None

    midpoint = (segment_midpoint["lat"], segment_midpoint["lng"])
    for obs in observations:
        distance_km = _haversine_km(midpoint, (obs["lat"], obs["lng"]))
        obs_time = obs.get("_parsed_time")
        if target_time_utc is not None and obs_time is not None:
            time_offset_min = abs((obs_time - target_time_utc).total_seconds()) / 60.0
        else:
            time_offset_min = 0.0

        # 90 minutes ~= 1 km score penalty
        score = distance_km + (time_offset_min / 90.0)
        if best_score is None or score < best_score:
            best = obs
            best_distance = distance_km
            best_time_offset_min = time_offset_min
            best_score = score

    return best, best_distance, best_time_offset_min


def _format_weather_context(
    observation: Optional[Dict[str, Any]],
    distance_km: Optional[float],
    time_offset_min: Optional[float],
) -> Dict[str, Any]:
    if observation is None:
        return {
            "status": "unknown",
            "source": "not_provided",
            "temperature_c": None,
            "precipitation_mm": None,
            "wind_kph": None,
            "condition": None,
            "observed_at_utc": None,
        }

    formatted = {
        "status": "observed",
        "source": observation.get("source", "external_weather_feed"),
        "temperature_c": observation.get("temperature_c"),
        "precipitation_mm": observation.get("precipitation_mm"),
        "wind_kph": observation.get("wind_kph"),
        "condition": observation.get("condition"),
        "observed_at_utc": _to_iso_z(observation.get("_parsed_time")),
        "distance_km_to_segment": round(distance_km, 3) if distance_km is not None else None,
        "time_offset_min": (
            round(time_offset_min, 1) if time_offset_min is not None else None
        ),
    }
    forecast = observation.get("forecast_24h")
    if isinstance(forecast, dict):
        formatted["forecast_24h"] = forecast
    return formatted


def _unknown_weather_forecast(
    window_hours: int,
    interval_min: Optional[int],
    source: str = "not_provided",
) -> Dict[str, Any]:
    return {
        "status": "unknown",
        "source": source,
        "window_hours": window_hours,
        "interval_min": interval_min,
        "worst_case_score": None,
        "worst_slots": [],
        "evaluated_slots": 0,
    }


def _format_traffic_context(
    observation: Optional[Dict[str, Any]],
    distance_km: Optional[float],
    time_offset_min: Optional[float],
) -> Dict[str, Any]:
    if observation is None:
        return {
            "status": "unknown",
            "source": "not_provided",
            "congestion_level": None,
            "speed_kmh": None,
            "incident_count": None,
            "observed_at_utc": None,
        }

    formatted = {
        "status": "observed",
        "source": observation.get("source", "external_traffic_feed"),
        "congestion_level": observation.get("congestion_level"),
        "speed_kmh": observation.get("speed_kmh"),
        "incident_count": observation.get("incident_count"),
        "observed_at_utc": _to_iso_z(observation.get("_parsed_time")),
        "distance_km_to_segment": round(distance_km, 3) if distance_km is not None else None,
        "time_offset_min": (
            round(time_offset_min, 1) if time_offset_min is not None else None
        ),
    }
    forecast = observation.get("forecast_24h")
    if isinstance(forecast, dict):
        formatted["forecast_24h"] = forecast
    return formatted


def _unknown_traffic_forecast(
    window_hours: int,
    interval_min: int,
    source: str = "not_provided",
) -> Dict[str, Any]:
    return {
        "status": "unknown",
        "source": source,
        "window_hours": window_hours,
        "interval_min": interval_min,
        "worst_case_delay_ratio": None,
        "worst_case_delay_seconds": None,
        "worst_slots": [],
        "evaluated_slots": 0,
    }


def _build_route_segments(
    stops: List[Dict[str, Any]],
    avg_speed_kmh: float,
    departure_time_utc: Optional[datetime],
) -> List[Dict[str, Any]]:
    if len(stops) < 2:
        return []

    segments = []
    elapsed_min = 0.0
    cumulative_km = 0.0
    for index in range(len(stops) - 1):
        start = stops[index]
        end = stops[index + 1]
        start_point = (float(start["lat"]), float(start["lng"]))
        end_point = (float(end["lat"]), float(end["lng"]))
        segment_distance_km = _haversine_km(start_point, end_point)
        cumulative_km += segment_distance_km
        if avg_speed_kmh > 0:
            elapsed_min += (segment_distance_km / avg_speed_kmh) * 60.0
        eta_dt = (
            departure_time_utc + timedelta(minutes=elapsed_min)
            if departure_time_utc is not None
            else None
        )

        midpoint = {
            "lat": (start_point[0] + end_point[0]) / 2.0,
            "lng": (start_point[1] + end_point[1]) / 2.0,
        }

        segments.append(
            {
                "segment_index": index,
                "from_stop_id": start.get("id"),
                "to_stop_id": end.get("id"),
                "distance_km": round(segment_distance_km, 3),
                "cumulative_distance_km": round(cumulative_km, 3),
                "eta_min_from_departure": round(elapsed_min, 1),
                "eta_utc": _to_iso_z(eta_dt),
                "midpoint": midpoint,
                "start": {"lat": start_point[0], "lng": start_point[1]},
                "end": {"lat": end_point[0], "lng": end_point[1]},
            }
        )
    return segments


def _score_location(
    distance_km: float,
    radius_km: float,
    category: str,
    semantic_categories: Set[str],
) -> float:
    proximity_score = max(0.0, 1.0 - (distance_km / radius_km))
    if not semantic_categories:
        semantic_score = 1.0
    elif category in semantic_categories:
        semantic_score = 1.0
    else:
        semantic_score = 0.25
    return (0.65 * proximity_score) + (0.35 * semantic_score)


def _semantic_locations_for_route(
    route: Dict[str, Any],
    candidate_locations: List[Dict[str, Any]],
    radius_km: float,
    semantic_categories: Set[str],
    top_k: int,
    stop_exclusion_km: float = 8.0,
) -> List[Dict[str, Any]]:
    stops = route.get("stops", [])
    if len(stops) < 2 or not candidate_locations:
        return []

    scored = []
    near_stop_scored = []
    stop_points: List[Tuple[float, float]] = []
    for stop in stops:
        lat = _safe_float(stop.get("lat"))
        lng = _safe_float(stop.get("lng"))
        if lat is None or lng is None:
            continue
        stop_points.append((lat, lng))
    for location in candidate_locations:
        distance_km, nearest_segment_index = _distance_to_route_km(location, stops)
        if math.isinf(distance_km) or distance_km > radius_km:
            continue

        category = location["semantic_category"]
        score = _score_location(distance_km, radius_km, category, semantic_categories)
        nearest_stop_distance_km = min(
            (_haversine_km((location["lat"], location["lng"]), stop) for stop in stop_points),
            default=float("inf"),
        )
        normalized_location = {
            "id": location["id"],
            "name": location.get("name"),
            "lat": location["lat"],
            "lng": location["lng"],
            "source": location.get("source"),
            "semantic_category": category,
            "distance_to_route_km": round(distance_km, 3),
            "estimated_detour_km": round(distance_km * 2.0, 3),
            "nearest_segment_index": nearest_segment_index,
            "distance_to_nearest_stop_km": (
                round(nearest_stop_distance_km, 3)
                if not math.isinf(nearest_stop_distance_km)
                else None
            ),
            "relevance_score": round(score, 4),
            "tags": location.get("tags", {}),
        }
        if stop_exclusion_km > 0 and nearest_stop_distance_km < stop_exclusion_km:
            near_stop_scored.append(normalized_location)
        else:
            scored.append(normalized_location)

    # Prefer POIs away from stop hubs (city centers). If all are near stops,
    # gracefully fall back instead of returning an empty set.
    if not scored and near_stop_scored:
        scored = near_stop_scored
    elif len(scored) < top_k and near_stop_scored:
        # Backfill with near-stop POIs starting from the ones furthest from
        # stops, so we increase quantity without returning to city-center bias.
        near_stop_scored.sort(
            key=lambda item: (
                -(item.get("distance_to_nearest_stop_km") or 0.0),
                -item["relevance_score"],
                item["distance_to_route_km"],
                str(item["id"]),
            )
        )
        scored_ids = {str(item.get("id")) for item in scored}
        for item in near_stop_scored:
            item_id = str(item.get("id"))
            if item_id in scored_ids:
                continue
            scored.append(item)
            scored_ids.add(item_id)
            if len(scored) >= top_k:
                break

    scored.sort(
        key=lambda item: (
            -item["relevance_score"],
            item["distance_to_route_km"],
            str(item["id"]),
        )
    )
    if len(scored) <= top_k:
        return scored

    # Keep one high-scoring location per segment first, then fill remaining
    # slots with a soft segment cap. This avoids UI clustering around one area.
    selected: List[Dict[str, Any]] = []
    selected_ids: Set[str] = set()
    covered_segments: Set[int] = set()
    unique_segments = {
        _safe_int(item.get("nearest_segment_index"), -1) for item in scored
    }
    segment_count = max(1, len(unique_segments))
    segment_cap = max(1, int(math.ceil(top_k / float(segment_count))))
    per_segment_count: Dict[int, int] = {}
    for item in scored:
        segment_index = _safe_int(item.get("nearest_segment_index"), -1)
        if segment_index in covered_segments:
            continue
        selected.append(item)
        selected_ids.add(str(item.get("id")))
        covered_segments.add(segment_index)
        per_segment_count[segment_index] = per_segment_count.get(segment_index, 0) + 1
        if len(selected) >= top_k:
            return selected

    for item in scored:
        item_id = str(item.get("id"))
        if item_id in selected_ids:
            continue
        segment_index = _safe_int(item.get("nearest_segment_index"), -1)
        if per_segment_count.get(segment_index, 0) >= segment_cap:
            continue
        selected.append(item)
        selected_ids.add(item_id)
        per_segment_count[segment_index] = per_segment_count.get(segment_index, 0) + 1
        if len(selected) >= top_k:
            break

    if len(selected) < top_k:
        for item in scored:
            item_id = str(item.get("id"))
            if item_id in selected_ids:
                continue
            selected.append(item)
            selected_ids.add(item_id)
            if len(selected) >= top_k:
                break
    return selected


def build_semantic_layer(
    vrp_result: Dict[str, Any],
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    raw_payload = payload if isinstance(payload, dict) else {}

    radius_km = _safe_float(raw_payload.get("semantic_corridor_radius_km"))
    if radius_km is None:
        radius_km = DEFAULT_SEMANTIC_RADIUS_KM
    radius_km = max(0.1, radius_km)

    top_k = _safe_int(raw_payload.get("semantic_top_k"), DEFAULT_TOP_K)
    top_k = max(1, top_k)
    stop_exclusion_km = _safe_float(raw_payload.get("semantic_stop_exclusion_km"))
    if stop_exclusion_km is None:
        stop_exclusion_km = 8.0
    stop_exclusion_km = max(0.0, stop_exclusion_km)

    avg_speed_kmh = _safe_float(raw_payload.get("route_avg_speed_kmh"))
    if avg_speed_kmh is None:
        avg_speed_kmh = DEFAULT_AVG_SPEED_KMH
    avg_speed_kmh = max(5.0, avg_speed_kmh)

    departure_time_utc = _parse_utc_datetime(raw_payload.get("departure_time_utc"))
    semantic_categories = _normalize_categories(raw_payload.get("semantic_categories"))
    candidate_locations = _normalize_locations(raw_payload.get("candidate_locations"))
    weather_observations = _normalize_observations(
        raw_payload.get("weather_observations")
    )
    traffic_observations = _normalize_observations(
        raw_payload.get("traffic_observations")
    )
    here_data_source = _resolve_here_data_source(raw_payload.get("here_data_source"))
    here_api_key = os.getenv("HERE_API_KEY", "").strip()
    here_requested = _safe_bool(raw_payload.get("use_here_platform"), True)
    here_enabled = (
        here_requested and here_data_source == "emulator"
    ) or (here_requested and here_data_source == "here" and bool(here_api_key))
    if not here_requested:
        here_api_key_source = "disabled"
    elif here_data_source == "emulator":
        here_api_key_source = "not_required_emulator"
    else:
        here_api_key_source = "env:HERE_API_KEY" if here_api_key else "missing_env:HERE_API_KEY"
    here_timeout_sec = max(
        3, _safe_int(raw_payload.get("here_timeout_sec"), DEFAULT_HERE_TIMEOUT_SEC)
    )
    here_traffic_radius_m = max(
        50,
        _safe_int(
            raw_payload.get("here_traffic_radius_m"), DEFAULT_HERE_TRAFFIC_RADIUS_M
        ),
    )
    here_forecast_window_hours = max(
        1,
        _safe_int(
            raw_payload.get("here_forecast_window_hours"),
            DEFAULT_HERE_FORECAST_WINDOW_HOURS,
        ),
    )
    here_forecast_interval_min = max(
        30,
        _safe_int(
            raw_payload.get("here_forecast_interval_min"),
            DEFAULT_HERE_FORECAST_INTERVAL_MIN,
        ),
    )
    municipality_step_km = _safe_float(
        raw_payload.get("municipality_step_km"), DEFAULT_MUNICIPALITY_STEP_KM
    )
    municipality_step_km = max(5.0, municipality_step_km or DEFAULT_MUNICIPALITY_STEP_KM)
    municipality_radius_km = _safe_float(
        raw_payload.get("municipality_radius_km"), DEFAULT_MUNICIPALITY_RADIUS_KM
    )
    municipality_radius_km = max(1.0, municipality_radius_km or DEFAULT_MUNICIPALITY_RADIUS_KM)
    municipality_timeout_sec = max(
        2, _safe_int(raw_payload.get("municipality_osm_timeout_sec"), DEFAULT_OSM_TIMEOUT_SEC)
    )
    azure_maps_subscription_key = str(
        raw_payload.get("azure_maps_subscription_key")
        or os.getenv("AZURE_MAPS_SUBSCRIPTION_KEY", "")
    ).strip()
    azure_maps_reverse_endpoint = str(
        raw_payload.get("azure_maps_reverse_endpoint")
        or os.getenv("AZURE_MAPS_REVERSE_ENDPOINT", DEFAULT_AZURE_MAPS_REVERSE_ENDPOINT)
    ).strip() or DEFAULT_AZURE_MAPS_REVERSE_ENDPOINT
    azure_maps_reverse_api_version = str(
        raw_payload.get("azure_maps_reverse_api_version")
        or os.getenv(
            "AZURE_MAPS_REVERSE_API_VERSION",
            DEFAULT_AZURE_MAPS_REVERSE_API_VERSION,
        )
    ).strip() or DEFAULT_AZURE_MAPS_REVERSE_API_VERSION
    municipality_reverse_source = _resolve_municipality_reverse_source(
        raw_payload.get("municipality_reverse_source")
        or os.getenv(
            "MUNICIPALITY_REVERSE_SOURCE",
            DEFAULT_MUNICIPALITY_REVERSE_SOURCE,
        )
    )
    province_capital_lookup_enabled = _safe_bool(
        raw_payload.get("province_capital_lookup_enabled"), True
    )
    province_capital_timeout_sec = max(
        2,
        _safe_int(
            raw_payload.get("province_capital_timeout_sec"),
            municipality_timeout_sec,
        ),
    )
    municipality_max_samples_per_segment = max(
        3,
        _safe_int(
            raw_payload.get("municipality_max_samples_per_segment"),
            DEFAULT_MUNICIPALITY_MAX_SAMPLES_PER_SEGMENT,
        ),
    )
    municipality_allow_sample_fallback = _safe_bool(
        raw_payload.get("municipality_allow_sample_fallback"), False
    )
    municipality_enrichment_enabled = _safe_bool(
        raw_payload.get("municipality_enrichment_enabled"), False
    )
    requested_reverse_min_interval = raw_payload.get("municipality_reverse_min_interval_ms")
    if requested_reverse_min_interval is None:
        if municipality_reverse_source == "azure_maps_reverse":
            default_reverse_min_interval = DEFAULT_AZURE_MAPS_REVERSE_MIN_INTERVAL_MS
        else:
            default_reverse_min_interval = DEFAULT_MUNICIPALITY_REVERSE_MIN_INTERVAL_MS
    else:
        default_reverse_min_interval = DEFAULT_MUNICIPALITY_REVERSE_MIN_INTERVAL_MS
    municipality_reverse_min_interval_ms = max(
        0,
        _safe_int(
            requested_reverse_min_interval,
            default_reverse_min_interval,
        ),
    )
    distance_mode = str(raw_payload.get("distance_mode", "direct")).strip().lower()
    osrm_base_url = str(
        raw_payload.get("osrm_base_url", "https://router.project-osrm.org")
    ).strip()
    municipality_route_geometry_timeout_sec = max(
        2,
        _safe_int(
            raw_payload.get("municipality_route_geometry_timeout_sec"),
            DEFAULT_OSRM_ROUTE_TIMEOUT_SEC,
        ),
    )
    municipality_use_route_geometry = _safe_bool(
        raw_payload.get("municipality_use_route_geometry"), True
    )
    municipality_llm_enrichment_requested = _safe_bool(
        raw_payload.get("municipality_llm_enrichment_enabled"), True
    )
    municipality_llm_enrichment_enabled = (
        municipality_enrichment_enabled and municipality_llm_enrichment_requested
    )
    municipality_llm_timeout_sec = max(
        3,
        _safe_int(
            raw_payload.get("municipality_llm_timeout_sec"),
            DEFAULT_MUNICIPALITY_LLM_TIMEOUT_SEC,
        ),
    )
    municipality_llm_retries = max(
        0,
        _safe_int(
            raw_payload.get("municipality_llm_retries"),
            DEFAULT_MUNICIPALITY_LLM_RETRIES,
        ),
    )
    municipality_llm_max_tokens = max(
        200,
        _safe_int(
            raw_payload.get("municipality_llm_max_tokens"),
            DEFAULT_MUNICIPALITY_LLM_MAX_TOKENS,
        ),
    )
    azure_openai_endpoint = str(
        raw_payload.get("azure_openai_endpoint")
        or raw_payload.get("municipality_llm_endpoint")
        or os.getenv("AZURE_OPENAI_ENDPOINT", "")
    ).strip()
    azure_openai_api_key = str(
        raw_payload.get("azure_openai_api_key")
        or raw_payload.get("municipality_llm_api_key")
        or os.getenv("AZURE_OPENAI_API_KEY", "")
    ).strip()
    azure_openai_deployment = str(
        raw_payload.get("azure_openai_deployment")
        or raw_payload.get("municipality_llm_deployment")
        or os.getenv("AZURE_OPENAI_DEPLOYMENT", "")
    ).strip()
    azure_openai_api_version = str(
        raw_payload.get("azure_openai_api_version")
        or raw_payload.get("municipality_llm_api_version")
        or os.getenv("AZURE_OPENAI_API_VERSION", DEFAULT_AZURE_OPENAI_API_VERSION)
    ).strip() or DEFAULT_AZURE_OPENAI_API_VERSION
    municipality_llm_configured = bool(
        azure_openai_endpoint and azure_openai_api_key and azure_openai_deployment
    )
    distance_source = str(
        (vrp_result.get("summary", {}) if isinstance(vrp_result, dict) else {}).get(
            "distance_source", ""
        )
        or ""
    ).strip().lower()
    municipality_route_geometry_enabled = (
        municipality_enrichment_enabled
        and municipality_use_route_geometry
        and distance_mode == "osrm"
        and distance_source.startswith("osrm")
    )
    here_client = None
    if here_enabled and here_data_source == "emulator":
        here_client = HerePlatformEmulator(
            timeout_sec=here_timeout_sec,
            traffic_radius_m=here_traffic_radius_m,
            forecast_window_hours=here_forecast_window_hours,
            forecast_step_min=here_forecast_interval_min,
            seed=raw_payload.get("here_emulator_seed"),
        )
    elif here_enabled and here_data_source == "here":
        here_client = HerePlatformClient(
            api_key=here_api_key,
            timeout_sec=here_timeout_sec,
            traffic_radius_m=here_traffic_radius_m,
            forecast_window_hours=here_forecast_window_hours,
            forecast_step_min=here_forecast_interval_min,
        )

    routes_output = []
    matched_locations = 0
    segment_records = 0
    here_errors: List[str] = []
    municipality_records = 0
    municipality_errors: List[str] = []
    municipality_llm_errors: List[str] = []
    municipality_llm_added_report: List[Dict[str, Any]] = []
    province_capital_errors: List[str] = []
    municipality_phase1_points: Dict[str, Dict[str, Any]] = {}
    municipality_phase2_points: Dict[str, Dict[str, Any]] = {}
    municipality_phase1_input_points: List[Dict[str, Any]] = []
    province_capital_cache: Dict[str, Dict[str, Any]] = {}
    municipality_lookup = _build_municipality_lookup(
        initial_book={},
        timeout_sec=municipality_timeout_sec,
        min_interval_ms=municipality_reverse_min_interval_ms,
        reverse_source=municipality_reverse_source,
        azure_maps_subscription_key=azure_maps_subscription_key,
        azure_maps_reverse_endpoint=azure_maps_reverse_endpoint,
        azure_maps_api_version=azure_maps_reverse_api_version,
    )
    segment_shape_cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
    segment_shape_stats: Dict[str, Any] = {
        "enabled": municipality_route_geometry_enabled,
        "attempted": 0,
        "fetched": 0,
        "cache_hits": 0,
        "failed": 0,
        "skipped_identical_endpoints": 0,
        "fallback_to_straight": 0,
    }
    municipality_address_book: Dict[str, Dict[str, Any]] = municipality_lookup["book"]
    municipality_phase1_report: Dict[str, Any] = {
        "status": "disabled",
        "ok": True,
        "message": "Municipality phase 1 disabled.",
        "coordinates_total": 0,
        "resolved": 0,
        "unknown": 0,
        "failed": 0,
        "http_requests": 0,
        "cache_hits": 0,
    }
    municipality_phase2_report: Dict[str, Any] = {
        "status": "disabled",
        "ok": True,
        "message": "Municipality phase 2 disabled.",
        "coordinates_total": 0,
        "resolved": 0,
        "unknown": 0,
        "failed": 0,
        "http_requests": 0,
        "cache_hits": 0,
    }
    phase2_snapshot_before = _lookup_snapshot(municipality_lookup)
    municipality_api: Dict[str, Any] = {
        "enabled": municipality_enrichment_enabled,
        "source": municipality_reverse_source,
        "status": "disabled",
        "ok": True,
        "message": "Municipality enrichment disabled.",
        "coordinates_total": 0,
        "resolved": 0,
        "unknown": 0,
        "failed": 0,
        "province_capitals": {
            "enabled": bool(province_capital_lookup_enabled),
            "status": "disabled",
            "resolved": 0,
            "total": 0,
            "errors": [],
        },
        "errors": [],
    }
    municipality_llm_report: Dict[str, Any] = {
        "enabled": municipality_llm_enrichment_enabled,
        "configured": municipality_llm_configured,
        "status": "disabled",
        "message": (
            "Municipality LLM enrichment disabled."
            if not municipality_llm_enrichment_enabled
            else (
                "Municipality LLM enrichment is enabled."
                if municipality_llm_configured
                else "Municipality LLM enrichment skipped because Azure OpenAI config is incomplete."
            )
        ),
        "temperature": MUNICIPALITY_LLM_TEMPERATURE,
        "retries": municipality_llm_retries,
        "max_tokens": municipality_llm_max_tokens,
        "attempted_routes": 0,
        "enriched_routes": 0,
        "skipped_routes": 0,
        "failed_routes": 0,
        "attempted_segments": 0,
        "enriched_segments": 0,
        "failed_segments": 0,
        "skipped_reasons": [],
        "errors": [],
        "added_municipalities_by_route": [],
    }
    municipality_api["llm"] = municipality_llm_report
    if municipality_enrichment_enabled:
        municipality_phase1_points = _collect_problem_coordinates(vrp_result, raw_payload)
        phase1_snapshot_before = _lookup_snapshot(municipality_lookup)
        for key in sorted(municipality_phase1_points.keys()):
            _resolve_municipality_point(
                point=municipality_phase1_points[key],
                lookup=municipality_lookup,
                errors=municipality_errors,
                context_label="municipality reverse geocode phase1",
                record_unknown=True,
            )
        phase1_snapshot_after = _lookup_snapshot(municipality_lookup)
        phase1_counts = _summarize_points(municipality_phase1_points, municipality_lookup["book"])
        phase1_delta = _lookup_delta(phase1_snapshot_before, phase1_snapshot_after)
        if phase1_counts["total"] == 0:
            phase1_status = "empty"
            phase1_ok = True
            phase1_message = "No VRP coordinates available for municipality phase 1."
        elif phase1_counts["failed"] == 0 and phase1_counts["unknown"] == 0:
            phase1_status = "ok"
            phase1_ok = True
            phase1_message = "Municipality phase 1 completed successfully."
        elif phase1_counts["resolved"] > 0:
            phase1_status = "partial"
            phase1_ok = False
            phase1_message = "Municipality phase 1 completed with unknown/failed coordinates."
        else:
            phase1_status = "failed"
            phase1_ok = False
            phase1_message = "Municipality phase 1 failed to resolve any municipality."
        municipality_phase1_report = {
            "status": phase1_status,
            "ok": phase1_ok,
            "message": phase1_message,
            "coordinates_total": phase1_counts["total"],
            "resolved": phase1_counts["resolved"],
            "unknown": phase1_counts["unknown"],
            "failed": phase1_counts["failed"],
            "http_requests": phase1_delta["http_requests"],
            "cache_hits": phase1_delta["cache_hits"],
        }
        municipality_phase1_input_points = _build_phase1_input_points(
            phase1_points=municipality_phase1_points,
            municipality_book=municipality_lookup["book"],
            province_capital_cache=province_capital_cache,
            province_capital_errors=province_capital_errors,
            province_capital_lookup_enabled=province_capital_lookup_enabled,
            province_capital_timeout_sec=province_capital_timeout_sec,
        )
        phase2_snapshot_before = _lookup_snapshot(municipality_lookup)

    for route_index, route in enumerate(vrp_result.get("routes", [])):
        stops = route.get("stops", [])
        segments = _build_route_segments(stops, avg_speed_kmh, departure_time_utc)
        route_stop_municipality_links = (
            _build_route_stop_municipality_links(stops, municipality_lookup["book"])
            if municipality_enrichment_enabled
            else []
        )
        route_municipality_vector: List[str] = []
        route_province_vector: List[str] = []
        route_province_capital_vector: List[str] = []
        route_road_vector: List[str] = []
        route_llm_summary: Dict[str, Any] = {
            "status": "disabled",
            "reason": (
                "llm_disabled"
                if not municipality_llm_enrichment_enabled
                else (
                    "llm_not_configured"
                    if not municipality_llm_configured
                    else "pending"
                )
            ),
            "added_municipalities": [],
        }
        semantic_locations = _semantic_locations_for_route(
            route,
            candidate_locations,
            radius_km,
            semantic_categories,
            top_k,
            stop_exclusion_km=stop_exclusion_km,
        )

        segment_context = []
        for segment in segments:
            eta_dt = _parse_utc_datetime(segment.get("eta_utc"))
            weather_obs, weather_dist, weather_time = _match_observation(
                segment["midpoint"], eta_dt, weather_observations
            )
            traffic_obs, traffic_dist, traffic_time = _match_observation(
                segment["midpoint"], eta_dt, traffic_observations
            )
            weather_context = _format_weather_context(
                weather_obs, weather_dist, weather_time
            )
            traffic_context = _format_traffic_context(
                traffic_obs, traffic_dist, traffic_time
            )
            if "forecast_24h" not in weather_context:
                weather_context["forecast_24h"] = _unknown_weather_forecast(
                    here_forecast_window_hours,
                    here_forecast_interval_min if here_client is not None else None,
                )
            if "forecast_24h" not in traffic_context:
                traffic_context["forecast_24h"] = _unknown_traffic_forecast(
                    here_forecast_window_hours, here_forecast_interval_min
                )

            if here_client is not None:
                segment_reference_time = eta_dt or departure_time_utc or datetime.now(
                    tz=timezone.utc
                )
                midpoint = segment["midpoint"]
                try:
                    weather_bundle = here_client.fetch_weather(
                        midpoint["lat"],
                        midpoint["lng"],
                        reference_time_utc=segment_reference_time,
                    )
                    weather_realtime = weather_bundle.get("realtime")
                    if isinstance(weather_realtime, dict):
                        if weather_realtime.get("status") == "observed":
                            weather_context = dict(weather_realtime)
                            weather_context["distance_km_to_segment"] = 0.0
                            weather_context["time_offset_min"] = 0.0
                        elif weather_context.get("status") == "unknown":
                            weather_context = dict(weather_realtime)

                    weather_forecast = weather_bundle.get("forecast_24h")
                    if isinstance(weather_forecast, dict):
                        weather_context["forecast_24h"] = weather_forecast
                except RuntimeError as exc:
                    here_errors.append(str(exc))
                    weather_context["here_error"] = str(exc)

                try:
                    traffic_realtime = here_client.fetch_traffic_status(
                        midpoint["lat"], midpoint["lng"]
                    )
                    if isinstance(traffic_realtime, dict):
                        if traffic_realtime.get("status") == "observed":
                            traffic_context = dict(traffic_realtime)
                            traffic_context["distance_km_to_segment"] = 0.0
                            traffic_context["time_offset_min"] = 0.0
                        elif traffic_context.get("status") == "unknown":
                            traffic_context = dict(traffic_realtime)
                except RuntimeError as exc:
                    here_errors.append(str(exc))
                    traffic_context["here_error"] = str(exc)

                try:
                    traffic_forecast = here_client.fetch_traffic_forecast(
                        segment["start"],
                        segment["end"],
                        reference_time_utc=segment_reference_time,
                    )
                    if isinstance(traffic_forecast, dict):
                        traffic_context["forecast_24h"] = traffic_forecast
                except RuntimeError as exc:
                    here_errors.append(str(exc))
                    traffic_context["forecast_24h"] = _unknown_traffic_forecast(
                        here_forecast_window_hours,
                        here_forecast_interval_min,
                        source="here_routing_v8",
                    )
                    traffic_context["forecast_24h"]["error"] = str(exc)

            if "forecast_24h" not in weather_context:
                weather_context["forecast_24h"] = _unknown_weather_forecast(
                    here_forecast_window_hours,
                    here_forecast_interval_min if here_client is not None else None,
                )
            if "forecast_24h" not in traffic_context:
                traffic_context["forecast_24h"] = _unknown_traffic_forecast(
                    here_forecast_window_hours,
                    here_forecast_interval_min,
                )

            municipality_trace = []
            segment_municipality_vector: List[str] = []
            segment_province_vector: List[str] = []
            segment_province_capital_vector: List[str] = []
            segment_road_vector: List[str] = []
            if municipality_enrichment_enabled:
                route_shape_points: Optional[List[Dict[str, float]]] = None
                if municipality_route_geometry_enabled:
                    start_key = _coordinate_key(
                        float(segment["start"]["lat"]), float(segment["start"]["lng"])
                    )
                    end_key = _coordinate_key(
                        float(segment["end"]["lat"]), float(segment["end"]["lng"])
                    )
                    shape_cache_key = (start_key, end_key)
                    if shape_cache_key in segment_shape_cache:
                        cached_shape = segment_shape_cache[shape_cache_key]
                        if isinstance(cached_shape, dict):
                            cached_points = cached_shape.get("points")
                            if isinstance(cached_points, list):
                                route_shape_points = cached_points
                            segment_road_vector = _normalize_road_vector(
                                cached_shape.get("road_names")
                            )
                        else:
                            route_shape_points = None
                        segment_shape_stats["cache_hits"] += 1
                    else:
                        if start_key == end_key:
                            route_shape_points = [
                                {
                                    "lat": float(segment["start"]["lat"]),
                                    "lng": float(segment["start"]["lng"]),
                                },
                                {
                                    "lat": float(segment["end"]["lat"]),
                                    "lng": float(segment["end"]["lng"]),
                                },
                            ]
                            segment_shape_cache[shape_cache_key] = {
                                "points": route_shape_points,
                                "road_names": [],
                            }
                            segment_shape_stats["skipped_identical_endpoints"] += 1
                        else:
                            segment_shape_stats["attempted"] += 1
                            try:
                                osrm_shape = _fetch_osrm_segment_geometry(
                                    start=segment["start"],
                                    end=segment["end"],
                                    osrm_base_url=osrm_base_url,
                                    timeout_sec=municipality_route_geometry_timeout_sec,
                                )
                                if not isinstance(osrm_shape, dict):
                                    raise RuntimeError(
                                        "OSRM shape response is not a dictionary."
                                    )
                                route_shape_points = (
                                    osrm_shape.get("points")
                                    if isinstance(osrm_shape.get("points"), list)
                                    else None
                                )
                                segment_road_vector = _normalize_road_vector(
                                    osrm_shape.get("road_names")
                                )
                                segment_shape_cache[shape_cache_key] = {
                                    "points": route_shape_points,
                                    "road_names": segment_road_vector,
                                }
                                segment_shape_stats["fetched"] += 1
                            except Exception as exc:  # noqa: BLE001
                                route_shape_points = None
                                segment_shape_cache[shape_cache_key] = None
                                segment_shape_stats["failed"] += 1
                                municipality_errors.append(
                                    "municipality geometry fetch failed "
                                    f"({start_key}->{end_key}): {exc}"
                                )
                    if route_shape_points is None:
                        segment_shape_stats["fallback_to_straight"] += 1

                municipality_trace = _build_municipality_trace_from_segment_samples(
                    segment=segment,
                    lookup=municipality_lookup,
                    errors=municipality_errors,
                    phase2_points=municipality_phase2_points,
                    step_km=municipality_step_km,
                    max_samples=municipality_max_samples_per_segment,
                    route_shape_points=route_shape_points,
                )
                (
                    segment_municipality_vector,
                    segment_province_vector,
                    segment_province_capital_vector,
                ) = _build_segment_admin_vectors(
                    municipality_trace=municipality_trace,
                    municipality_book=municipality_lookup["book"],
                    province_capital_cache=province_capital_cache,
                    province_capital_errors=province_capital_errors,
                    province_capital_lookup_enabled=province_capital_lookup_enabled,
                    province_capital_timeout_sec=province_capital_timeout_sec,
                )
                for municipality_name in segment_municipality_vector:
                    _append_unique_in_order(route_municipality_vector, municipality_name)
                for province_name in segment_province_vector:
                    _append_unique_in_order(route_province_vector, province_name)
                for capital_name in segment_province_capital_vector:
                    _append_unique_in_order(route_province_capital_vector, capital_name)
                for road_name in segment_road_vector:
                    _append_unique_in_order(route_road_vector, road_name)
            municipality_records += len(municipality_trace)

            segment_context.append(
                {
                    "segment_index": segment["segment_index"],
                    "from_stop_id": segment["from_stop_id"],
                    "to_stop_id": segment["to_stop_id"],
                    "distance_km": segment["distance_km"],
                    "cumulative_distance_km": segment["cumulative_distance_km"],
                    "eta_min_from_departure": segment["eta_min_from_departure"],
                    "eta_utc": segment["eta_utc"],
                    "midpoint": segment["midpoint"],
                    "municipality_trace": municipality_trace,
                    "municipality_names": segment_municipality_vector,
                    "province_names": segment_province_vector,
                    "province_capital_names": segment_province_capital_vector,
                    "road_names": segment_road_vector,
                    "weather": weather_context,
                    "traffic": traffic_context,
                }
            )

        by_segment_index = {
            segment["segment_index"]: segment for segment in segment_context
        }
        for location in semantic_locations:
            linked = by_segment_index.get(location.get("nearest_segment_index"))
            if linked is None:
                continue
            location["weather"] = linked.get("weather")
            location["traffic"] = linked.get("traffic")

        if municipality_llm_enrichment_enabled and municipality_llm_configured:
            debug_first_route = route_index == 0
            served_customer_ids = route.get("served_customer_ids")
            has_served_customers = (
                isinstance(served_customer_ids, list) and len(served_customer_ids) > 0
            )
            has_zero_distance_segment = _route_has_zero_distance_segment(segment_context)
            if not has_served_customers:
                route_llm_summary = {
                    "status": "skipped",
                    "reason": "empty_route_no_served_customers",
                    "added_municipalities": [],
                }
                municipality_llm_report["skipped_routes"] += 1
                if len(municipality_llm_report["skipped_reasons"]) < 40:
                    municipality_llm_report["skipped_reasons"].append(
                        {
                            "vehicle": route.get("vehicle"),
                            "reason": route_llm_summary["reason"],
                        }
                    )
                if debug_first_route:
                    _safe_console_print(
                        "[municipality-llm][route-1][skip] "
                        "empty_route_no_served_customers"
                    )
            elif has_zero_distance_segment:
                route_llm_summary = {
                    "status": "skipped",
                    "reason": "contains_zero_distance_segment",
                    "added_municipalities": [],
                }
                municipality_llm_report["skipped_routes"] += 1
                if len(municipality_llm_report["skipped_reasons"]) < 40:
                    municipality_llm_report["skipped_reasons"].append(
                        {
                            "vehicle": route.get("vehicle"),
                            "reason": route_llm_summary["reason"],
                        }
                    )
                if debug_first_route:
                    _safe_console_print(
                        "[municipality-llm][route-1][skip] contains_zero_distance_segment"
                    )
            else:
                municipality_llm_report["attempted_routes"] += 1
                llm_result = _enrich_route_municipality_vectors_with_llm(
                    route=route,
                    route_stop_municipality_links=route_stop_municipality_links,
                    segment_context=segment_context,
                    endpoint=azure_openai_endpoint,
                    api_key=azure_openai_api_key,
                    deployment=azure_openai_deployment,
                    api_version=azure_openai_api_version,
                    timeout_sec=municipality_llm_timeout_sec,
                    retries=municipality_llm_retries,
                    max_tokens=municipality_llm_max_tokens,
                    debug_first_route=debug_first_route,
                )
                municipality_llm_report["attempted_segments"] += int(
                    llm_result.get("attempted_segments", 0)
                )
                municipality_llm_report["enriched_segments"] += int(
                    llm_result.get("enriched_segments", 0)
                )
                municipality_llm_report["failed_segments"] += int(
                    llm_result.get("failed_segments", 0)
                )
                if str(llm_result.get("status")).strip().lower() == "ok":
                    segment_vectors = llm_result.get("segment_vectors", {})
                    llm_warnings = (
                        llm_result.get("warnings", [])
                        if isinstance(llm_result.get("warnings"), list)
                        else []
                    )
                    if isinstance(segment_vectors, dict) and segment_vectors:
                        for segment in segment_context:
                            if not isinstance(segment, dict):
                                continue
                            segment_index = segment.get("segment_index")
                            if (
                                isinstance(segment_index, int)
                                and segment_index in segment_vectors
                            ):
                                segment["municipality_names"] = segment_vectors[segment_index]

                        rebuilt_route_vector: List[str] = []
                        for segment in segment_context:
                            if not isinstance(segment, dict):
                                continue
                            for municipality_name in _normalize_municipality_vector(
                                segment.get("municipality_names")
                            ):
                                _append_unique_in_order(
                                    rebuilt_route_vector, municipality_name
                                )
                        if rebuilt_route_vector:
                            route_municipality_vector = rebuilt_route_vector
                    elif llm_result.get("route_municipality_vector"):
                        route_municipality_vector = _normalize_municipality_vector(
                            llm_result.get("route_municipality_vector")
                        )

                    municipality_llm_report["enriched_routes"] += 1
                    added_rows = (
                        llm_result.get("added_municipalities", [])
                        if isinstance(llm_result.get("added_municipalities"), list)
                        else []
                    )
                    added_rows = added_rows[:20]
                    if added_rows:
                        municipality_llm_added_report.append(
                            {
                                "vehicle": route.get("vehicle"),
                                "added_municipalities": added_rows,
                            }
                        )
                    route_llm_summary = {
                        "status": "ok",
                        "reason": "enriched",
                        "added_municipalities": added_rows,
                    }
                    if llm_warnings:
                        route_llm_summary["warnings"] = llm_warnings[:20]
                        if len(municipality_llm_report["errors"]) < 40:
                            for warning_text in llm_warnings:
                                if len(municipality_llm_report["errors"]) >= 40:
                                    break
                                municipality_llm_report["errors"].append(
                                    f"vehicle={route.get('vehicle')}: {warning_text}"
                                )
                else:
                    error_text = str(llm_result.get("error") or "Unknown LLM error").strip()
                    municipality_llm_report["failed_routes"] += 1
                    municipality_llm_errors.append(
                        f"vehicle={route.get('vehicle')}: {error_text}"
                    )
                    if len(municipality_llm_report["errors"]) < 40:
                        municipality_llm_report["errors"].append(
                            f"vehicle={route.get('vehicle')}: {error_text}"
                        )
                    route_llm_summary = {
                        "status": "failed",
                        "reason": error_text,
                        "added_municipalities": [],
                    }
                    if debug_first_route:
                        _safe_console_print(
                            f"[municipality-llm][route-1][failure] {error_text}"
                        )
        elif municipality_llm_enrichment_enabled:
            route_llm_summary = {
                "status": "skipped",
                "reason": "llm_not_configured",
                "added_municipalities": [],
            }
            municipality_llm_report["skipped_routes"] += 1
            if len(municipality_llm_report["skipped_reasons"]) < 40:
                municipality_llm_report["skipped_reasons"].append(
                    {
                        "vehicle": route.get("vehicle"),
                        "reason": "llm_not_configured",
                    }
                )
            if route_index == 0:
                _safe_console_print("[municipality-llm][route-1][skip] llm_not_configured")

        segment_records += len(segment_context)
        matched_locations += len(semantic_locations)
        routes_output.append(
            {
                "vehicle": route.get("vehicle"),
                "route_distance_km": route.get("distance_km"),
                "served_customer_ids": route.get("served_customer_ids", []),
                "stop_municipality_links": route_stop_municipality_links,
                "province_vector": route_province_vector,
                "province_capital_vector": route_province_capital_vector,
                "municipality_vector": route_municipality_vector,
                "road_vector": route_road_vector,
                "municipality_llm": route_llm_summary,
                "semantic_locations": semantic_locations,
                "segment_context": segment_context,
            }
        )

    if municipality_llm_enrichment_enabled:
        municipality_llm_report["added_municipalities_by_route"] = municipality_llm_added_report[
            :20
        ]
        if not municipality_llm_configured:
            municipality_llm_report["status"] = "misconfigured"
            municipality_llm_report["message"] = (
                "Municipality LLM enrichment was requested but Azure OpenAI endpoint/api_key/deployment is missing."
            )
        else:
            attempted_routes = int(municipality_llm_report.get("attempted_routes", 0))
            enriched_routes = int(municipality_llm_report.get("enriched_routes", 0))
            failed_routes = int(municipality_llm_report.get("failed_routes", 0))
            skipped_routes = int(municipality_llm_report.get("skipped_routes", 0))
            if attempted_routes == 0 and skipped_routes > 0:
                municipality_llm_report["status"] = "skipped"
                municipality_llm_report["message"] = (
                    "Municipality LLM enrichment skipped for all routes due to guard conditions."
                )
            elif failed_routes == 0 and enriched_routes > 0:
                municipality_llm_report["status"] = "ok"
                municipality_llm_report["message"] = (
                    "Municipality LLM enrichment completed successfully."
                )
            elif failed_routes > 0 and enriched_routes > 0:
                municipality_llm_report["status"] = "partial"
                municipality_llm_report["message"] = (
                    "Municipality LLM enrichment completed with partial failures."
                )
            elif failed_routes > 0:
                municipality_llm_report["status"] = "failed"
                municipality_llm_report["message"] = (
                    "Municipality LLM enrichment failed for all attempted routes."
                )
            else:
                municipality_llm_report["status"] = "empty"
                municipality_llm_report["message"] = (
                    "Municipality LLM enrichment had no applicable routes."
                )

    municipality_address_book = municipality_lookup["book"]
    municipality_post_output_notice = (
        "Municipality fallback warning: municipality enrichment disabled."
    )
    municipality_post_output_warnings: List[str] = []
    municipality_post_output_infos: List[str] = []
    if municipality_enrichment_enabled:
        phase2_snapshot_after = _lookup_snapshot(municipality_lookup)
        phase2_counts = _summarize_points(municipality_phase2_points, municipality_lookup["book"])
        phase2_delta = _lookup_delta(phase2_snapshot_before, phase2_snapshot_after)
        if phase2_counts["total"] == 0:
            phase2_status = "empty"
            phase2_ok = True
            phase2_message = "No route sample points available for municipality phase 2."
        elif (
            phase2_counts["failed"] == 0
            and phase2_counts["unknown"] == 0
            and int(segment_shape_stats.get("fallback_to_straight", 0)) == 0
        ):
            phase2_status = "ok"
            phase2_ok = True
            phase2_message = "Municipality phase 2 route sampling completed successfully."
        elif phase2_counts["resolved"] > 0:
            phase2_status = "partial"
            phase2_ok = False
            if int(segment_shape_stats.get("fallback_to_straight", 0)) > 0:
                phase2_message = (
                    "Municipality phase 2 used straight-line fallback in some segments."
                )
            else:
                phase2_message = (
                    "Municipality phase 2 route sampling completed with unknown/failed points."
                )
        else:
            phase2_status = "failed"
            phase2_ok = False
            phase2_message = "Municipality phase 2 route sampling failed to resolve municipalities."
        municipality_phase2_report = {
            "status": phase2_status,
            "ok": phase2_ok,
            "message": phase2_message,
            "coordinates_total": phase2_counts["total"],
            "resolved": phase2_counts["resolved"],
            "unknown": phase2_counts["unknown"],
            "failed": phase2_counts["failed"],
            "http_requests": phase2_delta["http_requests"],
            "cache_hits": phase2_delta["cache_hits"],
            "route_geometry": dict(segment_shape_stats),
        }

        overall_failed = (
            int(municipality_phase1_report.get("failed", 0))
            + int(municipality_phase2_report.get("failed", 0))
        )
        overall_unknown = (
            int(municipality_phase1_report.get("unknown", 0))
            + int(municipality_phase2_report.get("unknown", 0))
        )
        if overall_failed == 0 and overall_unknown == 0:
            municipality_status = "ok"
            municipality_ok = True
            municipality_message = "Municipality enrichment completed successfully."
        elif (
            int(municipality_phase1_report.get("resolved", 0))
            + int(municipality_phase2_report.get("resolved", 0))
            > 0
        ):
            municipality_status = "partial"
            municipality_ok = False
            municipality_message = "Municipality enrichment completed with partial coverage."
        else:
            municipality_status = "failed"
            municipality_ok = False
            municipality_message = "Municipality enrichment failed."

        municipality_api = {
            "enabled": True,
            "source": municipality_reverse_source,
            "status": municipality_status,
            "ok": municipality_ok,
            "message": municipality_message,
            "coordinates_total": int(municipality_phase1_report.get("coordinates_total", 0)),
            "resolved": int(municipality_phase1_report.get("resolved", 0)),
            "unknown": int(municipality_phase1_report.get("unknown", 0)),
            "failed": int(municipality_phase1_report.get("failed", 0)),
            "phase1": municipality_phase1_report,
            "phase2": municipality_phase2_report,
            "lookup_stats": {
                "http_requests": int(municipality_lookup.get("http_requests", 0)),
                "cache_hits": int(municipality_lookup.get("cache_hits", 0)),
                "address_book_size": len(municipality_address_book),
            },
            "province_capitals": {
                "enabled": bool(province_capital_lookup_enabled),
                "status": (
                    "ok"
                    if all(
                        str(entry.get("status") or "").strip().lower() == "resolved"
                        for entry in province_capital_cache.values()
                    )
                    else (
                        "partial"
                        if any(
                            str(entry.get("status") or "").strip().lower() == "resolved"
                            for entry in province_capital_cache.values()
                        )
                        else (
                            "failed"
                            if province_capital_cache
                            else "empty"
                        )
                    )
                ),
                "resolved": sum(
                    1
                    for entry in province_capital_cache.values()
                    if str(entry.get("status") or "").strip().lower() == "resolved"
                ),
                "total": len(province_capital_cache),
                "errors": province_capital_errors[:20],
            },
            "llm": municipality_llm_report,
            "route_geometry": dict(segment_shape_stats),
            "errors": (
                municipality_errors + municipality_llm_errors + province_capital_errors
            )[:40],
        }
        fallback_to_straight = int(
            municipality_api.get("route_geometry", {}).get("fallback_to_straight", 0)
        )
        phase1_unknown = int(municipality_api.get("phase1", {}).get("unknown", 0))
        phase1_failed = int(municipality_api.get("phase1", {}).get("failed", 0))
        api_status = str(municipality_api.get("status") or "").strip().lower()
        if fallback_to_straight > 0:
            municipality_post_output_warnings.append(
                "WARNING: Municipality tracing used straight-line fallback in "
                f"{fallback_to_straight} segment(s) because OSRM route geometry was unavailable."
            )
        if phase1_unknown > 0 or phase1_failed > 0:
            municipality_post_output_warnings.append(
                "WARNING: Municipality phase 1 has unresolved coordinates "
                f"(unknown={phase1_unknown}, failed={phase1_failed})."
            )
        if api_status and api_status != "ok":
            municipality_post_output_warnings.append(
                "WARNING: Municipality API status is "
                f"'{municipality_api.get('status')}'. Review municipality_api.phase1/phase2."
            )
        llm_status = str(
            municipality_api.get("llm", {}).get("status")
            if isinstance(municipality_api.get("llm"), dict)
            else ""
        ).strip().lower()
        if municipality_llm_enrichment_enabled and llm_status not in {"ok", "empty", "skipped"}:
            municipality_post_output_warnings.append(
                "WARNING: Municipality LLM status is "
                f"'{municipality_api.get('llm', {}).get('status')}'. Review municipality_api.llm."
            )
        llm_added_by_route = (
            municipality_api.get("llm", {}).get("added_municipalities_by_route", [])
            if isinstance(municipality_api.get("llm"), dict)
            else []
        )
        if isinstance(llm_added_by_route, list) and llm_added_by_route:
            route_chunks: List[str] = []
            for row in llm_added_by_route[:8]:
                if not isinstance(row, dict):
                    continue
                vehicle = row.get("vehicle")
                additions = (
                    row.get("added_municipalities", [])
                    if isinstance(row.get("added_municipalities"), list)
                    else []
                )
                names = [
                    _normalize_llm_text(item.get("name"))
                    for item in additions
                    if isinstance(item, dict)
                ]
                names = [name for name in names if name]
                if not names:
                    continue
                route_chunks.append(f"V{vehicle}: " + ", ".join(names[:10]))
            if route_chunks:
                municipality_post_output_infos.append(
                    "INFO: LLM-added municipalities -> " + " | ".join(route_chunks)
                )
        if municipality_post_output_warnings or municipality_post_output_infos:
            municipality_post_output_notice = " | ".join(
                municipality_post_output_warnings + municipality_post_output_infos
            )
        else:
            municipality_post_output_notice = (
                "Municipality fallback warning: none. Municipality tracing completed without fallback."
            )

    return {
        "version": "0.9.1",
        "generated_at_utc": _to_iso_z(datetime.now(tz=timezone.utc)),
        "config": {
            "semantic_corridor_radius_km": round(radius_km, 3),
            "semantic_top_k": top_k,
            "semantic_stop_exclusion_km": round(stop_exclusion_km, 3),
            "route_avg_speed_kmh": round(avg_speed_kmh, 3),
            "semantic_categories": sorted(semantic_categories),
            "departure_time_utc": _to_iso_z(departure_time_utc),
            "use_here_platform": bool(here_client),
            "here_data_source": here_data_source,
            "here_api_key_source": here_api_key_source,
            "here_timeout_sec": here_timeout_sec,
            "here_traffic_radius_m": here_traffic_radius_m,
            "here_forecast_window_hours": here_forecast_window_hours,
            "here_forecast_interval_min": here_forecast_interval_min,
            "here_pipeline_mode": str(raw_payload.get("here_pipeline_mode", "postprocessing")),
            "municipality_step_km": round(municipality_step_km, 3),
            "municipality_radius_km": round(municipality_radius_km, 3),
            "municipality_osm_timeout_sec": municipality_timeout_sec,
            "municipality_reverse_timeout_sec": municipality_timeout_sec,
            "municipality_max_samples_per_segment": municipality_max_samples_per_segment,
            "municipality_allow_sample_fallback": municipality_allow_sample_fallback,
            "municipality_reverse_min_interval_ms": municipality_reverse_min_interval_ms,
            "municipality_trace_strategy": (
                "segment_osrm_geometry_reverse_geocode_samples"
                if municipality_route_geometry_enabled
                else "segment_straight_line_reverse_geocode_samples"
            ),
            "municipality_reverse_source": municipality_reverse_source,
            "azure_maps_reverse_enabled": (
                municipality_reverse_source == "azure_maps_reverse"
                and bool(azure_maps_subscription_key)
            ),
            "azure_maps_reverse_endpoint": azure_maps_reverse_endpoint,
            "azure_maps_reverse_api_version": azure_maps_reverse_api_version,
            "municipality_enrichment_enabled": municipality_enrichment_enabled,
            "municipality_llm_enrichment_enabled": municipality_llm_enrichment_enabled,
            "municipality_llm_configured": municipality_llm_configured,
            "municipality_llm_timeout_sec": municipality_llm_timeout_sec,
            "municipality_llm_retries": municipality_llm_retries,
            "municipality_llm_max_tokens": municipality_llm_max_tokens,
            "municipality_llm_temperature": MUNICIPALITY_LLM_TEMPERATURE,
            "municipality_llm_api_version": azure_openai_api_version,
            "municipality_osm_enabled": False,
            "municipality_use_route_geometry": municipality_use_route_geometry,
            "municipality_route_geometry_enabled": municipality_route_geometry_enabled,
            "municipality_route_geometry_timeout_sec": municipality_route_geometry_timeout_sec,
            "province_capital_lookup_enabled": bool(province_capital_lookup_enabled),
            "province_capital_timeout_sec": province_capital_timeout_sec,
            "distance_mode": distance_mode,
            "distance_source": distance_source,
        },
        "summary": {
            "routes_enriched": len(routes_output),
            "segment_context_records": segment_records,
            "candidate_locations_received": len(candidate_locations),
            "matched_semantic_locations": matched_locations,
            "weather_observations_received": len(weather_observations),
            "traffic_observations_received": len(traffic_observations),
            "here_platform_enabled": bool(here_client),
            "here_data_source": here_data_source,
            "here_errors": len(here_errors),
            "municipality_records": municipality_records,
            "municipality_api_status": municipality_api.get("status"),
            "municipality_coordinates_total": municipality_api.get("coordinates_total"),
            "municipality_coordinates_resolved": municipality_api.get("resolved"),
            "municipality_coordinates_unknown": municipality_api.get("unknown"),
            "municipality_coordinates_failed": municipality_api.get("failed"),
            "municipality_phase2_coordinates_total": (
                municipality_api.get("phase2", {}).get("coordinates_total")
                if isinstance(municipality_api.get("phase2"), dict)
                else 0
            ),
            "municipality_phase2_resolved": (
                municipality_api.get("phase2", {}).get("resolved")
                if isinstance(municipality_api.get("phase2"), dict)
                else 0
            ),
            "municipality_phase2_unknown": (
                municipality_api.get("phase2", {}).get("unknown")
                if isinstance(municipality_api.get("phase2"), dict)
                else 0
            ),
            "municipality_phase2_failed": (
                municipality_api.get("phase2", {}).get("failed")
                if isinstance(municipality_api.get("phase2"), dict)
                else 0
            ),
            "municipality_route_geometry_fetched": (
                municipality_api.get("route_geometry", {}).get("fetched")
                if isinstance(municipality_api.get("route_geometry"), dict)
                else 0
            ),
            "municipality_route_geometry_fallback_to_straight": (
                municipality_api.get("route_geometry", {}).get("fallback_to_straight")
                if isinstance(municipality_api.get("route_geometry"), dict)
                else 0
            ),
            "municipality_route_geometry_skipped_identical_endpoints": (
                municipality_api.get("route_geometry", {}).get("skipped_identical_endpoints")
                if isinstance(municipality_api.get("route_geometry"), dict)
                else 0
            ),
            "municipality_llm_status": (
                municipality_api.get("llm", {}).get("status")
                if isinstance(municipality_api.get("llm"), dict)
                else "disabled"
            ),
            "municipality_llm_attempted_routes": (
                municipality_api.get("llm", {}).get("attempted_routes")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_enriched_routes": (
                municipality_api.get("llm", {}).get("enriched_routes")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_skipped_routes": (
                municipality_api.get("llm", {}).get("skipped_routes")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_failed_routes": (
                municipality_api.get("llm", {}).get("failed_routes")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_attempted_segments": (
                municipality_api.get("llm", {}).get("attempted_segments")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_enriched_segments": (
                municipality_api.get("llm", {}).get("enriched_segments")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_failed_segments": (
                municipality_api.get("llm", {}).get("failed_segments")
                if isinstance(municipality_api.get("llm"), dict)
                else 0
            ),
            "municipality_llm_added_routes": (
                len(
                    municipality_api.get("llm", {}).get("added_municipalities_by_route", [])
                )
                if isinstance(municipality_api.get("llm"), dict)
                and isinstance(
                    municipality_api.get("llm", {}).get("added_municipalities_by_route"),
                    list,
                )
                else 0
            ),
            "municipality_llm_added_municipalities": (
                sum(
                    len(
                        row.get("added_municipalities", [])
                        if isinstance(row, dict)
                        and isinstance(row.get("added_municipalities"), list)
                        else []
                    )
                    for row in municipality_api.get("llm", {}).get(
                        "added_municipalities_by_route", []
                    )
                )
                if isinstance(municipality_api.get("llm"), dict)
                and isinstance(
                    municipality_api.get("llm", {}).get("added_municipalities_by_route"),
                    list,
                )
                else 0
            ),
            "municipality_address_records": len(municipality_address_book),
            "municipality_phase1_input_points": len(municipality_phase1_input_points),
            "province_capital_records": len(province_capital_cache),
            "province_capital_resolved": sum(
                1
                for entry in province_capital_cache.values()
                if str(entry.get("status") or "").strip().lower() == "resolved"
            ),
            "municipality_post_output_notice": municipality_post_output_notice,
            "here_client_stats": here_client.stats() if here_client is not None else {},
        },
        "errors": (
            here_errors + municipality_errors + municipality_llm_errors + province_capital_errors
        )[:40],
        "municipality_api": municipality_api,
        "municipality_address_book": municipality_address_book,
        "municipality_phase1_input_points": municipality_phase1_input_points,
        "municipality_post_output_notice": municipality_post_output_notice,
        "municipality_post_output_warnings": municipality_post_output_warnings,
        "municipality_post_output_infos": municipality_post_output_infos,
        "routes": routes_output,
    }

