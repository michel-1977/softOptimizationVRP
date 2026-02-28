from datetime import datetime, timedelta, timezone
import json
import math
import os
import time
from typing import Any, Dict, List, Optional, Set, Tuple
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
DEFAULT_MUNICIPALITY_STEP_KM = 20.0
DEFAULT_MUNICIPALITY_RADIUS_KM = 5.0
DEFAULT_OSM_TIMEOUT_SEC = 8
DEFAULT_OSRM_ROUTE_TIMEOUT_SEC = 10
DEFAULT_MUNICIPALITY_MAX_SAMPLES_PER_SEGMENT = 12
DEFAULT_MUNICIPALITY_REVERSE_MIN_INTERVAL_MS = 1100
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
    text = str(value or "").strip()
    if not text:
        return
    if items and items[-1].casefold() == text.casefold():
        return
    items.append(text)


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
) -> Dict[str, Any]:
    status = "error" if error else "unknown"
    return {
        "status": status,
        "source": "nominatim_reverse",
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


def _reverse_geocode_stop_address(
    lat: float,
    lng: float,
    timeout_sec: int,
) -> Dict[str, Any]:
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

    municipality_name, municipality_source_field = _extract_municipality_from_reverse_payload(payload)
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


def _build_municipality_lookup(
    initial_book: Optional[Dict[str, Dict[str, Any]]],
    timeout_sec: int,
    min_interval_ms: int,
) -> Dict[str, Any]:
    return {
        "book": dict(initial_book) if isinstance(initial_book, dict) else {},
        "timeout_sec": max(2, int(timeout_sec)),
        "min_interval_ms": max(0, int(min_interval_ms)),
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
        row = _reverse_geocode_stop_address(
            lat=lat,
            lng=lng,
            timeout_sec=int(lookup.get("timeout_sec", DEFAULT_OSM_TIMEOUT_SEC)),
        )
    except Exception as exc:  # noqa: BLE001
        lookup["http_requests"] = int(lookup.get("http_requests", 0)) + 1
        lookup["last_request_ts"] = time.monotonic()
        if errors is not None:
            errors.append(f"{context_label} failed at {round(lat, 6)},{round(lng, 6)}: {exc}")
        row = _empty_municipality_entry(lat, lng, error=str(exc))
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


def _fetch_osrm_segment_geometry(
    start: Dict[str, float],
    end: Dict[str, float],
    osrm_base_url: str,
    timeout_sec: int,
) -> List[Dict[str, float]]:
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

    coords = f"{start_lng},{start_lat};{end_lng},{end_lat}"
    encoded_coords = urllib.parse.quote(coords, safe=";,")
    base = str(osrm_base_url or "").strip().rstrip("/")
    if not base:
        raise RuntimeError("OSRM base URL is empty.")
    url = (
        f"{base}/route/v1/driving/{encoded_coords}"
        "?overview=full&geometries=geojson&steps=false"
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
    geometry = routes[0].get("geometry", {})
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
    return points


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
) -> List[Dict[str, Any]]:
    stops = route.get("stops", [])
    if len(stops) < 2 or not candidate_locations:
        return []

    scored = []
    for location in candidate_locations:
        distance_km, nearest_segment_index = _distance_to_route_km(location, stops)
        if math.isinf(distance_km) or distance_km > radius_km:
            continue

        category = location["semantic_category"]
        score = _score_location(distance_km, radius_km, category, semantic_categories)
        scored.append(
            {
                "id": location["id"],
                "name": location.get("name"),
                "lat": location["lat"],
                "lng": location["lng"],
                "source": location.get("source"),
                "semantic_category": category,
                "distance_to_route_km": round(distance_km, 3),
                "estimated_detour_km": round(distance_km * 2.0, 3),
                "nearest_segment_index": nearest_segment_index,
                "relevance_score": round(score, 4),
                "tags": location.get("tags", {}),
            }
        )

    scored.sort(
        key=lambda item: (
            -item["relevance_score"],
            item["distance_to_route_km"],
            str(item["id"]),
        )
    )
    return scored[:top_k]


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
    municipality_reverse_min_interval_ms = max(
        0,
        _safe_int(
            raw_payload.get("municipality_reverse_min_interval_ms"),
            DEFAULT_MUNICIPALITY_REVERSE_MIN_INTERVAL_MS,
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
    province_capital_errors: List[str] = []
    municipality_phase1_points: Dict[str, Dict[str, Any]] = {}
    municipality_phase2_points: Dict[str, Dict[str, Any]] = {}
    municipality_phase1_input_points: List[Dict[str, Any]] = []
    province_capital_cache: Dict[str, Dict[str, Any]] = {}
    municipality_lookup = _build_municipality_lookup(
        initial_book={},
        timeout_sec=municipality_timeout_sec,
        min_interval_ms=municipality_reverse_min_interval_ms,
    )
    segment_shape_cache: Dict[Tuple[str, str], Optional[List[Dict[str, float]]]] = {}
    segment_shape_stats: Dict[str, Any] = {
        "enabled": municipality_route_geometry_enabled,
        "attempted": 0,
        "fetched": 0,
        "cache_hits": 0,
        "failed": 0,
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
        "source": "nominatim_reverse",
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

    for route in vrp_result.get("routes", []):
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
        semantic_locations = _semantic_locations_for_route(
            route,
            candidate_locations,
            radius_km,
            semantic_categories,
            top_k,
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
                        route_shape_points = segment_shape_cache[shape_cache_key]
                        segment_shape_stats["cache_hits"] += 1
                    else:
                        segment_shape_stats["attempted"] += 1
                        try:
                            route_shape_points = _fetch_osrm_segment_geometry(
                                start=segment["start"],
                                end=segment["end"],
                                osrm_base_url=osrm_base_url,
                                timeout_sec=municipality_route_geometry_timeout_sec,
                            )
                            segment_shape_cache[shape_cache_key] = route_shape_points
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
                "semantic_locations": semantic_locations,
                "segment_context": segment_context,
            }
        )

    municipality_address_book = municipality_lookup["book"]
    municipality_post_output_notice = (
        "Municipality fallback warning: municipality enrichment disabled."
    )
    municipality_post_output_warnings: List[str] = []
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
            "source": "nominatim_reverse",
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
            "route_geometry": dict(segment_shape_stats),
            "errors": (municipality_errors + province_capital_errors)[:40],
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
        if municipality_post_output_warnings:
            municipality_post_output_notice = " | ".join(municipality_post_output_warnings)
        else:
            municipality_post_output_notice = (
                "Municipality fallback warning: none. Municipality tracing completed without fallback."
            )

    return {
        "version": "0.9",
        "generated_at_utc": _to_iso_z(datetime.now(tz=timezone.utc)),
        "config": {
            "semantic_corridor_radius_km": round(radius_km, 3),
            "semantic_top_k": top_k,
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
            "municipality_reverse_source": "nominatim_reverse",
            "municipality_enrichment_enabled": municipality_enrichment_enabled,
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
        "errors": (here_errors + municipality_errors + province_capital_errors)[:40],
        "municipality_api": municipality_api,
        "municipality_address_book": municipality_address_book,
        "municipality_phase1_input_points": municipality_phase1_input_points,
        "municipality_post_output_notice": municipality_post_output_notice,
        "municipality_post_output_warnings": municipality_post_output_warnings,
        "routes": routes_output,
    }
