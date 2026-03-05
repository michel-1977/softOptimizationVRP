import json
import hashlib
import math
import os
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request

import azure.functions as func

from solve_vrp import solve_vrp_nearest_neighbor
from solve_vrp.here_emulator import HerePlatformEmulator
from solve_vrp.here_platform import HerePlatformClient
from solve_vrp.scraping_cache import MemoryTTLCache, RedisScrapingCache, ScrapingCache
from solve_vrp.semantic_layer import build_semantic_layer

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

DEFAULT_OVERPASS_ENDPOINTS = (
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.openstreetmap.fr/api/interpreter",
)

AUTO_POI_TAG_FILTERS = {
    "amenity": ("charging_station",),
    "highway": ("rest_area", "services"),
}

AUTO_POI_CATEGORY_MAP = {
    ("amenity", "charging_station"): "charging",
    ("highway", "rest_area"): "rest_area",
    ("highway", "services"): "rest_area",
}

DEFAULT_POI_AUTO_RADIUS_KM = 3.0
DEFAULT_POI_AUTO_QUERY_MAX_RADIUS_KM = 2.5
DEFAULT_POI_AUTO_TIMEOUT_SEC = 5
DEFAULT_POI_AUTO_MAX_SAMPLES = 10
DEFAULT_POI_AUTO_MAX_CANDIDATES = 250
DEFAULT_POI_AUTO_CHUNK_SIZE = 3
DEFAULT_POI_AUTO_MAX_CHUNK_QUERIES = 4
DEFAULT_POI_AUTO_MAX_ENDPOINTS = 4
SCRAPING_TEMP_FILENAME = "social_scraping_latest.txt"
DEFAULT_SCRAPING_KEYWORDS = (
    "accident OR robbery OR protest OR fire OR risk OR flood OR storm OR incident"
)
DEFAULT_SCRAPING_PER_LOCATION_LIMIT = 5
DEFAULT_SCRAPING_RADIUS_KM = 15
DEFAULT_SCRAPING_MINUTES_BACK = 300
DEFAULT_SCRAPING_PREVIEW_LIMIT = 60
DEFAULT_SCRAPING_LANG = ""
DEFAULT_SCRAPING_FALLBACK_MAX_POSTS = 3
DEFAULT_SCRAPING_CACHE_TTL_SEC = 1800
DEFAULT_SCRAPING_CACHE_GEOHASH_PRECISION = 6
DEFAULT_SCRAPING_CACHE_MAX_ERRORS = 20
DEFAULT_SCRAPING_CACHE_MISS_DEBUG_LIMIT = 120
DEFAULT_SCRAPING_FORWARD_GEOCODE_ENABLED = True
DEFAULT_SCRAPING_FORWARD_GEOCODE_TIMEOUT_SEC = 4
DEFAULT_SCRAPING_FORWARD_GEOCODE_LIMIT = 20
DEFAULT_SCRAPING_CACHE_BACKEND = "memory"
DEFAULT_SCRAPING_STAGE_POLICY = "enrich_only"
DEFAULT_BLUESKY_API_BASE = "https://api.bsky.app"
SCRAPING_CACHE_KEY_VERSION = "v2"
FORWARD_GEOCODE_DISAMBIG_VERSION = "nominatim_v2"
FORWARD_GEOCODE_QUERY_MODES = {"none", "structured_city", "free_text"}
NOMINATIM_LOCALITY_ADDRESS_TYPES = {
    "municipality",
    "city",
    "town",
    "village",
    "hamlet",
    "locality",
}
NOMINATIM_BROAD_ADMIN_ADDRESS_TYPES = {
    "province",
    "state",
    "county",
    "region",
    "road",
}
_SCRAPING_MEMORY_CACHE = MemoryTTLCache(max_entries=5000)


def _as_bool(value, default: bool) -> bool:
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


def _safe_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _value_to_serializable(value):
    if value is None:
        return ""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return ""
    if isinstance(value, (int, bool)):
        return value
    if isinstance(value, float):
        return round(value, 6)
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        try:
            return isoformat()
        except Exception:  # noqa: BLE001 - fallback keeps responses stable
            pass
    return str(value)


def _dedupe_text_values(values):
    out = []
    seen = set()
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _normalize_text_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFD", text.casefold())
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def _normalize_match_key(value: Any) -> str:
    normalized = _normalize_text_key(value)
    if not normalized:
        return ""
    return re.sub(r"[^a-z0-9]+", "", normalized)


def _location_slug(value: Any) -> str:
    normalized = _normalize_text_key(value)
    if not normalized:
        return ""
    slug = re.sub(r"[^a-z0-9]+", "-", normalized)
    return slug.strip("-")


def _iter_routes_from_payload(result_payload: dict) -> List[dict]:
    routes: List[dict] = []
    if not isinstance(result_payload, dict):
        return routes

    root_routes = result_payload.get("routes")
    if isinstance(root_routes, list):
        routes.extend(route for route in root_routes if isinstance(route, dict))

    semantic = result_payload.get("semantic_layer")
    if isinstance(semantic, dict):
        semantic_routes = semantic.get("routes")
        if isinstance(semantic_routes, list):
            routes.extend(route for route in semantic_routes if isinstance(route, dict))
    return routes


def _iter_routes_by_priority(result_payload: dict) -> List[Tuple[str, dict]]:
    prioritized: List[Tuple[str, dict]] = []
    if not isinstance(result_payload, dict):
        return prioritized

    semantic = result_payload.get("semantic_layer")
    if isinstance(semantic, dict):
        semantic_routes = semantic.get("routes")
        if isinstance(semantic_routes, list):
            for route in semantic_routes:
                if isinstance(route, dict):
                    prioritized.append(("semantic", route))

    root_routes = result_payload.get("routes")
    if isinstance(root_routes, list):
        for route in root_routes:
            if isinstance(route, dict):
                prioritized.append(("root", route))

    return prioritized


def _resolve_municipality_reverse_source(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"azure", "azure_maps", "azure_maps_reverse", "azmaps"}:
        return "azure_maps_reverse"
    if raw in {"nominatim", "nominatim_reverse", "osm"}:
        return "nominatim_reverse"
    return "nominatim_reverse"


def _empty_forward_geocode_diag() -> Dict[str, Any]:
    return {
        "forward_geocode_query_mode": "none",
        "forward_geocode_selected_addresstype": "",
        "forward_geocode_selected_place_rank": None,
        "forward_geocode_candidate_count": 0,
        "forward_geocode_rejected_broad_admin_count": 0,
    }


def _sanitize_forward_geocode_query_mode(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in FORWARD_GEOCODE_QUERY_MODES:
        return raw
    return "none"


def _extract_forward_geocode_diag(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    diag = _empty_forward_geocode_diag()
    if not isinstance(row, dict):
        return diag

    diag["forward_geocode_query_mode"] = _sanitize_forward_geocode_query_mode(
        row.get("forward_geocode_query_mode")
    )

    selected_addresstype = str(
        row.get("forward_geocode_selected_addresstype", "")
        or row.get("addresstype", "")
    ).strip().lower()
    diag["forward_geocode_selected_addresstype"] = selected_addresstype

    selected_place_rank = _safe_int(
        row.get("forward_geocode_selected_place_rank"), -1
    )
    if selected_place_rank < 0:
        selected_place_rank = _safe_int(row.get("place_rank"), -1)
    diag["forward_geocode_selected_place_rank"] = (
        selected_place_rank if selected_place_rank >= 0 else None
    )
    diag["forward_geocode_candidate_count"] = max(
        0,
        _safe_int(row.get("forward_geocode_candidate_count"), 0),
    )
    diag["forward_geocode_rejected_broad_admin_count"] = max(
        0,
        _safe_int(row.get("forward_geocode_rejected_broad_admin_count"), 0),
    )
    return diag


def _iter_forward_geocode_name_variants(location_name: str) -> List[str]:
    base = " ".join(str(location_name or "").split())
    if not base:
        return []

    values: List[str] = []
    seen = set()

    def _append(value: Any) -> None:
        text = " ".join(str(value or "").split())
        token = _normalize_match_key(text)
        if not token or token in seen:
            return
        seen.add(token)
        values.append(text)

    _append(base)
    if "/" in base:
        _append(re.sub(r"\s*/\s*", " / ", base))
        for part in base.split("/"):
            _append(part)

    return values


def _search_nominatim_forward_candidates(
    params: Dict[str, Any], timeout_sec: int
) -> List[Dict[str, Any]]:
    url = (
        "https://nominatim.openstreetmap.org/search?"
        + urllib.parse.urlencode(params)
    )
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "softOptimizationVRP/municipality-forward-geocoder",
            "Accept": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=max(2, int(timeout_sec))) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        return []
    return [row for row in payload if isinstance(row, dict)]


def _nominatim_candidate_match_keys(candidate: Dict[str, Any]) -> set:
    keys = set()

    def _append(value: Any) -> None:
        token = _normalize_match_key(value)
        if token:
            keys.add(token)

    _append(candidate.get("name"))
    address = candidate.get("address")
    if isinstance(address, dict):
        for field in (
            "municipality",
            "city",
            "town",
            "village",
            "hamlet",
            "locality",
            "suburb",
            "city_district",
            "county",
            "province",
            "state",
        ):
            _append(address.get(field))
    return keys


def _search_nominatim_forward_municipality(
    location_name: str, timeout_sec: int, country_codes: str = ""
) -> Optional[Dict[str, Any]]:
    query = str(location_name or "").strip()
    if not query:
        return None

    normalized_country_codes = ",".join(
        token.strip().lower()
        for token in str(country_codes or "").split(",")
        if token.strip()
    )

    variants = _iter_forward_geocode_name_variants(query)
    target_match_keys = {
        _normalize_match_key(value)
        for value in variants
        if _normalize_match_key(value)
    }
    if not variants:
        variants = [query]
        target_match_keys.add(_normalize_match_key(query))

    query_specs: List[Tuple[str, str, Dict[str, Any]]] = []
    for variant in variants:
        params = {
            "city": variant,
            "format": "jsonv2",
            "addressdetails": "1",
            "limit": "5",
        }
        if normalized_country_codes:
            params["countrycodes"] = normalized_country_codes
        query_specs.append(("structured_city", variant, params))
    for variant in variants:
        params = {
            "q": variant,
            "format": "jsonv2",
            "addressdetails": "1",
            "limit": "8",
        }
        if normalized_country_codes:
            params["countrycodes"] = normalized_country_codes
        query_specs.append(("free_text", variant, params))

    candidates: List[Dict[str, Any]] = []
    seen_candidates = set()
    for query_mode, variant, params in query_specs:
        rows = _search_nominatim_forward_candidates(params=params, timeout_sec=timeout_sec)
        for row in rows:
            dedupe_key = (
                f"{row.get('osm_type', '')}:{row.get('osm_id', '')}:"
                f"{_normalize_match_key(row.get('display_name'))}"
            )
            if dedupe_key in seen_candidates:
                continue
            seen_candidates.add(dedupe_key)
            candidate = dict(row)
            candidate["forward_geocode_query_mode"] = query_mode
            candidate["forward_geocode_query_variant"] = variant
            candidates.append(candidate)

    diag = _empty_forward_geocode_diag()
    diag["forward_geocode_candidate_count"] = len(candidates)

    rejected_broad_admin_count = 0
    ranked_candidates: List[Tuple[Tuple[int, int, int, int, float], Dict[str, Any]]] = []
    for candidate in candidates:
        addresstype = str(candidate.get("addresstype", "")).strip().lower()
        if addresstype in NOMINATIM_BROAD_ADMIN_ADDRESS_TYPES:
            rejected_broad_admin_count += 1
            continue
        if addresstype not in NOMINATIM_LOCALITY_ADDRESS_TYPES:
            continue

        lat = _safe_float(candidate.get("lat"), None)
        lng = _safe_float(candidate.get("lon"), None)
        if lat is None or lng is None:
            continue

        place_rank = _safe_int(candidate.get("place_rank"), -1)
        query_mode = _sanitize_forward_geocode_query_mode(
            candidate.get("forward_geocode_query_mode")
        )
        is_locality_type = addresstype in NOMINATIM_LOCALITY_ADDRESS_TYPES
        candidate_match_keys = _nominatim_candidate_match_keys(candidate)
        exact_name_match = bool(candidate_match_keys.intersection(target_match_keys))
        importance = _safe_float(candidate.get("importance"), 0.0) or 0.0

        base_score = 0
        if query_mode == "structured_city":
            base_score -= 40
        if is_locality_type:
            base_score -= 90
        else:
            base_score += 20
        if exact_name_match:
            base_score -= 80
        else:
            base_score += 60
        if place_rank >= 0:
            if 14 <= place_rank <= 18:
                base_score -= 25
            elif place_rank <= 12:
                base_score += 50
            elif place_rank > 20:
                base_score += min(place_rank - 20, 20)

        rank_key = (
            base_score,
            0 if is_locality_type else 1,
            0 if exact_name_match else 1,
            abs(place_rank - 16) if place_rank >= 0 else 99,
            -float(importance),
        )
        ranked_candidates.append((rank_key, candidate))

    diag["forward_geocode_rejected_broad_admin_count"] = rejected_broad_admin_count

    if not ranked_candidates:
        return {
            "coordinate_source": "forward_geocode_nominatim",
            "resolution_reason": "forward_geocode_nominatim_no_locality_match",
            "coordinate_confidence": "unknown",
            "provider": "nominatim_reverse",
            "forward_geocode_disambiguation_version": FORWARD_GEOCODE_DISAMBIG_VERSION,
            **diag,
        }

    ranked_candidates.sort(key=lambda item: item[0])
    selected = ranked_candidates[0][1]
    lat = _safe_float(selected.get("lat"), None)
    lng = _safe_float(selected.get("lon"), None)
    selected_mode = _sanitize_forward_geocode_query_mode(
        selected.get("forward_geocode_query_mode")
    )
    selected_addresstype = str(selected.get("addresstype", "")).strip().lower()
    selected_place_rank = _safe_int(selected.get("place_rank"), -1)

    return {
        "lat": round(float(lat), 6),
        "lng": round(float(lng), 6),
        "coordinate_source": "forward_geocode_nominatim",
        "resolution_reason": "forward_geocode_nominatim_disambiguated",
        "coordinate_confidence": "high",
        "provider": "nominatim_reverse",
        "forward_geocode_disambiguation_version": FORWARD_GEOCODE_DISAMBIG_VERSION,
        "forward_geocode_query_mode": selected_mode,
        "forward_geocode_selected_addresstype": selected_addresstype,
        "forward_geocode_selected_place_rank": (
            selected_place_rank if selected_place_rank >= 0 else None
        ),
        "forward_geocode_candidate_count": diag["forward_geocode_candidate_count"],
        "forward_geocode_rejected_broad_admin_count": rejected_broad_admin_count,
    }


def _search_azure_maps_forward_municipality(
    location_name: str, timeout_sec: int, country_codes: str = ""
) -> Optional[Dict[str, Any]]:
    query = str(location_name or "").strip()
    if not query:
        return None

    azure_key = str(os.getenv("AZURE_MAPS_SUBSCRIPTION_KEY", "")).strip()
    if not azure_key:
        raise RuntimeError("AZURE_MAPS_SUBSCRIPTION_KEY is not configured.")

    endpoint_raw = str(
        os.getenv("AZURE_MAPS_REVERSE_ENDPOINT", "https://atlas.microsoft.com/reverseGeocode")
    ).strip()
    if endpoint_raw.endswith("/reverseGeocode"):
        endpoint = endpoint_raw[: -len("/reverseGeocode")] + "/search/address/json"
    else:
        endpoint = endpoint_raw
    endpoint = endpoint.rstrip("/")
    if not endpoint.endswith("/search/address/json"):
        endpoint = endpoint + "/search/address/json"
    endpoint_lower = endpoint.lower()

    # Azure Maps /search/address/json uses 1.0; newer 2025-* versions apply to /geocode.
    forward_api_version = str(
        os.getenv("AZURE_MAPS_FORWARD_API_VERSION", "")
    ).strip()
    if forward_api_version:
        api_version = forward_api_version
    else:
        configured_reverse_version = str(
            os.getenv("AZURE_MAPS_REVERSE_API_VERSION", "2025-01-01")
        ).strip()
        if "/search/address/json" in endpoint_lower:
            api_version = "1.0"
        else:
            api_version = configured_reverse_version or "1.0"

    params = {
        "api-version": api_version,
        "subscription-key": azure_key,
        "query": query,
        "limit": "1",
    }
    normalized_country_codes = ",".join(
        token.strip().upper()
        for token in str(country_codes or "").split(",")
        if token.strip()
    )
    if normalized_country_codes:
        params["countrySet"] = normalized_country_codes
    url = endpoint + "?" + urllib.parse.urlencode(params)
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "softOptimizationVRP/municipality-forward-geocoder",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=max(2, int(timeout_sec))) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not isinstance(payload, dict):
        return None
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        return None
    first = results[0]
    if not isinstance(first, dict):
        return None
    position = first.get("position")
    if not isinstance(position, dict):
        return None
    lat = _safe_float(position.get("lat"), None)
    lng = _safe_float(position.get("lon"), None)
    if lat is None or lng is None:
        return None
    return {
        "lat": round(float(lat), 6),
        "lng": round(float(lng), 6),
        "coordinate_source": "forward_geocode_azure_maps",
        "resolution_reason": "forward_geocode_azure_maps",
        "coordinate_confidence": "high",
        "provider": "azure_maps_reverse",
        "forward_geocode_disambiguation_version": FORWARD_GEOCODE_DISAMBIG_VERSION,
        "forward_geocode_query_mode": "none",
        "forward_geocode_selected_addresstype": "",
        "forward_geocode_selected_place_rank": None,
        "forward_geocode_candidate_count": len(results),
        "forward_geocode_rejected_broad_admin_count": 0,
    }


def _forward_geocode_municipality(
    location_name: str,
    reverse_source: str,
    timeout_sec: int,
    country_codes: str = "",
) -> Optional[Dict[str, Any]]:
    provider = _resolve_municipality_reverse_source(reverse_source)
    if provider == "azure_maps_reverse":
        return _search_azure_maps_forward_municipality(
            location_name=location_name,
            timeout_sec=timeout_sec,
            country_codes=country_codes,
        )
    return _search_nominatim_forward_municipality(
        location_name=location_name,
        timeout_sec=timeout_sec,
        country_codes=country_codes,
    )


def _infer_forward_geocode_country_codes(result_payload: Optional[dict]) -> str:
    counts: Dict[str, int] = {}

    def _add_country(value: Any) -> None:
        token = str(value or "").strip().upper()
        if len(token) != 2:
            return
        counts[token] = counts.get(token, 0) + 1

    if isinstance(result_payload, dict):
        semantic = result_payload.get("semantic_layer")
        if isinstance(semantic, dict):
            address_book = semantic.get("municipality_address_book")
            if isinstance(address_book, dict):
                for row in address_book.values():
                    if not isinstance(row, dict):
                        continue
                    address = row.get("address")
                    if isinstance(address, dict):
                        _add_country(address.get("country_code"))
                    _add_country(row.get("country_code"))

        for route in _iter_routes_from_payload(result_payload):
            links = route.get("stop_municipality_links")
            if not isinstance(links, list):
                continue
            for link in links:
                if not isinstance(link, dict):
                    continue
                _add_country(link.get("country_code"))

    if not counts:
        return ""
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return ranked[0][0]


def _coordinate_confidence_from_source(coordinate_source: Any) -> str:
    source = str(coordinate_source or "").strip().lower()
    if source in {
        "address_book",
        "segment_trace_municipality",
        "forward_geocode_nominatim",
        "forward_geocode_azure_maps",
    }:
        return "high"
    if source == "segment_trace_query_point":
        return "medium"
    if source.startswith("segment_midpoint"):
        return "low"
    return "unknown"


def _resolve_coordinate_confidence(coord: Optional[Dict[str, Any]]) -> str:
    if not isinstance(coord, dict):
        return "unknown"
    explicit = str(coord.get("coordinate_confidence", "")).strip().lower()
    if explicit in {"high", "medium", "low", "unknown"}:
        return explicit
    return _coordinate_confidence_from_source(coord.get("coordinate_source"))


def _resolve_municipality_coordinate(
    result_payload: dict, location_name: str
) -> Optional[Dict[str, Any]]:
    target_key = _normalize_text_key(location_name)
    if not target_key or not isinstance(result_payload, dict):
        return None

    candidates: List[Tuple[int, Dict[str, Any]]] = []

    def _append_candidate(
        *,
        score: int,
        lat: Any,
        lng: Any,
        coordinate_source: str,
        resolution_reason: str,
        coordinate_confidence: str,
    ) -> None:
        safe_lat = _safe_float(lat, None)
        safe_lng = _safe_float(lng, None)
        if safe_lat is None or safe_lng is None:
            return
        candidates.append(
            (
                int(score),
                {
                    "lat": round(float(safe_lat), 6),
                    "lng": round(float(safe_lng), 6),
                    "coordinate_source": coordinate_source,
                    "resolution_reason": resolution_reason,
                    "coordinate_confidence": (
                        str(coordinate_confidence or "unknown").strip().lower()
                        or "unknown"
                    ),
                },
            )
        )

    semantic = result_payload.get("semantic_layer")
    if isinstance(semantic, dict):
        address_book = semantic.get("municipality_address_book")
        if isinstance(address_book, dict):
            for row in address_book.values():
                if not isinstance(row, dict):
                    continue
                row_name = row.get("municipality_name")
                if _normalize_text_key(row_name) != target_key:
                    continue
                _append_candidate(
                    score=10,
                    lat=row.get("lat"),
                    lng=row.get("lng"),
                    coordinate_source="address_book",
                    resolution_reason="address_book_exact_name",
                    coordinate_confidence="high",
                )

    prioritized_routes = _iter_routes_by_priority(result_payload)
    for route_source, route in prioritized_routes:
        segments = route.get("segment_context")
        if not isinstance(segments, list):
            continue
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            municipality_trace = segment.get("municipality_trace")
            if isinstance(municipality_trace, list):
                for trace_item in municipality_trace:
                    if not isinstance(trace_item, dict):
                        continue
                    municipality_obj = (
                        trace_item.get("municipality")
                        if isinstance(trace_item.get("municipality"), dict)
                        else {}
                    )
                    candidate_name = (
                        municipality_obj.get("name")
                        or trace_item.get("municipality_name")
                        or trace_item.get("name")
                    )
                    if _normalize_text_key(candidate_name) != target_key:
                        continue
                    source_boost = 0 if route_source == "semantic" else 1
                    _append_candidate(
                        score=20 + source_boost,
                        lat=municipality_obj.get("lat"),
                        lng=municipality_obj.get("lng"),
                        coordinate_source="segment_trace_municipality",
                        resolution_reason=(
                            f"{route_source}_segment_trace_municipality_exact_name"
                        ),
                        coordinate_confidence="high",
                    )
                    query_point = (
                        trace_item.get("query_point")
                        if isinstance(trace_item.get("query_point"), dict)
                        else {}
                    )
                    _append_candidate(
                        score=30 + source_boost,
                        lat=query_point.get("lat"),
                        lng=query_point.get("lng"),
                        coordinate_source="segment_trace_query_point",
                        resolution_reason=(
                            f"{route_source}_segment_trace_query_point_exact_name"
                        ),
                        coordinate_confidence="medium",
                    )

            segment_names = (
                segment.get("municipality_names")
                if isinstance(segment.get("municipality_names"), list)
                else []
            )
            normalized_segment_names = {
                _normalize_text_key(name)
                for name in segment_names
                if _normalize_text_key(name)
            }
            # Midpoint is only trusted when a segment is unambiguous for one municipality.
            if target_key in normalized_segment_names and len(normalized_segment_names) == 1:
                midpoint = segment.get("midpoint")
                midpoint = midpoint if isinstance(midpoint, dict) else {}
                source_boost = 0 if route_source == "semantic" else 1
                _append_candidate(
                    score=40 + source_boost,
                    lat=midpoint.get("lat"),
                    lng=midpoint.get("lng"),
                    coordinate_source="segment_midpoint_municipality_name",
                    resolution_reason=(
                        f"{route_source}_segment_midpoint_municipality_names_exact_name"
                    ),
                    coordinate_confidence="low",
                )

        segment_by_index: Dict[int, dict] = {}
        for segment in segments:
            if not isinstance(segment, dict):
                continue
            s_index = segment.get("segment_index")
            if isinstance(s_index, int):
                segment_by_index[s_index] = segment

        municipality_llm = (
            route.get("municipality_llm")
            if isinstance(route.get("municipality_llm"), dict)
            else {}
        )
        additions = (
            municipality_llm.get("added_municipalities")
            if isinstance(municipality_llm.get("added_municipalities"), list)
            else []
        )
        for row in additions:
            if not isinstance(row, dict):
                continue
            if _normalize_text_key(row.get("name")) != target_key:
                continue
            llm_segment_index = row.get("segment_index")
            if not isinstance(llm_segment_index, int):
                continue
            segment = segment_by_index.get(llm_segment_index)
            midpoint = (
                segment.get("midpoint")
                if isinstance(segment, dict) and isinstance(segment.get("midpoint"), dict)
                else {}
            )
            source_boost = 0 if route_source == "semantic" else 1
            _append_candidate(
                score=50 + source_boost,
                lat=midpoint.get("lat"),
                lng=midpoint.get("lng"),
                coordinate_source="segment_midpoint_llm_added",
                resolution_reason=f"{route_source}_llm_added_segment_midpoint_exact_name",
                coordinate_confidence="low",
            )

    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    return candidates[0][1]


def _parse_scraping_cache_backend(value: Any) -> str:
    backend = str(value or DEFAULT_SCRAPING_CACHE_BACKEND).strip().lower()
    if backend in {"redis", "memory", "none"}:
        return backend
    return DEFAULT_SCRAPING_CACHE_BACKEND


def _resolve_scraping_stage_policy(payload: dict) -> str:
    raw = str(
        payload.get("scraping_stage", DEFAULT_SCRAPING_STAGE_POLICY)
        if isinstance(payload, dict)
        else DEFAULT_SCRAPING_STAGE_POLICY
    ).strip().lower()
    if raw in {"both", "all"}:
        return "both"
    if raw in {"solve_only", "solve-only", "solve"}:
        return "solve_only"
    if raw in {"enrich_only", "enrich-only", "enrich"}:
        return "enrich_only"
    return DEFAULT_SCRAPING_STAGE_POLICY


def _is_scraping_stage_allowed(source_stage: str, policy: str) -> bool:
    normalized_stage = str(source_stage or "").strip().lower()
    if policy == "both":
        return True
    if policy == "solve_only":
        return normalized_stage == "solve_vrp"
    return normalized_stage == "enrich_municipality"


def _resolve_bluesky_api_base(raw_value: Any) -> Tuple[str, Optional[str]]:
    raw = str(raw_value or DEFAULT_BLUESKY_API_BASE).strip()
    if not raw:
        raw = DEFAULT_BLUESKY_API_BASE
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urllib.parse.urlparse(raw)
    host = (parsed.netloc or "").strip().casefold()
    scheme = (parsed.scheme or "https").strip().lower() or "https"
    path = (parsed.path or "").rstrip("/")
    effective = raw.rstrip("/")

    if host == "public.api.bsky.app":
        effective = f"{scheme}://api.bsky.app{path}"
        return (
            effective,
            (
                "BLUESKY_API_BASE public.api.bsky.app is deprecated for searchPosts; "
                "using https://api.bsky.app instead."
            ),
        )

    return effective, None


def _bounded_append_error(errors: List[str], message: str, max_errors: int) -> None:
    if len(errors) >= max_errors:
        return
    errors.append(str(message))


def _build_scraping_cache_key(
    query_type: str,
    geohash_or_loc: str,
    keywords: str,
    minutes_back: int,
    limit: int,
    api_base: str,
    lang_filter: str = "",
    extra_namespace: str = "",
) -> str:
    keywords_hash = hashlib.sha1(
        str(keywords or "").strip().casefold().encode("utf-8")
    ).hexdigest()[:12]
    lang_hash = hashlib.sha1(
        str(lang_filter or "").strip().casefold().encode("utf-8")
    ).hexdigest()[:8]
    base_hash = hashlib.sha1(
        str(api_base or "").strip().casefold().encode("utf-8")
    ).hexdigest()[:8]
    extra_hash = hashlib.sha1(
        str(extra_namespace or "").strip().casefold().encode("utf-8")
    ).hexdigest()[:8]
    loc_token = str(geohash_or_loc or "unknown").strip().lower() or "unknown"
    key = (
        f"scrape:{SCRAPING_CACHE_KEY_VERSION}:{query_type}:{base_hash}:{loc_token}:"
        f"{keywords_hash}:{lang_hash}:{int(minutes_back)}:{int(limit)}"
    )
    if str(extra_namespace or "").strip():
        key = f"{key}:{extra_hash}"
    return key


def _encode_geohash(lat: Optional[float], lng: Optional[float], precision: int) -> str:
    if lat is None or lng is None:
        return ""
    try:
        import pygeohash as pgh  # type: ignore
    except Exception:
        return ""
    try:
        return str(pgh.encode(float(lat), float(lng), precision=max(1, int(precision))))
    except Exception:
        return ""


def _create_scraping_cache(
    errors: List[str], max_errors: int
) -> Tuple[Optional[ScrapingCache], str]:
    backend = _parse_scraping_cache_backend(os.getenv("SCRAPING_CACHE_BACKEND", "memory"))
    redis_url = str(os.getenv("SCRAPING_CACHE_REDIS_URL", "")).strip()

    if backend == "none":
        return None, "none"

    if backend == "redis":
        if not redis_url:
            _bounded_append_error(
                errors,
                "Redis backend configured without SCRAPING_CACHE_REDIS_URL; using memory fallback.",
                max_errors=max_errors,
            )
            return _SCRAPING_MEMORY_CACHE, "memory"
        try:
            return RedisScrapingCache(redis_url=redis_url), "redis"
        except Exception as exc:  # noqa: BLE001 - cache failures must not break solve flow
            _bounded_append_error(
                errors,
                f"Redis cache unavailable; using memory fallback. Reason: {exc}",
                max_errors=max_errors,
            )
            return _SCRAPING_MEMORY_CACHE, "memory"

    return _SCRAPING_MEMORY_CACHE, "memory"


def _collect_route_municipality_names(result_payload: dict) -> List[str]:
    names: List[Any] = []

    def collect_from_routes(routes: List[dict]) -> None:
        for route in routes:
            links = route.get("stop_municipality_links")
            if isinstance(links, list):
                for link in links:
                    if isinstance(link, dict):
                        names.append(link.get("municipality_name"))

            segments = route.get("segment_context")
            if isinstance(segments, list):
                for segment in segments:
                    if not isinstance(segment, dict):
                        continue
                    municipality_names = segment.get("municipality_names")
                    if isinstance(municipality_names, list):
                        names.extend(municipality_names)

                    municipality_trace = segment.get("municipality_trace")
                    if isinstance(municipality_trace, list):
                        for trace_item in municipality_trace:
                            if not isinstance(trace_item, dict):
                                continue
                            names.append(trace_item.get("municipality_name"))
                            municipality_obj = trace_item.get("municipality")
                            if isinstance(municipality_obj, dict):
                                names.append(municipality_obj.get("name"))

            municipality_vector = route.get("municipality_vector")
            if isinstance(municipality_vector, list):
                names.extend(municipality_vector)

    collect_from_routes(_iter_routes_from_payload(result_payload))

    semantic = result_payload.get("semantic_layer") if isinstance(result_payload, dict) else None

    if isinstance(semantic, dict):
        address_book = semantic.get("municipality_address_book")
        if isinstance(address_book, dict):
            for row in address_book.values():
                if isinstance(row, dict):
                    names.append(row.get("municipality_name"))

    return _dedupe_text_values(names)


def _write_scraping_temp_file(report_text: str) -> Dict[str, str]:
    base_dir = os.path.dirname(__file__)
    latest_path = os.path.join(base_dir, SCRAPING_TEMP_FILENAME)
    snapshot_name = (
        "social_scraping_"
        + datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        + ".txt"
    )
    snapshot_path = os.path.join(base_dir, snapshot_name)

    latest_error = ""
    snapshot_error = ""

    try:
        with open(latest_path, "w", encoding="utf-8") as handle:
            handle.write(report_text)
    except Exception as exc:  # noqa: BLE001 - diagnostics should not fail solve flow
        latest_error = str(exc)

    try:
        with open(snapshot_path, "w", encoding="utf-8") as handle:
            handle.write(report_text)
    except Exception as exc:  # noqa: BLE001 - diagnostics should not fail solve flow
        snapshot_error = str(exc)

    return {
        "path": latest_path,
        "error": latest_error,
        "snapshot_path": snapshot_path,
        "snapshot_error": snapshot_error,
    }


def _build_scraping_report_text(meta: dict) -> str:
    lines = []
    lines.append("Social scraping report (Bluesky)")
    generated_at_utc = str(meta.get("generated_at_utc", "")).strip()
    if not generated_at_utc:
        generated_at_utc = datetime.now(tz=timezone.utc).isoformat()
    lines.append(f"generated_at_utc: {generated_at_utc}")
    source_stage = str(meta.get("source_stage", "")).strip()
    if source_stage:
        lines.append(f"source_stage: {source_stage}")
    lines.append(f"stage_policy: {meta.get('stage_policy', DEFAULT_SCRAPING_STAGE_POLICY)}")
    lines.append(f"stage_allowed: {meta.get('stage_allowed', False)}")
    stage_skip_reason = str(meta.get("stage_skip_reason", "")).strip()
    if stage_skip_reason:
        lines.append(f"stage_skip_reason: {stage_skip_reason}")
    lines.append(f"status: {meta.get('status', 'unknown')}")
    lines.append(f"message: {meta.get('message', '')}")
    lines.append(f"keywords: {meta.get('keywords', '')}")
    lines.append(f"scraping_lang: {meta.get('scraping_lang', '')}")
    lines.append(f"locations_requested: {meta.get('locations_requested', 0)}")

    requested_keys = meta.get("locations_requested_keys", [])
    requested_names = meta.get("locations_requested_names", [])
    if isinstance(requested_keys, list) and requested_keys:
        lines.append(f"locations_requested_keys: {', '.join(str(x) for x in requested_keys)}")
    if isinstance(requested_names, list) and requested_names:
        lines.append(
            f"locations_requested_names: {', '.join(str(x) for x in requested_names)}"
        )

    lines.append(f"locations_with_results: {meta.get('locations_with_results', 0)}")
    lines.append(f"tweets_total: {meta.get('tweets_total', 0)}")
    lines.append(f"per_location_limit: {meta.get('per_location_limit', 0)}")
    lines.append(f"risk_posts_total: {meta.get('risk_posts_total', 0)}")
    lines.append(f"fallback_posts_total: {meta.get('fallback_posts_total', 0)}")
    lines.append(f"locations_with_risk: {meta.get('locations_with_risk', 0)}")
    lines.append(f"locations_with_fallback: {meta.get('locations_with_fallback', 0)}")
    lines.append(f"fallback_mode_used: {meta.get('fallback_mode_used', False)}")
    lines.append(f"cache_backend: {meta.get('cache_backend', 'none')}")
    lines.append(
        f"bluesky_api_base_effective: {meta.get('bluesky_api_base_effective', '')}"
    )
    lines.append(f"cache_key_version: {meta.get('cache_key_version', SCRAPING_CACHE_KEY_VERSION)}")
    lines.append(f"cache_hits: {meta.get('cache_hits', 0)}")
    lines.append(f"cache_misses: {meta.get('cache_misses', 0)}")
    lines.append(f"cache_writes: {meta.get('cache_writes', 0)}")
    lines.append(f"network_queries_attempted: {meta.get('network_queries_attempted', 0)}")
    lines.append(f"network_queries_succeeded: {meta.get('network_queries_succeeded', 0)}")
    lines.append(f"forward_geocode_attempts: {meta.get('forward_geocode_attempts', 0)}")
    lines.append(f"forward_geocode_successes: {meta.get('forward_geocode_successes', 0)}")
    lines.append(f"forward_geocode_failures: {meta.get('forward_geocode_failures', 0)}")
    lines.append(
        "forward_geocode_disambiguation_version: "
        f"{meta.get('forward_geocode_disambiguation_version', FORWARD_GEOCODE_DISAMBIG_VERSION)}"
    )
    lines.append(
        f"forward_geocode_bad_admin_rejections: {meta.get('forward_geocode_bad_admin_rejections', 0)}"
    )
    query_modes_used = meta.get("forward_geocode_query_modes_used", [])
    if isinstance(query_modes_used, list) and query_modes_used:
        lines.append(
            "forward_geocode_query_modes_used: "
            + ", ".join(str(mode) for mode in query_modes_used)
        )
    else:
        lines.append("forward_geocode_query_modes_used: none")
    lines.append(
        f"forward_geocode_country_codes: {meta.get('forward_geocode_country_codes', '')}"
    )
    lines.append(
        f"low_confidence_resolutions_blocked: {meta.get('low_confidence_resolutions_blocked', 0)}"
    )
    lines.append(
        f"cache_miss_debug_limit: {meta.get('cache_miss_debug_limit', DEFAULT_SCRAPING_CACHE_MISS_DEBUG_LIMIT)}"
    )
    lines.append(
        f"cache_miss_details_count: {meta.get('cache_miss_details_count', 0)}"
    )
    lines.append(
        f"cache_miss_details_truncated: {meta.get('cache_miss_details_truncated', 0)}"
    )
    network_errors = meta.get("network_errors", [])
    if isinstance(network_errors, list) and network_errors:
        lines.append("network_errors:")
        for err in network_errors:
            lines.append(f"- {err}")
    else:
        lines.append("network_errors: none")

    miss_by_reason = meta.get("cache_miss_by_reason", {})
    if isinstance(miss_by_reason, dict) and miss_by_reason:
        lines.append("cache_miss_by_reason:")
        for reason, count in miss_by_reason.items():
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("cache_miss_by_reason: none")

    miss_by_query = meta.get("cache_miss_by_query_type", {})
    if isinstance(miss_by_query, dict) and miss_by_query:
        lines.append("cache_miss_by_query_type:")
        for query_type, count in miss_by_query.items():
            lines.append(f"- {query_type}: {count}")
    else:
        lines.append("cache_miss_by_query_type: none")

    cache_miss_details = meta.get("cache_miss_details", [])
    if isinstance(cache_miss_details, list) and cache_miss_details:
        lines.append("cache_miss_details:")
        for row in cache_miss_details:
            if not isinstance(row, dict):
                continue
            location_name = str(row.get("location_name", "")).strip()
            query_type = str(row.get("query_type", "")).strip()
            reason = str(row.get("reason", "")).strip()
            cache_key = str(row.get("cache_key", "")).strip()
            lines.append(
                f"- {location_name} [{query_type}] reason={reason} cache_key={cache_key}"
            )
        truncated = _safe_int(meta.get("cache_miss_details_truncated"), 0)
        if truncated > 0:
            lines.append(f"- ... {truncated} additional miss rows were truncated")
    else:
        lines.append("cache_miss_details: none")

    locations_without_icon = meta.get("locations_with_posts_but_no_icon", [])
    if isinstance(locations_without_icon, list) and locations_without_icon:
        lines.append("locations_with_posts_but_no_icon:")
        for name in locations_without_icon:
            lines.append(f"- {name}")
    else:
        lines.append("locations_with_posts_but_no_icon: none")

    location_resolution = meta.get("location_resolution", [])
    if isinstance(location_resolution, list) and location_resolution:
        lines.append("location_resolution:")
        for row in location_resolution:
            if not isinstance(row, dict):
                continue
            location_name = str(row.get("location_name", "")).strip()
            posts_count = _safe_int(row.get("posts_count"), 0)
            resolved = bool(row.get("resolved"))
            lat = row.get("lat")
            lng = row.get("lng")
            source = str(row.get("coordinate_source", "")).strip()
            reason = str(row.get("resolution_reason", "")).strip()
            confidence = str(row.get("coordinate_confidence", "")).strip()
            override_attempted = bool(row.get("override_attempted"))
            override_result = str(row.get("override_result", "")).strip()
            forward_query_mode = str(
                row.get("forward_geocode_query_mode", "none")
            ).strip()
            forward_selected_addresstype = str(
                row.get("forward_geocode_selected_addresstype", "")
            ).strip()
            forward_selected_place_rank = row.get(
                "forward_geocode_selected_place_rank"
            )
            forward_candidate_count = _safe_int(
                row.get("forward_geocode_candidate_count"), 0
            )
            forward_rejected_admin = _safe_int(
                row.get("forward_geocode_rejected_broad_admin_count"), 0
            )
            lines.append(
                f"- {location_name}: posts={posts_count} resolved={resolved} "
                f"lat={lat} lng={lng} source={source} reason={reason} "
                f"confidence={confidence} override_attempted={override_attempted} "
                f"override_result={override_result} "
                f"forward_mode={forward_query_mode} "
                f"forward_addresstype={forward_selected_addresstype} "
                f"forward_place_rank={forward_selected_place_rank} "
                f"forward_candidates={forward_candidate_count} "
                f"forward_rejected_admin={forward_rejected_admin}"
            )
    else:
        lines.append("location_resolution: none")
    lines.append("")
    lines.append("results_by_location:")

    by_location = meta.get("results_by_location", {})
    if isinstance(by_location, dict) and by_location:
        for location_name, count in by_location.items():
            lines.append(f"- {location_name}: {count}")
    else:
        lines.append("- none")

    lines.append("")
    lines.append("preview_rows:")
    preview_rows = meta.get("preview_rows", [])
    if isinstance(preview_rows, list) and preview_rows:
        for row in preview_rows:
            if not isinstance(row, dict):
                continue
            created_at = row.get("created_at", "")
            location_name = row.get("location_name", "")
            username = row.get("username", "")
            classification = str(row.get("classification", "")).strip()
            text = str(row.get("text", "")).strip()
            tweet_url = str(row.get("tweet_url", "")).strip()
            if len(text) > 240:
                text = text[:237] + "..."
            prefix = f"[{classification}] " if classification else ""
            lines.append(
                f"- [{created_at}] {prefix}{location_name} @{username}: {text}"
            )
            if tweet_url:
                lines.append(f"  {tweet_url}")
    else:
        lines.append("- none")

    lines.append("")
    return "\n".join(lines)


def _run_social_scraping(
    payload: dict,
    municipality_names: List[str],
    source_stage: str = "solve_vrp",
    result_payload: Optional[dict] = None,
) -> dict:
    enabled = _as_bool(payload.get("scraping_enabled"), False)
    generated_at_utc = datetime.now(tz=timezone.utc).isoformat()
    stage_policy = _resolve_scraping_stage_policy(payload)
    stage_allowed = _is_scraping_stage_allowed(source_stage=source_stage, policy=stage_policy)
    force_refresh = _as_bool(payload.get("scraping_force_refresh"), False)

    requested_locations_raw = payload.get("scraping_locations")
    requested_locations_explicit = (
        _dedupe_text_values(requested_locations_raw)
        if isinstance(requested_locations_raw, list)
        else []
    )
    municipality_locations = _dedupe_text_values(municipality_names or [])
    requested_location_names = (
        requested_locations_explicit
        if requested_locations_explicit
        else municipality_locations
    )
    scraping_lang = str(
        payload.get(
            "scraping_lang",
            os.getenv("SCRAPING_LANG", DEFAULT_SCRAPING_LANG),
        )
    ).strip().lower().replace("_", "-")
    if "," in scraping_lang:
        scraping_lang = scraping_lang.split(",", 1)[0].strip()
    scraping_lang = re.sub(r"[^a-z0-9-]", "", scraping_lang)

    max_cache_errors = max(
        1,
        _safe_int(
            os.getenv("SCRAPING_CACHE_MAX_ERRORS"), DEFAULT_SCRAPING_CACHE_MAX_ERRORS
        ),
    )
    forward_geocode_enabled = _as_bool(
        payload.get("scraping_forward_geocode_enabled"),
        _as_bool(
            os.getenv("SCRAPING_FORWARD_GEOCODE_ENABLED"),
            DEFAULT_SCRAPING_FORWARD_GEOCODE_ENABLED,
        ),
    )
    forward_geocode_timeout_sec = max(
        2,
        _safe_int(
            payload.get("scraping_forward_geocode_timeout_sec"),
            _safe_int(
                os.getenv("SCRAPING_FORWARD_GEOCODE_TIMEOUT_SEC"),
                DEFAULT_SCRAPING_FORWARD_GEOCODE_TIMEOUT_SEC,
            ),
        ),
    )
    forward_geocode_limit = max(
        0,
        min(
            _safe_int(
                payload.get("scraping_forward_geocode_limit"),
                _safe_int(
                    os.getenv("SCRAPING_FORWARD_GEOCODE_LIMIT"),
                    DEFAULT_SCRAPING_FORWARD_GEOCODE_LIMIT,
                ),
            ),
            200,
        ),
    )
    forward_geocode_country_codes = str(
        payload.get(
            "scraping_forward_geocode_country_codes",
            os.getenv("SCRAPING_FORWARD_GEOCODE_COUNTRY_CODES", ""),
        )
    ).strip()
    if not forward_geocode_country_codes:
        forward_geocode_country_codes = _infer_forward_geocode_country_codes(result_payload)
    cache_miss_debug_limit = max(
        0,
        min(
            _safe_int(
                payload.get("scraping_cache_miss_debug_limit"),
                _safe_int(
                    os.getenv("SCRAPING_CACHE_MISS_DEBUG_LIMIT"),
                    DEFAULT_SCRAPING_CACHE_MISS_DEBUG_LIMIT,
                ),
            ),
            500,
        ),
    )

    cache_errors: List[str] = []
    network_errors: List[str] = []
    cache_miss_details: List[dict] = []
    cache_miss_by_reason: Dict[str, int] = {}
    cache_miss_by_query_type: Dict[str, int] = {}
    cache_miss_details_truncated = 0
    cache_miss_total = 0
    cache_backend: str = "none"
    location_resolution: List[dict] = []
    locations_with_posts_but_no_icon: List[str] = []
    forward_geocode_attempts = 0
    forward_geocode_successes = 0
    forward_geocode_failures = 0
    forward_geocode_bad_admin_rejections = 0
    forward_geocode_query_modes_used = set()
    low_confidence_resolutions_blocked = 0

    meta = {
        "enabled": enabled,
        "generated_at_utc": generated_at_utc,
        "source_stage": str(source_stage or "solve_vrp"),
        "stage_policy": stage_policy,
        "stage_allowed": stage_allowed,
        "stage_skip_reason": "",
        "status": "disabled",
        "message": "Social scraping disabled.",
        "keywords": "",
        "scraping_lang": scraping_lang,
        "locations_requested": len(requested_location_names),
        "locations_requested_keys": list(requested_location_names),
        "locations_requested_names": list(requested_location_names),
        "locations_with_results": 0,
        "tweets_total": 0,
        "per_location_limit": 0,
        "results_by_location": {},
        "preview_rows": [],
        "cache_backend": cache_backend,
        "bluesky_api_base_effective": "",
        "cache_key_version": SCRAPING_CACHE_KEY_VERSION,
        "cache_hits": 0,
        "cache_misses": 0,
        "cache_writes": 0,
        "cache_errors": cache_errors,
        "cache_miss_debug_limit": cache_miss_debug_limit,
        "cache_miss_details": cache_miss_details,
        "cache_miss_details_count": 0,
        "cache_miss_details_truncated": 0,
        "cache_miss_by_reason": cache_miss_by_reason,
        "cache_miss_by_query_type": cache_miss_by_query_type,
        "location_resolution": location_resolution,
        "locations_with_posts_but_no_icon": locations_with_posts_but_no_icon,
        "forward_geocode_enabled": forward_geocode_enabled,
        "forward_geocode_timeout_sec": forward_geocode_timeout_sec,
        "forward_geocode_limit": forward_geocode_limit,
        "forward_geocode_country_codes": forward_geocode_country_codes,
        "forward_geocode_disambiguation_version": FORWARD_GEOCODE_DISAMBIG_VERSION,
        "forward_geocode_attempts": 0,
        "forward_geocode_successes": 0,
        "forward_geocode_failures": 0,
        "forward_geocode_bad_admin_rejections": 0,
        "forward_geocode_query_modes_used": [],
        "low_confidence_resolutions_blocked": 0,
        "network_queries_attempted": 0,
        "network_queries_succeeded": 0,
        "network_errors": network_errors,
        "risk_posts_total": 0,
        "fallback_posts_total": 0,
        "locations_with_risk": 0,
        "locations_with_fallback": 0,
        "fallback_mode_used": False,
        "municipality_points": [],
        "unresolved_locations_for_icons": [],
        "scraping_force_refresh": force_refresh,
    }

    raw_bluesky_api_base = os.getenv("BLUESKY_API_BASE", DEFAULT_BLUESKY_API_BASE)
    bluesky_api_base, api_base_warning = _resolve_bluesky_api_base(raw_bluesky_api_base)
    meta["bluesky_api_base_effective"] = bluesky_api_base
    if api_base_warning:
        _bounded_append_error(network_errors, api_base_warning, max_errors=max_cache_errors)

    if not enabled:
        return meta

    if not stage_allowed:
        stage_skip_reason = (
            f"source_stage={source_stage} is blocked by scraping_stage={stage_policy}."
        )
        meta.update(
            status="skipped_stage",
            stage_skip_reason=stage_skip_reason,
            message=(
                "Scraping skipped because the current stage is blocked by "
                "scraping_stage policy."
            ),
        )
        file_write = _write_scraping_temp_file(_build_scraping_report_text(meta))
        meta["output_file"] = file_write["path"]
        if file_write["error"]:
            meta["output_file_error"] = file_write["error"]
        meta["output_file_snapshot"] = file_write["snapshot_path"]
        if file_write["snapshot_error"]:
            meta["output_file_snapshot_error"] = file_write["snapshot_error"]
        return meta

    cache: Optional[ScrapingCache] = None
    cache, cache_backend = _create_scraping_cache(
        errors=cache_errors, max_errors=max_cache_errors
    )
    meta["cache_backend"] = cache_backend

    if not requested_location_names:
        meta.update(
            status="skipped",
            message=(
                "No municipality locations were found in routes. "
                "Run Municipality Trace first or pass scraping_locations."
            ),
        )
        file_write = _write_scraping_temp_file(_build_scraping_report_text(meta))
        meta["output_file"] = file_write["path"]
        if file_write["error"]:
            meta["output_file_error"] = file_write["error"]
        meta["output_file_snapshot"] = file_write["snapshot_path"]
        if file_write["snapshot_error"]:
            meta["output_file_snapshot_error"] = file_write["snapshot_error"]
        return meta

    try:
        from solve_vrp.social_geo_scraper import SocialGeoScraper
    except Exception as exc:  # noqa: BLE001 - response should still include diagnostics
        meta.update(
            status="unavailable",
            message=f"Social scraper dependencies unavailable: {exc}",
        )
        file_write = _write_scraping_temp_file(_build_scraping_report_text(meta))
        meta["output_file"] = file_write["path"]
        if file_write["error"]:
            meta["output_file_error"] = file_write["error"]
        meta["output_file_snapshot"] = file_write["snapshot_path"]
        if file_write["snapshot_error"]:
            meta["output_file_snapshot_error"] = file_write["snapshot_error"]
        return meta

    keywords = str(payload.get("scraping_keywords", DEFAULT_SCRAPING_KEYWORDS)).strip()
    if not keywords:
        keywords = DEFAULT_SCRAPING_KEYWORDS
    per_location_limit = max(
        1,
        min(
            _safe_int(
                payload.get("scraping_per_location_limit"),
                DEFAULT_SCRAPING_PER_LOCATION_LIMIT,
            ),
            100,
        ),
    )
    fallback_max_posts = max(
        1,
        min(
            _safe_int(
                payload.get("scraping_fallback_max_posts"),
                _safe_int(
                    os.getenv("SCRAPING_FALLBACK_MAX_POSTS"),
                    DEFAULT_SCRAPING_FALLBACK_MAX_POSTS,
                ),
            ),
            3,
        ),
    )
    radius_km = max(
        1, _safe_int(payload.get("scraping_radius_km"), DEFAULT_SCRAPING_RADIUS_KM)
    )
    minutes_back = max(
        1, _safe_int(payload.get("scraping_minutes_back"), DEFAULT_SCRAPING_MINUTES_BACK)
    )
    pause_seconds = max(
        0.0, _safe_float(payload.get("scraping_pause_seconds"), 0.0) or 0.0
    )
    preview_limit = max(
        1,
        min(
            _safe_int(payload.get("scraping_preview_limit"), DEFAULT_SCRAPING_PREVIEW_LIMIT),
            200,
        ),
    )
    cache_ttl_sec = max(
        10,
        _safe_int(os.getenv("SCRAPING_CACHE_TTL_SEC"), DEFAULT_SCRAPING_CACHE_TTL_SEC),
    )
    geohash_precision = max(
        1,
        min(
            _safe_int(
                os.getenv("SCRAPING_CACHE_GEOHASH_PRECISION"),
                DEFAULT_SCRAPING_CACHE_GEOHASH_PRECISION,
            ),
            12,
        ),
    )

    cache_hits = 0
    cache_misses = 0
    cache_writes = 0
    active_cache = cache
    active_cache_backend = cache_backend
    network_queries_attempted = 0
    network_queries_succeeded = 0

    def _degrade_cache_if_needed(reason: str) -> None:
        nonlocal active_cache, active_cache_backend
        if active_cache_backend != "redis":
            return
        _bounded_append_error(
            cache_errors,
            f"Redis cache error; using memory fallback. Reason: {reason}",
            max_errors=max_cache_errors,
        )
        active_cache = _SCRAPING_MEMORY_CACHE
        active_cache_backend = "memory"

    def _is_valid_cached_row(row: dict) -> bool:
        if not isinstance(row, dict):
            return False
        source_platform = str(row.get("source_platform", "")).strip().lower()
        if source_platform and source_platform != "bluesky":
            return False
        tweet_url = str(row.get("tweet_url", "")).strip()
        if tweet_url and not tweet_url.startswith("https://bsky.app/profile/"):
            return False
        post_uri = str(row.get("post_uri", "")).strip()
        if post_uri and not post_uri.startswith("at://"):
            return False
        post_langs = row.get("post_langs")
        if post_langs is not None:
            if not isinstance(post_langs, list):
                return False
            for item in post_langs:
                if not isinstance(item, str):
                    return False
        return True

    def _record_cache_miss(
        *,
        location_name: str,
        query_type: str,
        cache_key: str,
        reason: str,
    ) -> None:
        nonlocal cache_miss_details_truncated, cache_miss_total
        reason_token = str(reason or "unknown").strip() or "unknown"
        query_token = str(query_type or "unknown").strip() or "unknown"
        cache_miss_total += 1
        cache_miss_by_reason[reason_token] = cache_miss_by_reason.get(reason_token, 0) + 1
        cache_miss_by_query_type[query_token] = (
            cache_miss_by_query_type.get(query_token, 0) + 1
        )
        if len(cache_miss_details) >= cache_miss_debug_limit:
            cache_miss_details_truncated += 1
            return
        cache_miss_details.append(
            {
                "location_name": str(location_name or ""),
                "query_type": query_token,
                "reason": reason_token,
                "cache_key": str(cache_key or ""),
            }
        )

    def _extract_cached_rows(
        payload_row: dict, expected_query_type: str, max_rows: int
    ) -> Tuple[Optional[List[dict]], str]:
        if not isinstance(payload_row, dict):
            return None, "cache_payload_not_dict"
        if str(payload_row.get("version", "")).strip() != SCRAPING_CACHE_KEY_VERSION:
            return None, "version_mismatch"
        if str(payload_row.get("query_type", "")).strip().lower() != expected_query_type:
            return None, "query_type_mismatch"
        rows = payload_row.get("rows")
        if not isinstance(rows, list):
            return None, "rows_missing_or_invalid"
        normalized_rows: List[dict] = []
        for row in rows:
            if not isinstance(row, dict):
                return None, "row_not_dict"
            if not _is_valid_cached_row(row):
                return None, "row_schema_invalid"
            normalized_rows.append(row)
        return normalized_rows[:max_rows], ""

    def _cache_get_rows(
        key: str,
        expected_query_type: str,
        max_rows: int,
        location_name: str,
    ) -> Optional[List[dict]]:
        nonlocal cache_hits, cache_misses, active_cache
        if force_refresh:
            _record_cache_miss(
                location_name=location_name,
                query_type=expected_query_type,
                cache_key=key,
                reason="force_refresh_bypass",
            )
            return None
        if active_cache is None:
            _record_cache_miss(
                location_name=location_name,
                query_type=expected_query_type,
                cache_key=key,
                reason="cache_backend_none",
            )
            return None
        try:
            payload_row = active_cache.get(key)
        except Exception as exc:  # noqa: BLE001
            _degrade_cache_if_needed(str(exc))
            if active_cache is not None and active_cache_backend == "memory":
                try:
                    payload_row = active_cache.get(key)
                except Exception as fallback_exc:  # noqa: BLE001
                    _bounded_append_error(
                        cache_errors,
                        f"Memory cache get failed: {fallback_exc}",
                        max_errors=max_cache_errors,
                    )
                    _record_cache_miss(
                        location_name=location_name,
                        query_type=expected_query_type,
                        cache_key=key,
                        reason="cache_get_error",
                    )
                    return None
            else:
                _record_cache_miss(
                    location_name=location_name,
                    query_type=expected_query_type,
                    cache_key=key,
                    reason="cache_get_error",
                )
                return None

        if not isinstance(payload_row, dict):
            cache_misses += 1
            _record_cache_miss(
                location_name=location_name,
                query_type=expected_query_type,
                cache_key=key,
                reason="not_found",
            )
            return None

        cached_rows, miss_reason = _extract_cached_rows(
            payload_row=payload_row,
            expected_query_type=expected_query_type,
            max_rows=max_rows,
        )
        if cached_rows is None:
            cache_misses += 1
            _record_cache_miss(
                location_name=location_name,
                query_type=expected_query_type,
                cache_key=key,
                reason=miss_reason or "cache_payload_invalid",
            )
            return None
        cache_hits += 1
        return cached_rows

    def _cache_set(key: str, value: dict) -> None:
        nonlocal cache_writes, active_cache
        if active_cache is None:
            return
        try:
            active_cache.set(key, value, cache_ttl_sec)
            cache_writes += 1
            return
        except Exception as exc:  # noqa: BLE001
            _degrade_cache_if_needed(str(exc))
            if active_cache is None or active_cache_backend != "memory":
                return
            try:
                active_cache.set(key, value, cache_ttl_sec)
                cache_writes += 1
            except Exception as fallback_exc:  # noqa: BLE001
                _bounded_append_error(
                    cache_errors,
                    f"Memory cache set failed: {fallback_exc}",
                    max_errors=max_cache_errors,
                )

    def _normalize_preview_row(
        row: dict,
        location_name: str,
        classification: str,
        is_fallback: bool,
        location_coord: Optional[Dict[str, Any]],
    ) -> dict:
        text = " ".join(str(_value_to_serializable(row.get("text", ""))).split())
        location_lat = (
            _safe_float(location_coord.get("lat"), None)
            if isinstance(location_coord, dict)
            else None
        )
        location_lng = (
            _safe_float(location_coord.get("lng"), None)
            if isinstance(location_coord, dict)
            else None
        )
        return {
            "created_at": _value_to_serializable(row.get("created_at")),
            "location_key": location_name,
            "location_name": location_name,
            "username": _value_to_serializable(row.get("username")),
            "tweet_id": _value_to_serializable(row.get("tweet_id")),
            "text": text,
            "tweet_url": _value_to_serializable(row.get("tweet_url")),
            "post_uri": _value_to_serializable(row.get("post_uri")),
            "like_count": _value_to_serializable(row.get("like_count")),
            "retweet_count": _value_to_serializable(row.get("retweet_count")),
            "reply_count": _value_to_serializable(row.get("reply_count")),
            "source_platform": _value_to_serializable(row.get("source_platform")),
            "classification": classification,
            "is_fallback": is_fallback,
            "location_lat": round(float(location_lat), 6) if location_lat is not None else None,
            "location_lng": round(float(location_lng), 6) if location_lng is not None else None,
        }

    def _append_post_url_warning(row: dict, location_name: str, classification: str) -> None:
        tweet_url = str(row.get("tweet_url", "")).strip()
        post_uri = str(row.get("post_uri", "")).strip()
        if tweet_url or not post_uri:
            return
        _bounded_append_error(
            network_errors,
            (
                f"{location_name} ({classification}): could not build a bsky.app URL "
                f"for post_uri={post_uri}"
            ),
            max_errors=max_cache_errors,
        )

    try:
        bluesky_identifier = str(os.getenv("BLUESKY_IDENTIFIER", "")).strip()
        bluesky_app_password = str(os.getenv("BLUESKY_APP_PASSWORD", "")).strip()
        timeout_sec = max(3, _safe_int(os.getenv("BLUESKY_TIMEOUT_SEC"), 10))

        scraper = SocialGeoScraper(
            bluesky_api_base=bluesky_api_base,
            bluesky_identifier=bluesky_identifier,
            bluesky_app_password=bluesky_app_password,
            timeout_sec=timeout_sec,
        )
        by_location: Dict[str, int] = {}
        preview_rows: List[dict] = []
        skipped_locations: List[str] = []
        municipality_points: List[dict] = []
        unresolved_locations_for_icons: List[str] = []
        unresolved_seen = set()
        tweets_total = 0
        risk_posts_total = 0
        fallback_posts_total = 0
        locations_with_results = 0
        locations_with_risk = 0
        locations_with_fallback = 0
        fallback_mode_used = False

        for index, location_name in enumerate(requested_location_names, start=1):
            location_coord = (
                _resolve_municipality_coordinate(result_payload, location_name)
                if isinstance(result_payload, dict)
                else None
            )
            geohash_token = _encode_geohash(
                _safe_float(location_coord.get("lat"), None)
                if isinstance(location_coord, dict)
                else None,
                _safe_float(location_coord.get("lng"), None)
                if isinstance(location_coord, dict)
                else None,
                precision=geohash_precision,
            )
            location_cache_token = geohash_token or _location_slug(location_name) or "unknown"

            risk_key = _build_scraping_cache_key(
                query_type="risk",
                geohash_or_loc=location_cache_token,
                keywords=keywords,
                minutes_back=minutes_back,
                limit=per_location_limit,
                api_base=bluesky_api_base,
                lang_filter=scraping_lang,
            )
            risk_rows: List[dict] = []
            cached_risk_rows = _cache_get_rows(
                key=risk_key,
                expected_query_type="risk",
                max_rows=per_location_limit,
                location_name=location_name,
            )
            if cached_risk_rows is not None:
                risk_rows = cached_risk_rows
            else:
                network_queries_attempted += 1
                try:
                    search_results = scraper.search_recent_posts(
                        location_name=location_name,
                        keywords=keywords,
                        per_location_limit=per_location_limit,
                        minutes_back=minutes_back,
                        lang=scraping_lang,
                    )
                    network_queries_succeeded += 1
                except Exception as exc:  # noqa: BLE001 - non-blocking per location
                    skipped_locations.append(f"{location_name}: {exc}")
                    _bounded_append_error(
                        network_errors,
                        f"{location_name}: {exc}",
                        max_errors=max_cache_errors,
                    )
                    search_results = []
                risk_rows = [row for row in search_results if isinstance(row, dict)][
                    :per_location_limit
                ]
                _cache_set(
                    risk_key,
                    {
                        "version": SCRAPING_CACHE_KEY_VERSION,
                        "query_type": "risk",
                        "rows": risk_rows,
                        "risk_count": len(risk_rows),
                        "fallback_count": 0,
                        "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
                    },
                )

            fallback_rows: List[dict] = []
            if len(risk_rows) == 0:
                fallback_mode_used = True
                fallback_key = _build_scraping_cache_key(
                    query_type="fallback",
                    geohash_or_loc=location_cache_token,
                    keywords="",
                    minutes_back=minutes_back,
                    limit=fallback_max_posts,
                    api_base=bluesky_api_base,
                    lang_filter=scraping_lang,
                )
                cached_fallback_rows = _cache_get_rows(
                    key=fallback_key,
                    expected_query_type="fallback",
                    max_rows=fallback_max_posts,
                    location_name=location_name,
                )
                if cached_fallback_rows is not None:
                    fallback_rows = cached_fallback_rows
                else:
                    network_queries_attempted += 1
                    try:
                        fallback_results = scraper.search_recent_posts(
                            location_name=location_name,
                            keywords="",
                            per_location_limit=fallback_max_posts,
                            minutes_back=minutes_back,
                            lang=scraping_lang,
                        )
                        network_queries_succeeded += 1
                    except Exception as exc:  # noqa: BLE001 - non-blocking per location
                        skipped_locations.append(f"{location_name} (fallback): {exc}")
                        _bounded_append_error(
                            network_errors,
                            f"{location_name} (fallback): {exc}",
                            max_errors=max_cache_errors,
                        )
                        fallback_results = []
                    fallback_rows = [
                        row for row in fallback_results if isinstance(row, dict)
                    ][:fallback_max_posts]
                    _cache_set(
                        fallback_key,
                        {
                            "version": SCRAPING_CACHE_KEY_VERSION,
                            "query_type": "fallback",
                            "rows": fallback_rows,
                            "risk_count": 0,
                            "fallback_count": len(fallback_rows),
                            "generated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
                        },
                    )

            risk_count = len(risk_rows)
            fallback_count = len(fallback_rows)
            combined_count = risk_count + fallback_count
            by_location[location_name] = combined_count

            if risk_count > 0:
                locations_with_risk += 1
            if fallback_count > 0:
                locations_with_fallback += 1
            if combined_count > 0:
                locations_with_results += 1

            risk_posts_total += risk_count
            fallback_posts_total += fallback_count
            tweets_total += combined_count

            for row in risk_rows:
                _append_post_url_warning(
                    row=row, location_name=location_name, classification="risk"
                )
                preview_rows.append(
                    _normalize_preview_row(
                        row=row,
                        location_name=location_name,
                        classification="risk",
                        is_fallback=False,
                        location_coord=location_coord,
                    )
                )
            for row in fallback_rows:
                _append_post_url_warning(
                    row=row, location_name=location_name, classification="fallback_info"
                )
                preview_rows.append(
                    _normalize_preview_row(
                        row=row,
                        location_name=location_name,
                        classification="fallback_info",
                        is_fallback=True,
                        location_coord=location_coord,
                    )
                )

            if combined_count > 0:
                resolved_coord = (
                    dict(location_coord)
                    if isinstance(location_coord, dict)
                    else None
                )
                forward_diag = _extract_forward_geocode_diag(resolved_coord)
                lat = (
                    _safe_float(resolved_coord.get("lat"), None)
                    if isinstance(resolved_coord, dict)
                    else None
                )
                lng = (
                    _safe_float(resolved_coord.get("lng"), None)
                    if isinstance(resolved_coord, dict)
                    else None
                )
                coordinate_source = (
                    str(resolved_coord.get("coordinate_source", "")).strip()
                    if isinstance(resolved_coord, dict)
                    else ""
                )
                coordinate_confidence = _resolve_coordinate_confidence(resolved_coord)
                override_attempted = False
                override_result = "not_needed"
                low_confidence_seed = (
                    coordinate_confidence == "low"
                    or coordinate_source.startswith("segment_midpoint")
                )
                forward_override_needed = lat is None or lng is None or low_confidence_seed
                if (
                    forward_override_needed
                    and forward_geocode_enabled
                    and forward_geocode_attempts < forward_geocode_limit
                ):
                    override_attempted = True
                    forward_key = _build_scraping_cache_key(
                        query_type="forward_geocode",
                        geohash_or_loc=location_cache_token,
                        keywords=location_name,
                        minutes_back=0,
                        limit=1,
                        api_base=bluesky_api_base,
                        extra_namespace=FORWARD_GEOCODE_DISAMBIG_VERSION,
                    )
                    cached_forward = _cache_get_rows(
                        key=forward_key,
                        expected_query_type="forward_geocode",
                        max_rows=1,
                        location_name=location_name,
                    )
                    if cached_forward is not None:
                        if cached_forward and isinstance(cached_forward[0], dict):
                            resolved_coord = dict(cached_forward[0])
                            forward_diag = _extract_forward_geocode_diag(resolved_coord)
                            lat = _safe_float(resolved_coord.get("lat"), None)
                            lng = _safe_float(resolved_coord.get("lng"), None)
                            override_result = "forward_geocode_cache_hit"
                        else:
                            override_result = "forward_geocode_cache_negative"
                    else:
                        forward_geocode_attempts += 1
                        try:
                            resolved_reverse_source = _resolve_municipality_reverse_source(
                                payload.get(
                                    "municipality_reverse_source",
                                    os.getenv("MUNICIPALITY_REVERSE_SOURCE", "nominatim_reverse"),
                                )
                            )
                            forward_coord = _forward_geocode_municipality(
                                location_name=location_name,
                                reverse_source=resolved_reverse_source,
                                timeout_sec=forward_geocode_timeout_sec,
                                country_codes=forward_geocode_country_codes,
                            )
                            forward_diag = _extract_forward_geocode_diag(forward_coord)
                            forward_geocode_bad_admin_rejections += _safe_int(
                                forward_diag.get(
                                    "forward_geocode_rejected_broad_admin_count"
                                ),
                                0,
                            )
                            query_mode_used = _sanitize_forward_geocode_query_mode(
                                forward_diag.get("forward_geocode_query_mode")
                            )
                            if query_mode_used != "none":
                                forward_geocode_query_modes_used.add(query_mode_used)
                            if isinstance(forward_coord, dict):
                                resolved_coord = dict(forward_coord)
                                lat = _safe_float(resolved_coord.get("lat"), None)
                                lng = _safe_float(resolved_coord.get("lng"), None)
                                if lat is not None and lng is not None:
                                    forward_geocode_successes += 1
                                    override_result = "forward_geocode_network_success"
                                    _cache_set(
                                        forward_key,
                                        {
                                            "version": SCRAPING_CACHE_KEY_VERSION,
                                            "query_type": "forward_geocode",
                                            "rows": [resolved_coord],
                                            "risk_count": 0,
                                            "fallback_count": 0,
                                            "generated_at_utc": datetime.now(
                                                tz=timezone.utc
                                            ).isoformat(),
                                        },
                                    )
                                else:
                                    forward_geocode_failures += 1
                                    override_result = "forward_geocode_network_empty"
                                    _cache_set(
                                        forward_key,
                                        {
                                            "version": SCRAPING_CACHE_KEY_VERSION,
                                            "query_type": "forward_geocode",
                                            "rows": [],
                                            "risk_count": 0,
                                            "fallback_count": 0,
                                            "generated_at_utc": datetime.now(
                                                tz=timezone.utc
                                            ).isoformat(),
                                        },
                                    )
                            else:
                                forward_geocode_failures += 1
                                override_result = "forward_geocode_network_empty"
                                _cache_set(
                                    forward_key,
                                    {
                                        "version": SCRAPING_CACHE_KEY_VERSION,
                                        "query_type": "forward_geocode",
                                        "rows": [],
                                        "risk_count": 0,
                                        "fallback_count": 0,
                                        "generated_at_utc": datetime.now(
                                            tz=timezone.utc
                                        ).isoformat(),
                                    },
                                )
                        except Exception as exc:  # noqa: BLE001
                            forward_geocode_failures += 1
                            override_result = "forward_geocode_network_error"
                            _bounded_append_error(
                                network_errors,
                                f"{location_name} (forward_geocode): {exc}",
                                max_errors=max_cache_errors,
                            )
                            _cache_set(
                                forward_key,
                                {
                                    "version": SCRAPING_CACHE_KEY_VERSION,
                                    "query_type": "forward_geocode",
                                    "rows": [],
                                    "risk_count": 0,
                                    "fallback_count": 0,
                                    "generated_at_utc": datetime.now(
                                        tz=timezone.utc
                                    ).isoformat(),
                                },
                            )

                resolution_reason = (
                    str(resolved_coord.get("resolution_reason", "")).strip()
                    if isinstance(resolved_coord, dict)
                    else ""
                )
                coordinate_source = (
                    str(resolved_coord.get("coordinate_source", "")).strip()
                    if isinstance(resolved_coord, dict)
                    else ""
                )
                coordinate_confidence = _resolve_coordinate_confidence(resolved_coord)
                forward_diag = _extract_forward_geocode_diag(resolved_coord) if (
                    isinstance(resolved_coord, dict)
                    and (
                        str(resolved_coord.get("forward_geocode_query_mode", "")).strip()
                        or _safe_int(
                            resolved_coord.get("forward_geocode_candidate_count"), 0
                        )
                    )
                ) else forward_diag

                # Prevent low-confidence midpoint placements from silently creating wrong icons.
                if (
                    lat is not None
                    and lng is not None
                    and coordinate_confidence == "low"
                    and override_result
                    in {
                        "not_needed",
                        "forward_geocode_cache_negative",
                        "forward_geocode_network_empty",
                        "forward_geocode_network_error",
                    }
                ):
                    low_confidence_resolutions_blocked += 1
                    lat = None
                    lng = None
                    resolution_reason = "low_confidence_coordinate_blocked_for_icon"
                location_resolution.append(
                    {
                        "location_name": location_name,
                        "posts_count": combined_count,
                        "resolved": lat is not None and lng is not None,
                        "lat": round(float(lat), 6) if lat is not None else None,
                        "lng": round(float(lng), 6) if lng is not None else None,
                        "coordinate_source": coordinate_source,
                        "resolution_reason": resolution_reason,
                        "coordinate_confidence": coordinate_confidence,
                        "override_attempted": override_attempted,
                        "override_result": override_result,
                        "forward_geocode_query_mode": forward_diag.get(
                            "forward_geocode_query_mode", "none"
                        ),
                        "forward_geocode_selected_addresstype": forward_diag.get(
                            "forward_geocode_selected_addresstype", ""
                        ),
                        "forward_geocode_selected_place_rank": forward_diag.get(
                            "forward_geocode_selected_place_rank"
                        ),
                        "forward_geocode_candidate_count": _safe_int(
                            forward_diag.get("forward_geocode_candidate_count"), 0
                        ),
                        "forward_geocode_rejected_broad_admin_count": _safe_int(
                            forward_diag.get(
                                "forward_geocode_rejected_broad_admin_count"
                            ),
                            0,
                        ),
                    }
                )
                if lat is None or lng is None:
                    normalized_name = _normalize_text_key(location_name)
                    if normalized_name and normalized_name not in unresolved_seen:
                        unresolved_seen.add(normalized_name)
                        unresolved_locations_for_icons.append(location_name)
                        locations_with_posts_but_no_icon.append(location_name)
                else:
                    municipality_points.append(
                        {
                            "location_name": location_name,
                            "lat": round(float(lat), 6),
                            "lng": round(float(lng), 6),
                            "icon_type": "risk" if risk_count > 0 else "info",
                            "risk_count": risk_count,
                            "fallback_count": fallback_count,
                            "coordinate_source": (
                                coordinate_source or "resolved"
                            ),
                            "resolution_reason": resolution_reason or coordinate_source or "",
                            "coordinate_confidence": coordinate_confidence,
                        }
                    )

            if pause_seconds > 0 and index < len(requested_location_names):
                time.sleep(pause_seconds)

        if len(preview_rows) > preview_limit:
            preview_rows = preview_rows[:preview_limit]

        status = "ok" if tweets_total > 0 else "no_results"
        message = (
            f"Scraping completed with {tweets_total} posts."
            if tweets_total > 0
            else "Scraping completed with no posts."
        )
        meta.update(
            status=status,
            message=message,
            keywords=keywords,
            scraping_lang=scraping_lang,
            locations_requested=len(requested_location_names),
            locations_requested_keys=requested_location_names,
            locations_requested_names=requested_location_names,
            locations_with_results=locations_with_results,
            tweets_total=tweets_total,
            per_location_limit=per_location_limit,
            radius_km=radius_km,
            minutes_back=minutes_back,
            skipped_locations=skipped_locations,
            results_by_location=by_location,
            preview_rows=preview_rows,
            cache_backend=active_cache_backend,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            cache_writes=cache_writes,
            cache_errors=cache_errors,
            cache_miss_details=cache_miss_details,
            cache_miss_details_count=cache_miss_total,
            cache_miss_details_truncated=cache_miss_details_truncated,
            cache_miss_by_reason=cache_miss_by_reason,
            cache_miss_by_query_type=cache_miss_by_query_type,
            location_resolution=location_resolution,
            locations_with_posts_but_no_icon=locations_with_posts_but_no_icon,
            forward_geocode_disambiguation_version=FORWARD_GEOCODE_DISAMBIG_VERSION,
            forward_geocode_attempts=forward_geocode_attempts,
            forward_geocode_successes=forward_geocode_successes,
            forward_geocode_failures=forward_geocode_failures,
            forward_geocode_bad_admin_rejections=forward_geocode_bad_admin_rejections,
            forward_geocode_query_modes_used=sorted(forward_geocode_query_modes_used),
            low_confidence_resolutions_blocked=low_confidence_resolutions_blocked,
            network_queries_attempted=network_queries_attempted,
            network_queries_succeeded=network_queries_succeeded,
            network_errors=network_errors,
            risk_posts_total=risk_posts_total,
            fallback_posts_total=fallback_posts_total,
            locations_with_risk=locations_with_risk,
            locations_with_fallback=locations_with_fallback,
            fallback_mode_used=fallback_mode_used,
            municipality_points=municipality_points,
            unresolved_locations_for_icons=unresolved_locations_for_icons,
            scraping_fallback_max_posts=fallback_max_posts,
        )
    except Exception as exc:  # noqa: BLE001 - keep solve endpoint stable
        meta.update(
            status="failed",
            message=f"Social scraping execution failed: {exc}",
            keywords=keywords,
            scraping_lang=scraping_lang,
            per_location_limit=per_location_limit,
            radius_km=radius_km,
            minutes_back=minutes_back,
            cache_backend=active_cache_backend,
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            cache_writes=cache_writes,
            cache_errors=cache_errors,
            cache_miss_details=cache_miss_details,
            cache_miss_details_count=cache_miss_total,
            cache_miss_details_truncated=cache_miss_details_truncated,
            cache_miss_by_reason=cache_miss_by_reason,
            cache_miss_by_query_type=cache_miss_by_query_type,
            location_resolution=location_resolution,
            locations_with_posts_but_no_icon=locations_with_posts_but_no_icon,
            forward_geocode_disambiguation_version=FORWARD_GEOCODE_DISAMBIG_VERSION,
            forward_geocode_attempts=forward_geocode_attempts,
            forward_geocode_successes=forward_geocode_successes,
            forward_geocode_failures=forward_geocode_failures,
            forward_geocode_bad_admin_rejections=forward_geocode_bad_admin_rejections,
            forward_geocode_query_modes_used=sorted(forward_geocode_query_modes_used),
            low_confidence_resolutions_blocked=low_confidence_resolutions_blocked,
            network_queries_attempted=network_queries_attempted,
            network_queries_succeeded=network_queries_succeeded,
            network_errors=network_errors,
        )

    file_write = _write_scraping_temp_file(_build_scraping_report_text(meta))
    meta["output_file"] = file_write["path"]
    if file_write["error"]:
        meta["output_file_error"] = file_write["error"]
    meta["output_file_snapshot"] = file_write["snapshot_path"]
    if file_write["snapshot_error"]:
        meta["output_file_snapshot_error"] = file_write["snapshot_error"]
    return meta


def _parse_utc_datetime(value):
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


def _resolve_here_pipeline_mode(value) -> str:
    mode = str(value or "postprocessing").strip().lower()
    if mode in {"before_vrp", "before-vrp", "before"}:
        return "before_vrp"
    return "postprocessing"


def _resolve_here_data_source(value) -> str:
    source = str(value or "here").strip().lower()
    if source in {"emulator", "mock", "simulated", "synthetic"}:
        return "emulator"
    return "here"


def _new_checkpoint_logger(flow_name: str):
    started_at = time.perf_counter()
    last_mark = started_at

    def checkpoint(step_name: str) -> None:
        nonlocal last_mark
        now = time.perf_counter()
        phase_elapsed_sec = now - last_mark
        total_elapsed_sec = now - started_at
        print(
            f"[checkpoint:{flow_name}] {step_name} | "
            f"phase={phase_elapsed_sec:.3f}s total={total_elapsed_sec:.3f}s"
        )
        last_mark = now

    return checkpoint


def _prefetch_here_point_observations(payload: dict, depot: dict, customers: list) -> dict:
    updated_payload = dict(payload)
    here_data_source = _resolve_here_data_source(payload.get("here_data_source"))
    api_key = os.getenv("HERE_API_KEY", "").strip()
    if here_data_source == "here" and not api_key:
        updated_payload["_here_prefetch"] = {
            "enabled": False,
            "data_source": "here",
            "error": "HERE_API_KEY environment variable is not set.",
        }
        return updated_payload

    timeout_sec = max(3, _safe_int(payload.get("here_timeout_sec"), 12))
    traffic_radius_m = max(50, _safe_int(payload.get("here_traffic_radius_m"), 300))
    forecast_window_hours = max(1, _safe_int(payload.get("here_forecast_window_hours"), 24))
    forecast_interval_min = max(30, _safe_int(payload.get("here_forecast_interval_min"), 120))
    departure_time_utc = _parse_utc_datetime(payload.get("departure_time_utc")) or datetime.now(
        tz=timezone.utc
    )

    if here_data_source == "emulator":
        client = HerePlatformEmulator(
            timeout_sec=timeout_sec,
            traffic_radius_m=traffic_radius_m,
            forecast_window_hours=forecast_window_hours,
            forecast_step_min=forecast_interval_min,
            seed=payload.get("here_emulator_seed"),
        )
    else:
        client = HerePlatformClient(
            api_key=api_key,
            timeout_sec=timeout_sec,
            traffic_radius_m=traffic_radius_m,
            forecast_window_hours=forecast_window_hours,
            forecast_step_min=forecast_interval_min,
        )

    weather_observations = list(
        payload.get("weather_observations", [])
        if isinstance(payload.get("weather_observations"), list)
        else []
    )
    traffic_observations = list(
        payload.get("traffic_observations", [])
        if isinstance(payload.get("traffic_observations"), list)
        else []
    )
    prefetch_errors = []

    depot_lat = _safe_float(depot.get("lat"))
    depot_lng = _safe_float(depot.get("lng"))
    points = [depot] + [c for c in customers if isinstance(c, dict)]

    for point in points:
        lat = _safe_float(point.get("lat"))
        lng = _safe_float(point.get("lng"))
        if lat is None or lng is None:
            continue

        try:
            weather_bundle = client.fetch_weather(lat, lng, reference_time_utc=departure_time_utc)
            realtime = weather_bundle.get("realtime", {})
            weather_observations.append(
                {
                    "lat": lat,
                    "lng": lng,
                    "time_utc": realtime.get("observed_at_utc") or departure_time_utc.isoformat().replace("+00:00", "Z"),
                    "temperature_c": realtime.get("temperature_c"),
                    "precipitation_mm": realtime.get("precipitation_mm"),
                    "wind_kph": realtime.get("wind_kph"),
                    "condition": realtime.get("condition"),
                    "source": realtime.get("source", "here_weather_v3"),
                    "forecast_24h": weather_bundle.get("forecast_24h"),
                }
            )
        except Exception as exc:  # noqa: BLE001 - keep VRP flow resilient
            prefetch_errors.append(f"weather prefetch failed at {lat},{lng}: {exc}")

        try:
            traffic_realtime = client.fetch_traffic_status(lat, lng)
            traffic_forecast = None
            if depot_lat is not None and depot_lng is not None and (lat != depot_lat or lng != depot_lng):
                traffic_forecast = client.fetch_traffic_forecast(
                    {"lat": depot_lat, "lng": depot_lng},
                    {"lat": lat, "lng": lng},
                    reference_time_utc=departure_time_utc,
                )
            traffic_observations.append(
                {
                    "lat": lat,
                    "lng": lng,
                    "time_utc": traffic_realtime.get("observed_at_utc") or departure_time_utc.isoformat().replace("+00:00", "Z"),
                    "congestion_level": traffic_realtime.get("congestion_level"),
                    "speed_kmh": traffic_realtime.get("speed_kmh"),
                    "incident_count": traffic_realtime.get("incident_count"),
                    "source": traffic_realtime.get("source", "here_traffic_v7"),
                    "forecast_24h": traffic_forecast,
                }
            )
        except Exception as exc:  # noqa: BLE001 - keep VRP flow resilient
            prefetch_errors.append(f"traffic prefetch failed at {lat},{lng}: {exc}")

    updated_payload["weather_observations"] = weather_observations
    updated_payload["traffic_observations"] = traffic_observations
    # In before_vrp mode, do not call HERE again in post-processing.
    updated_payload["use_here_platform"] = False
    updated_payload["_here_prefetch"] = {
        "enabled": True,
        "data_source": here_data_source,
        "points_queried": len(points),
        "errors": prefetch_errors[:20],
        "client_stats": client.stats(),
    }
    return updated_payload


def _coordinate_key(lat: float, lng: float) -> str:
    return f"{float(lat):.6f},{float(lng):.6f}"


def _downsample_points(
    points: List[Dict[str, float]], max_samples: int
) -> List[Dict[str, float]]:
    if max_samples <= 0 or len(points) <= max_samples:
        return points
    if max_samples == 1:
        return [points[0]]
    last_index = len(points) - 1
    chosen_indexes = sorted(
        {
            int(round((slot * last_index) / float(max_samples - 1)))
            for slot in range(max_samples)
        }
    )
    return [points[idx] for idx in chosen_indexes]


def _build_route_probe_points(
    vrp_result: Dict[str, Any], max_samples: int
) -> List[Dict[str, float]]:
    points: List[Dict[str, float]] = []
    seen: set[str] = set()
    routes = vrp_result.get("routes", [])
    if not isinstance(routes, list):
        return []

    for route in routes:
        if not isinstance(route, dict):
            continue
        stops = route.get("stops", [])
        if not isinstance(stops, list) or len(stops) < 2:
            continue
        for idx in range(len(stops) - 1):
            start = stops[idx]
            end = stops[idx + 1]
            if not isinstance(start, dict) or not isinstance(end, dict):
                continue
            lat_a = _safe_float(start.get("lat"))
            lng_a = _safe_float(start.get("lng"))
            lat_b = _safe_float(end.get("lat"))
            lng_b = _safe_float(end.get("lng"))
            if (
                lat_a is None
                or lng_a is None
                or lat_b is None
                or lng_b is None
            ):
                continue

            for fraction in (0.0, 0.25, 0.5, 0.75, 1.0):
                lat = lat_a + ((lat_b - lat_a) * fraction)
                lng = lng_a + ((lng_b - lng_a) * fraction)
                key = _coordinate_key(lat, lng)
                if key in seen:
                    continue
                seen.add(key)
                points.append({"lat": lat, "lng": lng})

    return _downsample_points(points, max_samples)


def _auto_poi_category(tags: Dict[str, Any]) -> Optional[str]:
    for key, value in tags.items():
        mapped = AUTO_POI_CATEGORY_MAP.get(
            (str(key).strip(), str(value).strip())
        )
        if mapped:
            return mapped
    return None


def _build_auto_poi_query(
    samples: List[Dict[str, float]],
    radius_km: float,
    timeout_sec: int,
    include_non_nodes: bool = False,
) -> str:
    radius_m = int(max(250.0, float(radius_km) * 1000.0))
    keys_regex = "|".join(sorted(AUTO_POI_TAG_FILTERS.keys()))
    values_set = sorted(
        {
            value
            for values in AUTO_POI_TAG_FILTERS.values()
            for value in values
        }
    )
    values_regex = "|".join(values_set)
    osm_selector = "nwr" if include_non_nodes else "node"
    clauses: List[str] = []
    for sample in samples:
        lat = float(sample["lat"])
        lng = float(sample["lng"])
        clauses.append(
            f'{osm_selector}(around:{radius_m},{lat},{lng})[~"^({keys_regex})$"~"^({values_regex})$"];'
        )
    timeout_value = max(5, int(timeout_sec))
    return (
        f"[out:json][timeout:{timeout_value}];\n(\n"
        + "\n".join(clauses)
        + "\n);\nout tags center;"
    )


def _chunk_points(
    points: List[Dict[str, float]], chunk_size: int
) -> List[List[Dict[str, float]]]:
    if chunk_size <= 0 or len(points) <= chunk_size:
        return [points]
    return [points[idx : idx + chunk_size] for idx in range(0, len(points), chunk_size)]


def _query_overpass_payload(
    endpoints: List[str],
    query: str,
    timeout_sec: int,
) -> tuple[Optional[Dict[str, Any]], str, List[str]]:
    body = urllib.parse.urlencode({"data": query}).encode("utf-8")
    endpoint_errors: List[str] = []
    for endpoint in endpoints:
        try:
            request = urllib.request.Request(
                endpoint,
                data=body,
                headers={
                    "User-Agent": "softOptimizationVRP/semantic-poi-auto",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                },
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=timeout_sec) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload, endpoint, endpoint_errors
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.append(f"{endpoint}: {exc}")
            time.sleep(0.12)
    return None, "", endpoint_errors


def _distance_km_to_degree_box(radius_km: float, lat: float) -> tuple[float, float]:
    lat_delta = max(0.02, float(radius_km) / 110.574)
    cos_lat = max(0.15, math.cos(math.radians(float(lat))))
    lng_delta = max(0.02, float(radius_km) / (111.320 * cos_lat))
    return lat_delta, lng_delta


def _haversine_km_coords(
    lat_a: float,
    lng_a: float,
    lat_b: float,
    lng_b: float,
) -> float:
    lat1 = math.radians(float(lat_a))
    lon1 = math.radians(float(lng_a))
    lat2 = math.radians(float(lat_b))
    lon2 = math.radians(float(lng_b))
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        (math.sin(dlat / 2.0) ** 2)
        + (math.cos(lat1) * math.cos(lat2) * (math.sin(dlon / 2.0) ** 2))
    )
    return 6371.0088 * 2.0 * math.asin(math.sqrt(h))


def _collect_route_stop_points(vrp_result: Dict[str, Any]) -> List[Dict[str, float]]:
    points: List[Dict[str, float]] = []
    seen: set[str] = set()
    routes = vrp_result.get("routes", [])
    if not isinstance(routes, list):
        return points
    for route in routes:
        if not isinstance(route, dict):
            continue
        stops = route.get("stops", [])
        if not isinstance(stops, list):
            continue
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            lat = _safe_float(stop.get("lat"))
            lng = _safe_float(stop.get("lng"))
            if lat is None or lng is None:
                continue
            key = _coordinate_key(lat, lng)
            if key in seen:
                continue
            seen.add(key)
            points.append({"lat": lat, "lng": lng})
    return points


def _collect_depot_points(vrp_result: Dict[str, Any]) -> List[Dict[str, float]]:
    points: List[Dict[str, float]] = []
    seen: set[str] = set()
    routes = vrp_result.get("routes", [])
    if not isinstance(routes, list):
        return points
    for route in routes:
        if not isinstance(route, dict):
            continue
        stops = route.get("stops", [])
        if not isinstance(stops, list):
            continue
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            if str(stop.get("id")) != "depot":
                continue
            lat = _safe_float(stop.get("lat"))
            lng = _safe_float(stop.get("lng"))
            if lat is None or lng is None:
                continue
            key = _coordinate_key(lat, lng)
            if key in seen:
                continue
            seen.add(key)
            points.append({"lat": lat, "lng": lng})
    return points


def _distance_to_nearest_point_km(
    lat: float,
    lng: float,
    points: List[Dict[str, float]],
) -> float:
    if not points:
        return float("inf")
    best = float("inf")
    for point in points:
        p_lat = _safe_float(point.get("lat"))
        p_lng = _safe_float(point.get("lng"))
        if p_lat is None or p_lng is None:
            continue
        distance = _haversine_km_coords(lat, lng, p_lat, p_lng)
        if distance < best:
            best = distance
    return best


def _filter_probe_points_away_from_stops(
    probe_points: List[Dict[str, float]],
    stop_points: List[Dict[str, float]],
    min_stop_distance_km: float,
    depot_points: Optional[List[Dict[str, float]]] = None,
    min_depot_distance_km: float = 0.0,
) -> List[Dict[str, float]]:
    if (
        not probe_points
        or (
            (not stop_points or min_stop_distance_km <= 0)
            and (not depot_points or min_depot_distance_km <= 0)
        )
    ):
        return list(probe_points)
    filtered: List[Dict[str, float]] = []
    for point in probe_points:
        lat = _safe_float(point.get("lat"))
        lng = _safe_float(point.get("lng"))
        if lat is None or lng is None:
            continue
        stop_distance_ok = (
            True
            if (not stop_points or min_stop_distance_km <= 0)
            else _distance_to_nearest_point_km(lat, lng, stop_points) >= min_stop_distance_km
        )
        depot_distance_ok = (
            True
            if (not depot_points or min_depot_distance_km <= 0)
            else _distance_to_nearest_point_km(lat, lng, depot_points) >= min_depot_distance_km
        )
        if stop_distance_ok and depot_distance_ok:
            filtered.append(point)
    return filtered


def _filter_candidates_away_from_stops(
    candidates: List[Dict[str, Any]],
    stop_points: List[Dict[str, float]],
    min_stop_distance_km: float,
    depot_points: Optional[List[Dict[str, float]]] = None,
    min_depot_distance_km: float = 0.0,
) -> List[Dict[str, Any]]:
    if (
        not candidates
        or (
            (not stop_points or min_stop_distance_km <= 0)
            and (not depot_points or min_depot_distance_km <= 0)
        )
    ):
        return list(candidates)
    filtered: List[Dict[str, Any]] = []
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        lat = _safe_float(candidate.get("lat"))
        lng = _safe_float(candidate.get("lng"))
        if lat is None or lng is None:
            continue
        stop_distance_ok = (
            True
            if (not stop_points or min_stop_distance_km <= 0)
            else _distance_to_nearest_point_km(lat, lng, stop_points) >= min_stop_distance_km
        )
        depot_distance_ok = (
            True
            if (not depot_points or min_depot_distance_km <= 0)
            else _distance_to_nearest_point_km(lat, lng, depot_points) >= min_depot_distance_km
        )
        if stop_distance_ok and depot_distance_ok:
            filtered.append(candidate)
    return filtered


def _fetch_openchargemap_candidates(
    *,
    samples: List[Dict[str, float]],
    radius_km: float,
    timeout_sec: int,
    max_candidates: int,
) -> tuple[List[Dict[str, Any]], List[str]]:
    if not samples or max_candidates <= 0:
        return [], []

    errors: List[str] = []
    by_id: Dict[str, Dict[str, Any]] = {}
    picked_samples = _downsample_points(samples, min(len(samples), 4))
    query_radius_km = max(3.0, min(12.0, float(radius_km)))
    for sample in picked_samples:
        if len(by_id) >= max_candidates:
            break
        lat = _safe_float(sample.get("lat"))
        lng = _safe_float(sample.get("lng"))
        if lat is None or lng is None:
            continue
        params = {
            "output": "json",
            "countrycode": "ES",
            "latitude": f"{lat:.6f}",
            "longitude": f"{lng:.6f}",
            "distance": f"{query_radius_km:.1f}",
            "distanceunit": "KM",
            "maxresults": "12",
            "compact": "true",
            "verbose": "false",
        }
        url = (
            "https://api.openchargemap.io/v3/poi/?"
            + urllib.parse.urlencode(params)
        )
        try:
            request = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "softOptimizationVRP/semantic-poi-auto",
                    "Accept": "application/json",
                },
                method="GET",
            )
            with urllib.request.urlopen(request, timeout=max(3, int(timeout_sec))) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"openchargemap: {exc}")
            continue

        if not isinstance(payload, list):
            continue
        for row in payload:
            if not isinstance(row, dict):
                continue
            addr = row.get("AddressInfo", {})
            if not isinstance(addr, dict):
                addr = {}
            poi_lat = _safe_float(addr.get("Latitude"))
            poi_lng = _safe_float(addr.get("Longitude"))
            if poi_lat is None or poi_lng is None:
                continue
            poi_id = str(row.get("ID") or "").strip()
            candidate_id = poi_id and f"ocm/{poi_id}" or f"ocm/{_coordinate_key(poi_lat, poi_lng)}"
            name = str(addr.get("Title") or "").strip() or f"Charging station {candidate_id}"
            candidate = {
                "id": candidate_id,
                "name": name,
                "lat": poi_lat,
                "lng": poi_lng,
                "semantic_category": "charging",
                "source": "openchargemap_auto",
                "tags": {
                    "amenity": "charging_station",
                },
            }
            _merge_auto_candidates(by_id, [candidate])
            if len(by_id) >= max_candidates:
                break
    return list(by_id.values())[:max_candidates], errors


def _fetch_nominatim_semantic_candidates(
    *,
    samples: List[Dict[str, float]],
    radius_km: float,
    timeout_sec: int,
    max_candidates: int,
) -> tuple[List[Dict[str, Any]], List[str]]:
    if not samples or max_candidates <= 0:
        return [], []

    errors: List[str] = []
    by_id: Dict[str, Dict[str, Any]] = {}
    picked_samples = _downsample_points(samples, min(len(samples), 4))
    # Wider search windows are needed when relying on Nominatim text search;
    # narrow windows under-represent intercity highway services.
    query_radius_km = max(6.0, min(14.0, float(radius_km) * 2.2))
    terms = (
        ("charging station", "charging"),
        ("electrolinera", "charging"),
        ("area de servicio", "rest_area"),
        ("gasolinera", "rest_area"),
    )
    request_delay_sec = 0.35
    sample_viewboxes: List[Dict[str, float]] = []
    for sample in picked_samples:
        lat = _safe_float(sample.get("lat"))
        lng = _safe_float(sample.get("lng"))
        if lat is None or lng is None:
            continue
        lat_delta, lng_delta = _distance_km_to_degree_box(query_radius_km, lat)
        sample_viewboxes.append(
            {
                "lat": lat,
                "lng": lng,
                "left": lng - lng_delta,
                "top": lat + lat_delta,
                "right": lng + lng_delta,
                "bottom": lat - lat_delta,
            }
        )

    consecutive_rate_limits = 0
    for term, semantic_category in terms:
        if len(by_id) >= max_candidates:
            break
        for sample_query in sample_viewboxes:
            if len(by_id) >= max_candidates:
                break
            sample_lat = float(sample_query["lat"])
            sample_lng = float(sample_query["lng"])
            left = float(sample_query["left"])
            top = float(sample_query["top"])
            right = float(sample_query["right"])
            bottom = float(sample_query["bottom"])
            params = {
                "format": "jsonv2",
                "addressdetails": "1",
                "bounded": "1",
                "limit": "8",
                "q": term,
                "viewbox": f"{left:.6f},{top:.6f},{right:.6f},{bottom:.6f}",
            }
            url = (
                "https://nominatim.openstreetmap.org/search?"
                + urllib.parse.urlencode(params)
            )
            try:
                request = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "softOptimizationVRP/semantic-poi-auto",
                        "Accept": "application/json",
                    },
                    method="GET",
                )
                with urllib.request.urlopen(
                    request, timeout=max(3, int(timeout_sec))
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"nominatim_search: {exc}")
                if isinstance(exc, urllib.error.HTTPError) and exc.code == 429:
                    consecutive_rate_limits += 1
                    if consecutive_rate_limits >= 6:
                        return list(by_id.values())[:max_candidates], errors
                else:
                    consecutive_rate_limits = 0
                continue
            finally:
                # Keep fallback resilient against Nominatim rate limits.
                time.sleep(request_delay_sec)

            consecutive_rate_limits = 0
            if not isinstance(payload, list):
                continue
            for row in payload:
                if not isinstance(row, dict):
                    continue
                row_type = str(row.get("type") or "").strip().lower()
                row_class = str(row.get("class") or "").strip().lower()
                display_name = str(row.get("display_name") or "").strip()
                display_low = display_name.lower()
                type_low = row_type.lower()
                class_low = row_class.lower()
                token_text = " ".join([display_low, type_low, class_low]).strip()
                if semantic_category == "charging":
                    charging_keywords = (
                        "recarga",
                        "charging",
                        "charger",
                        "electrolinera",
                    )
                    charging_types = {"charging_station"}
                    is_relevant = (
                        any(keyword in token_text for keyword in charging_keywords)
                        or type_low in charging_types
                    )
                else:
                    rest_keywords = (
                        "area de servicio",
                        "service area",
                        "rest area",
                        "gasolinera",
                        "estacion de servicio",
                        "fuel",
                        "services",
                    )
                    rest_types = {"service_area", "services", "fuel", "rest_area"}
                    is_relevant = (
                        any(keyword in token_text for keyword in rest_keywords)
                        or type_low in rest_types
                    )
                if not is_relevant:
                    continue
                poi_lat = _safe_float(row.get("lat"))
                poi_lng = _safe_float(row.get("lon"))
                if poi_lat is None or poi_lng is None:
                    continue
                # Nominatim may return results outside the intended viewbox for
                # generic terms; keep only local hits around sampled route points.
                sample_distance_km = _haversine_km_coords(
                    sample_lat,
                    sample_lng,
                    poi_lat,
                    poi_lng,
                )
                if sample_distance_km > (query_radius_km * 1.8):
                    continue
                osm_type = str(row.get("osm_type") or "").strip()
                osm_id = str(row.get("osm_id") or "").strip()
                candidate_id = (
                    f"nominatim/{osm_type}/{osm_id}"
                    if osm_type and osm_id
                    else f"nominatim/{_coordinate_key(poi_lat, poi_lng)}"
                )
                display_name = str(row.get("display_name") or "").strip()
                name = display_name.split(",")[0].strip() if display_name else ""
                if not name:
                    name = f"Rest area {candidate_id}"
                candidate = {
                    "id": candidate_id,
                    "name": name,
                    "lat": poi_lat,
                    "lng": poi_lng,
                    "semantic_category": semantic_category,
                    "source": "nominatim_auto",
                    "tags": (
                        {"amenity": "charging_station"}
                        if semantic_category == "charging"
                        else {"highway": "services"}
                    ),
                }
                _merge_auto_candidates(by_id, [candidate])
                if len(by_id) >= max_candidates:
                    break
    return list(by_id.values())[:max_candidates], errors


def _merge_auto_candidates(
    base: Dict[str, Dict[str, Any]], candidates: List[Dict[str, Any]]
) -> None:
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        candidate_id = str(candidate.get("id") or "").strip()
        if not candidate_id:
            continue
        previous = base.get(candidate_id)
        if previous is None:
            base[candidate_id] = candidate
            continue
        prev_name = str(previous.get("name") or "").strip()
        new_name = str(candidate.get("name") or "").strip()
        if len(new_name) > len(prev_name):
            base[candidate_id] = candidate


def _extract_auto_poi_candidates(
    overpass_payload: Dict[str, Any], max_candidates: int
) -> List[Dict[str, Any]]:
    elements = overpass_payload.get("elements", [])
    if not isinstance(elements, list):
        return []

    by_ref: Dict[str, Dict[str, Any]] = {}
    for element in elements:
        if not isinstance(element, dict):
            continue
        tags = element.get("tags", {})
        if not isinstance(tags, dict):
            continue

        category = _auto_poi_category(tags)
        if not category:
            continue

        lat = _safe_float(element.get("lat"))
        lng = _safe_float(element.get("lon"))
        if lat is None or lng is None:
            center = element.get("center")
            if isinstance(center, dict):
                lat = _safe_float(center.get("lat"))
                lng = _safe_float(center.get("lon"))
        if lat is None or lng is None:
            continue

        osm_type = str(element.get("type") or "element").strip() or "element"
        osm_id = str(element.get("id") or "").strip()
        if not osm_id:
            continue
        osm_ref = f"{osm_type}/{osm_id}"

        normalized_tags: Dict[str, str] = {}
        for key, value in tags.items():
            key_text = str(key).strip()
            value_text = str(value).strip()
            if not key_text or not value_text:
                continue
            normalized_tags[key_text] = value_text

        candidate = {
            "id": osm_ref,
            "name": str(tags.get("name") or f"{category}:{osm_ref}"),
            "lat": lat,
            "lng": lng,
            "semantic_category": category,
            "source": "osm_overpass_auto",
            "tags": normalized_tags,
        }
        previous = by_ref.get(osm_ref)
        if previous is None:
            by_ref[osm_ref] = candidate
            continue

        if (
            len(str(previous.get("name") or "").strip())
            < len(str(candidate.get("name") or "").strip())
        ):
            by_ref[osm_ref] = candidate

    candidates = list(by_ref.values())
    candidates.sort(
        key=lambda row: (
            str(row.get("semantic_category") or ""),
            str(row.get("name") or "").lower(),
            str(row.get("id") or ""),
        )
    )
    return candidates[:max(1, int(max_candidates))]


def _auto_populate_candidate_locations(
    semantic_payload: Dict[str, Any], vrp_result: Dict[str, Any]
) -> tuple[Dict[str, Any], Dict[str, Any]]:
    auto_enabled = _as_bool(semantic_payload.get("poi_auto_enabled"), False)
    current_candidates = semantic_payload.get("candidate_locations")
    if isinstance(current_candidates, list) and len(current_candidates) > 0:
        return semantic_payload, {
            "enabled": auto_enabled,
            "status": "input_provided",
            "message": "candidate_locations already provided in payload.",
            "input_count": len(current_candidates),
        }
    if not auto_enabled:
        return semantic_payload, {
            "enabled": False,
            "status": "disabled",
            "message": "POI auto-candidate fetch disabled.",
            "input_count": 0,
        }

    max_samples = max(
        1,
        _safe_int(
            semantic_payload.get("poi_auto_max_samples"), DEFAULT_POI_AUTO_MAX_SAMPLES
        ),
    )
    radius_km = _safe_float(
        semantic_payload.get("poi_auto_radius_km"), DEFAULT_POI_AUTO_RADIUS_KM
    )
    radius_km = max(0.2, float(radius_km or DEFAULT_POI_AUTO_RADIUS_KM))
    query_radius_limit_km = _safe_float(
        semantic_payload.get("poi_auto_query_radius_max_km"),
        DEFAULT_POI_AUTO_QUERY_MAX_RADIUS_KM,
    )
    query_radius_limit_km = max(
        0.5, float(query_radius_limit_km or DEFAULT_POI_AUTO_QUERY_MAX_RADIUS_KM)
    )
    effective_query_radius_km = min(radius_km, query_radius_limit_km)
    timeout_sec = max(
        4,
        _safe_int(
            semantic_payload.get("poi_auto_timeout_sec"), DEFAULT_POI_AUTO_TIMEOUT_SEC
        ),
    )
    max_candidates = max(
        10,
        _safe_int(
            semantic_payload.get("poi_auto_max_candidates"),
            DEFAULT_POI_AUTO_MAX_CANDIDATES,
        ),
    )
    chunk_size = max(
        1,
        _safe_int(
            semantic_payload.get("poi_auto_chunk_size"), DEFAULT_POI_AUTO_CHUNK_SIZE
        ),
    )
    max_chunk_queries = max(
        0,
        _safe_int(
            semantic_payload.get("poi_auto_max_chunk_queries"),
            DEFAULT_POI_AUTO_MAX_CHUNK_QUERIES,
        ),
    )
    stop_buffer_km = _safe_float(
        semantic_payload.get("poi_auto_stop_buffer_km"),
        12.0,
    )
    stop_buffer_km = max(0.0, float(stop_buffer_km or 0.0))
    depot_buffer_km = _safe_float(
        semantic_payload.get("poi_auto_depot_buffer_km"),
        18.0,
    )
    depot_buffer_km = max(0.0, float(depot_buffer_km or 0.0))

    sample_points = _build_route_probe_points(vrp_result, max_samples)
    if not sample_points:
        return semantic_payload, {
            "enabled": True,
            "status": "skipped",
            "message": "No valid route probe points available for POI auto-fetch.",
            "probe_points": 0,
            "fetched_candidates": 0,
        }
    stop_points = _collect_route_stop_points(vrp_result)
    depot_points = _collect_depot_points(vrp_result)
    query_sample_points = _filter_probe_points_away_from_stops(
        sample_points,
        stop_points,
        stop_buffer_km,
        depot_points=depot_points,
        min_depot_distance_km=depot_buffer_km,
    )
    if len(query_sample_points) < 3:
        query_sample_points = sample_points

    raw_endpoints = semantic_payload.get("poi_auto_overpass_endpoints")
    if isinstance(raw_endpoints, list):
        endpoints = [
            str(endpoint).strip()
            for endpoint in raw_endpoints
            if str(endpoint).strip()
        ]
    else:
        endpoints = []
    max_endpoints = max(
        1,
        _safe_int(
            semantic_payload.get("poi_auto_max_endpoints"),
            DEFAULT_POI_AUTO_MAX_ENDPOINTS,
        ),
    )
    if not endpoints:
        endpoints = list(DEFAULT_OVERPASS_ENDPOINTS)
    endpoints = endpoints[:max_endpoints]
    primary_endpoints = endpoints[: min(2, len(endpoints))]
    if not primary_endpoints:
        primary_endpoints = endpoints

    started_at = time.time()
    errors: List[str] = []
    endpoints_used: List[str] = []
    by_id: Dict[str, Dict[str, Any]] = {}
    query_attempts = 0
    query_successes = 0
    fallback_sources: List[str] = []

    def run_overpass_query(
        samples: List[Dict[str, float]],
        *,
        include_non_nodes: bool,
        query_timeout_sec: int,
        endpoint_pool: List[str],
    ) -> None:
        nonlocal query_attempts, query_successes
        if not samples or not endpoint_pool:
            return

        query = _build_auto_poi_query(
            samples,
            effective_query_radius_km,
            query_timeout_sec,
            include_non_nodes=include_non_nodes,
        )
        query_attempts += 1
        response_payload, used_endpoint, endpoint_errors = _query_overpass_payload(
            endpoints=endpoint_pool,
            query=query,
            timeout_sec=query_timeout_sec,
        )
        if response_payload is not None:
            remark = str(response_payload.get("remark") or "").strip()
            if remark:
                errors.append(f"{used_endpoint or 'overpass'}: {remark}")
                response_payload = None
        if response_payload is None:
            errors.extend(endpoint_errors)
            return

        query_successes += 1
        if used_endpoint:
            endpoints_used.append(used_endpoint)
        _merge_auto_candidates(
            by_id,
            _extract_auto_poi_candidates(response_payload, max_candidates),
        )

    # Stage 1: quick node-only query first (lighter than nwr).
    run_overpass_query(
        query_sample_points,
        include_non_nodes=False,
        query_timeout_sec=max(4, min(timeout_sec, 6)),
        endpoint_pool=primary_endpoints,
    )

    # Stage 1b: if still empty, try full nwr query (ways+relations included).
    if len(by_id) == 0:
        heavy_points = _downsample_points(
            query_sample_points, max(3, min(len(query_sample_points), 6))
        )
        run_overpass_query(
            heavy_points,
            include_non_nodes=True,
            query_timeout_sec=timeout_sec,
            endpoint_pool=endpoints,
        )

    # Stage 2: if still empty, retry a few chunked nwr queries.
    # Keep this bounded to avoid very long waits on unstable Overpass mirrors.
    if len(by_id) == 0 and max_chunk_queries > 0:
        chunk_timeout_sec = max(4, min(timeout_sec, 5))
        chunk_queries_used = 0
        for chunk in _chunk_points(query_sample_points, chunk_size):
            if len(by_id) >= max_candidates or chunk_queries_used >= max_chunk_queries:
                break
            chunk_queries_used += 1
            run_overpass_query(
                chunk,
                include_non_nodes=True,
                query_timeout_sec=chunk_timeout_sec,
                endpoint_pool=endpoints,
            )

    # Stage 3: Overpass fallback sources if no candidates were obtained.
    if len(by_id) == 0:
        fallback_timeout_sec = max(3, min(timeout_sec, 6))
        ocm_candidates, ocm_errors = _fetch_openchargemap_candidates(
            samples=query_sample_points,
            radius_km=max(radius_km, effective_query_radius_km),
            timeout_sec=fallback_timeout_sec,
            max_candidates=max_candidates,
        )
        if ocm_candidates:
            fallback_sources.append("openchargemap")
            _merge_auto_candidates(by_id, ocm_candidates)
        elif ocm_errors:
            errors.extend(ocm_errors[:6])

    if len(by_id) == 0:
        remaining = max_candidates
        nominatim_candidates, nominatim_errors = _fetch_nominatim_semantic_candidates(
            samples=query_sample_points,
            radius_km=max(radius_km, effective_query_radius_km),
            timeout_sec=max(3, min(timeout_sec, 6)),
            max_candidates=remaining,
        )
        if nominatim_candidates:
            fallback_sources.append("nominatim_search")
            _merge_auto_candidates(by_id, nominatim_candidates)
        elif nominatim_errors:
            errors.extend(nominatim_errors[:6])

    elapsed_ms = int((time.time() - started_at) * 1000)
    candidates = list(by_id.values())[:max_candidates]
    summary = vrp_result.get("summary", {})
    route_distance_km = _safe_float(
        summary.get("total_distance_km")
        if isinstance(summary, dict)
        else None,
        0.0,
    )
    if not route_distance_km:
        route_distance_km = sum(
            _safe_float(route.get("distance_km"), 0.0)
            for route in (vrp_result.get("routes", []) if isinstance(vrp_result.get("routes"), list) else [])
            if isinstance(route, dict)
        )
    desired_min_candidates = min(
        max_candidates,
        max(6, int(round(float(route_distance_km or 0.0) / 120.0)) + 2),
    )
    effective_stop_buffer_km = stop_buffer_km
    if (stop_buffer_km > 0 or depot_buffer_km > 0) and (stop_points or depot_points):
        candidate_sets: List[tuple[float, List[Dict[str, Any]]]] = []
        buffer_levels = [
            stop_buffer_km,
            max(0.0, stop_buffer_km - 2.0),
            max(0.0, stop_buffer_km - 4.0),
            max(0.0, stop_buffer_km - 6.0),
        ]
        # Preserve order and de-duplicate.
        normalized_levels: List[float] = []
        seen_levels: set[float] = set()
        for level in buffer_levels:
            key = round(level, 3)
            if key in seen_levels:
                continue
            seen_levels.add(key)
            normalized_levels.append(level)

        for level in normalized_levels:
            filtered = _filter_candidates_away_from_stops(
                candidates,
                stop_points,
                level,
                depot_points=depot_points,
                min_depot_distance_km=depot_buffer_km,
            )
            candidate_sets.append((level, filtered))
            if len(filtered) >= desired_min_candidates:
                effective_stop_buffer_km = level
                candidates = filtered[:max_candidates]
                break
        else:
            best_level, best_set = max(
                candidate_sets,
                key=lambda row: len(row[1]),
                default=(stop_buffer_km, []),
            )
            if best_set:
                effective_stop_buffer_km = best_level
                candidates = best_set[:max_candidates]
    candidates.sort(
        key=lambda row: (
            str(row.get("semantic_category") or ""),
            str(row.get("name") or "").lower(),
            str(row.get("id") or ""),
        )
    )
    unique_endpoints = sorted({endpoint for endpoint in endpoints_used if endpoint})

    if len(candidates) == 0:
        status = "empty" if query_successes > 0 else "failed"
        return semantic_payload, {
            "enabled": True,
            "status": status,
            "message": (
                "POI auto-candidate fetch completed but found no matching charging/rest-area candidates."
                if status == "empty"
                else "POI auto-candidate fetch failed."
            ),
            "probe_points": len(sample_points),
            "query_probe_points": len(query_sample_points),
            "fetched_candidates": 0,
            "radius_km": radius_km,
            "effective_query_radius_km": effective_query_radius_km,
            "max_candidates": max_candidates,
            "stop_buffer_km": stop_buffer_km,
            "effective_stop_buffer_km": effective_stop_buffer_km,
            "depot_buffer_km": depot_buffer_km,
            "desired_min_candidates": desired_min_candidates,
            "chunk_size": chunk_size,
            "max_endpoints": max_endpoints,
            "query_attempts": query_attempts,
            "query_successes": query_successes,
            "endpoints_used": unique_endpoints,
            "fallback_sources": sorted(set(fallback_sources)),
            "errors": errors[:8],
            "error": errors[-1] if errors else "",
            "elapsed_ms": elapsed_ms,
        }

    updated_payload = dict(semantic_payload)
    updated_payload["candidate_locations"] = candidates

    status = "ok" if query_successes == query_attempts else "partial"
    return updated_payload, {
        "enabled": True,
        "status": status,
        "message": (
            "POI auto-candidate fetch completed."
            if status == "ok"
            else "POI auto-candidate fetch partially completed."
        ),
        "probe_points": len(sample_points),
        "query_probe_points": len(query_sample_points),
        "fetched_candidates": len(candidates),
        "radius_km": radius_km,
        "effective_query_radius_km": effective_query_radius_km,
        "max_candidates": max_candidates,
        "stop_buffer_km": stop_buffer_km,
        "effective_stop_buffer_km": effective_stop_buffer_km,
        "depot_buffer_km": depot_buffer_km,
        "desired_min_candidates": desired_min_candidates,
        "chunk_size": chunk_size,
        "max_endpoints": max_endpoints,
        "query_attempts": query_attempts,
        "query_successes": query_successes,
        "endpoints_used": unique_endpoints,
        "fallback_sources": sorted(set(fallback_sources)),
        "errors": errors[:8],
        "elapsed_ms": elapsed_ms,
    }


HTML_PAGE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Basic VRP with OpenStreetMap</title>
    <link
      rel="stylesheet"
      href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin=""
    />
    <style>
      body { font-family: Arial, sans-serif; margin: 0; display: grid; grid-template-columns: 340px 1fr; height: 100vh; }
      #panel { padding: 12px; border-right: 1px solid #ddd; overflow: auto; }
      #map { height: 100vh; }
      .row { margin-bottom: 10px; }
      label { display:block; font-size: 13px; margin-bottom: 3px; }
      input, button, select { width: 100%; padding: 7px; }
      button { cursor: pointer; }
      pre { background: #f7f7f7; padding: 10px; font-size: 12px; overflow:auto; }
      .small { font-size: 12px; color: #555; }
      .inline-check {
        display: flex;
        align-items: center;
        gap: 8px;
      }
      .inline-check input {
        width: auto;
        margin: 0;
        padding: 0;
      }
      .semantic-anchor-shell {
        background: transparent;
        border: none;
      }
      .semantic-anchor-icon {
        width: 16px;
        height: 16px;
        border-radius: 50%;
        border: 2px solid #274c77;
        color: #274c77;
        background: #ffffff;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 9px;
        font-weight: 700;
        box-shadow: 0 1px 4px rgba(0,0,0,0.25);
      }
      .semantic-segment-icon {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 1px solid #274c77;
        background: #ffffff;
        box-shadow: 0 1px 3px rgba(0,0,0,0.22);
      }
      .legend {
        margin-top: 8px;
        padding: 8px;
        border: 1px solid #ddd;
        background: #fafafa;
        border-radius: 6px;
      }
      .legend-title {
        font-size: 12px;
        font-weight: 700;
        margin-bottom: 6px;
      }
      .legend-row {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 4px;
        font-size: 12px;
      }
      .legend-leaflet-pin {
        width: 12px;
        height: 20px;
        display: inline-block;
        object-fit: contain;
      }
      .legend-dot-customer {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 2px solid #222;
        background: #ffd54f;
        display: inline-block;
      }
      .legend-dot-poi {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 2px solid #274c77;
        background: #d6e7ff;
        display: inline-block;
      }
      .legend-dot-poi-charging {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 2px solid #0a8754;
        background: #d7ffe9;
        display: inline-block;
      }
      .legend-dot-poi-rest {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 2px solid #8b5a00;
        background: #ffe8c2;
        display: inline-block;
      }
      .legend-dot-segment {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        border: 1px solid #274c77;
        background: #ffffff;
        display: inline-block;
      }
      .legend-dot-scrape-risk {
        width: 14px;
        height: 14px;
        border-radius: 50%;
        border: 2px solid #7a1b16;
        background: #ffcabf;
        display: inline-block;
      }
      .legend-dot-scrape-info {
        width: 14px;
        height: 14px;
        border-radius: 50%;
        border: 2px solid #184c73;
        background: #d8ecff;
        display: inline-block;
      }
      .semantic-poi-icon {
        width: 18px;
        height: 18px;
        border-radius: 50%;
        border: 2px solid #274c77;
        color: #1a1a1a;
        background: #d6e7ff;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 8px;
        font-weight: 700;
        box-shadow: 0 1px 4px rgba(0,0,0,0.28);
      }
      .semantic-poi-icon.poi-charging {
        border-color: #0a8754;
        background: #d7ffe9;
      }
      .semantic-poi-icon.poi-rest {
        border-color: #8b5a00;
        background: #ffe8c2;
      }
      .scraping-marker-icon {
        width: 19px;
        height: 19px;
        border-radius: 50%;
        border: 2px solid #184c73;
        background: #d8ecff;
        color: #103c5f;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        font-weight: 700;
        box-shadow: 0 1px 4px rgba(0,0,0,0.3);
      }
      .scraping-marker-icon.risk {
        border-color: #7a1b16;
        background: #ffcabf;
        color: #7a1b16;
      }
      .scraping-marker-icon.info {
        border-color: #184c73;
        background: #d8ecff;
        color: #103c5f;
      }
      .semantic-popup { font-size: 12px; line-height: 1.35; }
      .semantic-popup h4 { margin: 0 0 5px 0; font-size: 13px; }
      .semantic-popup .muted { color: #666; }
      .llm-added-box {
        margin-top: 6px;
        padding: 8px;
        border: 1px solid #ddd;
        border-radius: 6px;
        background: #fafafa;
        font-size: 12px;
        line-height: 1.35;
        white-space: normal;
      }
      .llm-added-token {
        color: #b43c00;
        font-weight: 700;
      }
      .enrichment-progress-track {
        width: 100%;
        height: 8px;
        border-radius: 999px;
        background: #e6e6e6;
        overflow: hidden;
        margin-top: 8px;
      }
      .enrichment-progress-fill {
        width: 0%;
        height: 100%;
        background: linear-gradient(90deg, #184c73 0%, #0a8754 100%);
        transition: width 180ms ease;
      }
      .enrichment-stage-list {
        margin-top: 8px;
        display: grid;
        gap: 6px;
      }
      .enrichment-stage-row {
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 5px 7px;
        border: 1px solid #d9d9d9;
        border-radius: 6px;
        background: #ffffff;
        font-size: 12px;
      }
      .enrichment-stage-dot {
        width: 10px;
        height: 10px;
        border-radius: 50%;
        background: #b3b3b3;
        border: 1px solid #9a9a9a;
        flex: 0 0 auto;
      }
      .enrichment-stage-label {
        flex: 1 1 auto;
      }
      .enrichment-stage-state {
        color: #555;
        font-weight: 700;
        text-transform: uppercase;
        font-size: 10px;
        letter-spacing: 0.03em;
      }
      .enrichment-stage-row.active {
        border-color: #184c73;
        background: #eaf4ff;
      }
      .enrichment-stage-row.active .enrichment-stage-dot {
        background: #184c73;
        border-color: #184c73;
      }
      .enrichment-stage-row.done {
        border-color: #0a8754;
        background: #ebfbf3;
      }
      .enrichment-stage-row.done .enrichment-stage-dot {
        background: #0a8754;
        border-color: #0a8754;
      }
      .enrichment-stage-row.error {
        border-color: #7a1b16;
        background: #ffecea;
      }
      .enrichment-stage-row.error .enrichment-stage-dot {
        background: #7a1b16;
        border-color: #7a1b16;
      }
    </style>
  </head>
  <body>
    <div id="panel">
      <h2>Basic VRP</h2>
      <p class="small">1) Click map to add points.<br/>2) First point is depot.<br/>3) Others are customers.</p>

      <div class="row">
        <label>Click mode</label>
        <select id="mode">
          <option value="depot">Set depot</option>
          <option value="customer" selected>Add customer</option>
        </select>
      </div>

      <div class="row">
        <label>Demand for new customer</label>
        <input id="demand" type="number" min="1" value="1" />
      </div>

      <div class="row">
        <label>Vehicles</label>
        <input id="vehicles" type="number" min="1" value="2" />
      </div>

      <div class="row">
        <label>Capacity per vehicle</label>
        <input id="capacity" type="number" min="1" value="5" />
      </div>

      <div class="row">
        <label>Distance calculation</label>
        <select id="distanceMode">
          <option value="direct">Direct (Haversine)</option>
          <option value="osrm" selected>Real road kms (OSRM)</option>
        </select>
      </div>

      <div class="row">
        <label class="inline-check">
          <input id="autoPoiEnabled" type="checkbox" checked />
          <span>Auto semantic POI candidates (OSM/Overpass)</span>
        </label>
        <div class="small">If enabled, backend fetches candidate POIs in phase 2 (when you click <code>Add Municipality Trace</code>).</div>
      </div>

      <div class="row">
        <label class="inline-check">
          <input id="scrapingEnabled" type="checkbox" checked />
          <span>Bluesky scraping for municipality risk signals</span>
        </label>
        <div class="small">When enabled, backend scrapes recent Bluesky posts for the municipalities detected in your routes.</div>
      </div>
      <div class="row">
        <label>Bluesky language filter (BCP-47)</label>
        <input id="scrapingLang" type="text" value="es" />
        <div class="small">Use codes like <code>es</code> or <code>es-ES</code>. Leave empty to disable language filtering.</div>
      </div>

      <div class="row" style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
        <div>
          <label>POI radius (km)</label>
          <input id="poiAutoRadiusKm" type="number" min="0.2" step="0.2" value="3" />
        </div>
        <div>
          <label>POI max candidates</label>
          <input id="poiAutoMaxCandidates" type="number" min="10" step="5" value="25" />
        </div>
      </div>

      <div class="row">
        <label>HERE data source</label>
        <select id="hereDataSource">
          <option value="here">Live HERE APIs</option>
          <option value="emulator" selected>HERE emulator (randomized)</option>
        </select>
      </div>

      <div class="row">
        <label>HERE pipeline</label>
        <select id="hereMode">
          <option value="postprocessing" selected>HERE postprocessing (after VRP)</option>
          <option value="before_vrp">HERE before VRP (prefetch)</option>
        </select>
        <div class="small">Live HERE mode uses env var <code>HERE_API_KEY</code> (local.settings.json or Azure App Settings). Emulator mode needs no key.</div>
      </div>

      <div class="row" style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
        <div>
          <label>Forecast interval (min)</label>
          <input id="hereForecastInterval" type="number" min="30" step="30" value="120" />
        </div>
        <div>
          <label>Traffic radius (m)</label>
          <input id="hereTrafficRadius" type="number" min="50" step="50" value="300" />
        </div>
      </div>

      <div class="row">
        <label class="inline-check">
          <input id="municipalityLlmEnabled" type="checkbox" checked />
          <span>Municipality LLM enrichment</span>
        </label>
        <div class="small">Disable this to validate municipality trace without Azure OpenAI calls.</div>
      </div>

      <div class="row">
        <label>Municipality reverse geocoder</label>
        <select id="municipalityReverseSource">
          <option value="azure_maps_reverse" selected>Azure Maps (default)</option>
          <option value="nominatim_reverse">Nominatim (OSM)</option>
        </select>
      </div>

      <div class="row" style="display:grid; grid-template-columns: 1fr 1fr; gap:8px;">
        <div>
          <label>LLM timeout (sec)</label>
          <input id="municipalityLlmTimeoutSec" type="number" min="10" step="5" value="50" />
        </div>
        <div>
          <label>LLM retries</label>
          <input id="municipalityLlmRetries" type="number" min="0" step="1" value="1" />
        </div>
      </div>
      <div class="row">
        <label>LLM max tokens</label>
        <input id="municipalityLlmMaxTokens" type="number" min="200" step="50" value="4000" />
      </div>

      <div class="row" style="display:flex; gap:8px;">
        <button id="solveBtn">Solve VRP</button>
        <button id="municipalityBtn" disabled>Add Municipality Trace</button>
        <button id="clearBtn">Clear</button>
      </div>
      <div class="small">Run VRP first, then click "Add Municipality Trace" to enrich segments using OSM.</div>
      <div class="row" style="display:grid; grid-template-columns: 1fr auto; gap:8px; align-items:end;">
        <div>
          <label>Autogenerate preset</label>
          <select id="autogenPreset">
            <option value="small" selected>VRP (small)</option>
            <option value="midsized">VRP (midsized)</option>
          </select>
        </div>
        <button id="autogenBtn" type="button">Autogenerate</button>
      </div>
      <div class="small">Small uses your current 1 depot + 3 customers example. Midsized keeps the 1 depot + 9 customers reference scenario.</div>

      <div class="row">
        <strong>Output</strong>
        <pre id="output">Waiting for data...</pre>
        <strong>Enrichment Progress</strong>
        <div id="enrichmentProgress" class="llm-added-box">Idle. Run municipality enrichment to see staged progress.</div>
        <div class="enrichment-progress-track"><div id="enrichmentProgressFill" class="enrichment-progress-fill"></div></div>
        <div id="enrichmentStageList" class="enrichment-stage-list"></div>
        <strong>LLM Added Municipalities</strong>
        <div id="llmAddedSummary" class="llm-added-box">Run municipality enrichment to see LLM-added municipalities.</div>
        <strong>Bluesky Preview Posts</strong>
        <div id="scrapingPreviewSummary" class="llm-added-box">Run municipality enrichment with scraping enabled to preview Bluesky posts.</div>
        <div class="small">Click map dots to inspect semantic + weather + traffic details.</div>
        <div class="legend">
          <div class="legend-title">Map Legend</div>
          <div class="legend-row">
            <img class="legend-leaflet-pin" src="https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png" alt="Depot marker" />
            <span>Depot</span>
          </div>
          <div class="legend-row"><span class="legend-dot-customer"></span><span>Customer</span></div>
          <div class="legend-row"><span class="legend-dot-poi-charging"></span><span>POI charging station</span></div>
          <div class="legend-row"><span class="legend-dot-poi-rest"></span><span>POI rest area / services</span></div>
          <div class="legend-row"><span class="legend-dot-segment"></span><span>Segment context marker</span></div>
          <div class="legend-row"><span class="legend-dot-scrape-risk"></span><span>Social risk marker (!)</span></div>
          <div class="legend-row"><span class="legend-dot-scrape-info"></span><span>Social fallback marker (i)</span></div>
        </div>
      </div>
    </div>
    <div id="map"></div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
    <script>
      const map = L.map('map').setView([40.4168, -3.7038], 6);
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors'
      }).addTo(map);

      let depot = null;
      let customers = [];
      let markers = [];
      let routeLayers = [];
      let semanticMarkers = [];
      let scrapingMarkers = [];
      let phase1ExtraMarkers = [];
      let lastCandidateLocations = [];
      let customerId = 1;
      let lastSolvePayload = null;
      let lastSolveResult = null;
      let phase1PointByCoord = new Map();
      let enrichmentStageState = [];
      const OSRM_PUBLIC_BASE_URL = 'https://router.project-osrm.org';

      const colors = ['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#a65628'];

      function coordKey(lat, lng) {
        return `${Number(lat).toFixed(6)},${Number(lng).toFixed(6)}`;
      }

      function pointAdminLabel(value) {
        const text = String(value ?? '').trim();
        return text.length > 0 ? escapeHtml(text) : 'n/a';
      }

      function pickProvinceName(address) {
        if (!address || typeof address !== 'object') {
          return '';
        }
        const fields = ['province', 'state', 'county', 'region', 'state_district'];
        for (const field of fields) {
          const value = String(address?.[field] || '').trim();
          if (value.length > 0) {
            return value;
          }
        }
        return '';
      }

      function sameCoords(aLat, aLng, bLat, bLng, eps = 1e-6) {
        return Math.abs(Number(aLat) - Number(bLat)) <= eps && Math.abs(Number(aLng) - Number(bLng)) <= eps;
      }

      function isKnownBasePoint(lat, lng) {
        if (depot && sameCoords(depot.lat, depot.lng, lat, lng)) {
          return true;
        }
        return customers.some(c => sameCoords(c.lat, c.lng, lat, lng));
      }

      function phase1PointPopupHtml(row) {
        const role = String(row?.role || 'customer').trim() || 'customer';
        return `
          <div class="semantic-popup">
            <h4>&bull; Phase 1 ${escapeHtml(role)}</h4>
            <div><strong>Municipality:</strong> ${pointAdminLabel(row?.municipality_name)}</div>
            <div><strong>Province:</strong> ${pointAdminLabel(row?.province_name)}</div>
            <div><strong>Province capital:</strong> ${pointAdminLabel(row?.province_capital_name)}</div>
            <div><strong>Status:</strong> ${pointAdminLabel(row?.status)}</div>
            <div class="muted">Lat/Lng: ${escapeHtml(row?.lat)}, ${escapeHtml(row?.lng)}</div>
          </div>
        `;
      }

      function clearPhase1ExtraMarkers() {
        phase1ExtraMarkers.forEach(m => map.removeLayer(m));
        phase1ExtraMarkers = [];
      }

      function renderPhase1ExtraMarkers() {
        clearPhase1ExtraMarkers();
        for (const row of phase1PointByCoord.values()) {
          if (typeof row?.lat !== 'number' || typeof row?.lng !== 'number') {
            continue;
          }
          const role = String(row?.role || '').trim().toLowerCase();
          const stopIds = Array.isArray(row?.stop_ids) ? row.stop_ids : [];
          const customerIds = Array.isArray(row?.customer_ids) ? row.customer_ids : [];
          if (role === 'depot' || role === 'customer' || stopIds.length > 0 || customerIds.length > 0) {
            continue;
          }
          if (isKnownBasePoint(row.lat, row.lng)) {
            continue;
          }
          const marker = L.circleMarker([row.lat, row.lng], {
            radius: 8,
            color: '#222',
            fillColor: '#ffd54f',
            fillOpacity: 0.95
          })
            .addTo(map)
            .bindPopup(phase1PointPopupHtml(row), { maxWidth: 320 });
          phase1ExtraMarkers.push(marker);
        }
      }

      function depotPopupHtml() {
        const info = depot ? phase1PointByCoord.get(coordKey(depot.lat, depot.lng)) : null;
        return `
          <div class="semantic-popup">
            <h4>&bull; Depot</h4>
            <div><strong>Municipality:</strong> ${pointAdminLabel(info?.municipality_name)}</div>
            <div><strong>Province:</strong> ${pointAdminLabel(info?.province_name)}</div>
            <div><strong>Province capital:</strong> ${pointAdminLabel(info?.province_capital_name)}</div>
            <div><strong>Status:</strong> ${pointAdminLabel(info?.status)}</div>
            <div class="muted">Lat/Lng: ${escapeHtml(depot?.lat)}, ${escapeHtml(depot?.lng)}</div>
          </div>
        `;
      }

      function customerPopupHtml(customer) {
        const info = customer ? phase1PointByCoord.get(coordKey(customer.lat, customer.lng)) : null;
        return `
          <div class="semantic-popup">
            <h4>&bull; Customer ${escapeHtml(customer?.id)}</h4>
            <div><strong>Demand:</strong> ${escapeHtml(customer?.demand)}</div>
            <div><strong>Municipality:</strong> ${pointAdminLabel(info?.municipality_name)}</div>
            <div><strong>Province:</strong> ${pointAdminLabel(info?.province_name)}</div>
            <div><strong>Province capital:</strong> ${pointAdminLabel(info?.province_capital_name)}</div>
            <div><strong>Status:</strong> ${pointAdminLabel(info?.status)}</div>
            <div class="muted">Lat/Lng: ${escapeHtml(customer?.lat)}, ${escapeHtml(customer?.lng)}</div>
          </div>
        `;
      }

      function redrawPoints() {
        markers.forEach(m => map.removeLayer(m));
        markers = [];

        if (depot) {
          markers.push(
            L.marker([depot.lat, depot.lng], { title: 'Depot' })
              .addTo(map)
              .bindPopup(depotPopupHtml(), { maxWidth: 320 })
          );
        }

        for (const c of customers) {
          markers.push(
            L.circleMarker([c.lat, c.lng], {
              radius: 8,
              color: '#222',
              fillColor: '#ffd54f',
              fillOpacity: 0.95
            })
              .addTo(map)
              .bindPopup(customerPopupHtml(c), { maxWidth: 320 })
          );
        }

        renderPhase1ExtraMarkers();
      }

      function clearRoutes() {
        routeLayers.forEach(l => map.removeLayer(l));
        routeLayers = [];
        semanticMarkers.forEach(m => map.removeLayer(m));
        semanticMarkers = [];
        scrapingMarkers.forEach(m => map.removeLayer(m));
        scrapingMarkers = [];
      }

      function setMunicipalityButtonState(enabled, busy = false) {
        const municipalityBtn = document.getElementById('municipalityBtn');
        municipalityBtn.disabled = !enabled || busy;
        municipalityBtn.textContent = busy ? 'Tracing municipalities...' : 'Add Municipality Trace';
      }

      function setEnrichmentProgress(message) {
        const box = document.getElementById('enrichmentProgress');
        if (!box) {
          return;
        }
        box.textContent = String(message || '').trim() || 'Idle.';
      }

      function renderEnrichmentStages() {
        const list = document.getElementById('enrichmentStageList');
        const fill = document.getElementById('enrichmentProgressFill');
        if (!list || !fill) {
          return;
        }
        if (!Array.isArray(enrichmentStageState) || enrichmentStageState.length === 0) {
          list.innerHTML = '';
          fill.style.width = '0%';
          return;
        }

        const doneCount = enrichmentStageState.filter(row => row?.status === 'done').length;
        const total = enrichmentStageState.length;
        const pct = total > 0 ? Math.round((doneCount / total) * 100) : 0;
        fill.style.width = `${pct}%`;

        const stateLabel = (status) => {
          if (status === 'done') {
            return 'Done';
          }
          if (status === 'active') {
            return 'Running';
          }
          if (status === 'error') {
            return 'Failed';
          }
          return 'Pending';
        };

        list.innerHTML = enrichmentStageState
          .map(row => {
            const status = String(row?.status || 'pending').trim().toLowerCase();
            const label = escapeHtml(String(row?.label || 'Stage'));
            const badge = escapeHtml(stateLabel(status));
            return `
              <div class="enrichment-stage-row ${escapeHtml(status)}">
                <span class="enrichment-stage-dot"></span>
                <span class="enrichment-stage-label">${label}</span>
                <span class="enrichment-stage-state">${badge}</span>
              </div>
            `;
          })
          .join('');
      }

      function setEnrichmentStages(labels) {
        enrichmentStageState = Array.isArray(labels)
          ? labels.map(label => ({ label: String(label || 'Stage'), status: 'pending' }))
          : [];
        renderEnrichmentStages();
      }

      function setEnrichmentStageStatus(index, status) {
        if (!Array.isArray(enrichmentStageState)) {
          return;
        }
        if (!Number.isInteger(index) || index < 0 || index >= enrichmentStageState.length) {
          return;
        }
        enrichmentStageState[index] = {
          ...enrichmentStageState[index],
          status: String(status || 'pending').trim().toLowerCase() || 'pending'
        };
        renderEnrichmentStages();
      }

      function resetEnrichmentPanels() {
        setEnrichmentProgress('Idle. Run municipality enrichment to see staged progress.');
        setEnrichmentStages([]);
        const llmBox = document.getElementById('llmAddedSummary');
        if (llmBox) {
          llmBox.textContent = 'Run municipality enrichment to see LLM-added municipalities.';
        }
        const scrapeBox = document.getElementById('scrapingPreviewSummary');
        if (scrapeBox) {
          scrapeBox.textContent = 'Run municipality enrichment with scraping enabled to preview Bluesky posts.';
        }
      }

      function readPoiAutoSettings() {
        const enabled = document.getElementById('autoPoiEnabled')?.checked === true;
        const radiusRaw = Number.parseFloat(document.getElementById('poiAutoRadiusKm')?.value || '3');
        const maxRaw = Number.parseInt(document.getElementById('poiAutoMaxCandidates')?.value || '25', 10);
        return {
          poi_auto_enabled: enabled,
          poi_auto_radius_km: Number.isFinite(radiusRaw) ? Math.max(0.2, radiusRaw) : 3,
          poi_auto_max_candidates: Number.isFinite(maxRaw) ? Math.max(10, maxRaw) : 25
        };
      }

      function readScrapingSettings() {
        const scrapingLang = String(
          document.getElementById('scrapingLang')?.value || ''
        ).trim().toLowerCase();
        return {
          scraping_enabled: document.getElementById('scrapingEnabled')?.checked === true,
          scraping_lang: scrapingLang
        };
      }

      function readMunicipalityLlmSettings() {
        const enabled = document.getElementById('municipalityLlmEnabled')?.checked === true;
        const timeoutRaw = Number.parseInt(
          document.getElementById('municipalityLlmTimeoutSec')?.value || '50',
          10
        );
        const retriesRaw = Number.parseInt(
          document.getElementById('municipalityLlmRetries')?.value || '1',
          10
        );
        const maxTokensRaw = Number.parseInt(
          document.getElementById('municipalityLlmMaxTokens')?.value || '4000',
          10
        );
        return {
          municipality_llm_enrichment_enabled: enabled,
          municipality_llm_timeout_sec: Number.isFinite(timeoutRaw) ? Math.max(10, timeoutRaw) : 50,
          municipality_llm_retries: Number.isFinite(retriesRaw) ? Math.max(0, retriesRaw) : 1,
          municipality_llm_max_tokens: Number.isFinite(maxTokensRaw) ? Math.max(200, maxTokensRaw) : 4000
        };
      }

      function readMunicipalityReverseSource() {
        const value = String(
          document.getElementById('municipalityReverseSource')?.value || 'azure_maps_reverse'
        ).trim().toLowerCase();
        return value === 'nominatim_reverse' ? 'nominatim_reverse' : 'azure_maps_reverse';
      }

      function escapeHtml(value) {
        return String(value ?? '')
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;')
          .replace(/'/g, '&#39;');
      }

      function pickSegmentContext(routeSemantic, nearestSegmentIndex) {
        const segments = Array.isArray(routeSemantic?.segment_context) ? routeSemantic.segment_context : [];
        return segments.find(s => s.segment_index === nearestSegmentIndex) || null;
      }

      function toUtcLabel(value) {
        if (!value) {
          return 'n/a';
        }
        try {
          return new Date(value).toISOString().slice(0, 16).replace('T', ' ') + 'Z';
        } catch (_) {
          return escapeHtml(value);
        }
      }

      function summarizeWeatherForecast(weather) {
        const forecast = weather?.forecast_24h || {};
        if (forecast?.status !== 'forecasted') {
          return 'unknown';
        }
        const score = Number(forecast?.worst_case_score ?? 0).toFixed(2);
        const slots = Array.isArray(forecast?.worst_slots) ? forecast.worst_slots : [];
        const labels = slots.slice(0, 3).map(slot => toUtcLabel(slot?.start_utc)).join(', ');
        return `score ${escapeHtml(score)} at ${escapeHtml(labels || 'n/a')}`;
      }

      function summarizeTrafficForecast(traffic) {
        const forecast = traffic?.forecast_24h || {};
        if (forecast?.status !== 'forecasted') {
          return 'unknown';
        }
        const ratio = Number(forecast?.worst_case_delay_ratio ?? 0).toFixed(3);
        const delay = Number(forecast?.worst_case_delay_seconds ?? 0).toFixed(0);
        const slots = Array.isArray(forecast?.worst_slots) ? forecast.worst_slots : [];
        const labels = slots.slice(0, 3).map(slot => toUtcLabel(slot?.departure_utc)).join(', ');
        return `ratio ${escapeHtml(ratio)} (+${escapeHtml(delay)}s) at ${escapeHtml(labels || 'n/a')}`;
      }

      function municipalityOutputNotice(data) {
        const semantic = data?.semantic_layer || {};
        const explicit = semantic?.municipality_post_output_notice;
        if (typeof explicit === 'string' && explicit.trim().length > 0) {
          return explicit.trim();
        }

        const routeGeometry = semantic?.municipality_api?.route_geometry || {};
        const fallbackToStraight = Number(
          routeGeometry?.fallback_to_straight
          ?? semantic?.summary?.municipality_route_geometry_fallback_to_straight
          ?? 0
        );
        const phase1Unknown = Number(semantic?.municipality_api?.phase1?.unknown ?? 0);
        const phase1Failed = Number(semantic?.municipality_api?.phase1?.failed ?? 0);
        const status = String(semantic?.municipality_api?.status || '').trim().toLowerCase();
        const warnings = [];

        if (fallbackToStraight > 0) {
          warnings.push(
            `WARNING: Municipality tracing used straight-line fallback in ${fallbackToStraight} segment(s).`
          );
        }
        if (phase1Unknown > 0 || phase1Failed > 0) {
          warnings.push(
            `WARNING: Municipality phase 1 unresolved coordinates (unknown=${phase1Unknown}, failed=${phase1Failed}).`
          );
        }
        if (status && status !== 'ok') {
          warnings.push(`WARNING: Municipality API status is '${status}'.`);
        }
        if (warnings.length === 0) {
          return 'Municipality fallback warning: none.';
        }
        return warnings.join(' | ');
      }

      function llmAddedMunicipalityRows(data) {
        const semantic = data?.semantic_layer || {};
        const rows = Array.isArray(semantic?.municipality_api?.llm?.added_municipalities_by_route)
          ? semantic.municipality_api.llm.added_municipalities_by_route
          : [];
        if (rows.length > 0) {
          return rows;
        }
        const routes = Array.isArray(semantic?.routes) ? semantic.routes : [];
        const fallback = [];
        for (const route of routes) {
          const added = Array.isArray(route?.municipality_llm?.added_municipalities)
            ? route.municipality_llm.added_municipalities
            : [];
          if (added.length > 0) {
            fallback.push({
              vehicle: route?.vehicle,
              added_municipalities: added
            });
          }
        }
        return fallback;
      }

      function renderLlmAddedMunicipalities(data) {
        const box = document.getElementById('llmAddedSummary');
        const semantic = data?.semantic_layer || {};
        const llm = semantic?.municipality_api?.llm || {};
        const status = String(llm?.status || '').trim();
        const rows = llmAddedMunicipalityRows(data);

        if (rows.length === 0) {
          if (status.length > 0 && status.toLowerCase() === 'ok') {
            box.textContent = 'LLM enrichment ran, but no extra municipalities were added for this route.';
          } else if (status.length > 0 && status.toLowerCase() !== 'ok') {
            const message = String(llm?.message || 'No additions reported').trim();
            const errors = Array.isArray(llm?.errors) ? llm.errors : [];
            const firstError = String(errors[0] || '').trim();
            box.textContent = `No LLM-added municipalities. LLM status: ${status}${message ? ` (${message})` : ''}${firstError ? ` First error: ${firstError}` : ''}.`;
          } else {
            box.textContent = 'No LLM-added municipalities reported.';
          }
          return;
        }

        const lines = [];
        for (const row of rows.slice(0, 12)) {
          const vehicle = escapeHtml(row?.vehicle ?? '?');
          const additions = Array.isArray(row?.added_municipalities) ? row.added_municipalities : [];
          const tokens = [];
          for (const item of additions.slice(0, 20)) {
            if (!item || typeof item !== 'object') {
              continue;
            }
            const name = String(item?.name || '').trim();
            if (!name) {
              continue;
            }
            const seg = Number.isInteger(item?.segment_index)
              ? ` (seg ${item.segment_index + 1})`
              : '';
            const reason = String(item?.reason || '').trim();
            const reasonLabel = reason.length > 0 ? ` - ${reason}` : '';
            tokens.push(`<span class="llm-added-token">${escapeHtml(name)}</span>${escapeHtml(seg)}${escapeHtml(reasonLabel)}`);
          }
          if (tokens.length > 0) {
            lines.push(`<strong>Vehicle ${vehicle}:</strong> ${tokens.join(', ')}`);
          }
        }
        box.innerHTML = lines.length > 0 ? lines.join('<br/>') : 'No LLM-added municipalities reported.';
      }

      function renderScrapingPreviewSummary(data) {
        const box = document.getElementById('scrapingPreviewSummary');
        if (!box) {
          return;
        }
        const scraping = data?.scraping || {};
        const rows = Array.isArray(scraping?.preview_rows) ? scraping.preview_rows : [];
        const status = String(scraping?.status || '').trim().toLowerCase();
        const enabled = scraping?.enabled === true;
        const stageAllowed = scraping?.stage_allowed !== false;

        if (!enabled) {
          box.textContent = 'Bluesky scraping is disabled.';
          return;
        }
        if (!stageAllowed || status === 'skipped_stage') {
          const reason = String(scraping?.stage_skip_reason || '').trim();
          box.textContent = reason
            ? `Bluesky scraping skipped by stage policy (${reason}).`
            : 'Bluesky scraping skipped by stage policy.';
          return;
        }
        if (rows.length === 0) {
          box.textContent = 'No Bluesky preview rows returned for this run.';
          return;
        }

        const topRows = rows.slice(0, 20);
        const rendered = [];
        for (const row of topRows) {
          const location = escapeHtml(String(row?.location_name || 'Unknown'));
          const username = escapeHtml(String(row?.username || 'unknown'));
          const created = escapeHtml(String(row?.created_at || 'n/a'));
          const classification = escapeHtml(String(row?.classification || '').trim() || 'post');
          const rawText = String(row?.text || '').trim();
          const text = rawText.length > 180 ? `${rawText.slice(0, 177)}...` : rawText;
          const safeText = escapeHtml(text || '(empty text)');
          const url = String(row?.tweet_url || '').trim();
          const linkHtml = url
            ? `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">Open post</a>`
            : '';
          rendered.push(
            `<strong>${location}</strong> [${classification}] @${username} (${created})<br/>${safeText}${linkHtml ? `<br/>${linkHtml}` : ''}`
          );
        }
        const more = rows.length > topRows.length
          ? `<br/><br/>Showing ${topRows.length} of ${rows.length} posts.`
          : '';
        box.innerHTML = rendered.join('<br/><br/>') + more;
      }

      function municipalityNameKey(value) {
        const text = String(value ?? '').trim();
        if (text.length === 0) {
          return '';
        }
        return text.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
      }

      function llmAddedNamesBySegment(routeSemantic) {
        const map = new Map();
        const additions = Array.isArray(routeSemantic?.municipality_llm?.added_municipalities)
          ? routeSemantic.municipality_llm.added_municipalities
          : [];
        for (const row of additions) {
          if (!row || typeof row !== 'object') {
            continue;
          }
          const segmentIndex = Number.isInteger(row?.segment_index) ? row.segment_index : null;
          const name = String(row?.name || '').trim();
          if (segmentIndex === null || name.length === 0) {
            continue;
          }
          const list = map.get(segmentIndex) || [];
          list.push(name);
          map.set(segmentIndex, list);
        }
        return map;
      }

      function mergeSegmentMunicipalityNamesWithLlm(items, llmAddedNames) {
        const names = Array.isArray(items)
          ? items.filter(name => typeof name === 'string' && name.trim().length > 0)
          : [];
        if (!Array.isArray(llmAddedNames) || llmAddedNames.length === 0 || names.length < 2) {
          return names;
        }
        const seen = new Set(names.map(name => municipalityNameKey(name)).filter(Boolean));
        let insertIndex = Math.max(1, names.length - 1);
        for (const candidateRaw of llmAddedNames) {
          const candidate = String(candidateRaw || '').trim();
          const key = municipalityNameKey(candidate);
          if (!key || seen.has(key)) {
            continue;
          }
          names.splice(insertIndex, 0, candidate);
          insertIndex += 1;
          seen.add(key);
        }
        return names;
      }

      function orderedVectorLabel(items, llmAddedNames = []) {
        const names = mergeSegmentMunicipalityNamesWithLlm(items, llmAddedNames);
        if (names.length === 0) {
          return 'n/a';
        }
        const highlighted = new Set(
          (Array.isArray(llmAddedNames) ? llmAddedNames : [])
            .map(name => municipalityNameKey(name))
            .filter(Boolean)
        );
        return names
          .map(name => {
            const escaped = escapeHtml(name);
            const key = municipalityNameKey(name);
            if (key && highlighted.has(key)) {
              return `<span class="llm-added-token">${escaped}</span>`;
            }
            return escaped;
          })
          .join(' -> ');
      }

      function segmentVectors(segmentContext, llmAddedNames = []) {
        return {
          province: orderedVectorLabel(segmentContext?.province_names),
          provinceCapital: orderedVectorLabel(segmentContext?.province_capital_names),
          municipality: orderedVectorLabel(segmentContext?.municipality_names, llmAddedNames),
          roads: orderedVectorLabel(segmentContext?.road_names)
        };
      }

      function semanticPopupHtml(routeSemantic, location, segmentContext) {
        const weather = segmentContext?.weather || {};
        const traffic = segmentContext?.traffic || {};
        const name = location?.name ? escapeHtml(location.name) : `Location ${escapeHtml(location?.id ?? '')}`;
        const category = escapeHtml(location?.semantic_category || 'other');
        const relevance = Number(location?.relevance_score ?? 0).toFixed(3);
        const dist = Number(location?.distance_to_route_km ?? 0).toFixed(3);
        const detour = Number(location?.estimated_detour_km ?? 0).toFixed(3);
        const eta = segmentContext?.eta_utc ? escapeHtml(segmentContext.eta_utc) : 'n/a';
        const weatherSummary = weather?.status === 'observed'
          ? `${escapeHtml(weather.condition || 'n/a')}, ${escapeHtml(weather.temperature_c ?? 'n/a')} C`
          : 'unknown';
        const trafficSummary = traffic?.status === 'observed'
          ? `congestion ${escapeHtml(traffic.congestion_level || 'n/a')}, speed ${escapeHtml(traffic.speed_kmh ?? 'n/a')} km/h`
          : 'unknown';
        const weatherForecastSummary = summarizeWeatherForecast(weather);
        const trafficForecastSummary = summarizeTrafficForecast(traffic);
        const bySegment = llmAddedNamesBySegment(routeSemantic);
        const segmentIndex = Number.isInteger(segmentContext?.segment_index)
          ? segmentContext.segment_index
          : null;
        const llmAddedNames = segmentIndex !== null ? (bySegment.get(segmentIndex) || []) : [];
        const segmentVector = segmentVectors(segmentContext, llmAddedNames);

        return `
          <div class="semantic-popup">
            <h4>&bull; ${name}</h4>
            <div><strong>Category:</strong> ${category}</div>
            <div><strong>Relevance:</strong> ${relevance}</div>
            <div><strong>Distance to route:</strong> ${dist} km</div>
            <div><strong>Estimated detour:</strong> ${detour} km</div>
            <div><strong>Segment ETA:</strong> ${eta}</div>
            <div><strong>Weather:</strong> ${weatherSummary}</div>
            <div><strong>Weather 24h worst:</strong> ${weatherForecastSummary}</div>
            <div><strong>Traffic:</strong> ${trafficSummary}</div>
            <div><strong>Traffic 24h worst:</strong> ${trafficForecastSummary}</div>
            <div><strong>Segment province vector:</strong> ${segmentVector.province}</div>
            <div><strong>Segment province capital vector:</strong> ${segmentVector.provinceCapital}</div>
            <div><strong>Segment municipality vector:</strong> ${segmentVector.municipality}</div>
            <div><strong>Segment road vector:</strong> ${segmentVector.roads}</div>
            <div class="muted">Lat/Lng: ${escapeHtml(location.lat)}, ${escapeHtml(location.lng)}</div>
          </div>
        `;
      }

      function segmentPopupHtml(routeSemantic, segmentContext) {
        const weather = segmentContext?.weather || {};
        const traffic = segmentContext?.traffic || {};
        const eta = segmentContext?.eta_utc ? escapeHtml(segmentContext.eta_utc) : 'n/a';
        const distance = Number(segmentContext?.distance_km ?? 0).toFixed(3);
        const weatherSummary = weather?.status === 'observed'
          ? `${escapeHtml(weather.condition || 'n/a')}, ${escapeHtml(weather.temperature_c ?? 'n/a')} C`
          : 'unknown';
        const trafficSummary = traffic?.status === 'observed'
          ? `congestion ${escapeHtml(traffic.congestion_level || 'n/a')}, speed ${escapeHtml(traffic.speed_kmh ?? 'n/a')} km/h`
          : 'unknown';
        const weatherForecastSummary = summarizeWeatherForecast(weather);
        const trafficForecastSummary = summarizeTrafficForecast(traffic);
        const bySegment = llmAddedNamesBySegment(routeSemantic);
        const segmentIndex = Number.isInteger(segmentContext?.segment_index)
          ? segmentContext.segment_index
          : null;
        const llmAddedNames = segmentIndex !== null ? (bySegment.get(segmentIndex) || []) : [];
        const segmentVector = segmentVectors(segmentContext, llmAddedNames);

        return `
          <div class="semantic-popup">
            <h4>&bull; Segment context</h4>
            <div><strong>Segment:</strong> #${escapeHtml((segmentContext?.segment_index ?? 0) + 1)}</div>
            <div><strong>Segment distance:</strong> ${distance} km</div>
            <div><strong>ETA:</strong> ${eta}</div>
            <div><strong>Weather:</strong> ${weatherSummary}</div>
            <div><strong>Weather 24h worst:</strong> ${weatherForecastSummary}</div>
            <div><strong>Traffic:</strong> ${trafficSummary}</div>
            <div><strong>Traffic 24h worst:</strong> ${trafficForecastSummary}</div>
            <div><strong>Segment province vector:</strong> ${segmentVector.province}</div>
            <div><strong>Segment province capital vector:</strong> ${segmentVector.provinceCapital}</div>
            <div><strong>Segment municipality vector:</strong> ${segmentVector.municipality}</div>
            <div><strong>Segment road vector:</strong> ${segmentVector.roads}</div>
            <div class="muted">No semantic POI matched in this corridor window.</div>
          </div>
        `;
      }

      function semanticPoiIconHtml(location) {
        const category = String(location?.semantic_category || '').trim().toLowerCase();
        let symbol = 'POI';
        let categoryClass = '';
        if (category === 'charging') {
          symbol = 'EV';
          categoryClass = 'poi-charging';
        } else if (category === 'rest_area') {
          symbol = 'RS';
          categoryClass = 'poi-rest';
        }
        return `<div class="semantic-poi-icon ${categoryClass}">${escapeHtml(symbol)}</div>`;
      }

      function candidatePoiPopupHtml(location) {
        const name = location?.name ? escapeHtml(location.name) : `Candidate ${escapeHtml(location?.id ?? '')}`;
        const category = escapeHtml(location?.semantic_category || 'other');
        const source = escapeHtml(location?.source || 'unknown');
        const idText = escapeHtml(location?.id || '');
        const tags = (location && typeof location === 'object' && location.tags && typeof location.tags === 'object')
          ? location.tags
          : {};
        const keyTags = ['amenity', 'highway', 'operator', 'name']
          .filter(k => Object.prototype.hasOwnProperty.call(tags, k))
          .map(k => `${k}=${tags[k]}`)
          .slice(0, 4)
          .join(', ');

        return `
          <div class="semantic-popup">
            <h4>&bull; ${name}</h4>
            <div><strong>Category:</strong> ${category}</div>
            <div><strong>Source:</strong> ${source}</div>
            <div><strong>ID:</strong> ${idText || 'n/a'}</div>
            <div><strong>Tags:</strong> ${escapeHtml(keyTags || 'n/a')}</div>
            <div class="muted">Lat/Lng: ${escapeHtml(location?.lat)}, ${escapeHtml(location?.lng)}</div>
            <div class="muted">Shown as fallback candidate (not matched in semantic corridor).</div>
          </div>
        `;
      }

      function renderSemanticAnchors(data) {
        semanticMarkers.forEach(m => map.removeLayer(m));
        semanticMarkers = [];

        const semanticRoutes = data?.semantic_layer?.routes;
        if (!Array.isArray(semanticRoutes)) {
          return;
        }
        const fallbackCandidates = Array.isArray(data?.candidate_locations) ? data.candidate_locations : [];
        const seenPoiKeys = new Set();
        let semanticPoiCount = 0;

        for (const routeSemantic of semanticRoutes) {
          const vehicle = routeSemantic?.vehicle;
          const color = colors[((Number(vehicle) || 1) - 1) % colors.length];
          const semanticLocations = Array.isArray(routeSemantic?.semantic_locations)
            ? routeSemantic.semantic_locations
            : [];
          const segmentContext = Array.isArray(routeSemantic?.segment_context)
            ? routeSemantic.segment_context
            : [];

          for (const location of semanticLocations) {
            if (typeof location?.lat !== 'number' || typeof location?.lng !== 'number') {
              continue;
            }

            const linkedSegment = pickSegmentContext(routeSemantic, location.nearest_segment_index);
            const popupHtml = semanticPopupHtml(routeSemantic, location, linkedSegment);
            const icon = L.divIcon({
              className: 'semantic-anchor-shell',
              html: semanticPoiIconHtml(location),
              iconSize: [18, 18],
              iconAnchor: [9, 9],
              popupAnchor: [0, -10]
            });

            const marker = L.marker([location.lat, location.lng], { icon }).addTo(map);
            marker.bindPopup(popupHtml, { maxWidth: 310 });
            semanticMarkers.push(marker);
            semanticPoiCount += 1;
            seenPoiKeys.add(coordKey(location.lat, location.lng));
          }

          if (segmentContext.length > 0) {
            let sampledSegments = segmentContext;
            if (segmentContext.length > 12) {
              const step = Math.ceil(segmentContext.length / 12);
              sampledSegments = segmentContext.filter((_, index) => index % step === 0).slice(0, 12);
            }

            for (const segment of sampledSegments) {
              const midpoint = segment?.midpoint;
              if (typeof midpoint?.lat !== 'number' || typeof midpoint?.lng !== 'number') {
                continue;
              }

              const popupHtml = segmentPopupHtml(routeSemantic, segment);
              const icon = L.divIcon({
                className: 'semantic-anchor-shell',
                html: `<div class="semantic-segment-icon" style="border-color:${color};"></div>`,
                iconSize: [12, 12],
                iconAnchor: [6, 6],
                popupAnchor: [0, -8]
              });

              const marker = L.marker([midpoint.lat, midpoint.lng], { icon }).addTo(map);
              marker.bindPopup(popupHtml, { maxWidth: 300 });
              semanticMarkers.push(marker);
            }
          }
        }

        if (fallbackCandidates.length > 0) {
          for (const location of fallbackCandidates.slice(0, 80)) {
            if (typeof location?.lat !== 'number' || typeof location?.lng !== 'number') {
              continue;
            }
            const key = coordKey(location.lat, location.lng);
            if (seenPoiKeys.has(key)) {
              continue;
            }
            seenPoiKeys.add(key);
            const icon = L.divIcon({
              className: 'semantic-anchor-shell',
              html: semanticPoiIconHtml(location),
              iconSize: [18, 18],
              iconAnchor: [9, 9],
              popupAnchor: [0, -10]
            });
            const marker = L.marker([location.lat, location.lng], { icon }).addTo(map);
            marker.bindPopup(candidatePoiPopupHtml(location), { maxWidth: 310 });
            semanticMarkers.push(marker);
          }
        }
      }

      function clearScrapingMarkers() {
        scrapingMarkers.forEach(m => map.removeLayer(m));
        scrapingMarkers = [];
      }

      function scrapingMarkerIconHtml(iconType) {
        const normalized = String(iconType || '').trim().toLowerCase() === 'risk' ? 'risk' : 'info';
        const symbol = normalized === 'risk' ? '!' : 'i';
        return `<div class="scraping-marker-icon ${normalized}">${escapeHtml(symbol)}</div>`;
      }

      function scrapingPopupHtml(point, previewRow) {
        const locationName = escapeHtml(point?.location_name || 'Unknown location');
        const iconType = String(point?.icon_type || 'info').trim().toLowerCase() === 'risk' ? 'risk' : 'info';
        const iconLabel = iconType === 'risk' ? 'Risk signal posts found' : 'Fallback informational posts (no risk hit)';
        const riskCount = Number(point?.risk_count ?? 0);
        const fallbackCount = Number(point?.fallback_count ?? 0);
        const coordSource = escapeHtml(point?.coordinate_source || 'unknown');
        const sourceLabel = iconType === 'risk' ? 'Risk marker (!)' : 'Info marker (i)';

        let previewHtml = '<div class="muted">No preview post available.</div>';
        if (previewRow && typeof previewRow === 'object') {
          const text = String(previewRow?.text || '').trim();
          const shortText = text.length > 180 ? `${text.slice(0, 177)}...` : text;
          const tweetUrl = String(previewRow?.tweet_url || '').trim();
          const created = escapeHtml(previewRow?.created_at || 'n/a');
          const username = escapeHtml(previewRow?.username || 'unknown');
          previewHtml = `
            <div><strong>Sample post:</strong> [${created}] @${username}</div>
            <div>${escapeHtml(shortText || '(empty text)')}</div>
            ${tweetUrl ? `<div><a href="${escapeHtml(tweetUrl)}" target="_blank" rel="noopener noreferrer">Open post</a></div>` : ''}
          `;
        }

        return `
          <div class="semantic-popup">
            <h4>&bull; ${locationName}</h4>
            <div><strong>Marker:</strong> ${escapeHtml(sourceLabel)}</div>
            <div><strong>Meaning:</strong> ${escapeHtml(iconLabel)}</div>
            <div><strong>Risk count:</strong> ${escapeHtml(riskCount)}</div>
            <div><strong>Fallback count:</strong> ${escapeHtml(fallbackCount)}</div>
            <div><strong>Coordinate source:</strong> ${coordSource}</div>
            ${previewHtml}
          </div>
        `;
      }

      function renderScrapingMarkers(data) {
        clearScrapingMarkers();
        const scraping = data?.scraping || {};
        const points = Array.isArray(scraping?.municipality_points) ? scraping.municipality_points : [];
        if (points.length === 0) {
          return;
        }

        const previewRows = Array.isArray(scraping?.preview_rows) ? scraping.preview_rows : [];
        const previewByLocation = new Map();
        for (const row of previewRows) {
          const key = municipalityNameKey(row?.location_name);
          if (!key || previewByLocation.has(key)) {
            continue;
          }
          previewByLocation.set(key, row);
        }

        for (const point of points) {
          if (typeof point?.lat !== 'number' || typeof point?.lng !== 'number') {
            continue;
          }
          const iconType = String(point?.icon_type || 'info').trim().toLowerCase() === 'risk' ? 'risk' : 'info';
          const previewRow = previewByLocation.get(municipalityNameKey(point?.location_name));
          const marker = L.marker([point.lat, point.lng], {
            icon: L.divIcon({
              className: 'semantic-anchor-shell',
              html: scrapingMarkerIconHtml(iconType),
              iconSize: [19, 19],
              iconAnchor: [9, 9],
              popupAnchor: [0, -10]
            })
          }).addTo(map);
          marker.bindPopup(scrapingPopupHtml(point, previewRow), { maxWidth: 320 });
          scrapingMarkers.push(marker);
        }
      }

      function scrapingDebugNotice(data) {
        const scraping = data?.scraping || {};
        const unresolved = Array.isArray(scraping?.locations_with_posts_but_no_icon)
          ? scraping.locations_with_posts_but_no_icon
          : [];
        if (unresolved.length === 0) {
          return '';
        }
        const unique = Array.from(new Set(unresolved.map(v => String(v || '').trim()).filter(Boolean)));
        if (unique.length === 0) {
          return '';
        }
        const sample = unique.slice(0, 20).join(', ');
        const suffix = unique.length > 20 ? ` (+${unique.length - 20} more)` : '';
        return `Social markers unresolved (posts found but no icon coordinate): ${sample}${suffix}`;
      }

      async function fetchOsrmRoadGeometry(stops) {
        if (!Array.isArray(stops) || stops.length < 2) {
          return null;
        }
        if (
          stops.length === 2
          && sameCoords(stops[0]?.lat, stops[0]?.lng, stops[1]?.lat, stops[1]?.lng, 1e-9)
        ) {
          return null;
        }

        const coords = stops.map(s => `${s.lng},${s.lat}`).join(';');
        const url = `${OSRM_PUBLIC_BASE_URL}/route/v1/driving/${coords}?overview=full&geometries=geojson&steps=false`;

        try {
          const resp = await fetch(url);
          if (!resp.ok) {
            return null;
          }
          const data = await resp.json();
          const geometry = data?.routes?.[0]?.geometry?.coordinates;
          if (!Array.isArray(geometry) || geometry.length < 2) {
            return null;
          }
          return geometry.map(([lng, lat]) => [lat, lng]);
        } catch (_) {
          return null;
        }
      }

      function endpointCandidates(url) {
        const raw = String(url || '').trim();
        if (!raw.startsWith('/')) {
          return [raw];
        }
        if (raw.startsWith('/api/')) {
          return [raw, raw.replace(/^\/api\//, '/')];
        }
        return [raw, `/api${raw}`];
      }

      async function requestJson(url, body, defaultErrorMessage) {
        const candidates = endpointCandidates(url);
        let lastError = null;

        for (const endpoint of candidates) {
          try {
            const resp = await fetch(endpoint, {
              method: 'POST',
              headers: {'Content-Type': 'application/json'},
              body: JSON.stringify(body)
            });

            if (!resp.ok) {
              let message = defaultErrorMessage;
              try {
                const errData = await resp.json();
                message = errData.error || message;
              } catch (_) {}
              lastError = new Error(`${message} (endpoint ${endpoint}, HTTP ${resp.status})`);
              continue;
            }
            return resp.json();
          } catch (err) {
            lastError = err;
          }
        }

        const detail = String(lastError?.message || 'Network error');
        throw new Error(
          `${defaultErrorMessage}. Unable to reach backend endpoint (${candidates.join(' or ')}). Detail: ${detail}`
        );
      }

      function applyPhase1InputPoints(data) {
        phase1PointByCoord = new Map();
        const semantic = data?.semantic_layer || {};
        const points = Array.isArray(semantic?.municipality_phase1_input_points)
          ? semantic.municipality_phase1_input_points
          : [];
        for (const row of points) {
          const key = typeof row?.coord_key === 'string' && row.coord_key.trim().length > 0
            ? row.coord_key.trim()
            : coordKey(row?.lat, row?.lng);
          if (typeof key === 'string' && key.length > 0) {
            phase1PointByCoord.set(key, row);
          }
        }

        const addressBook = (
          semantic?.municipality_address_book
          && typeof semantic.municipality_address_book === 'object'
          && !Array.isArray(semantic.municipality_address_book)
        ) ? semantic.municipality_address_book : {};
        const routes = Array.isArray(semantic?.routes) ? semantic.routes : [];
        for (const route of routes) {
          const links = Array.isArray(route?.stop_municipality_links)
            ? route.stop_municipality_links
            : [];
          for (const link of links) {
            if (typeof link?.lat !== 'number' || typeof link?.lng !== 'number') {
              continue;
            }
            const key = coordKey(link.lat, link.lng);
            if (phase1PointByCoord.has(key)) {
              continue;
            }
            const addressRef = String(link?.address_ref || key).trim() || key;
            const addressRow = (
              addressBook
              && typeof addressBook[addressRef] === 'object'
              && !Array.isArray(addressBook[addressRef])
            ) ? addressBook[addressRef] : null;
            const municipalityName = String(
              link?.municipality_name
              || addressRow?.municipality_name
              || ''
            ).trim();
            const provinceName = String(
              pickProvinceName(addressRow?.address)
              || ''
            ).trim();
            const stopId = link?.stop_id;
            const role = String(stopId) === 'depot' ? 'depot' : 'customer';
            phase1PointByCoord.set(key, {
              coord_key: key,
              role,
              lat: link.lat,
              lng: link.lng,
              stop_ids: [stopId],
              customer_ids: role === 'customer' ? [stopId] : [],
              status: String(link?.status || addressRow?.status || 'resolved'),
              resolution_note: String(addressRow?.resolution_note || ''),
              municipality_name: municipalityName,
              municipality_source_field: String(addressRow?.municipality_source_field || ''),
              province_name: provinceName,
              province_source_field: '',
              province_capital_name: '',
              province_capital_status: '',
              country_code: String(addressRow?.address?.country_code || '').toUpperCase(),
              address_ref: addressRef
            });
          }
        }
      }

      async function renderResult(data, payload) {
        const jsonOutput = JSON.stringify(data, null, 2);
        const fallbackNotice = municipalityOutputNotice(data);
        const scrapingNotice = scrapingDebugNotice(data);
        const notices = [fallbackNotice, scrapingNotice].filter(Boolean);
        document.getElementById('output').textContent = notices.length > 0
          ? `${jsonOutput}\n\n${notices.join('\\n\\n')}`
          : jsonOutput;
        lastCandidateLocations = Array.isArray(data?.candidate_locations) ? data.candidate_locations : [];
        renderLlmAddedMunicipalities(data);
        renderScrapingPreviewSummary(data);
        applyPhase1InputPoints(data);
        redrawPoints();

        clearRoutes();
        await Promise.all(data.routes.map(async (r, idx) => {
          let latlngs = r.stops.map(s => [s.lat, s.lng]);
          const hasServedCustomers = Array.isArray(r?.served_customer_ids) && r.served_customer_ids.length > 0;
          if (payload.distance_mode === 'osrm' && hasServedCustomers) {
            const roadLatLngs = await fetchOsrmRoadGeometry(r.stops);
            if (roadLatLngs) {
              latlngs = roadLatLngs;
            }
          }
          const line = L.polyline(latlngs, { color: colors[idx % colors.length], weight: 4 }).addTo(map);
          routeLayers.push(line);
        }));
        renderSemanticAnchors(data);
        renderScrapingMarkers(data);
        return data;
      }

      async function solveAndRender(payload) {
        const data = await requestJson('/solve_vrp', payload, 'Error solving VRP');
        return renderResult(data, payload);
      }

      async function enrichMunicipalityAndRender(payload, vrpResult) {
        const data = await requestJson(
          '/enrich_municipality',
          { payload, vrp_result: vrpResult },
          'Error computing municipality trace'
        );
        return renderResult(data, payload);
      }

      map.on('click', (e) => {
        phase1PointByCoord = new Map();
        const mode = document.getElementById('mode').value;
        if (mode === 'depot') {
          depot = { lat: e.latlng.lat, lng: e.latlng.lng, id: 'depot' };
        } else {
          const demand = parseInt(document.getElementById('demand').value || '1', 10);
          customers.push({ id: customerId++, lat: e.latlng.lat, lng: e.latlng.lng, demand });
        }
        redrawPoints();
      });

      document.getElementById('clearBtn').addEventListener('click', () => {
        depot = null;
        customers = [];
        customerId = 1;
        lastCandidateLocations = [];
        lastSolvePayload = null;
        lastSolveResult = null;
        phase1PointByCoord = new Map();
        setMunicipalityButtonState(false);
        clearRoutes();
        redrawPoints();
        document.getElementById('output').textContent = 'Waiting for data...';
        resetEnrichmentPanels();
      });

      function loadAutogenPreset(preset) {
        const selected = String(preset || '').trim().toLowerCase();
        if (selected === 'midsized') {
          depot = { id: 'depot', lat: 40.413496049701955, lng: -3.7792968750000004 };
          customers = [
            { id: 1, lat: 42.58544425738491, lng: -5.559082031250001, demand: 2 },
            { id: 2, lat: 42.342305278572816, lng: -7.558593750000001, demand: 2 },
            { id: 3, lat: 41.57436130598913, lng: -0.9008789062500001, demand: 2 },
            { id: 4, lat: 40.44694705960048, lng: -1.8676757812500002, demand: 2 },
            { id: 5, lat: 37.94419750075404, lng: -5.009765625000001, demand: 2 },
            { id: 6, lat: 38.95940879245423, lng: -1.0546875000000002, demand: 2 },
            { id: 7, lat: 39.554883059924016, lng: -4.724121093750001, demand: 2 },
            { id: 8, lat: 40.027614437486655, lng: -6.35009765625, demand: 2 },
            { id: 9, lat: 37.54457732085584, lng: -2.3291015625000004, demand: 2 }
          ];
          customerId = 10;
          document.getElementById('vehicles').value = 5;
          document.getElementById('capacity').value = 5;
          document.getElementById('demand').value = 2;
          map.setView([40.413496049701955, -3.7792968750000004], 6);
          document.getElementById('output').textContent = 'Autogenerated VRP (midsized) loaded. Click Solve VRP.';
        } else {
          depot = { id: 'depot', lat: 40.388397003511436, lng: -3.6862203718887647 };
          customers = [
            { id: 3, lat: 42.47614855950297, lng: -2.411919586737126, demand: 1 },
            { id: 2, lat: 42.92022918730693, lng: -2.697538728236644, demand: 1 },
            { id: 1, lat: 42.309815415956166, lng: -3.6807276960906936, demand: 1 }
          ];
          customerId = 4;
          document.getElementById('vehicles').value = 1;
          document.getElementById('capacity').value = 5;
          document.getElementById('demand').value = 1;
          map.setView([41.2, -3.2], 6);
          document.getElementById('output').textContent = 'Autogenerated VRP (small) loaded. Click Solve VRP.';
        }

        lastSolvePayload = null;
        lastSolveResult = null;
        lastCandidateLocations = [];
        phase1PointByCoord = new Map();
        setMunicipalityButtonState(false);
        clearRoutes();
        redrawPoints();
        resetEnrichmentPanels();
      }

      document.getElementById('autogenBtn').addEventListener('click', () => {
        const preset = document.getElementById('autogenPreset')?.value || 'small';
        loadAutogenPreset(preset);
      });

      document.getElementById('solveBtn').addEventListener('click', async () => {
        if (!depot) {
          alert('You must define a depot.');
          return;
        }
        if (!customers.length) {
          alert('Add at least one customer.');
          return;
        }

        const payload = {
          depot,
          customers,
          vehicles: parseInt(document.getElementById('vehicles').value, 10),
          capacity: parseInt(document.getElementById('capacity').value, 10),
          distance_mode: document.getElementById('distanceMode').value,
          include_semantic_layer: true,
          departure_time_utc: new Date().toISOString(),
          here_pipeline_mode: document.getElementById('hereMode').value,
          here_data_source: document.getElementById('hereDataSource').value,
          use_here_platform: true,
          municipality_enrichment_enabled: false
        };
        const poiAutoSettings = readPoiAutoSettings();
        payload.poi_auto_enabled = poiAutoSettings.poi_auto_enabled;
        payload.poi_auto_radius_km = poiAutoSettings.poi_auto_radius_km;
        payload.poi_auto_max_candidates = poiAutoSettings.poi_auto_max_candidates;
        const scrapingSettings = readScrapingSettings();
        payload.scraping_enabled = scrapingSettings.scraping_enabled;
        payload.scraping_lang = scrapingSettings.scraping_lang;
        const llmSettings = readMunicipalityLlmSettings();
        payload.municipality_llm_enrichment_enabled = llmSettings.municipality_llm_enrichment_enabled;
        payload.municipality_llm_timeout_sec = llmSettings.municipality_llm_timeout_sec;
        payload.municipality_llm_retries = llmSettings.municipality_llm_retries;
        payload.municipality_llm_max_tokens = llmSettings.municipality_llm_max_tokens;
        payload.municipality_reverse_source = readMunicipalityReverseSource();

        payload.here_forecast_window_hours = 24;
        payload.here_forecast_interval_min = Math.max(30, parseInt(document.getElementById('hereForecastInterval').value || '120', 10));
        payload.here_traffic_radius_m = Math.max(50, parseInt(document.getElementById('hereTrafficRadius').value || '300', 10));

        phase1PointByCoord = new Map();
        setMunicipalityButtonState(false);
        document.getElementById('output').textContent = payload.poi_auto_enabled
          ? 'Solving VRP + HERE enrichment... (POI check deferred to Municipality Trace phase)'
          : 'Solving VRP + HERE enrichment...';
        resetEnrichmentPanels();
        setEnrichmentProgress('Solving VRP... Municipality staged enrichment will be available after solve.');
        try {
          const data = await solveAndRender(payload);
          const returnedCandidates = Array.isArray(data?.candidate_locations)
            ? data.candidate_locations
            : [];
          lastSolvePayload = returnedCandidates.length > 0
            ? { ...payload, candidate_locations: returnedCandidates }
            : payload;
          lastSolveResult = data;
          setEnrichmentProgress('VRP completed. Click "Add Municipality Trace" to run staged municipality/LLM/scraping updates.');
          setMunicipalityButtonState(true);
        } catch (err) {
          document.getElementById('output').textContent = err.message || 'Error solving VRP';
          setEnrichmentProgress('VRP failed. Fix the error and run Solve VRP again.');
          document.getElementById('llmAddedSummary').textContent = 'LLM municipality additions unavailable due to solve error.';
          document.getElementById('scrapingPreviewSummary').textContent = 'Bluesky preview unavailable due to solve error.';
          lastSolvePayload = null;
          lastSolveResult = null;
          setMunicipalityButtonState(false);
        }
      });

      document.getElementById('municipalityBtn').addEventListener('click', async () => {
        if (!lastSolvePayload || !lastSolveResult) {
          alert('Run Solve VRP first.');
          return;
        }
        const basePayload = {
          ...lastSolvePayload,
          departure_time_utc: new Date().toISOString(),
          municipality_enrichment_enabled: true
        };
        const poiAutoSettings = readPoiAutoSettings();
        basePayload.poi_auto_enabled = poiAutoSettings.poi_auto_enabled;
        basePayload.poi_auto_radius_km = poiAutoSettings.poi_auto_radius_km;
        basePayload.poi_auto_max_candidates = poiAutoSettings.poi_auto_max_candidates;
        const scrapingSettings = readScrapingSettings();
        basePayload.scraping_enabled = scrapingSettings.scraping_enabled;
        basePayload.scraping_lang = scrapingSettings.scraping_lang;
        const llmSettings = readMunicipalityLlmSettings();
        basePayload.municipality_llm_enrichment_enabled = llmSettings.municipality_llm_enrichment_enabled;
        basePayload.municipality_llm_timeout_sec = llmSettings.municipality_llm_timeout_sec;
        basePayload.municipality_llm_retries = llmSettings.municipality_llm_retries;
        basePayload.municipality_llm_max_tokens = llmSettings.municipality_llm_max_tokens;
        basePayload.municipality_reverse_source = readMunicipalityReverseSource();

        const llmEnabled = basePayload.municipality_llm_enrichment_enabled === true;
        const scrapingEnabled = basePayload.scraping_enabled === true;
        const stages = [];
        stages.push({
          label: 'Municipality trace + POI (before LLM)',
          apply: (payload) => {
            payload.municipality_llm_enrichment_enabled = false;
            payload.scraping_enabled = false;
          }
        });
        if (llmEnabled) {
          stages.push({
            label: 'LLM municipality enrichment',
            apply: (payload) => {
              payload.municipality_llm_enrichment_enabled = true;
              payload.scraping_enabled = false;
            }
          });
        }
        if (scrapingEnabled) {
          stages.push({
            label: 'Social scraping + marker update',
            apply: (payload) => {
              payload.municipality_llm_enrichment_enabled = llmEnabled;
              payload.scraping_enabled = true;
            }
          });
        }

        setMunicipalityButtonState(true, true);
        let workingResult = lastSolveResult;
        let workingPayload = { ...basePayload };
        let currentStageIndex = -1;
        setEnrichmentStages(stages.map(stage => stage.label));
        try {
          for (let i = 0; i < stages.length; i += 1) {
            const stage = stages[i];
            currentStageIndex = i;
            const stagePayload = {
              ...workingPayload,
              departure_time_utc: new Date().toISOString(),
              municipality_enrichment_enabled: true
            };
            stage.apply(stagePayload);
            const stageHeader = `Stage ${i + 1}/${stages.length}: ${stage.label}`;
            setEnrichmentProgress(`${stageHeader}...`);
            setEnrichmentStageStatus(i, 'active');
            document.getElementById('output').textContent = `${stageHeader}...`;

            const data = await enrichMunicipalityAndRender(stagePayload, workingResult);
            workingResult = data;
            const returnedCandidates = Array.isArray(data?.candidate_locations)
              ? data.candidate_locations
              : [];
            workingPayload = {
              ...workingPayload,
              municipality_llm_enrichment_enabled: stagePayload.municipality_llm_enrichment_enabled,
              scraping_enabled: stagePayload.scraping_enabled,
              ...(returnedCandidates.length > 0
                ? { candidate_locations: returnedCandidates }
                : {})
            };
            setEnrichmentStageStatus(i, 'done');
            setEnrichmentProgress(`${stageHeader} completed.`);
          }

          const finalCandidates = Array.isArray(workingResult?.candidate_locations)
            ? workingResult.candidate_locations
            : [];
          lastSolvePayload = {
            ...lastSolvePayload,
            ...workingPayload,
            municipality_enrichment_enabled: true,
            ...(finalCandidates.length > 0
              ? { candidate_locations: finalCandidates }
              : {})
          };
          lastSolveResult = workingResult;
          setEnrichmentProgress(
            `Municipality enrichment finished (${stages.length} stage${stages.length === 1 ? '' : 's'}).`
          );
          setMunicipalityButtonState(true);
        } catch (err) {
          document.getElementById('output').textContent = err.message || 'Error computing municipality trace';
          document.getElementById('llmAddedSummary').textContent = 'LLM municipality additions unavailable due to enrichment error.';
          document.getElementById('scrapingPreviewSummary').textContent = 'Bluesky preview unavailable due to enrichment error.';
          if (Number.isInteger(currentStageIndex) && currentStageIndex >= 0) {
            setEnrichmentStageStatus(currentStageIndex, 'error');
          }
          setEnrichmentProgress(`Municipality enrichment failed: ${String(err?.message || 'unknown error')}`);
          setMunicipalityButtonState(true);
        }
      });
    </script>
  </body>
</html>
"""


def _solve(req: func.HttpRequest) -> func.HttpResponse:
    checkpoint = _new_checkpoint_logger("solve_vrp")
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            mimetype="application/json",
            status_code=400,
        )
    checkpoint("request parsed")

    depot = payload.get("depot")
    customers = payload.get("customers", [])
    vehicles = max(1, int(payload.get("vehicles", 1)))
    capacity = max(1, int(payload.get("capacity", 1)))
    distance_mode = str(payload.get("distance_mode", "direct")).lower().strip()
    osrm_base_url = str(
        payload.get("osrm_base_url", "https://router.project-osrm.org")
    ).strip()

    if not depot or not isinstance(customers, list) or len(customers) == 0:
        return func.HttpResponse(
            json.dumps({"error": "depot and customers are required"}),
            mimetype="application/json",
            status_code=400,
        )

    here_pipeline_mode = _resolve_here_pipeline_mode(payload.get("here_pipeline_mode"))
    here_data_source = _resolve_here_data_source(payload.get("here_data_source"))
    semantic_payload = dict(payload)
    semantic_payload["here_pipeline_mode"] = here_pipeline_mode
    semantic_payload["here_data_source"] = here_data_source

    if here_pipeline_mode == "before_vrp" and _as_bool(
        semantic_payload.get("use_here_platform"), True
    ):
        semantic_payload = _prefetch_here_point_observations(
            semantic_payload, depot, customers
        )
    checkpoint("here prefetch completed")

    try:
        result = solve_vrp_nearest_neighbor(
            depot,
            customers,
            vehicles,
            capacity,
            distance_mode=distance_mode,
            osrm_base_url=osrm_base_url,
        )
    except RuntimeError as exc:
        return func.HttpResponse(
            json.dumps({"error": str(exc)}),
            mimetype="application/json",
            status_code=502,
        )
    except Exception as exc:  # noqa: BLE001 - keep response stable
        return func.HttpResponse(
            json.dumps({"error": f"Unexpected VRP error: {exc}"}),
            mimetype="application/json",
            status_code=500,
        )
    checkpoint("vrp solver completed")

    if _as_bool(semantic_payload.get("poi_auto_enabled"), False):
        result["poi_auto_fetch"] = {
            "enabled": True,
            "status": "deferred",
            "message": "POI auto-candidate fetch is deferred to municipality enrichment phase.",
        }
    else:
        result["poi_auto_fetch"] = {
            "enabled": False,
            "status": "disabled",
            "message": "POI auto-candidate fetch disabled.",
        }

    if _as_bool(semantic_payload.get("include_semantic_layer"), True):
        try:
            result["semantic_layer"] = build_semantic_layer(result, semantic_payload)
        except Exception as exc:  # noqa: BLE001 - never block VRP result
            result["semantic_layer"] = {
                "status": "failed",
                "error": str(exc),
                "pipeline_mode": here_pipeline_mode,
                "here_data_source": here_data_source,
            }
            result["semantic_layer_error"] = (
                "Semantic enrichment failed; VRP result remains valid."
            )
    checkpoint("semantic layer completed")

    municipality_names = _collect_route_municipality_names(result)
    result["scraping"] = _run_social_scraping(
        payload=semantic_payload,
        municipality_names=municipality_names,
        source_stage="solve_vrp",
        result_payload=result,
    )
    checkpoint("scraping completed")

    if "_here_prefetch" in semantic_payload:
        result["here_prefetch"] = semantic_payload["_here_prefetch"]

    checkpoint("response ready")
    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)


def _merge_municipality_semantic(
    base_semantic: dict, municipality_semantic: dict
) -> dict:
    merged = dict(base_semantic)

    base_config = (
        dict(base_semantic.get("config", {}))
        if isinstance(base_semantic.get("config"), dict)
        else {}
    )
    municipality_config = (
        municipality_semantic.get("config", {})
        if isinstance(municipality_semantic.get("config"), dict)
        else {}
    )
    for key, value in municipality_config.items():
        if (
            str(key).startswith("municipality_")
            or str(key).startswith("province_")
            or key in {"distance_mode", "distance_source"}
        ):
            base_config[key] = value
    if base_config:
        merged["config"] = base_config

    base_summary = (
        dict(base_semantic.get("summary", {}))
        if isinstance(base_semantic.get("summary"), dict)
        else {}
    )
    municipality_summary = (
        municipality_semantic.get("summary", {})
        if isinstance(municipality_semantic.get("summary"), dict)
        else {}
    )
    for key, value in municipality_summary.items():
        if (
            str(key).startswith("municipality_")
            or str(key).startswith("province_")
            or key in {"candidate_locations_received", "matched_semantic_locations"}
        ):
            base_summary[key] = value
    if base_summary:
        merged["summary"] = base_summary

    for key in (
        "municipality_api",
        "municipality_address_book",
        "municipality_phase1_input_points",
        "municipality_post_output_notice",
        "municipality_post_output_warnings",
        "municipality_post_output_infos",
    ):
        if key in municipality_semantic:
            merged[key] = municipality_semantic[key]

    merged["version"] = municipality_semantic.get(
        "version", base_semantic.get("version")
    )
    merged["generated_at_utc"] = municipality_semantic.get(
        "generated_at_utc", base_semantic.get("generated_at_utc")
    )

    base_errors = (
        list(base_semantic.get("errors", []))
        if isinstance(base_semantic.get("errors"), list)
        else []
    )
    municipality_errors = (
        list(municipality_semantic.get("errors", []))
        if isinstance(municipality_semantic.get("errors"), list)
        else []
    )
    merged["errors"] = (base_errors + municipality_errors)[:40]

    base_routes = (
        base_semantic.get("routes", [])
        if isinstance(base_semantic.get("routes"), list)
        else []
    )
    municipality_routes = (
        municipality_semantic.get("routes", [])
        if isinstance(municipality_semantic.get("routes"), list)
        else []
    )
    municipality_by_vehicle = {
        str(route.get("vehicle")): route
        for route in municipality_routes
        if isinstance(route, dict)
    }

    merged_routes = []
    for base_route in base_routes:
        if not isinstance(base_route, dict):
            continue
        route = dict(base_route)
        municipality_route = municipality_by_vehicle.get(str(base_route.get("vehicle")))
        if not isinstance(municipality_route, dict):
            merged_routes.append(route)
            continue

        if isinstance(municipality_route.get("stop_municipality_links"), list):
            route["stop_municipality_links"] = municipality_route["stop_municipality_links"]
        if isinstance(municipality_route.get("municipality_llm"), dict):
            route["municipality_llm"] = municipality_route["municipality_llm"]
        if isinstance(municipality_route.get("semantic_locations"), list):
            route["semantic_locations"] = municipality_route["semantic_locations"]
        for key in (
            "province_vector",
            "province_capital_vector",
            "municipality_vector",
            "road_vector",
        ):
            if isinstance(municipality_route.get(key), list):
                route[key] = municipality_route[key]

        base_segments = (
            base_route.get("segment_context", [])
            if isinstance(base_route.get("segment_context"), list)
            else []
        )
        municipality_segments = (
            municipality_route.get("segment_context", [])
            if isinstance(municipality_route.get("segment_context"), list)
            else []
        )
        municipality_segments_by_index = {
            int(segment.get("segment_index")): segment
            for segment in municipality_segments
            if isinstance(segment, dict)
        }

        merged_segments = []
        for base_segment in base_segments:
            if not isinstance(base_segment, dict):
                continue
            segment = dict(base_segment)
            segment_index = base_segment.get("segment_index")
            if isinstance(segment_index, int):
                municipality_segment = municipality_segments_by_index.get(segment_index)
                if isinstance(municipality_segment, dict):
                    segment["municipality_trace"] = municipality_segment.get(
                        "municipality_trace", []
                    )
                    segment["municipality_names"] = municipality_segment.get(
                        "municipality_names", []
                    )
                    segment["province_names"] = municipality_segment.get(
                        "province_names", []
                    )
                    segment["province_capital_names"] = municipality_segment.get(
                        "province_capital_names", []
                    )
                    segment["road_names"] = municipality_segment.get("road_names", [])
            merged_segments.append(segment)
        if merged_segments:
            route["segment_context"] = merged_segments

        merged_routes.append(route)

    if merged_routes:
        merged["routes"] = merged_routes

    return merged


def _enrich_municipality(req: func.HttpRequest) -> func.HttpResponse:
    checkpoint = _new_checkpoint_logger("enrich_municipality")
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            mimetype="application/json",
            status_code=400,
        )
    checkpoint("request parsed")

    if not isinstance(body, dict):
        return func.HttpResponse(
            json.dumps({"error": "Request body must be a JSON object"}),
            mimetype="application/json",
            status_code=400,
        )

    vrp_result = body.get("vrp_result")
    if not isinstance(vrp_result, dict) or not isinstance(vrp_result.get("routes"), list):
        return func.HttpResponse(
            json.dumps({"error": "vrp_result with routes is required"}),
            mimetype="application/json",
            status_code=400,
        )

    payload = body.get("payload")
    semantic_payload = dict(payload) if isinstance(payload, dict) else {}
    semantic_payload["include_semantic_layer"] = True
    semantic_payload["municipality_enrichment_enabled"] = True
    semantic_payload["use_here_platform"] = False
    semantic_payload["here_pipeline_mode"] = _resolve_here_pipeline_mode(
        semantic_payload.get("here_pipeline_mode")
    )
    semantic_payload["here_data_source"] = _resolve_here_data_source(
        semantic_payload.get("here_data_source")
    )
    if not (
        isinstance(semantic_payload.get("candidate_locations"), list)
        and len(semantic_payload.get("candidate_locations", [])) > 0
    ):
        carried_candidates = vrp_result.get("candidate_locations")
        if isinstance(carried_candidates, list) and len(carried_candidates) > 0:
            semantic_payload["candidate_locations"] = carried_candidates

    result = dict(vrp_result)
    semantic_payload, poi_auto_meta = _auto_populate_candidate_locations(
        semantic_payload, vrp_result
    )
    checkpoint("poi auto-population completed")
    result["poi_auto_fetch"] = poi_auto_meta
    if isinstance(semantic_payload.get("candidate_locations"), list):
        result["candidate_locations"] = semantic_payload["candidate_locations"]
    existing_semantic = result.get("semantic_layer")
    try:
        municipality_semantic = build_semantic_layer(vrp_result, semantic_payload)
    except Exception as exc:  # noqa: BLE001 - keep base result stable
        if isinstance(existing_semantic, dict):
            result["semantic_layer"] = existing_semantic
        result["semantic_layer_error"] = (
            "Municipality enrichment failed; base VRP result remains valid."
        )
        result["municipality_enrichment_error"] = str(exc)
        return func.HttpResponse(
            json.dumps(result), mimetype="application/json", status_code=200
        )
    checkpoint("municipality semantic layer completed")

    if isinstance(existing_semantic, dict):
        result["semantic_layer"] = _merge_municipality_semantic(
            existing_semantic, municipality_semantic
        )
    else:
        result["semantic_layer"] = municipality_semantic
    municipality_names = _collect_route_municipality_names(result)
    result["scraping"] = _run_social_scraping(
        payload=semantic_payload,
        municipality_names=municipality_names,
        source_stage="enrich_municipality",
        result_payload=result,
    )
    checkpoint("scraping completed")

    checkpoint("response ready")
    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)


@app.route(route="", methods=["GET"])
def ui(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(HTML_PAGE, mimetype="text/html", status_code=200)


@app.route(route="api", methods=["GET"])
def ui_api(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(HTML_PAGE, mimetype="text/html", status_code=200)


@app.route(route="solve_vrp", methods=["POST"])
def solve_vrp(req: func.HttpRequest) -> func.HttpResponse:
    return _solve(req)


@app.route(route="api/solve_vrp", methods=["POST"])
def solve_vrp_api(req: func.HttpRequest) -> func.HttpResponse:
    return _solve(req)


@app.route(route="enrich_municipality", methods=["POST"])
def enrich_municipality(req: func.HttpRequest) -> func.HttpResponse:
    return _enrich_municipality(req)


@app.route(route="api/enrich_municipality", methods=["POST"])
def enrich_municipality_api(req: func.HttpRequest) -> func.HttpResponse:
    return _enrich_municipality(req)
