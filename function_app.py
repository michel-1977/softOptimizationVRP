import json
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import urllib.error
import urllib.parse
import urllib.request

import azure.functions as func

from solve_vrp import solve_vrp_nearest_neighbor
from solve_vrp.here_emulator import HerePlatformEmulator
from solve_vrp.here_platform import HerePlatformClient
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
        <strong>LLM Added Municipalities</strong>
        <div id="llmAddedSummary" class="llm-added-box">Run municipality enrichment to see LLM-added municipalities.</div>
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
      let phase1ExtraMarkers = [];
      let lastCandidateLocations = [];
      let customerId = 1;
      let lastSolvePayload = null;
      let lastSolveResult = null;
      let phase1PointByCoord = new Map();
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
      }

      function setMunicipalityButtonState(enabled, busy = false) {
        const municipalityBtn = document.getElementById('municipalityBtn');
        municipalityBtn.disabled = !enabled || busy;
        municipalityBtn.textContent = busy ? 'Tracing municipalities...' : 'Add Municipality Trace';
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
        document.getElementById('output').textContent = `${jsonOutput}\n\n${fallbackNotice}`;
        lastCandidateLocations = Array.isArray(data?.candidate_locations) ? data.candidate_locations : [];
        renderLlmAddedMunicipalities(data);
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
        document.getElementById('llmAddedSummary').textContent = 'Run municipality enrichment to see LLM-added municipalities.';
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
        document.getElementById('llmAddedSummary').textContent = 'Run municipality enrichment to see LLM-added municipalities.';
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
        const llmSettings = readMunicipalityLlmSettings();
        payload.municipality_llm_enrichment_enabled = llmSettings.municipality_llm_enrichment_enabled;
        payload.municipality_llm_timeout_sec = llmSettings.municipality_llm_timeout_sec;
        payload.municipality_llm_retries = llmSettings.municipality_llm_retries;
        payload.municipality_llm_max_tokens = llmSettings.municipality_llm_max_tokens;

        payload.here_forecast_window_hours = 24;
        payload.here_forecast_interval_min = Math.max(30, parseInt(document.getElementById('hereForecastInterval').value || '120', 10));
        payload.here_traffic_radius_m = Math.max(50, parseInt(document.getElementById('hereTrafficRadius').value || '300', 10));

        phase1PointByCoord = new Map();
        setMunicipalityButtonState(false);
        document.getElementById('output').textContent = payload.poi_auto_enabled
          ? 'Solving VRP + HERE enrichment... (POI check deferred to Municipality Trace phase)'
          : 'Solving VRP + HERE enrichment...';
        document.getElementById('llmAddedSummary').textContent = 'Run municipality enrichment to see LLM-added municipalities.';
        try {
          const data = await solveAndRender(payload);
          const returnedCandidates = Array.isArray(data?.candidate_locations)
            ? data.candidate_locations
            : [];
          lastSolvePayload = returnedCandidates.length > 0
            ? { ...payload, candidate_locations: returnedCandidates }
            : payload;
          lastSolveResult = data;
          setMunicipalityButtonState(true);
        } catch (err) {
          document.getElementById('output').textContent = err.message || 'Error solving VRP';
          document.getElementById('llmAddedSummary').textContent = 'LLM municipality additions unavailable due to solve error.';
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
        const payload = {
          ...lastSolvePayload,
          departure_time_utc: new Date().toISOString(),
          municipality_enrichment_enabled: true
        };
        const poiAutoSettings = readPoiAutoSettings();
        payload.poi_auto_enabled = poiAutoSettings.poi_auto_enabled;
        payload.poi_auto_radius_km = poiAutoSettings.poi_auto_radius_km;
        payload.poi_auto_max_candidates = poiAutoSettings.poi_auto_max_candidates;
        const llmSettings = readMunicipalityLlmSettings();
        payload.municipality_llm_enrichment_enabled = llmSettings.municipality_llm_enrichment_enabled;
        payload.municipality_llm_timeout_sec = llmSettings.municipality_llm_timeout_sec;
        payload.municipality_llm_retries = llmSettings.municipality_llm_retries;
        payload.municipality_llm_max_tokens = llmSettings.municipality_llm_max_tokens;
        setMunicipalityButtonState(true, true);
        document.getElementById('output').textContent = payload.municipality_llm_enrichment_enabled
          ? 'Computing municipality trace with OSM + LLM...'
          : 'Computing municipality trace with OSM (LLM disabled)...';
        try {
          const data = await enrichMunicipalityAndRender(payload, lastSolveResult);
          const returnedCandidates = Array.isArray(data?.candidate_locations)
            ? data.candidate_locations
            : [];
          lastSolvePayload = {
            ...lastSolvePayload,
            municipality_enrichment_enabled: true,
            ...(returnedCandidates.length > 0
              ? { candidate_locations: returnedCandidates }
              : {})
          };
          lastSolveResult = data;
          setMunicipalityButtonState(true);
        } catch (err) {
          document.getElementById('output').textContent = err.message || 'Error computing municipality trace';
          document.getElementById('llmAddedSummary').textContent = 'LLM municipality additions unavailable due to enrichment error.';
          setMunicipalityButtonState(true);
        }
      });
    </script>
  </body>
</html>
"""


def _solve(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            mimetype="application/json",
            status_code=400,
        )

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

    if "_here_prefetch" in semantic_payload:
        result["here_prefetch"] = semantic_payload["_here_prefetch"]

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
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            mimetype="application/json",
            status_code=400,
        )

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

    if isinstance(existing_semantic, dict):
        result["semantic_layer"] = _merge_municipality_semantic(
            existing_semantic, municipality_semantic
        )
    else:
        result["semantic_layer"] = municipality_semantic

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
