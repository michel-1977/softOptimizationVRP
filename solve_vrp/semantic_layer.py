from datetime import datetime, timedelta, timezone
import math
import os
from typing import Any, Dict, List, Optional, Set, Tuple

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

    for route in vrp_result.get("routes", []):
        stops = route.get("stops", [])
        segments = _build_route_segments(stops, avg_speed_kmh, departure_time_utc)
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
                "semantic_locations": semantic_locations,
                "segment_context": segment_context,
            }
        )

    return {
        "version": "0.5",
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
            "here_client_stats": here_client.stats() if here_client is not None else {},
        },
        "errors": here_errors[:20],
        "routes": routes_output,
    }
