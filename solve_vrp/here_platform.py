from datetime import datetime, timedelta, timezone
import json
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_TIMEOUT_SEC = 12
DEFAULT_TRAFFIC_RADIUS_M = 300
DEFAULT_FORECAST_WINDOW_HOURS = 24
DEFAULT_FORECAST_STEP_MIN = 120


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_iso_z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _parse_utc_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, (int, float)):
        stamp = float(value)
        if stamp > 1_000_000_000_000:
            stamp /= 1000.0
        try:
            return datetime.fromtimestamp(stamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None

    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    # Accept offsets like +0000 or -0500.
    if len(raw) >= 5 and raw[-5] in {"+", "-"} and raw[-3] != ":":
        raw = raw[:-2] + ":" + raw[-2:]

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M",
        ):
            try:
                parsed = datetime.strptime(raw, fmt)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _walk_dicts(value: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for inner in value.values():
            yield from _walk_dicts(inner)
    elif isinstance(value, list):
        for inner in value:
            yield from _walk_dicts(inner)


def _nested_get(value: Dict[str, Any], path: str) -> Any:
    current: Any = value
    for token in path.split("."):
        if isinstance(current, dict):
            current = current.get(token)
        else:
            return None
    return current


def _extract_scalar(candidate: Any) -> Optional[float]:
    if isinstance(candidate, (int, float)):
        return float(candidate)
    if isinstance(candidate, dict):
        for key in ("value", "amount", "metric", "kmh", "kph", "mps"):
            picked = _safe_float(candidate.get(key))
            if picked is not None:
                return picked
    return None


def _pick_number(item: Dict[str, Any], keys: List[str]) -> Optional[float]:
    for key in keys:
        raw = _nested_get(item, key) if "." in key else item.get(key)
        value = _extract_scalar(raw)
        if value is not None:
            return value
    return None


def _pick_string(item: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        raw = _nested_get(item, key) if "." in key else item.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return None


def _first_path(value: Dict[str, Any], paths: List[Tuple[Any, ...]]) -> Any:
    for path in paths:
        current: Any = value
        is_valid = True
        for token in path:
            if isinstance(token, int):
                if isinstance(current, list) and 0 <= token < len(current):
                    current = current[token]
                else:
                    is_valid = False
                    break
            else:
                if isinstance(current, dict):
                    current = current.get(token)
                else:
                    is_valid = False
                    break
        if is_valid and current is not None:
            return current
    return None


def _to_utc_hour(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _weather_severity_score(
    condition: Optional[str],
    precipitation_mm: Optional[float],
    wind_kph: Optional[float],
    precipitation_probability: Optional[float],
) -> float:
    score = 0.0
    if precipitation_mm is not None:
        score += max(0.0, precipitation_mm) * 1.8
    if precipitation_probability is not None:
        probability = precipitation_probability
        if probability > 1.0:
            probability /= 100.0
        probability = max(0.0, min(1.0, probability))
        score += probability * 2.5
    if wind_kph is not None:
        score += max(0.0, wind_kph - 25.0) / 8.0

    normalized = (condition or "").lower()
    if any(token in normalized for token in ("thunder", "hail", "tornado", "storm")):
        score += 8.0
    elif any(token in normalized for token in ("freezing", "blizzard", "sleet", "snow")):
        score += 5.0
    elif "heavy rain" in normalized:
        score += 5.0
    elif "rain" in normalized:
        score += 3.0
    elif "fog" in normalized:
        score += 2.0
    return round(score, 3)


def _congestion_level(jam_factor: Optional[float]) -> Optional[str]:
    if jam_factor is None:
        return None
    if jam_factor < 4.0:
        return "low"
    if jam_factor < 7.0:
        return "medium"
    return "high"


class HerePlatformClient:
    def __init__(
        self,
        api_key: str,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        traffic_radius_m: int = DEFAULT_TRAFFIC_RADIUS_M,
        forecast_window_hours: int = DEFAULT_FORECAST_WINDOW_HOURS,
        forecast_step_min: int = DEFAULT_FORECAST_STEP_MIN,
    ) -> None:
        self.api_key = api_key.strip()
        self.timeout_sec = max(3, int(timeout_sec))
        self.traffic_radius_m = max(50, int(traffic_radius_m))
        self.forecast_window_hours = max(1, int(forecast_window_hours))
        self.forecast_step_min = max(30, int(forecast_step_min))

        self._http_cache: Dict[str, Dict[str, Any]] = {}
        self._weather_cache: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        self._traffic_cache: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        self._routing_cache: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

        self._stats = {
            "cache_hits": 0,
            "http_requests": 0,
            "weather_queries": 0,
            "traffic_queries": 0,
            "routing_queries": 0,
            "errors": 0,
        }

    def _get_json(
        self,
        url: str,
        params: Dict[str, Any],
        key_param: Optional[str] = "apiKey",
    ) -> Dict[str, Any]:
        query_params = dict(params)
        if key_param and key_param not in query_params:
            query_params[key_param] = self.api_key
        encoded = urllib.parse.urlencode(query_params, doseq=True)
        full_url = f"{url}?{encoded}"

        cached = self._http_cache.get(full_url)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        request = urllib.request.Request(full_url, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_sec) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            self._stats["errors"] += 1
            raise RuntimeError(f"HERE request failed for {url}: {exc}") from exc

        self._http_cache[full_url] = payload
        self._stats["http_requests"] += 1
        return payload

    def _extract_weather_observation(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        candidate = _first_path(
            payload,
            [
                ("places", 0, "observations", 0),
                ("places", 0, "observation", 0),
                ("places", 0, "observation"),
                ("observations", 0),
                ("observation", 0),
                ("observation",),
            ],
        )
        if isinstance(candidate, dict):
            return candidate

        for item in _walk_dicts(payload):
            if _pick_number(item, ["temperature", "temp", "airTemperature"]) is not None:
                if _pick_string(item, ["utcTime", "time", "observationTime", "validFrom"]):
                    return item
        return None

    def _extract_weather_forecast_entries(
        self, payload: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        candidate = _first_path(
            payload,
            [
                ("places", 0, "forecastHourly"),
                ("places", 0, "hourlyForecasts"),
                ("forecastHourly",),
                ("hourlyForecasts",),
                ("forecasts", "hourly"),
            ],
        )
        if isinstance(candidate, list):
            return [entry for entry in candidate if isinstance(entry, dict)]

        entries = []
        for item in _walk_dicts(payload):
            has_time = _pick_string(item, ["utcTime", "time", "startTime", "validFrom"])
            if not has_time:
                continue
            has_weather_value = (
                _pick_number(item, ["temperature", "temp", "airTemperature"]) is not None
                or _pick_number(item, ["precipitation", "rain", "snowfall"]) is not None
                or _pick_number(item, ["windSpeed", "wind.speed"]) is not None
                or _pick_string(item, ["description", "condition", "iconName"]) is not None
            )
            if has_weather_value:
                entries.append(item)
        return entries

    def fetch_weather(
        self,
        lat: float,
        lng: float,
        reference_time_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        reference_time = reference_time_utc or datetime.now(tz=timezone.utc)
        reference_time = reference_time.astimezone(timezone.utc)
        cache_key = (round(lat, 4), round(lng, 4), _to_utc_hour(reference_time))
        cached = self._weather_cache.get(cache_key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        payload = self._get_json(
            "https://weather.hereapi.com/v3/report",
            {
                "products": "observation,forecastHourly",
                "location": f"{lat:.6f},{lng:.6f}",
                "units": "metric",
            },
            key_param="apiKey",
        )
        self._stats["weather_queries"] += 1

        observation = self._extract_weather_observation(payload)
        if observation is None:
            realtime = {
                "status": "unknown",
                "source": "here_weather_v3",
                "temperature_c": None,
                "precipitation_mm": None,
                "wind_kph": None,
                "condition": None,
                "observed_at_utc": None,
            }
        else:
            wind_kph = _pick_number(
                observation,
                ["windSpeedKph", "wind.speedKph", "windSpeedKmH"],
            )
            if wind_kph is None:
                wind_mps = _pick_number(
                    observation,
                    ["windSpeedMps", "wind.speedMps"],
                )
                if wind_mps is not None:
                    wind_kph = round(wind_mps * 3.6, 3)
            if wind_kph is None:
                wind_kph = _pick_number(observation, ["windSpeed", "wind.speed", "wind"])

            realtime = {
                "status": "observed",
                "source": "here_weather_v3",
                "temperature_c": _pick_number(
                    observation,
                    ["temperature", "temp", "airTemperature", "temperature.value"],
                ),
                "precipitation_mm": _pick_number(
                    observation,
                    [
                        "precipitation",
                        "precipitationAmount",
                        "rainfall",
                        "rain",
                        "snowfall",
                    ],
                ),
                "wind_kph": wind_kph,
                "condition": _pick_string(
                    observation,
                    [
                        "description",
                        "condition",
                        "iconName",
                        "daySegment",
                        "phrase",
                    ],
                ),
                "observed_at_utc": _to_iso_z(
                    _parse_utc_datetime(
                        _pick_string(
                            observation,
                            ["utcTime", "time", "observationTime", "validFrom"],
                        )
                    )
                ),
            }

        window_end = reference_time + timedelta(hours=self.forecast_window_hours)
        forecast_entries = []
        for entry in self._extract_weather_forecast_entries(payload):
            slot_start = _parse_utc_datetime(
                _pick_string(entry, ["utcTime", "time", "startTime", "validFrom"])
            )
            if slot_start is None:
                continue
            if slot_start < reference_time or slot_start > window_end:
                continue

            slot_end = _parse_utc_datetime(
                _pick_string(entry, ["endTime", "validTo"])
            )
            if slot_end is None:
                slot_end = slot_start + timedelta(hours=1)

            wind_kph = _pick_number(entry, ["windSpeedKph", "wind.speedKph"])
            if wind_kph is None:
                wind_mps = _pick_number(entry, ["windSpeedMps", "wind.speedMps"])
                if wind_mps is not None:
                    wind_kph = round(wind_mps * 3.6, 3)
            if wind_kph is None:
                wind_kph = _pick_number(entry, ["windSpeed", "wind.speed", "wind"])

            precipitation_mm = _pick_number(
                entry,
                [
                    "precipitation",
                    "precipitationAmount",
                    "rainfall",
                    "rain",
                    "snowfall",
                ],
            )
            precipitation_probability = _pick_number(
                entry,
                ["precipitationProbability", "rainProbability", "pop"],
            )
            condition = _pick_string(
                entry,
                ["description", "condition", "iconName", "daySegment", "phrase"],
            )
            severity = _weather_severity_score(
                condition,
                precipitation_mm,
                wind_kph,
                precipitation_probability,
            )

            forecast_entries.append(
                {
                    "start_utc": _to_iso_z(slot_start),
                    "end_utc": _to_iso_z(slot_end),
                    "temperature_c": _pick_number(
                        entry,
                        ["temperature", "temp", "airTemperature", "temperature.value"],
                    ),
                    "precipitation_mm": precipitation_mm,
                    "precipitation_probability": precipitation_probability,
                    "wind_kph": wind_kph,
                    "condition": condition,
                    "severity_score": severity,
                }
            )

        if not forecast_entries:
            forecast = {
                "status": "unknown",
                "source": "here_weather_v3",
                "window_hours": self.forecast_window_hours,
                "interval_min": None,
                "worst_case_score": None,
                "worst_slots": [],
                "evaluated_slots": 0,
            }
        else:
            worst_score = max(entry["severity_score"] for entry in forecast_entries)
            worst_slots = [
                entry
                for entry in forecast_entries
                if abs(entry["severity_score"] - worst_score) <= 0.05
            ]
            forecast = {
                "status": "forecasted",
                "source": "here_weather_v3",
                "window_hours": self.forecast_window_hours,
                "interval_min": self.forecast_step_min,
                "worst_case_score": round(worst_score, 3),
                "worst_slots": worst_slots[:6],
                "evaluated_slots": len(forecast_entries),
            }

        result = {"realtime": realtime, "forecast_24h": forecast}
        self._weather_cache[cache_key] = result
        return result

    def fetch_traffic_status(self, lat: float, lng: float) -> Dict[str, Any]:
        cache_key = (round(lat, 4), round(lng, 4), self.traffic_radius_m)
        cached = self._traffic_cache.get(cache_key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        in_filter = f"circle:{lat:.6f},{lng:.6f};r={self.traffic_radius_m}"

        flow_payload = self._get_json(
            "https://data.traffic.hereapi.com/v7/flow",
            {"in": in_filter, "locationReferencing": "shape"},
            key_param="apiKey",
        )
        incidents_payload = self._get_json(
            "https://data.traffic.hereapi.com/v7/incidents",
            {"in": in_filter, "locationReferencing": "shape"},
            key_param="apiKey",
        )
        self._stats["traffic_queries"] += 1

        flow_rows = flow_payload.get("results")
        if not isinstance(flow_rows, list):
            flow_rows = []
        first_flow = flow_rows[0] if flow_rows else {}
        current_flow = first_flow.get("currentFlow", {})
        if not isinstance(current_flow, dict):
            current_flow = {}

        jam_factor = _pick_number(current_flow, ["jamFactor"])
        speed_kmh = _pick_number(current_flow, ["speed"])
        free_flow_speed_kmh = _pick_number(current_flow, ["freeFlow"])
        if jam_factor is None and speed_kmh is not None and free_flow_speed_kmh:
            if free_flow_speed_kmh > 0:
                jam_factor = max(
                    0.0, min(10.0, (1.0 - (speed_kmh / free_flow_speed_kmh)) * 10.0)
                )

        incidents = incidents_payload.get("results")
        incident_count = len(incidents) if isinstance(incidents, list) else 0

        observed_at = _parse_utc_datetime(flow_payload.get("sourceUpdated"))
        if observed_at is None:
            observed_at = datetime.now(tz=timezone.utc)

        result = {
            "status": "observed",
            "source": "here_traffic_v7",
            "congestion_level": _congestion_level(jam_factor),
            "speed_kmh": speed_kmh,
            "free_flow_speed_kmh": free_flow_speed_kmh,
            "jam_factor": round(jam_factor, 3) if jam_factor is not None else None,
            "confidence": _pick_number(current_flow, ["confidence"]),
            "incident_count": incident_count,
            "observed_at_utc": _to_iso_z(observed_at),
            "area_radius_m": self.traffic_radius_m,
        }
        self._traffic_cache[cache_key] = result
        return result

    def _fetch_route_summary(
        self,
        origin: Dict[str, float],
        destination: Dict[str, float],
        departure_time_utc: datetime,
    ) -> Optional[Dict[str, Any]]:
        departure = departure_time_utc.astimezone(timezone.utc).replace(microsecond=0)
        cache_key = (
            round(origin["lat"], 5),
            round(origin["lng"], 5),
            round(destination["lat"], 5),
            round(destination["lng"], 5),
            departure.isoformat(),
        )
        cached = self._routing_cache.get(cache_key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        payload = self._get_json(
            "https://router.hereapi.com/v8/routes",
            {
                "transportMode": "car",
                "origin": f"{origin['lat']:.6f},{origin['lng']:.6f}",
                "destination": f"{destination['lat']:.6f},{destination['lng']:.6f}",
                "return": "summary",
                "departureTime": _to_iso_z(departure),
                "apikey": self.api_key,
            },
            key_param=None,
        )
        self._stats["routing_queries"] += 1

        routes = payload.get("routes")
        if not isinstance(routes, list) or not routes:
            return None
        sections = routes[0].get("sections")
        if not isinstance(sections, list) or not sections:
            return None
        summary = sections[0].get("summary")
        if not isinstance(summary, dict):
            return None

        duration = _safe_int(summary.get("duration"))
        base_duration = _safe_int(summary.get("baseDuration"))
        if duration is None or base_duration is None or base_duration <= 0:
            return None

        extracted = {
            "duration_seconds": duration,
            "base_duration_seconds": base_duration,
        }
        self._routing_cache[cache_key] = extracted
        return extracted

    def fetch_traffic_forecast(
        self,
        origin: Dict[str, float],
        destination: Dict[str, float],
        reference_time_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        reference_time = reference_time_utc or datetime.now(tz=timezone.utc)
        reference_time = _to_utc_hour(reference_time)

        slots = []
        end_time = reference_time + timedelta(hours=self.forecast_window_hours)
        current = reference_time
        while current <= end_time:
            summary = self._fetch_route_summary(origin, destination, current)
            if summary is not None:
                delay_seconds = max(
                    0,
                    summary["duration_seconds"] - summary["base_duration_seconds"],
                )
                delay_ratio = (
                    float(summary["duration_seconds"])
                    / float(summary["base_duration_seconds"])
                )
                slots.append(
                    {
                        "departure_utc": _to_iso_z(current),
                        "duration_seconds": summary["duration_seconds"],
                        "base_duration_seconds": summary["base_duration_seconds"],
                        "delay_seconds": delay_seconds,
                        "delay_ratio": round(delay_ratio, 4),
                    }
                )
            current += timedelta(minutes=self.forecast_step_min)

        if not slots:
            return {
                "status": "unknown",
                "source": "here_routing_v8",
                "window_hours": self.forecast_window_hours,
                "interval_min": self.forecast_step_min,
                "worst_case_delay_ratio": None,
                "worst_case_delay_seconds": None,
                "worst_slots": [],
                "evaluated_slots": 0,
            }

        worst_ratio = max(slot["delay_ratio"] for slot in slots)
        worst_delay = max(slot["delay_seconds"] for slot in slots)
        worst_slots = [
            slot for slot in slots if abs(slot["delay_ratio"] - worst_ratio) <= 0.01
        ]
        return {
            "status": "forecasted",
            "source": "here_routing_v8",
            "window_hours": self.forecast_window_hours,
            "interval_min": self.forecast_step_min,
            "worst_case_delay_ratio": round(worst_ratio, 4),
            "worst_case_delay_seconds": int(worst_delay),
            "worst_slots": worst_slots[:6],
            "evaluated_slots": len(slots),
        }

    def stats(self) -> Dict[str, Any]:
        return dict(self._stats)
