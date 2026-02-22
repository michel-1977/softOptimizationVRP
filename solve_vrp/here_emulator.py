from datetime import datetime, timedelta, timezone
import hashlib
import math
import random
from typing import Any, Dict, Optional, Tuple


DEFAULT_TIMEOUT_SEC = 12
DEFAULT_TRAFFIC_RADIUS_M = 300
DEFAULT_FORECAST_WINDOW_HOURS = 24
DEFAULT_FORECAST_STEP_MIN = 120
EARTH_RADIUS_KM = 6371.0


def _to_iso_z(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_utc_hour(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return EARTH_RADIUS_KM * 2.0 * math.asin(math.sqrt(h))


def _congestion_level(jam_factor: Optional[float]) -> Optional[str]:
    if jam_factor is None:
        return None
    if jam_factor < 4.0:
        return "low"
    if jam_factor < 7.0:
        return "medium"
    return "high"


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
        p = precipitation_probability
        if p > 1.0:
            p /= 100.0
        p = max(0.0, min(1.0, p))
        score += p * 2.5
    if wind_kph is not None:
        score += max(0.0, wind_kph - 25.0) / 8.0

    normalized = (condition or "").lower()
    if any(token in normalized for token in ("thunder", "hail", "storm")):
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


def _comfort_phrase(temp_c: float) -> str:
    if temp_c <= 2:
        return "Cold."
    if temp_c <= 8:
        return "Chilly."
    if temp_c <= 16:
        return "Cool."
    if temp_c <= 24:
        return "Mild."
    if temp_c <= 31:
        return "Warm."
    return "Hot."


def _condition_phrase(
    cloudiness: float,
    precipitation_mm: Optional[float],
    thunder_prob: float,
) -> str:
    if thunder_prob >= 0.85:
        return "Thunderstorms."
    if precipitation_mm is not None and precipitation_mm >= 7.0:
        return "Heavy rain."
    if precipitation_mm is not None and precipitation_mm >= 1.0:
        return "Rain."
    if cloudiness < 0.15:
        return "Sunny."
    if cloudiness < 0.30:
        return "Mostly clear."
    if cloudiness < 0.50:
        return "Partly cloudy."
    if cloudiness < 0.70:
        return "Scattered clouds."
    if cloudiness < 0.88:
        return "Cloudy."
    return "Overcast."


class HerePlatformEmulator:
    def __init__(
        self,
        timeout_sec: int = DEFAULT_TIMEOUT_SEC,
        traffic_radius_m: int = DEFAULT_TRAFFIC_RADIUS_M,
        forecast_window_hours: int = DEFAULT_FORECAST_WINDOW_HOURS,
        forecast_step_min: int = DEFAULT_FORECAST_STEP_MIN,
        seed: Optional[str] = None,
    ) -> None:
        self.timeout_sec = max(3, int(timeout_sec))
        self.traffic_radius_m = max(50, int(traffic_radius_m))
        self.forecast_window_hours = max(1, int(forecast_window_hours))
        self.forecast_step_min = max(30, int(forecast_step_min))
        self.seed = str(seed or "here-emulator-v1")

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
            "emulated": True,
        }

    def _rng(self, *parts: Any) -> random.Random:
        material = "|".join(str(part) for part in parts)
        digest = hashlib.sha256(f"{self.seed}|{material}".encode("utf-8")).hexdigest()
        # 64-bit deterministic seed.
        return random.Random(int(digest[:16], 16))

    def _simulate_weather_at(self, lat: float, lng: float, dt: datetime) -> Dict[str, Any]:
        hour = dt.hour
        doy = dt.timetuple().tm_yday
        rng = self._rng("weather", round(lat, 3), round(lng, 3), dt.strftime("%Y%m%d%H"))

        seasonal = 14.0 + (9.0 * math.sin(2.0 * math.pi * (doy - 170) / 365.0))
        lat_adjust = -abs(lat - 40.0) * 0.22
        diurnal = 5.8 * math.sin((2.0 * math.pi * (hour - 14)) / 24.0)
        temp_c = seasonal + lat_adjust + diurnal + rng.uniform(-1.8, 1.8)

        cloudiness = min(1.0, max(0.0, 0.45 + 0.30 * math.sin((2.0 * math.pi * (hour + 3)) / 24.0) + rng.uniform(-0.25, 0.25)))
        rain_trigger = max(0.0, cloudiness - 0.50) + rng.uniform(-0.15, 0.25)
        thunder_prob = max(0.0, min(1.0, rain_trigger - 0.55))

        precipitation_mm: Optional[float] = None
        if rain_trigger > 0.15:
            precipitation_mm = round(max(0.0, rng.gammavariate(1.3, 1.4) * rain_trigger), 2)
            if precipitation_mm == 0:
                precipitation_mm = None

        precipitation_probability = round(max(0.0, min(1.0, rain_trigger)), 2)
        wind_kph = round(max(0.0, 4.0 + cloudiness * 16.0 + rng.uniform(-3.0, 10.0)), 2)
        condition_main = _condition_phrase(cloudiness, precipitation_mm, thunder_prob)
        condition = f"{condition_main} {_comfort_phrase(temp_c)}".strip()

        return {
            "temperature_c": round(temp_c, 1),
            "precipitation_mm": precipitation_mm,
            "precipitation_probability": precipitation_probability,
            "wind_kph": wind_kph,
            "condition": condition,
        }

    def fetch_weather(
        self,
        lat: float,
        lng: float,
        reference_time_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        reference_time = _to_utc_hour(reference_time_utc or datetime.now(tz=timezone.utc))
        cache_key = (round(lat, 4), round(lng, 4), reference_time.isoformat())
        cached = self._weather_cache.get(cache_key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        self._stats["weather_queries"] += 1
        self._stats["http_requests"] += 1

        observed = self._simulate_weather_at(lat, lng, reference_time)
        realtime = {
            "status": "observed",
            "source": "here_weather_v3",
            "temperature_c": observed["temperature_c"],
            "precipitation_mm": observed["precipitation_mm"],
            "wind_kph": observed["wind_kph"],
            "condition": observed["condition"],
            "observed_at_utc": _to_iso_z(reference_time),
        }

        forecast_entries = []
        for hour_index in range(1, self.forecast_window_hours + 1):
            slot_start = reference_time + timedelta(hours=hour_index)
            slot_end = slot_start + timedelta(hours=1)
            slot = self._simulate_weather_at(lat, lng, slot_start)
            severity = _weather_severity_score(
                slot["condition"],
                slot["precipitation_mm"],
                slot["wind_kph"],
                slot["precipitation_probability"],
            )
            forecast_entries.append(
                {
                    "start_utc": _to_iso_z(slot_start),
                    "end_utc": _to_iso_z(slot_end),
                    "temperature_c": slot["temperature_c"],
                    "precipitation_mm": slot["precipitation_mm"],
                    "precipitation_probability": slot["precipitation_probability"],
                    "wind_kph": slot["wind_kph"],
                    "condition": slot["condition"],
                    "severity_score": severity,
                }
            )

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
        now_utc = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
        five_min_bucket = now_utc - timedelta(minutes=now_utc.minute % 5)
        cache_key = (
            round(lat, 4),
            round(lng, 4),
            self.traffic_radius_m,
            five_min_bucket.isoformat(),
        )
        cached = self._traffic_cache.get(cache_key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        self._stats["traffic_queries"] += 1
        self._stats["http_requests"] += 2

        rng = self._rng(
            "traffic",
            round(lat, 3),
            round(lng, 3),
            self.traffic_radius_m,
            five_min_bucket.strftime("%Y%m%d%H%M"),
        )
        hour = five_min_bucket.hour
        rush_wave = (
            math.exp(-(((hour - 8.0) / 2.2) ** 2))
            + math.exp(-(((hour - 17.5) / 2.8) ** 2))
        ) * 4.8
        random_wave = rng.uniform(0.0, 2.6)
        jam_factor = max(0.0, min(10.0, round(rush_wave + random_wave, 2)))

        free_flow_speed_kmh = round(rng.uniform(22.0, 95.0), 6)
        realized_ratio = max(0.18, 1.0 - (jam_factor / 11.5) + rng.uniform(-0.06, 0.04))
        speed_kmh = round(free_flow_speed_kmh * realized_ratio, 6)
        confidence = round(min(0.99, max(0.55, rng.uniform(0.62, 0.98))), 2)

        # Mirror live API behavior where some areas have sparse flow coverage.
        sparse_flow = rng.random() < 0.30
        if sparse_flow:
            congestion_level = None
            speed_kmh_out = None
            free_flow_speed_out = None
            jam_factor_out = None
            confidence_out = None
        else:
            congestion_level = _congestion_level(jam_factor)
            speed_kmh_out = speed_kmh
            free_flow_speed_out = free_flow_speed_kmh
            jam_factor_out = jam_factor
            confidence_out = confidence

        incident_count = int(max(0, round((jam_factor * 0.25) + rng.uniform(-1.0, 2.0))))

        result = {
            "status": "observed",
            "source": "here_traffic_v7",
            "congestion_level": congestion_level,
            "speed_kmh": speed_kmh_out,
            "free_flow_speed_kmh": free_flow_speed_out,
            "jam_factor": jam_factor_out,
            "confidence": confidence_out,
            "incident_count": incident_count,
            "observed_at_utc": _to_iso_z(five_min_bucket),
            "area_radius_m": self.traffic_radius_m,
        }
        self._traffic_cache[cache_key] = result
        return result

    def fetch_traffic_forecast(
        self,
        origin: Dict[str, float],
        destination: Dict[str, float],
        reference_time_utc: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        origin_lat = _safe_float(origin.get("lat"))
        origin_lng = _safe_float(origin.get("lng"))
        dest_lat = _safe_float(destination.get("lat"))
        dest_lng = _safe_float(destination.get("lng"))
        if (
            origin_lat is None
            or origin_lng is None
            or dest_lat is None
            or dest_lng is None
        ):
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

        reference_time = _to_utc_hour(reference_time_utc or datetime.now(tz=timezone.utc))
        cache_key = (
            round(origin_lat, 5),
            round(origin_lng, 5),
            round(dest_lat, 5),
            round(dest_lng, 5),
            reference_time.isoformat(),
            self.forecast_step_min,
        )
        cached = self._routing_cache.get(cache_key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        self._stats["routing_queries"] += 1
        self._stats["http_requests"] += 1

        distance_km = max(
            1.0,
            _haversine_km((origin_lat, origin_lng), (dest_lat, dest_lng)) * 1.18,
        )
        base_speed_kmh = max(22.0, 76.0 - (distance_km * 0.04))
        base_duration_seconds = int((distance_km / base_speed_kmh) * 3600.0)

        slots = []
        end_time = reference_time + timedelta(hours=self.forecast_window_hours)
        current = reference_time
        while current <= end_time:
            rng = self._rng(
                "routing",
                round(origin_lat, 3),
                round(origin_lng, 3),
                round(dest_lat, 3),
                round(dest_lng, 3),
                current.strftime("%Y%m%d%H"),
            )
            hour = current.hour
            rush = (
                math.exp(-(((hour - 8.0) / 2.1) ** 2))
                + math.exp(-(((hour - 17.0) / 2.6) ** 2))
            )
            weekend_factor = 0.75 if current.weekday() >= 5 else 1.0
            ratio = 1.0 + ((0.03 + (0.09 * rush * weekend_factor)) * rng.uniform(0.55, 1.45))
            ratio = max(1.0, round(ratio, 4))

            duration_seconds = int(round(base_duration_seconds * ratio))
            delay_seconds = max(0, duration_seconds - base_duration_seconds)
            slots.append(
                {
                    "departure_utc": _to_iso_z(current),
                    "duration_seconds": duration_seconds,
                    "base_duration_seconds": base_duration_seconds,
                    "delay_seconds": delay_seconds,
                    "delay_ratio": ratio,
                }
            )
            current += timedelta(minutes=self.forecast_step_min)

        worst_ratio = max(slot["delay_ratio"] for slot in slots)
        worst_delay = max(slot["delay_seconds"] for slot in slots)
        worst_slots = [
            slot for slot in slots if abs(slot["delay_ratio"] - worst_ratio) <= 0.01
        ]

        result = {
            "status": "forecasted",
            "source": "here_routing_v8",
            "window_hours": self.forecast_window_hours,
            "interval_min": self.forecast_step_min,
            "worst_case_delay_ratio": round(worst_ratio, 4),
            "worst_case_delay_seconds": int(worst_delay),
            "worst_slots": worst_slots[:6],
            "evaluated_slots": len(slots),
        }
        self._routing_cache[cache_key] = result
        return result

    def stats(self) -> Dict[str, Any]:
        return dict(self._stats)
