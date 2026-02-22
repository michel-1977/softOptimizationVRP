import json
import os
from datetime import datetime, timezone

import azure.functions as func

from solve_vrp import solve_vrp_nearest_neighbor
from solve_vrp.here_emulator import HerePlatformEmulator
from solve_vrp.here_platform import HerePlatformClient
from solve_vrp.semantic_layer import build_semantic_layer

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


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
      .legend-dot-poi {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        border: 2px solid #274c77;
        background: #d6e7ff;
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
      .semantic-popup { font-size: 12px; line-height: 1.35; }
      .semantic-popup h4 { margin: 0 0 5px 0; font-size: 13px; }
      .semantic-popup .muted { color: #666; }
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
          <option value="direct" selected>Direct (Haversine)</option>
          <option value="osrm">Real road kms (OSRM)</option>
        </select>
      </div>

      <div class="row">
        <label>HERE data source</label>
        <select id="hereDataSource">
          <option value="here" selected>Live HERE APIs</option>
          <option value="emulator">HERE emulator (randomized)</option>
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

      <div class="row" style="display:flex; gap:8px;">
        <button id="solveBtn">Solve VRP</button>
        <button id="clearBtn">Clear</button>
      </div>
      <div class="row">
        <button id="autogenBtn" type="button">Autogenerate VRP Problem</button>
        <div class="small">Loads your 1 depot + 9 customers reference scenario.</div>
      </div>

      <div class="row">
        <strong>Output</strong>
        <pre id="output">Waiting for data...</pre>
        <div class="small">Click map dots to inspect semantic + weather + traffic details.</div>
        <div class="legend">
          <div class="legend-title">Map Legend</div>
          <div class="legend-row"><span class="legend-dot-poi"></span><span>Semantic POI match</span></div>
          <div class="legend-row"><span class="legend-dot-segment"></span><span>Segment context fallback</span></div>
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
      let customerId = 1;
      const OSRM_PUBLIC_BASE_URL = 'https://router.project-osrm.org';

      const colors = ['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#a65628'];

      function redrawPoints() {
        markers.forEach(m => map.removeLayer(m));
        markers = [];

        if (depot) {
          markers.push(L.marker([depot.lat, depot.lng], {title:'Depot'}).addTo(map).bindPopup('Depot'));
        }

        for (const c of customers) {
          markers.push(L.circleMarker([c.lat, c.lng], {radius:8, color:'#222', fillColor:'#ffd54f', fillOpacity:0.95})
            .addTo(map)
            .bindPopup(`Customer ${c.id} | Demand: ${c.demand}`));
        }
      }

      function clearRoutes() {
        routeLayers.forEach(l => map.removeLayer(l));
        routeLayers = [];
        semanticMarkers.forEach(m => map.removeLayer(m));
        semanticMarkers = [];
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

      function semanticPopupHtml(vehicle, location, segmentContext) {
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

        return `
          <div class="semantic-popup">
            <h4>&bull; ${name}</h4>
            <div><strong>Route:</strong> Vehicle ${escapeHtml(vehicle ?? '?')}</div>
            <div><strong>Category:</strong> ${category}</div>
            <div><strong>Relevance:</strong> ${relevance}</div>
            <div><strong>Distance to route:</strong> ${dist} km</div>
            <div><strong>Estimated detour:</strong> ${detour} km</div>
            <div><strong>Segment ETA:</strong> ${eta}</div>
            <div><strong>Weather:</strong> ${weatherSummary}</div>
            <div><strong>Weather 24h worst:</strong> ${weatherForecastSummary}</div>
            <div><strong>Traffic:</strong> ${trafficSummary}</div>
            <div><strong>Traffic 24h worst:</strong> ${trafficForecastSummary}</div>
            <div class="muted">Lat/Lng: ${escapeHtml(location.lat)}, ${escapeHtml(location.lng)}</div>
          </div>
        `;
      }

      function segmentPopupHtml(vehicle, segmentContext) {
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

        return `
          <div class="semantic-popup">
            <h4>&bull; Segment context</h4>
            <div><strong>Route:</strong> Vehicle ${escapeHtml(vehicle ?? '?')}</div>
            <div><strong>Segment:</strong> #${escapeHtml((segmentContext?.segment_index ?? 0) + 1)}</div>
            <div><strong>Segment distance:</strong> ${distance} km</div>
            <div><strong>ETA:</strong> ${eta}</div>
            <div><strong>Weather:</strong> ${weatherSummary}</div>
            <div><strong>Weather 24h worst:</strong> ${weatherForecastSummary}</div>
            <div><strong>Traffic:</strong> ${trafficSummary}</div>
            <div><strong>Traffic 24h worst:</strong> ${trafficForecastSummary}</div>
            <div class="muted">No semantic POI matched in this corridor window.</div>
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
            const popupHtml = semanticPopupHtml(vehicle, location, linkedSegment);
            const icon = L.divIcon({
              className: 'semantic-anchor-shell',
              html: `<div class="semantic-anchor-icon" style="border-color:${color};color:${color};background:#d6e7ff;">&bull;</div>`,
              iconSize: [16, 16],
              iconAnchor: [8, 8],
              popupAnchor: [0, -10]
            });

            const marker = L.marker([location.lat, location.lng], { icon }).addTo(map);
            marker.bindPopup(popupHtml, { maxWidth: 310 });
            semanticMarkers.push(marker);
          }

          if (semanticLocations.length === 0 && segmentContext.length > 0) {
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

              const popupHtml = segmentPopupHtml(vehicle, segment);
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
      }

      async function fetchOsrmRoadGeometry(stops) {
        if (!Array.isArray(stops) || stops.length < 2) {
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

      map.on('click', (e) => {
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
        clearRoutes();
        redrawPoints();
        document.getElementById('output').textContent = 'Waiting for data...';
      });

      document.getElementById('autogenBtn').addEventListener('click', () => {
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

        clearRoutes();
        redrawPoints();
        map.setView([40.413496049701955, -3.7792968750000004], 6);
        document.getElementById('output').textContent = 'Autogenerated reference VRP loaded. Click Solve VRP.';
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
          use_here_platform: true
        };

        payload.here_forecast_window_hours = 24;
        payload.here_forecast_interval_min = Math.max(30, parseInt(document.getElementById('hereForecastInterval').value || '120', 10));
        payload.here_traffic_radius_m = Math.max(50, parseInt(document.getElementById('hereTrafficRadius').value || '300', 10));

        const resp = await fetch('/solve_vrp', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });

        if (!resp.ok) {
          let message = 'Error solving VRP';
          try {
            const errData = await resp.json();
            message = errData.error || message;
          } catch (_) {}
          document.getElementById('output').textContent = message;
          return;
        }

        const data = await resp.json();
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);

        clearRoutes();
        await Promise.all(data.routes.map(async (r, idx) => {
          let latlngs = r.stops.map(s => [s.lat, s.lng]);
          if (payload.distance_mode === 'osrm') {
            const roadLatLngs = await fetchOsrmRoadGeometry(r.stops);
            if (roadLatLngs) {
              latlngs = roadLatLngs;
            }
          }
          const line = L.polyline(latlngs, { color: colors[idx % colors.length], weight: 4 }).addTo(map);
          routeLayers.push(line);
        }));
        renderSemanticAnchors(data);
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
