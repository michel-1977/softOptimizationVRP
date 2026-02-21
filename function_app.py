import json

import azure.functions as func

from solve_vrp import solve_vrp_nearest_neighbor

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


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

      <div class="row" style="display:flex; gap:8px;">
        <button id="solveBtn">Solve VRP</button>
        <button id="clearBtn">Clear</button>
      </div>

      <div class="row">
        <strong>Output</strong>
        <pre id="output">Waiting for data...</pre>
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
          distance_mode: document.getElementById('distanceMode').value
        };

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
