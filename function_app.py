import json
import math
from typing import Dict, List, Tuple

import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def route_distance_km(route: List[Dict]) -> float:
    if len(route) < 2:
        return 0.0
    total = 0.0
    for i in range(len(route) - 1):
        p1 = (route[i]["lat"], route[i]["lng"])
        p2 = (route[i + 1]["lat"], route[i + 1]["lng"])
        total += haversine_km(p1, p2)
    return total


def solve_vrp_nearest_neighbor(
    depot: Dict, customers: List[Dict], vehicles: int, capacity: int
) -> Dict:
    pending = [dict(c) for c in customers]
    routes = []

    for vehicle_id in range(vehicles):
        current = dict(depot)
        load_left = capacity
        route = [dict(depot)]
        served_ids = []

        while pending:
            feasible = [c for c in pending if int(c.get("demand", 1)) <= load_left]
            if not feasible:
                break

            next_customer = min(
                feasible,
                key=lambda c: haversine_km(
                    (current["lat"], current["lng"]), (c["lat"], c["lng"])
                ),
            )

            route.append(next_customer)
            served_ids.append(next_customer["id"])
            load_left -= int(next_customer.get("demand", 1))
            current = next_customer
            pending = [c for c in pending if c["id"] != next_customer["id"]]

        route.append(dict(depot))
        routes.append(
            {
                "vehicle": vehicle_id + 1,
                "capacity": capacity,
                "used": capacity - load_left,
                "distance_km": round(route_distance_km(route), 3),
                "stops": route,
                "served_customer_ids": served_ids,
            }
        )

    unserved = [c["id"] for c in pending]
    total_distance = round(sum(r["distance_km"] for r in routes), 3)

    return {
        "routes": routes,
        "unserved_customer_ids": unserved,
        "summary": {
            "vehicles": vehicles,
            "customers": len(customers),
            "served": len(customers) - len(unserved),
            "unserved": len(unserved),
            "total_distance_km": total_distance,
        },
    }


HTML_PAGE = """
<!doctype html>
<html lang=\"es\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\" />
    <title>VRP básico con OpenStreetMap</title>
    <link
      rel=\"stylesheet\"
      href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\"
      integrity=\"sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=\"
      crossorigin=\"\"
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
    <div id=\"panel\">
      <h2>VRP básico</h2>
      <p class=\"small\">1) Haz clic en el mapa para marcar puntos.<br/>2) El primer punto es el depósito.<br/>3) El resto son clientes con demanda.</p>

      <div class=\"row\">
        <label>Modo de clic</label>
        <select id=\"mode\">
          <option value=\"depot\">Definir depósito</option>
          <option value=\"customer\" selected>Añadir cliente</option>
        </select>
      </div>

      <div class=\"row\">
        <label>Demanda para nuevo cliente</label>
        <input id=\"demand\" type=\"number\" min=\"1\" value=\"1\" />
      </div>

      <div class=\"row\">
        <label>Número de vehículos</label>
        <input id=\"vehicles\" type=\"number\" min=\"1\" value=\"2\" />
      </div>

      <div class=\"row\">
        <label>Capacidad por vehículo</label>
        <input id=\"capacity\" type=\"number\" min=\"1\" value=\"5\" />
      </div>

      <div class=\"row\" style=\"display:flex; gap:8px;\">
        <button id=\"solveBtn\">Resolver VRP</button>
        <button id=\"clearBtn\">Limpiar</button>
      </div>

      <div class=\"row\">
        <strong>Salida</strong>
        <pre id=\"output\">Esperando datos...</pre>
      </div>
    </div>
    <div id=\"map\"></div>

    <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\" integrity=\"sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=\" crossorigin=\"\"></script>
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

      const colors = ['#e41a1c','#377eb8','#4daf4a','#984ea3','#ff7f00','#a65628'];

      function redrawPoints() {
        markers.forEach(m => map.removeLayer(m));
        markers = [];

        if (depot) {
          markers.push(L.marker([depot.lat, depot.lng], {title:'Depósito'}).addTo(map).bindPopup('Depósito'));
        }

        for (const c of customers) {
          markers.push(L.circleMarker([c.lat, c.lng], {radius:8, color:'#222', fillColor:'#ffd54f', fillOpacity:0.95})
            .addTo(map)
            .bindPopup(`Cliente ${c.id} | Demanda: ${c.demand}`));
        }
      }

      function clearRoutes() {
        routeLayers.forEach(l => map.removeLayer(l));
        routeLayers = [];
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
        document.getElementById('output').textContent = 'Esperando datos...';
      });

      document.getElementById('solveBtn').addEventListener('click', async () => {
        if (!depot) {
          alert('Debes definir un depósito.');
          return;
        }
        if (!customers.length) {
          alert('Añade al menos un cliente.');
          return;
        }

        const payload = {
          depot,
          customers,
          vehicles: parseInt(document.getElementById('vehicles').value, 10),
          capacity: parseInt(document.getElementById('capacity').value, 10)
        };

        const resp = await fetch('/api/solve_vrp', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });

        if (!resp.ok) {
          document.getElementById('output').textContent = 'Error resolviendo VRP';
          return;
        }

        const data = await resp.json();
        document.getElementById('output').textContent = JSON.stringify(data, null, 2);

        clearRoutes();
        data.routes.forEach((r, idx) => {
          const latlngs = r.stops.map(s => [s.lat, s.lng]);
          const line = L.polyline(latlngs, { color: colors[idx % colors.length], weight: 4 }).addTo(map);
          routeLayers.push(line);
        });
      });
    </script>
  </body>
</html>
"""


@app.route(route="", methods=["GET"])
def ui(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(HTML_PAGE, mimetype="text/html", status_code=200)


@app.route(route="solve_vrp", methods=["POST"])
def solve_vrp(req: func.HttpRequest) -> func.HttpResponse:
    try:
        payload = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "JSON inválido"}),
            mimetype="application/json",
            status_code=400,
        )

    depot = payload.get("depot")
    customers = payload.get("customers", [])
    vehicles = int(payload.get("vehicles", 1))
    capacity = int(payload.get("capacity", 1))

    if not depot or not isinstance(customers, list) or len(customers) == 0:
        return func.HttpResponse(
            json.dumps({"error": "Faltan depot o customers"}),
            mimetype="application/json",
            status_code=400,
        )

    result = solve_vrp_nearest_neighbor(depot, customers, vehicles, capacity)
    return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)
