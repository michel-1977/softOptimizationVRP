# Basic VRP with OpenStreetMap + Azure Functions

This project provides a small Vehicle Routing Problem (VRP) demo:

- Web UI with OpenStreetMap (Leaflet)
- HTTP API to solve VRP with a Clarke & Wright savings heuristic and capacity limits
- Azure Functions Python app ready for local run and deployment

## Project structure

- `function_app.py`: HTTP routes and UI page
- `solve_vrp/__init__.py`: VRP solver logic
- `solve_vrp/semantic_layer.py`: Route semantic enrichment (locations + weather/traffic context)
- `host.json`: Function host settings
- `local.settings.json`: Local runtime settings
- `requirements.txt`: Dependencies

## Requirements

- Python 3.10+
- Azure Functions Core Tools v4 (`func`)

## Run locally

### PowerShell (Windows)

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
func start
```

### Bash

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
func start
```

Open one of these URLs:

- `http://localhost:7071/`
- `http://localhost:7071/api`

Both solve endpoints are available:

- `POST http://localhost:7071/solve_vrp`
- `POST http://localhost:7071/api/solve_vrp`

## Semantic Layer (v0.3)

Every solve response now includes `semantic_layer` by default (disable with `include_semantic_layer=false`).

Optional request fields for enrichment:

- `candidate_locations`: list of candidate POIs, each with `lat`, `lng`, optional `id`, `name`, `semantic_category`, `tags`, `source`
- `semantic_categories`: list of category filters (for relevance scoring)
- `semantic_corridor_radius_km`: max distance from route to consider location relevant
- `semantic_top_k`: max ranked locations returned per route
- `departure_time_utc`: ISO UTC timestamp for ETA-linked context
- `route_avg_speed_kmh`: speed assumption used to compute segment ETAs
- `weather_observations`: structured weather points (`lat`, `lng`, `time_utc`, and weather fields)
- `traffic_observations`: structured traffic points (`lat`, `lng`, `time_utc`, and traffic fields)

The response `semantic_layer` contains:

- `routes[].semantic_locations`: ranked near-route locations with distance/detour and relevance
- `routes[].segment_context`: per-segment ETA, midpoint, matched weather, matched traffic
