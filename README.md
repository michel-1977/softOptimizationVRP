# Basic VRP with OpenStreetMap + Azure Functions

This project provides a small Vehicle Routing Problem (VRP) demo:

- Web UI with OpenStreetMap (Leaflet)
- HTTP API to solve VRP with a Clarke & Wright savings heuristic and capacity limits
- Azure Functions Python app ready for local run and deployment

## Project structure

- `function_app.py`: HTTP routes and UI page
- `solve_vrp/__init__.py`: VRP solver logic
- `solve_vrp/semantic_layer.py`: Route semantic enrichment (locations + weather/traffic context)
- `solve_vrp/here_platform.py`: HERE API integration (weather + traffic real-time and forecast aggregation)
- `solve_vrp/here_emulator.py`: HERE-like emulator for weather/traffic structures
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

## Semantic Layer (v0.5)

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
- `use_here_platform`: enable/disable HERE API integration (`true` by default when API key exists)
- `here_data_source`: `here` (live HERE APIs) or `emulator` (randomized HERE-like responses)
- `here_pipeline_mode`: `postprocessing` (default, solve first) or `before_vrp` (prefetch before solve)
- `here_timeout_sec`: HTTP timeout for HERE calls
- `here_traffic_radius_m`: real-time traffic query radius around each segment midpoint
- `here_forecast_window_hours`: forecast window size (default 24)
- `here_forecast_interval_min`: sampling interval for forecast slots (default 120)

The response `semantic_layer` contains:

- `routes[].semantic_locations`: ranked near-route locations with distance/detour and relevance
- `routes[].segment_context`: per-segment ETA, midpoint, matched weather/traffic plus `forecast_24h`
- `routes[].semantic_locations[].weather|traffic`: location-linked context copied from nearest segment

### HERE data behavior

When `here_data_source=here` and env var `HERE_API_KEY` is configured, each segment includes:

- `weather`: near real-time weather status from HERE + `forecast_24h` with worst-case score and time slots
- `traffic`: near real-time traffic status from HERE + `forecast_24h` with worst delay ratio/seconds and time slots

When `here_data_source=emulator`, the app generates randomized weather/traffic values in the same normalized HERE output shape used by this project (no API key required).
Without HERE key in live mode, the system falls back to user-provided `weather_observations` / `traffic_observations`.

### Local key setup

For local Azure Functions runs, set your HERE key in `local.settings.json`:

```json
{
  "Values": {
    "HERE_API_KEY": "YOUR_HERE_KEY"
  }
}
```

### Pipeline modes

- `postprocessing` (recommended): VRP is solved first, then HERE enrichment is applied.
- `before_vrp`: HERE snapshots are prefetched first and then VRP is solved; useful for experimentation with pre-solve context capture.

Combine data source and pipeline independently for 4 operating modes:

- live HERE + postprocessing
- live HERE + before_vrp
- emulator + postprocessing
- emulator + before_vrp
