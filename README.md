# Basic VRP with OpenStreetMap + Azure Functions

This project provides a small Vehicle Routing Problem (VRP) demo:

- Web UI with OpenStreetMap (Leaflet)
- HTTP API to solve VRP with a Clarke & Wright savings heuristic and capacity limits
- Azure Functions Python app ready for local run and deployment

## Project structure

- `function_app.py`: HTTP routes and UI page
- `solve_vrp/__init__.py`: VRP solver logic
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
