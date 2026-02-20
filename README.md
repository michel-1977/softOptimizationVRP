# VRP básico con OpenStreetMap + Azure Functions

Este repositorio contiene un programa base para resolver un **Vehicle Routing Problem (VRP)** usando un heurístico simple (vecino más cercano con restricción de capacidad), con:

- **Interfaz gráfica web** sobre **OpenStreetMap** (Leaflet).
- **Entrada/salida visual** (clic para crear depósito/clientes y visualización de rutas).
- **Backend en Azure Functions** listo para desplegar en Azure.

## Estructura

- `function_app.py`: Azure Function HTTP con dos endpoints:
  - `GET /api/` devuelve la interfaz HTML.
  - `POST /api/solve_vrp` resuelve el VRP y devuelve JSON.
- `requirements.txt`: dependencias Python.
- `host.json`: configuración de Azure Functions.

## Requisitos

- Python 3.10+
- Azure Functions Core Tools (`func`)

## Ejecución local

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
func start
```

Abre en navegador:

- `http://localhost:7071/api/`

## Uso rápido

1. Selecciona **Definir depósito** y haz clic en el mapa.
2. Cambia a **Añadir cliente** y haz clic para añadir clientes.
3. Ajusta demanda, número de vehículos y capacidad.
4. Haz clic en **Resolver VRP**.
5. Verás rutas dibujadas en el mapa y la salida JSON en el panel.

## Despliegue en Azure

Con Azure CLI y Core Tools autenticados:

```bash
az login
az group create --name rg-vrp-demo --location westeurope
az storage account create --name vrpstorage$RANDOM --location westeurope --resource-group rg-vrp-demo --sku Standard_LRS
az functionapp create \
  --resource-group rg-vrp-demo \
  --consumption-plan-location westeurope \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name <NOMBRE_UNICO_FUNC_APP> \
  --storage-account <STORAGE_ACCOUNT>

func azure functionapp publish <NOMBRE_UNICO_FUNC_APP>
```

> Nota: para producción, conviene reemplazar el heurístico por OR-Tools o un solver más robusto.
