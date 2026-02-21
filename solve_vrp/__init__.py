import math
import urllib.parse
import urllib.request
import json
from typing import Dict, List, Tuple


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


def build_distance_matrix_km(
    points: List[Dict], distance_mode: str, osrm_base_url: str
) -> List[List[float]]:
    if distance_mode == "direct":
        return [
            [
                haversine_km((a["lat"], a["lng"]), (b["lat"], b["lng"]))
                for b in points
            ]
            for a in points
        ]

    coords = ";".join(f"{p['lng']},{p['lat']}" for p in points)
    encoded_coords = urllib.parse.quote(coords, safe=";,")
    base = osrm_base_url.rstrip("/")
    url = f"{base}/table/v1/driving/{encoded_coords}?annotations=distance"

    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            data = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to query OSRM table API: {exc}") from exc

    if data.get("code") != "Ok" or "distances" not in data:
        raise RuntimeError("OSRM did not return a valid distance matrix.")

    matrix_m = data["distances"]
    matrix_km = []
    for row in matrix_m:
        matrix_km.append(
            [float("inf") if value is None else float(value) / 1000.0 for value in row]
        )
    return matrix_km


def route_distance_from_matrix_km(
    route: List[Dict], idx_by_id: Dict, distance_matrix_km: List[List[float]]
) -> float:
    if len(route) < 2:
        return 0.0

    total = 0.0
    for i in range(len(route) - 1):
        a = route[i]
        b = route[i + 1]
        ai = idx_by_id.get(a["id"])
        bi = idx_by_id.get(b["id"])

        if ai is None or bi is None:
            total += haversine_km((a["lat"], a["lng"]), (b["lat"], b["lng"]))
            continue

        leg = distance_matrix_km[ai][bi]
        if math.isinf(leg):
            total += haversine_km((a["lat"], a["lng"]), (b["lat"], b["lng"]))
        else:
            total += leg
    return total


def solve_vrp_nearest_neighbor(
    depot: Dict,
    customers: List[Dict],
    vehicles: int,
    capacity: int,
    distance_mode: str = "direct",
    osrm_base_url: str = "https://router.project-osrm.org",
) -> Dict:
    if distance_mode not in {"direct", "osrm"}:
        raise RuntimeError("distance_mode must be either 'direct' or 'osrm'.")

    pending = [dict(c) for c in customers]
    routes = []
    points = [dict(depot)] + [dict(c) for c in customers]
    idx_by_id = {p["id"]: idx for idx, p in enumerate(points)}
    distance_matrix_km = build_distance_matrix_km(points, distance_mode, osrm_base_url)

    for vehicle_id in range(vehicles):
        current = dict(depot)
        current_idx = idx_by_id[current["id"]]
        load_left = capacity
        route = [dict(depot)]
        served_ids = []

        while pending:
            feasible = [c for c in pending if int(c.get("demand", 1)) <= load_left]
            if not feasible:
                break

            next_customer = min(
                feasible,
                key=lambda c: distance_matrix_km[current_idx][idx_by_id[c["id"]]],
            )

            route.append(next_customer)
            served_ids.append(next_customer["id"])
            load_left -= int(next_customer.get("demand", 1))
            current = next_customer
            current_idx = idx_by_id[current["id"]]
            pending = [c for c in pending if c["id"] != next_customer["id"]]

        route.append(dict(depot))
        routes.append(
            {
                "vehicle": vehicle_id + 1,
                "capacity": capacity,
                "used": capacity - load_left,
                "distance_km": round(
                    route_distance_from_matrix_km(route, idx_by_id, distance_matrix_km),
                    3,
                ),
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
