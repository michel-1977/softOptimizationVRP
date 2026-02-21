import math
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
