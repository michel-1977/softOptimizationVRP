import math
import urllib.parse
import urllib.request
import json
import time
from typing import Dict, List, Optional, Tuple


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


class Node:
    def __init__(
        self,
        node_id,
        lat: float,
        lng: float,
        demand: float,
        payload: Optional[Dict] = None,
    ) -> None:
        self.ID = node_id
        self.lat = lat
        self.lng = lng
        self.demand = demand
        self.payload = dict(payload) if payload is not None else {}
        self.in_route: Optional["Route"] = None
        self.is_interior = False
        self.dn_edge: Optional["Edge"] = None
        self.nd_edge: Optional["Edge"] = None


class Edge:
    def __init__(self, origin: Node, end: Node, cost: float = 0.0) -> None:
        self.origin = origin
        self.end = end
        self.cost = cost
        self.savings = 0.0
        self.inv_edge: Optional["Edge"] = None


class Route:
    def __init__(self) -> None:
        self.cost = 0.0
        self.edges: List[Edge] = []
        self.demand = 0.0

    def reverse(self) -> None:
        self.edges = [edge.inv_edge for edge in reversed(self.edges) if edge.inv_edge]


def build_distance_matrix_km(
    points: List[Dict], distance_mode: str, osrm_base_url: str
) -> List[List[float]]:
    matrix, _ = _build_distance_matrix_km_with_meta(points, distance_mode, osrm_base_url)
    return matrix


def _build_distance_matrix_km_with_meta(
    points: List[Dict], distance_mode: str, osrm_base_url: str
) -> Tuple[List[List[float]], Dict[str, Optional[str]]]:
    if distance_mode == "direct":
        return [
            [
                haversine_km((a["lat"], a["lng"]), (b["lat"], b["lng"]))
                for b in points
            ]
            for a in points
        ], {"distance_source": "direct", "warning": None}

    coords = ";".join(f"{p['lng']},{p['lat']}" for p in points)
    encoded_coords = urllib.parse.quote(coords, safe=";,")
    base = osrm_base_url.rstrip("/")
    url = f"{base}/table/v1/driving/{encoded_coords}?annotations=distance"

    data = None
    last_exc = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=25) as response:
                data = json.loads(response.read().decode("utf-8"))
            break
        except Exception as exc:
            last_exc = exc
            if attempt < 2:
                time.sleep(0.35 * (attempt + 1))

    if data is None:
        # Keep the solve path available even when public OSRM is overloaded.
        return [
            [
                haversine_km((a["lat"], a["lng"]), (b["lat"], b["lng"]))
                for b in points
            ]
            for a in points
        ], {
            "distance_source": "direct_fallback",
            "warning": f"OSRM table unavailable, using direct distances. Reason: {last_exc}",
        }

    if data.get("code") != "Ok" or "distances" not in data:
        return [
            [
                haversine_km((a["lat"], a["lng"]), (b["lat"], b["lng"]))
                for b in points
            ]
            for a in points
        ], {
            "distance_source": "direct_fallback",
            "warning": "OSRM returned invalid table payload, using direct distances.",
        }

    matrix_m = data["distances"]
    matrix_km = []
    for row in matrix_m:
        matrix_km.append(
            [float("inf") if value is None else float(value) / 1000.0 for value in row]
        )
    return matrix_km, {"distance_source": "osrm", "warning": None}


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


def _check_merging_conditions(
    i_node: Node, j_node: Node, i_route: Route, j_route: Route, capacity: int
) -> bool:
    if i_route is j_route:
        return False
    if i_node.is_interior or j_node.is_interior:
        return False
    if i_route.demand + j_route.demand > capacity:
        return False
    return True


def _get_depot_edge(a_route: Route, a_node: Node, depot: Node) -> Edge:
    first = a_route.edges[0]
    if (first.origin is a_node and first.end is depot) or (
        first.origin is depot and first.end is a_node
    ):
        return first
    return a_route.edges[-1]


def _route_stops(route: Route, depot: Node) -> List[Dict]:
    if not route.edges:
        return [dict(depot.payload), dict(depot.payload)]

    if route.edges[0].origin is not depot:
        route.reverse()

    stops = [dict(route.edges[0].origin.payload)]
    for edge in route.edges:
        stops.append(dict(edge.end.payload))

    if stops[0].get("id") != depot.ID:
        stops.insert(0, dict(depot.payload))
    if stops[-1].get("id") != depot.ID:
        stops.append(dict(depot.payload))
    return stops


def _route_customer_count(route: Route, depot: Node) -> int:
    return sum(1 for edge in route.edges if edge.end is not depot)


def _build_clarke_wright_routes(
    depot: Dict,
    customers: List[Dict],
    capacity: int,
    distance_matrix_km: List[List[float]],
    idx_by_id: Dict,
) -> Tuple[List[Route], Node]:
    depot_node = Node(
        depot["id"],
        float(depot["lat"]),
        float(depot["lng"]),
        0.0,
        payload=depot,
    )
    nodes = [depot_node]
    for customer in customers:
        nodes.append(
            Node(
                customer["id"],
                float(customer["lat"]),
                float(customer["lng"]),
                float(customer.get("demand", 1)),
                payload=customer,
            )
        )

    depot_idx = idx_by_id[depot_node.ID]
    for node in nodes[1:]:
        node_idx = idx_by_id[node.ID]
        dn_cost = distance_matrix_km[depot_idx][node_idx]
        nd_cost = distance_matrix_km[node_idx][depot_idx]
        dn_edge = Edge(depot_node, node, dn_cost)
        nd_edge = Edge(node, depot_node, nd_cost)
        dn_edge.inv_edge = nd_edge
        nd_edge.inv_edge = dn_edge
        node.dn_edge = dn_edge
        node.nd_edge = nd_edge

    savings_list: List[Edge] = []
    for i in range(1, len(nodes) - 1):
        i_node = nodes[i]
        i_idx = idx_by_id[i_node.ID]
        for j in range(i + 1, len(nodes)):
            j_node = nodes[j]
            j_idx = idx_by_id[j_node.ID]

            ij_edge = Edge(i_node, j_node, distance_matrix_km[i_idx][j_idx])
            ji_edge = Edge(j_node, i_node, distance_matrix_km[j_idx][i_idx])
            ij_edge.inv_edge = ji_edge
            ji_edge.inv_edge = ij_edge

            ij_edge.savings = i_node.nd_edge.cost + j_node.dn_edge.cost - ij_edge.cost
            ji_edge.savings = j_node.nd_edge.cost + i_node.dn_edge.cost - ji_edge.cost
            savings_list.extend((ij_edge, ji_edge))

    savings_list.sort(key=lambda edge: edge.savings, reverse=True)

    routes: List[Route] = []
    for node in nodes[1:]:
        route = Route()
        route.edges.append(node.dn_edge)
        route.cost += node.dn_edge.cost
        route.edges.append(node.nd_edge)
        route.cost += node.nd_edge.cost
        route.demand += node.demand
        node.in_route = route
        node.is_interior = False
        routes.append(route)

    while savings_list:
        ij_edge = savings_list.pop(0)
        i_node = ij_edge.origin
        j_node = ij_edge.end
        i_route = i_node.in_route
        j_route = j_node.in_route

        if i_route is None or j_route is None:
            continue
        if not _check_merging_conditions(i_node, j_node, i_route, j_route, capacity):
            continue

        i_edge = _get_depot_edge(i_route, i_node, depot_node)
        i_route.edges.remove(i_edge)
        i_route.cost -= i_edge.cost
        if len(i_route.edges) > 1:
            i_node.is_interior = True
        if i_route.edges and i_route.edges[0].origin is not depot_node:
            i_route.reverse()

        j_edge = _get_depot_edge(j_route, j_node, depot_node)
        j_route.edges.remove(j_edge)
        j_route.cost -= j_edge.cost
        if len(j_route.edges) > 1:
            j_node.is_interior = True
        if j_route.edges and j_route.edges[0].origin is depot_node:
            j_route.reverse()

        i_route.edges.append(ij_edge)
        i_route.cost += ij_edge.cost
        i_route.demand += j_node.demand
        j_node.in_route = i_route

        for edge in j_route.edges:
            i_route.edges.append(edge)
            i_route.cost += edge.cost
            i_route.demand += edge.end.demand
            edge.end.in_route = i_route

        routes.remove(j_route)

    return routes, depot_node


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
    warnings: List[str] = []
    distance_source = "direct" if distance_mode == "direct" else "osrm"

    eligible_customers = [
        dict(customer)
        for customer in customers
        if float(customer.get("demand", 1)) <= capacity
    ]
    points = [dict(depot)] + eligible_customers
    idx_by_id = {point["id"]: idx for idx, point in enumerate(points)}

    if len(points) > 1:
        distance_matrix_km, matrix_meta = _build_distance_matrix_km_with_meta(
            points, distance_mode, osrm_base_url
        )
        distance_source = matrix_meta.get("distance_source") or distance_source
        warning = matrix_meta.get("warning")
        if warning:
            warnings.append(str(warning))
    else:
        distance_matrix_km = [[0.0]]

    reachable_customers = []
    for customer in eligible_customers:
        customer_idx = idx_by_id[customer["id"]]
        if not math.isinf(distance_matrix_km[0][customer_idx]) and not math.isinf(
            distance_matrix_km[customer_idx][0]
        ):
            reachable_customers.append(customer)

    if len(reachable_customers) != len(eligible_customers):
        points = [dict(depot)] + reachable_customers
        idx_by_id = {point["id"]: idx for idx, point in enumerate(points)}
        if len(points) > 1:
            distance_matrix_km, matrix_meta = _build_distance_matrix_km_with_meta(
                points, distance_mode, osrm_base_url
            )
            distance_source = matrix_meta.get("distance_source") or distance_source
            warning = matrix_meta.get("warning")
            if warning:
                warnings.append(str(warning))
        else:
            distance_matrix_km = [[0.0]]

    cw_routes: List[Route] = []
    depot_node = Node(
        depot["id"],
        float(depot["lat"]),
        float(depot["lng"]),
        0.0,
        payload=depot,
    )
    if reachable_customers:
        cw_routes, depot_node = _build_clarke_wright_routes(
            depot, reachable_customers, capacity, distance_matrix_km, idx_by_id
        )

    selected_routes = list(cw_routes)
    if len(selected_routes) > vehicles:
        selected_routes.sort(
            key=lambda route: (
                -_route_customer_count(route, depot_node),
                -route.demand,
                route.cost,
            )
        )
        selected_routes = selected_routes[:vehicles]

    routes = []
    served_customer_ids = set()
    for vehicle_id in range(vehicles):
        if vehicle_id < len(selected_routes):
            route_obj = selected_routes[vehicle_id]
            stops = _route_stops(route_obj, depot_node)
            served_ids = [
                stop["id"] for stop in stops if stop.get("id") != depot.get("id")
            ]
            for served_id in served_ids:
                served_customer_ids.add(served_id)
            used = sum(float(stop.get("demand", 0)) for stop in stops[1:-1])
        else:
            stops = [dict(depot), dict(depot)]
            served_ids = []
            used = 0.0

        routes.append(
            {
                "vehicle": vehicle_id + 1,
                "capacity": capacity,
                "used": int(used) if float(used).is_integer() else round(used, 3),
                "distance_km": round(
                    route_distance_from_matrix_km(stops, idx_by_id, distance_matrix_km),
                    3,
                ),
                "stops": stops,
                "served_customer_ids": served_ids,
            }
        )

    unserved = [c["id"] for c in customers if c["id"] not in served_customer_ids]
    total_distance = round(sum(r["distance_km"] for r in routes), 3)

    return {
        "routes": routes,
        "unserved_customer_ids": unserved,
        "warnings": warnings[:5],
        "summary": {
            "vehicles": vehicles,
            "customers": len(customers),
            "served": len(customers) - len(unserved),
            "unserved": len(unserved),
            "total_distance_km": total_distance,
            "distance_source": distance_source,
        },
    }
