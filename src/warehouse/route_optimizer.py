import os
import sys
import pandas as pd
import numpy as np
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from src.utils import haversine_road_meters
import math

def haversine_distance(lat1, lon1, lat2, lon2):
    """İki GPS koordinatı arasındaki mesafeyi kilometre cinsinden hesaplar."""
    R = 6371.0  # Dünya'nın yarıçapı (km)
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    
    a = math.sin(dlat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)


def create_distance_and_demand_matrices(hub_coords, order_coords):
    """OR-Tools için mesafe matrisi ve talep dizisini hazırlar.

    Önemli not: OR-Tools maliyetleri integer ister. Önceki sürüm km cinsinden
    float mesafeyi int matrise yazdığı için 1 km altındaki yollar 0'a yuvarlanıyordu.
    Bu da jüri demosunda rotaların gerçekçilik kalitesini bozabilir. Bu sürümde
    yol eğriliği katsayısı uygulanmış metre değeri kullanılır.
    """
    all_points = [hub_coords] + list(order_coords)
    num_points = len(all_points)

    distance_matrix = np.zeros((num_points, num_points), dtype=int)
    for i in range(num_points):
        for j in range(num_points):
            if i == j:
                distance_matrix[i][j] = 0
            else:
                distance_matrix[i][j] = haversine_road_meters(
                    all_points[i][0], all_points[i][1],
                    all_points[j][0], all_points[j][1]
                )

    demands = [0] + [1] * (num_points - 1)
    return distance_matrix.tolist(), demands, all_points



def build_greedy_fallback_routes(hub_id, hub_row, hub_orders, vehicle_capacity):
    """OR-Tools çok büyük veri üzerinde süre içinde çözüm bulamazsa demo için güvenli rota üretir.

    Bu yöntem optimizasyon iddiası taşımaz; siparişleri hub'a olan mesafeye göre sıralar,
    araç kapasitesine göre parçalara böler ve her parçayı en yakın komşu mantığıyla dolaştırır.
    Böylece Streamlit demosu boş harita yerine çalışan bir rota katmanı gösterebilir.
    """
    if hub_orders.empty:
        return []

    hub_lat, hub_lon = float(hub_row['lat']), float(hub_row['lon'])
    orders = hub_orders.copy().reset_index(drop=True)
    orders['_depot_dist'] = orders.apply(
        lambda r: haversine_road_meters(hub_lat, hub_lon, float(r['lat']), float(r['lon'])), axis=1
    )
    orders = orders.sort_values('_depot_dist').reset_index(drop=True)

    route_results = []
    for vehicle_id, start in enumerate(range(0, len(orders), max(1, int(vehicle_capacity)))):
        chunk = orders.iloc[start:start + max(1, int(vehicle_capacity))].copy().reset_index(drop=True)
        current_lat, current_lon = hub_lat, hub_lon
        remaining = chunk.to_dict(orient='records')
        sequence = [{'order_id': 'DEPOT', 'lat': hub_lat, 'lon': hub_lon}]

        while remaining:
            next_idx = min(
                range(len(remaining)),
                key=lambda i: haversine_road_meters(current_lat, current_lon, float(remaining[i]['lat']), float(remaining[i]['lon']))
            )
            nxt = remaining.pop(next_idx)
            sequence.append({'order_id': nxt.get('order_id', ''), 'lat': float(nxt['lat']), 'lon': float(nxt['lon'])})
            current_lat, current_lon = float(nxt['lat']), float(nxt['lon'])

        sequence.append({'order_id': 'DEPOT', 'lat': hub_lat, 'lon': hub_lon})
        for seq_idx, point in enumerate(sequence):
            route_results.append({
                'hub_id': hub_id,
                'vehicle_id': vehicle_id,
                'sequence_no': seq_idx,
                'order_id': point['order_id'],
                'lat': point['lat'],
                'lon': point['lon'],
                'solver': 'greedy_fallback'
            })
    return route_results


def solve_vrp_for_hub(hub_id, hub_row, hub_orders, vehicle_count, vehicle_capacity):
    """
    Belirli bir hub ve ona atanmış siparişler için dinamik kurye ve kapasite kısıtlarıyla
    en kısa rotayı optimize eder.
    """
    hub_coords = (hub_row['lat'], hub_row['lon'])
    order_coords = hub_orders[['lat', 'lon']].values
    order_ids = hub_orders['order_id'].values

    if len(hub_orders) == 0:
        return []

    # Mesafe ve talep matrislerini oluştur
    distance_matrix, demands, all_points = create_distance_and_demand_matrices(hub_coords, order_coords)

    #  ARTIK DİNAMİK: Parametre olarak gelen kurye sayısı (vehicle_count) modele geçiliyor
    manager = pywrapcp.RoutingIndexManager(len(distance_matrix), vehicle_count, 0)
    routing = pywrapcp.RoutingModel(manager)

    def distance_callback(from_index, to_index):
        return distance_matrix[manager.IndexToNode(from_index)][manager.IndexToNode(to_index)]

    transit_callback_index = routing.RegisterTransitCallback(distance_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    def demand_callback(from_index):
        return demands[manager.IndexToNode(from_index)]

    #  ARTIK DİNAMİK: Parametre olarak gelen araç kapasitesi modele işleniyor
    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,
        [vehicle_capacity] * vehicle_count,  # Her bir kuryenin esnek kapasitesi
        True,
        'Capacity'
    )

    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.seconds = 8

    solution = routing.SolveWithParameters(search_parameters)

    route_results = []
    if solution:
        for vehicle_id in range(vehicle_count):
            index = routing.Start(vehicle_id)
            node_sequence = []

            while not routing.IsEnd(index):
                node_index = manager.IndexToNode(index)
                node_sequence.append(node_index)
                index = solution.Value(routing.NextVar(index))

            # Sadece depodan çıkış yapan aktif araçların rotasını kaydet.
            # Rota görsel olarak kapansın diye son depot dönüşünü de ekliyoruz.
            if len(node_sequence) > 1:
                node_sequence.append(0)
                for seq_idx, node_id in enumerate(node_sequence):
                    ord_id = "DEPOT" if node_id == 0 else order_ids[node_id - 1]
                    route_results.append({
                        "hub_id": hub_id,
                        "vehicle_id": vehicle_id,
                        "sequence_no": seq_idx,
                        "order_id": ord_id,
                        "lat": all_points[node_id][0],
                        "lon": all_points[node_id][1],
                        "solver": "ortools_cvrp"
                    })
    if not route_results:
        print(f"    OR-Tools süre içinde çözüm bulamadı; demo için greedy fallback rota üretildi.")
        return build_greedy_fallback_routes(hub_id, hub_row, hub_orders, vehicle_capacity)

    return route_results


def run_route_optimization_engine(user_vehicle_capacity=25, manual_courier_extra=0):
    """
    Ana Rota Yönetim Motoru.
    user_vehicle_capacity: Arayüzden seçilen araç kargo kapasitesi
    manual_courier_extra: Minimum kurye sayısının üzerine yöneticinin eklemek istediği ekstra kurye sayısı
    """
    print("\n Dinamik Rota Optimizasyon Motoru Başlatıldı...")

    hubs_path = os.path.join(ROOT_DIR, "data", "active_hubs.csv")
    orders_path = os.path.join(ROOT_DIR, "data", "orders_with_hubs.csv")
    output_path = os.path.join(ROOT_DIR, "data", "optimized_routes.csv")

    if not os.path.exists(hubs_path) or not os.path.exists(orders_path):
        print(" Hata: Gerekli ön veriler bulunamadı! Önce hub_optimizer'ı çalıştırın.")
        return

    df_hubs = pd.read_csv(hubs_path)
    df_orders = pd.read_csv(orders_path)

    all_optimized_routes = []

    for idx, hub_row in df_hubs.iterrows():
        hub_id = int(hub_row['hub_id'])
        hub_orders = df_orders[df_orders['assigned_hub'] == hub_id]
        total_hub_orders = len(hub_orders)

        if total_hub_orders == 0:
            continue

        #  AKILLI KÖPRÜ: Matematiksel olarak kilitlenmeyi engelleyen minimum kurye sınırını buluyoruz
        min_required_couriers = int(np.ceil(total_hub_orders / user_vehicle_capacity))

        # Yöneticinin panelden artırdığı kurye sayısını minimum kurye sayısının üzerine ekliyoruz
        final_courier_count = min_required_couriers + manual_courier_extra

        print(f" Hub {hub_id} Analizi -> Toplam Kargo: {total_hub_orders} | Araç Kapasitesi: {user_vehicle_capacity}")
        print(
            f"    Sistem Hesabı -> Güvenli Minimum Kurye: {min_required_couriers} | Sahaya Sürelen Aktif Kurye: {final_courier_count}")

        # Dinamik parametrelerle çözücüyü tetikliyoruz
        hub_routes = solve_vrp_for_hub(hub_id, hub_row, hub_orders, final_courier_count, user_vehicle_capacity)
        all_optimized_routes.extend(hub_routes)

    if all_optimized_routes:
        df_routes = pd.DataFrame(all_optimized_routes)
        df_routes.to_csv(output_path, index=False)
        print(f" Rotalar Çizildi! '{output_path}' dosyasına kaydedildi.")
        return df_routes.to_dict(orient="records")

    print(" Uyarı: Uygun bir rota planı üretilemedi.")
    return []


if __name__ == "__main__":
    # Test Çalıştırması: Araç kapasitesini 20 kargo seçelim ve minimum kuryelerin üzerine 1 ekstra kurye ekleyelim
    run_route_optimization_engine(user_vehicle_capacity=20, manual_courier_extra=1)