import os
import sys
import pandas as pd
import numpy as np
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)


def haversine_distance(lat1, lon1, lat2, lon2):
    """İki koordinat arasındaki mesafeyi yol eğrilik çarpanı (1.3) ile hesaplar."""
    R = 6371000  # Dünya yarıçapı (metre)
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    # 1.3 Yol Eğrilik Katsayısı (Edirne sokak simülasyonu için)
    return int(R * c * 1.3)


def create_distance_and_demand_matrices(hub_coords, order_coords):
    """OR-Tools için mesafe matrisi ve talep dizisini hazırlar."""
    all_points = [hub_coords] + list(order_coords)
    num_points = len(all_points)

    distance_matrix = np.zeros((num_points, num_points), dtype=int)
    for i in range(num_points):
        for j in range(num_points):
            if i == j:
                distance_matrix[i][j] = 0
            else:
                distance_matrix[i][j] = haversine_distance(
                    all_points[i][0], all_points[i][1],
                    all_points[j][0], all_points[j][1]
                )

    demands = [0] + [1] * (num_points - 1)
    return distance_matrix.tolist(), demands, all_points


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
    search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_parameters.time_limit.seconds = 2

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

            # Sadece depodan çıkış yapan aktif araçların rotasını kaydet
            if len(node_sequence) > 1:
                for seq_idx, node_id in enumerate(node_sequence):
                    ord_id = "DEPOT" if node_id == 0 else order_ids[node_id - 1]
                    route_results.append({
                        "hub_id": hub_id,
                        "vehicle_id": vehicle_id,
                        "sequence_no": seq_idx,
                        "order_id": ord_id,
                        "lat": all_points[node_id][0],
                        "lon": all_points[node_id][1]
                    })
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
        print(f" Rotalar Çizildi! Günlük kurye planı '{output_path}' dosyasına kaydedildi.")
    else:
        print(" Uyarı: Uygun bir rota planı üretilemedi.")


if __name__ == "__main__":
    # Test Çalıştırması: Araç kapasitesini 20 kargo seçelim ve minimum kuryelerin üzerine 1 ekstra kurye ekleyelim
    run_route_optimization_engine(user_vehicle_capacity=20, manual_courier_extra=1)