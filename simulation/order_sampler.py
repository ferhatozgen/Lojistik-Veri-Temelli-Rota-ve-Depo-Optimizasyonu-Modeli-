import os
import sys
import pandas as pd
import numpy as np
import random
from datetime import timedelta
from simulation.district_profiles import DISTRICT_PROFILES
from simulation.demand_engine import get_calendar_features

def haversine(lat1, lon1, lat2, lon2):
    import math
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))



def cluster_nodes_by_district(nodes_df):
    """OSMNX kütüphanesiyle çektiğimiz sokak düğümlerini yazdığımız district merkezine göre gruplar."""
    district_pools = {district: [] for district in DISTRICT_PROFILES.keys()}

    for _, node in nodes_df.iterrows():
        node_lat, node_lon = node['lat'], node['lon']

        # Her noktanın 5 bölge merkezine olan mesafesini hesapla
        distances = {}
        for d_name, d_profile in DISTRICT_PROFILES.items():
            dist = haversine(node_lat, node_lon, d_profile["center"][0], d_profile["center"][1])
            distances[d_name] = dist

        # En yakın bölgeyi bul ve o bölgenin havuzuna ekle
        closest_district = min(distances, key=distances.get)

        # Eğer çok uç bir noktaysa (örneğin merkeze 4km'den uzaksa) lojistik alan dışı bıraktım
        if distances[closest_district] <= 4.0:
            district_pools[closest_district].append((node_lat, node_lon))

    return district_pools


def sample_individual_orders(demand_csv_path, nodes_csv_path, output_csv_path):
    print(" Sipariş Üretim Fabrikası çalıştırılıyor...")

    # Verileri yükle
    if not os.path.exists(nodes_csv_path) or not os.path.exists(demand_csv_path):
        print(" Hata: Gerekli kaynak veri dosyaları (nodes veya hourly_demand) bulunamadı!")
        return

    nodes_df = pd.read_csv(nodes_csv_path)
    demand_df = pd.read_csv(demand_csv_path)

    # Sokak düğümlerini bölgelerine göre kategorize et
    district_pools = cluster_nodes_by_district(nodes_df)

    simulated_orders = []
    order_id_counter = 100000

    print(" Saatlik talepler gerçek sokak koordinatlarına dağıtılıyor...")
    for _, row in demand_df.iterrows():
        dt = pd.to_datetime(row["datetime"])
        district = row["district"]
        demand_count = int(row["demand"])
        weather = row["weather_label"]
        weekend = row["is_weekend"]

        pool = district_pools[district]
        if not pool:
            continue  # Eğer o bölgeye ait sokak düğümü bulunamadıysa es geç

        # O saatteki talep sayısı kadar sokak havuzundan rastgele koordinat seçiyoruz
        # Replace=True: Aynı sokaktan aynı saatte birden fazla sipariş gelebilir (Apartmanlar vb.)
        chosen_nodes = random.choices(pool, k=demand_count)

        for node in chosen_nodes:
            # Siparişlerin tam saat başında değil, o saatin içine (0-59 dk) rastsallıkla dağılması !sunum için kritiktir
            exact_time = dt + timedelta(minutes=random.randint(0, 59), seconds=random.randint(0, 59))

            cal = get_calendar_features(dt)
            simulated_orders.append({
                "order_id": f"ORD_{order_id_counter}",
                "timestamp": exact_time,
                "district": district,
                "lat": node[0],
                "lon": node[1],
                "weather_label": weather,
                "is_weekend": weekend,
                "is_special_day": cal["is_special_day"],
                "is_semester_break": cal["is_semester_break"],
                "is_summer_break": cal["is_summer_break"],
                "is_prep_week": cal["is_prep_week"],
                "exam_engineering": cal["exam_engineering"],
                "exam_medicine": cal["exam_medicine"],
                "exam_dentistry": cal["exam_dentistry"]
            })

    # DataFrame oluştur ve kaydet
    final_orders_df = pd.DataFrame(simulated_orders)
    final_orders_df.sort_values(by="timestamp", inplace=True)

    # Çıktı klasörünü kontrol et
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    final_orders_df.to_csv(output_csv_path, index=False)

    print(f" Başarılı! Toplam {len(final_orders_df)} adet tekil sipariş üretildi.")
    print(f" Dosya konumu: {output_csv_path}")


if __name__ == "__main__":
    sample_individual_orders(
        demand_csv_path=("../data/hourly_demand.csv"),
        nodes_csv_path=("../data/edirne_nodes.csv"),
        output_csv_path=os.path.join("../data/simulated_orders.csv")
    )