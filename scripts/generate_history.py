import pandas as pd
import numpy as np
import random
from datetime import timedelta
from src.data_logic.feature_engine import calculate_logistics_metrics, haversine
from src.data_logic.locations import EDIRNE_LOCATIONS


def get_node_weights(nodes_list):
    """Her koordinata, kritik noktalara uzaklığına göre bir 'seçilme ağırlığı' verir."""
    from src.data_logic.locations import EDIRNE_LOCATIONS
    weights = []

    for node in nodes_list:
        lat, lon = node[0], node[1]

        # 1. Mesafeleri hesapla
        dists = {name: haversine(lat, lon, *coords) for name, coords in EDIRNE_LOCATIONS.items()}

        # 2. Temel ağırlık
        weight = 1.0

        # --- ÖZEL BÖLGE YOĞUNLUKLARI ---
        # Delta ve Balkan (Öğrenci bölgeleri en yoğun yerler)
        if dists["DELTA_DORMS"] < 0.8 or dists["BALKAN_DORMS"] < 0.8:
            weight *= 30.0

            # Şehir Merkezi ve Ayşekadın (Ticari yoğunluk)
        elif dists["CENTER_SELIMIYE"] < 1.0 or dists["AYSEKADIN_ZUBEYDE"] < 0.8:
            weight *= 8.0

        # Yeni eklediğimiz yerleşim yerleri (Fatih, Cumhuriyet, Zağra Hattı)
        elif dists["FATIH_MAH"] < 1.0 or dists.get("CUMHURIYET", (0, 0))[0] != 0 and dists["CUMHURİYET"] < 1.0:
            weight *= 5.0

        # 3. UZAKLIK KISITLAMASI (Gürültüyü engelleme)
        # Eğer nokta, belirlediğimiz stratejik noktaların hiçbirine 3.5 km'den yakın değilse
        # (Yani çok uçta, ıssız bir yerse) ağırlığını çok düşür.
        min_dist = min(dists.values())
        if min_dist > 3.5:
            weight *= 0.01

        weights.append(weight)

    return weights

def run_history_generation(weather_csv, nodes_csv, output_csv):
    print("🚀 Veri Üretim Fabrikası senin Feature Engine mantığınla çalışıyor...")

    # Verileri Yükle
    df_weather = pd.read_csv(weather_csv)
    df_weather['time'] = pd.to_datetime(df_weather['time'])


    df_nodes = pd.read_csv(nodes_csv)
    nodes_list = df_nodes[['lat', 'lon']].values.tolist()
    node_weights = get_node_weights(nodes_list)

    history_data = []

    for _, row in df_weather.iterrows():
        dt = row['time']
        wl = row['weather_label']

        # Her saat başında senin fonksiyonundan geçecek örnek sipariş sayısını belirliyoruz
        # (Buradaki sayı, her saat için kaç tane 'örnek nokta' seçeceğimizi belirler)
        # Poisson burada kaç farklı sipariş lokasyonu oluşacağını belirler
        hour = dt.hour
        expected_orders = 20 if (18 <= hour <= 22) else 10
        count = np.random.poisson(expected_orders)

        for _ in range(count):
            # OSMnx'ten gelen gerçek sokak noktası
            chosen_node = random.choices(nodes_list, weights=node_weights, k=1)[0]
            lat, lon= chosen_node[0], chosen_node[1]

            # SENİN FONKSİYONUN: Tüm zeki hesaplamaları burada yapıyoruz
            t_idx, vol, is_event = calculate_logistics_metrics(lat, lon, dt, wl)

            history_data.append({
                'delivery_timestamp': dt + timedelta(minutes=random.randint(0, 59)),
                'lat': lat,
                'lon': lon,
                'weather_label': wl,
                'traffic_index': t_idx,
                'order_volume': vol,
                'is_special_event': is_event,
                'hour': hour,
                'day_of_week': dt.weekday()
            })

    # Kaydetme
    final_df = pd.DataFrame(history_data)
    final_df.sort_values(by='delivery_timestamp', inplace=True)
    final_df.to_csv(output_csv, index=False)
    print(f"✅ BÜYÜK BİRLEŞTİRME TAMAMLANDI! '{output_csv}' hazır.")


if __name__ == "__main__":
    run_history_generation(
        weather_csv='../data/edirne_weather_2025_2026.csv',
        nodes_csv='../data/edirne_nodes.csv',
        output_csv='../data/orders_history.csv'
    )