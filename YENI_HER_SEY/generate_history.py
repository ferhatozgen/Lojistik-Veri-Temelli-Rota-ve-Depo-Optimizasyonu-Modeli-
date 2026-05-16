import pandas as pd
import numpy as np
import random
from datetime import timedelta
from src.data_logic.feature_engine import calculate_logistics_metrics


def run_history_generation(weather_csv, output_csv):
    print("🚀 Veri üretim fabrikası çalışıyor...")
    df_weather = pd.read_csv(weather_csv)
    df_weather['time'] = pd.to_datetime(df_weather['time'])

    history_data = []

    for _, row in df_weather.iterrows():
        dt = row['time']
        wl = row['weather_label']

        # O saatteki toplam sipariş sayısını Poisson ile belirle (Feature engine'deki mantığa paralel)
        # Örnek: Ortalama 20 sipariş/saat
        avg_orders = 20 if (18 <= dt.hour <= 22) else 10
        count = np.random.poisson(avg_orders)

        for _ in range(count):
            lat = random.uniform(41.63, 41.71)
            lon = random.uniform(26.50, 26.62)

            t_idx, vol = calculate_logistics_metrics(lat, lon, dt, wl)

            history_data.append({
                'delivery_timestamp': dt + timedelta(minutes=random.randint(0, 59)),
                'lat': lat,
                'lon': lon,
                'weather_label': wl,
                'traffic_index': t_idx,
                'order_volume': vol,
                'hour': dt.hour,
                'day_of_week': dt.weekday(),
                'is_special_event': 0  # Kırkpınar vb. tarihler buraya manuel eklenebilir
            })

    final_df = pd.DataFrame(history_data)
    final_df.to_csv(output_csv, index=False)
    print(f"✅ İşlem tamam! {len(final_df)} satır 'data/orders_history.csv' içine yazıldı.")


if __name__ == "__main__":
    run_history_generation('data/edirne_weather_2025_2026.csv', 'data/orders_history.csv')