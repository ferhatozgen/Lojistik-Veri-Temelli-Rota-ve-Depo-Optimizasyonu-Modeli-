import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine


def generate_refined_logistic_data(n_samples=20000):
    print("--- Veri Simülasyonu Başlatıldı ---")
    base_lat, base_lon = 41.67, 26.55
    priorities = ['Low', 'Medium', 'High']
    weather_types = ['Sunny', 'Rainy', 'Snowy']

    data_list = []
    start_date = datetime(2025, 1, 1)

    for i in range(n_samples):
        current_time = start_date + timedelta(seconds=np.random.randint(0, 365 * 24 * 3600))
        hour = current_time.hour
        month = current_time.month
        day_of_week = current_time.weekday()

        # Hava Durumu
        if month in [12, 1, 2]:
            weather = np.random.choice(weather_types, p=[0.4, 0.4, 0.2])
        else:
            weather = np.random.choice(weather_types, p=[0.8, 0.15, 0.05])

        # Talep (Gaussian Noise)
        multiplier = np.random.normal(2.5, 0.35) if month == 11 else np.random.normal(1.0, 0.15)
        order_vol = max(1, int(np.random.poisson(12) * multiplier))

        # Trafik
        traffic_base = 0.2
        if hour in [7, 8, 9, 17, 18, 19]: traffic_base += 0.5
        if weather != 'Sunny': traffic_base += 0.2
        traffic_idx = np.clip(np.random.normal(traffic_base, 0.07), 0, 1)

        data_list.append({
            'timestamp': current_time,
            'lat': np.random.normal(base_lat, 0.025),
            'lon': np.random.normal(base_lon, 0.025),
            'order_volume': order_vol,
            'traffic_index': traffic_idx,
            'priority': np.random.choice(priorities),
            'weather': weather,
            'hour': hour,
            'day_of_week': day_of_week,
            'month': month
        })

    return pd.DataFrame(data_list)


def save_to_db(df, db_uri):
    """Veriyi PostgreSQL'e basar."""
    try:
        engine = create_engine(db_uri)
        df.to_sql('orders', engine, if_exists='replace', index=False)
        print(" Veriler PostgreSQL 'orders' tablosuna başarıyla yazıldı.")
    except Exception as e:
        print(f" DB Hatası: {e}")


if __name__ == "__main__":
    df = generate_refined_logistic_data()

    # 1. DOSYA KAYDETME (src içinden bir üst klasöre, data içine)
    # os.path.dirname(__file__) mevcut dosyanın (src) yolunu verir.
    # '..' bir üst dizine çıkar.
    current_dir = os.path.dirname(__file__)
    data_dir = os.path.join(current_dir, '..', 'data')

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    file_path = os.path.join(data_dir, 'raw_logistic_data.csv')
    df.to_csv(file_path, index=False)
    print(f" CSV kaydedildi: {file_path}")

    # 2. DB'YE KAYDETME (Bilgilerini buraya göre güncelle)
    # Format: postgresql://kullanici:sifre@localhost:5432/db_adi
    DB_URI = "postgresql://postgres:furkan77@localhost:5432/lojistic_db"
    save_to_db(df, DB_URI)