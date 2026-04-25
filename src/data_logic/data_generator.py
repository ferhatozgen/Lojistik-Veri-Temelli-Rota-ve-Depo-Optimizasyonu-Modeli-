import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class DataGenerator:
    def __init__(self, city_center=(41.67, 26.55)):
        self.base_lat, self.base_lon = city_center
        self.hotspots = [(41.68, 26.56, 0.012), (41.66, 26.54, 0.008), (41.69, 26.58, 0.015)]

    def generate_batch(self, n_samples=20000):
        data = []
        start_date = datetime(2025, 1, 1)

        for _ in range(n_samples):
            dt = start_date + timedelta(seconds=np.random.randint(0, 365 * 24 * 3600))

            # --- 1. GERÇEKÇİ TRAFİK (Çift Tepe - Gaussian Mixture) ---
            # Sabah piki (8.5) ve Akşam piki (18.0) için çan eğrisi
            morning_peak = 0.5 * np.exp(-((dt.hour + dt.minute / 60 - 8.5) ** 2) / (2 * 1.5 ** 2))
            evening_peak = 0.6 * np.exp(-((dt.hour + dt.minute / 60 - 18.5) ** 2) / (2 * 2.0 ** 2))

            traffic_base = 0.15 + morning_peak + evening_peak
            # Rastgele günlük dalgalanma (Noise) ekleyelim
            traffic_idx = np.clip(traffic_base + np.random.normal(0, 0.05), 0, 1)

            # --- 2. GERÇEKÇİ MEVSİMSELLİK (Sinüs + Trend) ---
            # Yılın sonuna doğru artan bir dalga
            seasonality = 0.3 * np.sin(2 * np.pi * (dt.month - 1) / 12) + (dt.month / 12 * 0.5)
            # Kasım-Aralık için ekstra yoğunluk (Black Friday vb.)
            promo_effect = 1.8 if dt.month in [11, 12] else 1.0

            volume_base = np.random.poisson(12 * (1 + seasonality) * promo_effect)

            # Koordinat üretimi
            idx = np.random.choice(len(self.hotspots))
            lat = np.random.normal(self.hotspots[idx][0], self.hotspots[idx][2])
            lon = np.random.normal(self.hotspots[idx][1], self.hotspots[idx][2])

            data.append({
                'timestamp': dt, 'lat': lat, 'lon': lon,
                'order_volume': max(1, int(volume_base)),
                'traffic_index': traffic_idx,
                'priority': np.random.choice(['Low', 'Medium', 'High'], p=[0.5, 0.3, 0.2]),
                'month': dt.month, 'hour': dt.hour
            })

        return pd.DataFrame(data)