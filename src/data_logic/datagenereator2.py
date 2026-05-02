import osmnx as ox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

class RealCityDataGenerator:
    def __init__(self, city_name="Edirne, Turkey", grid_size=10):
        print(f"{city_name} için yol ağı indiriliyor/yükleniyor... Lütfen bekleyin.")
        self.graph = ox.graph_from_place(city_name, network_type='drive')
        self.nodes, _ = ox.graph_to_gdfs(self.graph)
        self.node_lats = self.nodes['y'].values
        self.node_lons = self.nodes['x'].values
        print(f"Toplam {len(self.node_lats)} adet kargo teslimat noktası (düğüm) hazır.")

        # --- GRID (IZGARA) SİSTEMİ ALTYAPISI ---
        self.grid_size = grid_size # 10x10'luk karelere böleceğiz
        self.min_lat, self.max_lat = min(self.node_lats), max(self.node_lats)
        self.min_lon, self.max_lon = min(self.node_lons), max(self.node_lons)

        self.lat_step = (self.max_lat - self.min_lat) / self.grid_size
        self.lon_step = (self.max_lon - self.min_lon) / self.grid_size
        print(f"Harita {self.grid_size}x{self.grid_size} (Toplam {self.grid_size**2} Bölge) grid'e bölündü.")

    def get_grid_id(self, lat, lon):
        """Koordinatı alır, hangi kare (Grid) içine düştüğünü bulur."""
        row = int((lat - self.min_lat) / self.lat_step)
        col = int((lon - self.min_lon) / self.lon_step)

        # Sınırlara tam basan noktalar için taşmayı engelle
        row = min(row, self.grid_size - 1)
        col = min(col, self.grid_size - 1)

        return f"Grid_{row}_{col}"

    def get_event_multiplier(self, dt):
        """Kampanya rampası ve yığılma (backlog) mantığı."""
        campaigns = {
            (2, 14): (2.5, 7),   # 14 Şubat: 7 gün önceden başlar
            (5, 12): (2.0, 5),
            (11, 24): (3.0, 10), # Efsane Cuma
            (12, 31): (2.2, 7)
        }
        max_mult = 1.0
        for (c_month, c_day), (peak_mult, impact_days) in campaigns.items():
            campaign_date = datetime(dt.year, c_month, c_day)
            delta_days = (campaign_date - dt).days

            if 0 <= delta_days <= impact_days:
                mult = 1.0 + (peak_mult - 1.0) * (1 - (delta_days / impact_days))
                max_mult = max(max_mult, mult)
            elif -3 <= delta_days < 0:
                mult = 1.0 + (peak_mult - 1.0) * (1 - abs(delta_days) / 4)
                max_mult = max(max_mult, mult)
        return max_mult

    def get_weather_traffic_penalty(self, dt):
        """Kötü hava teslimatı etkilemez, trafiği artırır."""
        weather_condition = "Clear"
        traffic_penalty = 0.0
        if dt.month in [11, 12, 1, 2, 3, 4] and random.random() < 0.25:
            weather_condition = "Rain/Snow"
            traffic_penalty = 0.35
        return weather_condition, traffic_penalty

    def generate_live_stream(self, start_datetime, hours_to_simulate=1):
        """
        DİJİTAL İKİZ KISMI: Belirtilen saatten itibaren 'hours_to_simulate' kadar
        canlı veri üretir. İster 1 saatlik veri üretip mevcut CSV'ye eklersin,
        ister 1 yıllık (8760 saat) üretip sıfırdan kurarsın.
        """
        data = []

        for hour_offset in range(hours_to_simulate):
            current_time = start_datetime + timedelta(hours=hour_offset)

            # Pazar günleri veya mesai dışı (08-19 arası değilse) kargo dağıtımı yok
            if current_time.weekday() == 6 or not (8 <= current_time.hour <= 18):
                continue

            event_mult = self.get_event_multiplier(current_time)
            weather, weather_traffic_penalty = self.get_weather_traffic_penalty(current_time)

            hour = current_time.hour
            if hour in [8, 9, 10]:
                base_demand, base_traffic = 45, 0.7
            elif hour in [12, 13]:
                base_demand, base_traffic = 20, 0.5
            elif hour in [17, 18]:
                base_demand, base_traffic = 30, 0.8
            else:
                base_demand, base_traffic = 35, 0.4

            packages_this_hour = np.random.poisson(base_demand * event_mult)

            for _ in range(packages_this_hour):
                minute = random.randint(0, 59)
                delivery_time = current_time.replace(minute=minute)

                traffic_idx = np.clip(base_traffic + weather_traffic_penalty + np.random.normal(0, 0.05), 0, 1)

                # Koordinat Seçimi
                idx = random.randint(0, len(self.node_lats) - 1)
                lat = self.node_lats[idx]
                lon = self.node_lons[idx]

                # YENİ: Koordinatı Grid'e (Kareye) Dönüştür
                grid_id = self.get_grid_id(lat, lon)

                data.append({
                    'delivery_timestamp': delivery_time,
                    'lat': lat,
                    'lon': lon,
                    'grid_id': grid_id,          # <--- LSTM İÇİN ALTIN DEĞER
                    'weather': weather,
                    'traffic_index': round(traffic_idx, 3),
                    'hour': delivery_time.hour,
                    'day_of_week': delivery_time.weekday(),
                    'is_special_event': 1 if event_mult > 1.0 else 0
                })

        return pd.DataFrame(data).sort_values(by='delivery_timestamp')

    def update_database(self, start_datetime, hours_to_simulate, csv_path):
        """
        DİJİTAL İKİZ TETİKLEYİCİSİ: Yeni veriyi üretir ve mevcut CSV dosyasının
        altına ekler (Append). Gerçek bir canlı sistem gibi çalışır.
        """
        new_data_df = self.generate_live_stream(start_datetime, hours_to_simulate)

        if new_data_df.empty:
            print(f"[{start_datetime}] Mesai dışı. Yeni veri üretilmedi.")
            return new_data_df

        # Dosya yoksa başlıklarla (header) oluştur, varsa altına ekle
        if not os.path.exists(csv_path):
            new_data_df.to_csv(csv_path, index=False)
            print(f"[{start_datetime}] Yeni veritabanı oluşturuldu: {len(new_data_df)} satır.")
        else:
            new_data_df.to_csv(csv_path, mode='a', header=False, index=False)
            print(f"[{start_datetime}] Canlı akış eklendi: +{len(new_data_df)} kargo.")

        return new_data_df