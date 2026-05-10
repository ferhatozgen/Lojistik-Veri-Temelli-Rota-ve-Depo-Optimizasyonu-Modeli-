import osmnx as ox
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# OSMnx 2.0+ Ayarları
ox.settings.timeout = 300
ox.settings.max_query_area_size = 25000000000
ox.settings.log_console = False

class AdvancedCityDataGenerator:
    def __init__(self, grid_size=10):
        # Edirne Merkez BBOX (Kuzey, Güney, Doğu, Batı)
        self.west, self.south = 26.500, 41.625
        self.east, self.north = 26.625, 41.715

        print("🗺️ Edirne Merkez Yol Ağı indiriliyor...")
        bbox_tuple = (self.west, self.south, self.east, self.north)
        self.graph = ox.graph_from_bbox(bbox=bbox_tuple, network_type='drive')

        self.nodes, _ = ox.graph_to_gdfs(self.graph)
        self.node_list = list(self.nodes.index)

        self.grid_size = grid_size
        self.lat_step = (self.north - self.south) / self.grid_size
        self.lon_step = (self.east - self.west) / self.grid_size
        print(f"✅ {len(self.node_list)} sokak noktası ve {grid_size}x{grid_size} Grid aktif.")

    def get_grid_id(self, lat, lon):
        row = int((lat - self.south) / self.lat_step)
        col = int((lon - self.west) / self.lon_step)
        return f"Grid_{np.clip(row, 0, self.grid_size-1)}_{np.clip(col, 0, self.grid_size-1)}"

    def calculate_smart_demand(self, dt, grid_id):
        """Edirne şehir dinamiklerine göre sipariş yoğunluğunu belirler."""
        hour = dt.hour
        month = dt.month
        is_special = 1 if (dt.month == 11 and dt.day >= 20) or (dt.month == 12 and dt.day >= 25) else 0

        # 1. BÖLGESEL YOĞUNLUK ANALİZİ (Koordinat tahminlerine göre)
        # Erasta/Margi/Kipa (Merkez: 4,4 ile 6,6 arası)
        is_center = "Grid_5" in grid_id or "Grid_4" in grid_id or "Grid_6" in grid_id
        # Karaağaç (Güneybatı: Düşük Row, Düşük Col)
        is_karaagac = "Grid_0" in grid_id or "Grid_1" in grid_id
        # Balkan Yerleşkesi (Güneydoğu: Düşük Row, Yüksek Col)
        is_campus = ("Grid_1" in grid_id or "Grid_2" in grid_id) and ("Grid_7" in grid_id or "Grid_8" in grid_id)

        # 2. ZAMANSAL ÇARPANLAR
        # Gece (00-07): Çok az | Sabah (08-11): Yoğun | Öğle (12-14): Stabil | Akşam (17-20): En Yoğun
        if 8 <= hour <= 11: time_mult = 1.8
        elif 12 <= hour <= 15: time_mult = 1.0
        elif 17 <= hour <= 21: time_mult = 2.2
        elif 22 <= hour <= 23: time_mult = 0.5
        else: time_mult = 0.05 # Gece

        # 3. MEVSİMSEL VE ÖĞRENCİ DÖNGÜSÜ
        seasonal_mult = 1.0
        if is_karaagac and month in [4, 5, 6, 7, 8, 9]: seasonal_mult = 2.0 # Yazın Karaağaç patlaması
        if is_campus and month in [7, 8]: seasonal_mult = 0.3 # Yazın öğrenciler yok

        # 4. TRAFİK İNDEKSİ (Öğle ve Akşam artar)
        traffic_base = 0.35
        if 12 <= hour <= 14: traffic_base = 0.65 # Öğle trafiği
        if 17 <= hour <= 19: traffic_base = 0.85 # İş çıkışı
        if hour < 7: traffic_base = 0.15 # Gece sakinliği

        traffic_idx = np.clip(traffic_base + np.random.normal(0, 0.05), 0, 1)

        # 5. NİHAİ YOĞUNLUK HESABI (Lambda)
        # Normal gün: 5-10 arası | Özel gün: 30-40 arası
        base_lambda = 35 if is_special else (8 if is_center else 4)
        final_lambda = base_lambda * time_mult * seasonal_mult

        return int(np.random.poisson(final_lambda)), round(traffic_idx, 3), is_special

    def generate_day_data(self, target_date):
        day_data = []
        # Tüm gün (24 saat) boyunca döner
        for hour in range(0, 24):
            dt = target_date.replace(hour=hour)

            # Her grid için ayrı ayrı hesapla (Dengeli dağılım için)
            for r in range(self.grid_size):
                for c in range(self.grid_size):
                    grid_id = f"Grid_{r}_{c}"

                    # Bu saate bu gridde kaç paket olacak?
                    count, traffic, is_special = self.calculate_smart_demand(dt, grid_id)

                    if count > 0:
                        # Bu grid'in içinden rastgele sokak noktaları seç
                        for _ in range(count):
                            node_id = random.choice(self.node_list)
                            lat = self.nodes.loc[node_id, 'y']
                            lon = self.nodes.loc[node_id, 'x']

                            # Eğer seçilen nokta bu grid'e ait değilse tekrar grid'ini bul
                            # (Alternatif: Sadece o grid'in içindeki nodelardan seçmek ama bu yavaşlatır)

                            day_data.append({
                                'delivery_timestamp': dt.replace(minute=random.randint(0,59)),
                                'lat': lat, 'lon': lon,
                                'grid_id': self.get_grid_id(lat, lon),
                                'weather': "Clear", # Hava durumu sonra eklenecek
                                'traffic_index': traffic,
                                'order_volume': np.random.choice([1, 5, 10, 20], p=[0.6, 0.25, 0.1, 0.05]),
                                'hour': hour,
                                'day_of_week': dt.weekday(),
                                'is_special_event': is_special
                            })
        return pd.DataFrame(day_data)

    def update_database(self, start_datetime, days, csv_path):
        all_data = []
        for d in range(days):
            current_day = start_datetime + timedelta(days=d)
            df_day = self.generate_day_data(current_day)
            all_data.append(df_day)
            if (d+1) % 30 == 0: print(f"📅 {d+1} gün simüle edildi...")

        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(csv_path, index=False)
        return final_df

# --- EKSİK SAATLERİ DOLDURMA (FILL MISSING) ---
def fill_missing_hours(df_lstm, grid_size=10):
    print("🧹 Boş saatler 0 ile mühürleniyor...")
    full_range = pd.date_range(start=df_lstm['saat_bazli_zaman'].min(), end=df_lstm['saat_bazli_zaman'].max(), freq='h')
    all_grids = [f"Grid_{r}_{c}" for r in range(grid_size) for c in range(grid_size)]

    index = pd.MultiIndex.from_product([full_range, all_grids], names=['saat_bazli_zaman', 'grid_id'])
    skeleton = pd.DataFrame(index=index).reset_index()

    df_final = pd.merge(skeleton, df_lstm, on=['saat_bazli_zaman', 'grid_id'], how='left')
    df_final['toplam_siparis_sayisi'] = df_final['toplam_siparis_sayisi'].fillna(0)
    df_final['hour'] = df_final['saat_bazli_zaman'].dt.hour
    df_final['day_of_week'] = df_final['saat_bazli_zaman'].dt.dayofweek
    df_final = df_final.sort_values(['grid_id', 'saat_bazli_zaman'])
    df_final['traffic_index'] = df_final['traffic_index'].ffill().fillna(0.15)
    df_final['is_special_event'] = df_final['is_special_event'].fillna(0)
    return df_final