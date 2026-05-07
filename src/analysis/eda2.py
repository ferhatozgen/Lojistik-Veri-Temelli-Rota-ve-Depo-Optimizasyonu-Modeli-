import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
import os
import sys
import numpy as np
from datetime import datetime

# PyCharm'da klasör yollarının karışmaması için
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.data_logic.datagenereator2 import AdvancedCityDataGenerator

# --- YENİ FONKSİYON: EKSİK SAATLERİ DOLDURMA ---
def fill_missing_hours(df_lstm, grid_size=10):
    """
    Sipariş gelmeyen saatleri ve bölgeleri bularak '0' değeriyle doldurur.
    LSTM'in zaman sürekliliği için bu adım KRİTİKTİR.
    """
    print("\n🧹 Eksik saatler ve bölgeler '0' ile dolduruluyor...")

    # 1. Tüm Zaman Aralığını Belirle (Saatlik frekans)
    full_range = pd.date_range(
        start=df_lstm['saat_bazli_zaman'].min(),
        end=df_lstm['saat_bazli_zaman'].max(),
        freq='h'
    )

    # 2. Tüm Olası Grid ID'lerini Oluştur (Örn: Grid_0_0 ... Grid_9_9)
    all_grids = [f"Grid_{r}_{c}" for r in range(grid_size) for c in range(grid_size)]

    # 3. "Master" İskeleti Oluştur (Her saat x Her grid kombinasyonu)
    index = pd.MultiIndex.from_product(
        [full_range, all_grids],
        names=['saat_bazli_zaman', 'grid_id']
    )
    skeleton_df = pd.DataFrame(index=index).reset_index()

    # 4. Mevcut veriyi iskeletle birleştir (Sipariş olmayan yerler NaN olacak)
    df_final = pd.merge(skeleton_df, df_lstm, on=['saat_bazli_zaman', 'grid_id'], how='left')

    # 5. Boş Verileri Akıllıca Doldur
    # Sipariş sayısına doğrudan 0 basıyoruz
    df_final['toplam_siparis_sayisi'] = df_final['toplam_siparis_sayisi'].fillna(0)

    # Hour ve Day_of_week bilgilerini timestamp'ten tazeleyelim (Boşluk kalmasın)
    df_final['hour'] = df_final['saat_bazli_zaman'].dt.hour
    df_final['day_of_week'] = df_final['saat_bazli_zaman'].dt.dayofweek

    # Hava durumu ve Trafik: Önceki saatin bilgisini kopyala (Forward Fill)
    # Eğer ilk satır boşsa 'Clear' ve '0.35' varsayılanını ata
    df_final = df_final.sort_values(['grid_id', 'saat_bazli_zaman'])
    df_final['weather'] = df_final['weather'].ffill().fillna('Clear')
    df_final['traffic_index'] = df_final['traffic_index'].ffill().fillna(0.35)
    df_final['is_special_event'] = df_final['is_special_event'].fillna(0)

    return df_final

class ModernLogisticEDA:
    def __init__(self, dataframe):
        self.df = dataframe
        if not os.path.exists('results'):
            os.makedirs('results')

    def save_visuals(self):
        plt.figure(figsize=(16, 6))
        plt.subplot(1, 2, 1)

        # Grafik için hazırlık
        daily_hourly = self.df.groupby([self.df['delivery_timestamp'].dt.date, 'hour', 'is_special_event']).size().reset_index(name='order_count')
        avg_hourly = daily_hourly.groupby(['hour', 'is_special_event'])['order_count'].mean().reset_index()

        sns.lineplot(
            data=avg_hourly, x='hour', y='order_count',
            hue='is_special_event', palette=['#3498db', '#e74c3c'], marker="o"
        )
        plt.title("Saatlik Ortalama Teslimat (Normal vs. Kampanya)")
        plt.legend(title='Gün Tipi', labels=['Normal Gün', 'Kampanya'])

        plt.subplot(1, 2, 2)
        weather_counts = self.df['weather'].value_counts().reset_index()
        sns.barplot(data=weather_counts, x='weather', y='count', palette='viridis', hue='weather', legend=False)
        plt.title("Hava Durumu Dağılımı")

        plt.tight_layout()
        plt.savefig('results/yeni_eda_raporu5.png', dpi=300)
        plt.close()

    def save_map(self, city_center=(41.675, 26.560)):
        m = folium.Map(location=city_center, zoom_start=14, tiles='CartoDB Positron')
        sample_df = self.df.sample(min(5000, len(self.df)))
        heat_data = [[row['lat'], row['lon']] for index, row in sample_df.iterrows()]
        HeatMap(heat_data, radius=12, blur=15).add_to(m)
        m.save('results/edirne_gercek_sokak_haritası5.html')

# --- ANA ÇALIŞTIRMA BLOĞU ---
if __name__ == "__main__":
    print("1. Dijital İkiz Sistemi Başlatılıyor...")
    generator = AdvancedCityDataGenerator(grid_size=10)
    baslangic_tarihi = datetime(2025, 1, 1)

    print("\n2. Temel Veri Üretiliyor...")
    csv_dosya_yolu = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/dijital_ikiz_veri.csv'))

    df_raw = generator.update_database(
        start_datetime=baslangic_tarihi,
        days=365,
        csv_path=csv_dosya_yolu
    )

    if not df_raw.empty:
        print("\n3. Görseller ve Harita çiziliyor...")
        eda = ModernLogisticEDA(df_raw)
        eda.save_visuals()
        eda.save_map()

        print("\n4. LSTM İçin Veri Hazırlanıyor (Aggregation)...")
        df_raw['saat_bazli_zaman'] = df_raw['delivery_timestamp'].dt.floor('h')

        # Ham gruplama
        df_lstm = df_raw.groupby(
            ['saat_bazli_zaman', 'grid_id', 'hour', 'day_of_week', 'weather', 'is_special_event']
        ).size().reset_index(name='toplam_siparis_sayisi')

        traffic_avg = df_raw.groupby(['saat_bazli_zaman', 'grid_id'])['traffic_index'].mean().reset_index()
        df_lstm = pd.merge(df_lstm, traffic_avg, on=['saat_bazli_zaman', 'grid_id'])

        # --- BURASI YENİ: Boşlukları Doldurma ---
        df_lstm_full = fill_missing_hours(df_lstm, grid_size=10)

        # Kaydetme
        lstm_csv_yolu = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/lstm_icin_hazir_veri.csv'))
        df_lstm_full.to_csv(lstm_csv_yolu, index=False)

        print(f"\n✅ 5. İşlem Tamam! LSTM Hazır Verisi: {lstm_csv_yolu}")
        print(f"📊 Toplam Satır (Boşluklar Dahil): {len(df_lstm_full)}")
        print(df_lstm_full.head())