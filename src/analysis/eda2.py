import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
import os
import sys
from datetime import datetime

# PyCharm'da klasör yollarının (path) karışmamasını sağlar
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Senin oluşturduğun dosyadan Veri Üretici sınıfını import ediyoruz
# Not: Dosya adındaki typo'ya (datagenereator2) göre yazdım :)
from src.data_logic.datagenereator2 import RealCityDataGenerator

class ModernLogisticEDA:
    def __init__(self, dataframe):
        self.df = dataframe
        # Proje ana dizininde results klasörü yoksa oluştur
        if not os.path.exists('results'):
            os.makedirs('results')

    def save_visuals(self):
        plt.figure(figsize=(16, 6))

        # 1. Grafik: Saatlik Sipariş Yoğunluğu (Normal vs Özel Gün)
        plt.subplot(1, 2, 1)

        # Gün başına düşen ortalama siparişi bulmak için önce günlere bölüyoruz
        # (Çünkü normal gün sayısı çok fazla, özel gün az. Toplam alırsak normal gün hep yüksek çıkar)
        daily_hourly = self.df.groupby([self.df['delivery_timestamp'].dt.date, 'hour', 'is_special_event']).size().reset_index(name='order_count')
        avg_hourly = daily_hourly.groupby(['hour', 'is_special_event'])['order_count'].mean().reset_index()

        sns.lineplot(
            data=avg_hourly,
            x='hour',
            y='order_count',
            hue='is_special_event', # Çizgileri 0 ve 1'e göre ayırır
            palette=['#3498db', '#e74c3c'], # Normal gün mavi, özel gün kırmızı
            marker="o",
            linewidth=2.5
        )

        plt.title("Saatlik Ortalama Kargo Teslimatı (Normal vs. Kampanya)")
        plt.xlabel("Günün Saati")
        plt.ylabel("Ortalama Teslim Edilen Paket")
        # Lejantı (Etiketleri) düzenle
        plt.legend(title='Gün Tipi', labels=['Normal Gün', 'Kampanya Günü (Efsane Cuma vb.)'])
        plt.grid(True, alpha=0.3)

        # 2. Grafik: Hava Durumuna Göre Siparişler
        plt.subplot(1, 2, 2)
        weather_counts = self.df['weather'].value_counts().reset_index()
        sns.barplot(data=weather_counts, x='weather', y='count', palette='viridis', hue='weather', legend=False)
        plt.title("Hava Durumuna Göre Sipariş Hacmi")
        plt.xlabel("Hava Durumu")
        plt.ylabel("Sipariş Sayısı")

        plt.tight_layout()
        plt.savefig('results/yeni_eda_raporu5.png', dpi=300)
        plt.close()

    def save_map(self, city_center=(41.67, 26.55)):
        m = folium.Map(location=city_center, zoom_start=14, tiles='CartoDB dark_matter')
        sample_df = self.df.sample(min(5000, len(self.df)))
        heat_data = [[row['lat'], row['lon']] for index, row in sample_df.iterrows()]
        HeatMap(heat_data, radius=12, blur=15).add_to(m)
        m.save('results/edirne_gercek_sokak_haritası5.html')


# --- İŞTE BURASI KODUN ATEŞLENDİĞİ YER ---
if __name__ == "__main__":
    print("1. Dijital İkiz Sistemi Başlatılıyor...")

    # İNDENTASYONLAR DÜZELTİLDİ (Hepsi if bloğunun içinde)
    generator = RealCityDataGenerator(city_name="Edirne, Turkey", grid_size=10)

    # Sistemin başlangıç tarihi (1 yıl öncesi)
    baslangic_tarihi = datetime(2025, 1, 1)

    print("\n2. Geçmiş 1 Yıllık (365 Gün) Temel Veri Üretiliyor... (Bu biraz sürebilir)")

    csv_dosya_yolu = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/dijital_ikiz_veri.csv'))
    os.makedirs(os.path.dirname(csv_dosya_yolu), exist_ok=True)

    # 365 gün * 24 saat = 8760 saatlik ana veri üretimi
    df_raw = generator.update_database(
        start_datetime=baslangic_tarihi,
        hours_to_simulate=365*24,
        csv_path=csv_dosya_yolu
    )

    if not df_raw.empty:
        print("\n3. Görseller ve Harita çiziliyor...")
        eda = ModernLogisticEDA(df_raw)
        eda.save_visuals()
        eda.save_map()

        print("\n4. LSTM İçin Veri Hazırlanıyor (Sipariş Sayıları Hesaplanıyor)...")
        # Senin istediğin "Sipariş Sayısı" mantığı!
        # Veriyi Saat, Gün, Hava Durumu ve Grid bazında grupluyoruz

        # Sadece saat bilgisini almak için timestamp'i yuvarlıyoruz
        df_raw['saat_bazli_zaman'] = df_raw['delivery_timestamp'].dt.floor('h')

        # Grid ve saate göre siparişleri SAY (size)
        df_lstm = df_raw.groupby(
            ['saat_bazli_zaman', 'grid_id', 'hour', 'day_of_week', 'weather', 'is_special_event']
        ).size().reset_index(name='toplam_siparis_sayisi')

        # Trafik indeksinin de saatlik ortalamasını alıyoruz
        traffic_avg = df_raw.groupby(['saat_bazli_zaman', 'grid_id'])['traffic_index'].mean().reset_index()

        # İkisini birleştiriyoruz
        df_lstm = pd.merge(df_lstm, traffic_avg, on=['saat_bazli_zaman', 'grid_id'])

        # LSTM için hazırlanan yeni CSV'yi kaydet
        lstm_csv_yolu = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/lstm_icin_hazir_veri.csv'))
        df_lstm.to_csv(lstm_csv_yolu, index=False)

        print(f"5. Harika! LSTM modelinin yiyeceği formatta veriniz hazırlandı: {lstm_csv_yolu}")
        print(df_lstm.head()) # Konsola örnek birkaç satır basar