import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import folium
from folium.plugins import HeatMap
import os


class LogisticEDA:
    """Veriyi analiz eder ve görsel raporlar sunar."""

    def __init__(self, dataframe):
        self.df = dataframe

    def save_visuals(self):
        plt.figure(figsize=(15, 5))

        # Aylık Talep - Warning Fixlendi
        plt.subplot(1, 2, 1)
        sns.barplot(data=self.df, x='month', y='order_volume', estimator=sum,
                    hue='month', palette='magma', legend=False)  # hue eklendi
        plt.title("Aylık Toplam Talep (Gerçekçi Mevsimsellik)")

        # Saatlik Trafik - Daha yumuşak eğri
        plt.subplot(1, 2, 2)
        sns.lineplot(data=self.df, x='hour', y='traffic_index', color='red', errorbar='sd')
        plt.title("Günlük Trafik Akışı (Sürekli Eğri)")

        plt.savefig('results/eda_report.png')
        plt.close()

    def save_map(self):
        """Edirne Isı Haritasını (Heatmap) oluşturur."""
        m = folium.Map(location=[41.67, 26.55], zoom_start=13)
        heat_data = [[row['lat'], row['lon'], row['order_volume']] for _, row in self.df.sample(2000).iterrows()]
        HeatMap(heat_data).add_to(m)
        m.save('results/heatmap.html')