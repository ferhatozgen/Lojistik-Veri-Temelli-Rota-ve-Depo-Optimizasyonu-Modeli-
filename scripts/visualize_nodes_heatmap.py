import osmnx as ox
import folium
from folium.plugins import HeatMap
from src.data_logic.locations import EDIRNE_LOCATIONS
import pandas as pd
import os


def generate_heatmap(nodes_csv_path, output_map_path):
    print("🗺️ Yoğunluk haritası (heatmap) oluşturuluyor...")

    if not os.path.exists(nodes_csv_path):
        print(f"❌ Hata: {nodes_csv_path} bulunamadı. Önce extract_edirne_nodes.py scriptini çalıştırmalısın.")
        return

    nodes_df = pd.read_csv(nodes_csv_path)

    # 2. Haritanın merkezini Edirne Merkez (Selimiye) olarak belirle
    edirne_coords = [41.6772, 26.5567]
    # 3. Temel Folium haritasını oluştur (CartoDB positron - sade bir arka plan)
    m = folium.Map(location=edirne_coords, zoom_start=13, tiles='CartoDB positron')

    # 4. Yoğunluk haritası verisini hazırla (lat, lon listesi)
    heat_data = [[row['lat'], row['lon']] for index, row in nodes_df.iterrows()]

    # 5. HeatMap katmanını ekle
    # radius: her noktanın etki alanı, blur: yumuşatma miktarı
    HeatMap(heat_data, radius=10, blur=15, min_opacity=0.2).add_to(m)

    # 6. Kritik lojistik noktalarımızı (referans için) haritaya işaretle
    for name, coords in EDIRNE_LOCATIONS.items():
        folium.Marker(
            location=coords,
            popup=name,
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(m)

    # 7. Haritayı kaydet
    m.save(output_map_path)
    print(f"✅ Başarılı! İnteraktif yoğunluk haritası '{output_map_path}' konumuna kaydedildi.")
    print("👉 Bu dosyayı tarayıcında (Chrome, Firefox vb.) açarak inceleyebilirsin.")


if __name__ == "__main__":
    # Önce feature_engine'den lokasyonları çekebilmek için doğru dizinde olduğumuzdan emin olmalıyız.
    # Bu scripti proje ana dizininden çalıştırmalısın: python scripts/visualize_nodes_heatmap.py
    generate_heatmap('../data/edirne_nodes.csv', '../results/edirne_delivery_heatmap.html')