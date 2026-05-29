import osmnx as ox
import pandas as pd
import os
import math
from src.utils import haversine


# Kritik lokasyonları buraya da ekliyoruz ki filtreleme yapabilelim
LOCATIONS = {
    "CENTER_SELIMIYE": (41.6772, 26.5567),    # Şehir Merkezi
    "MERIC_BRIDGE": (41.663358, 26.552126), # Köprü Darboğazı
    "TUNCA_BRIDGE": (41.667802, 26.5541),   # Köprü darbogazı
    "ERASTA_AVM": (41.666047, 26.570551),          # Ticari Merkez
    "AYSEKADIN_ZUBEYDE": (41.667622, 26.577653),   # Zübeyde Hanım Cad. (Yoğun Trafik)
    "SANAYI_SITESI": (41.657980, 26.580567),       # Lojistik Girişi
    "DELTA_DORMS_1": (41.643916, 26.615970),          # Öğrenci Yurtları (Yüksek Sipariş)
    "SUKRUPASA": (41.667665, 26.597498),
    "FATIH_MAH": (41.659218, 26.599756),
    "DELTA_DORMS_2": (41.638264, 26.612267)
}

def extract_urban_nodes():
    print("🗺️ Edirne lojistik ağı indiriliyor ve filtreleniyor...")

    center_point = (41.6772, 26.5567)
    dist = 5500  # Geniş alanı çek, sonra içinden ayıklayacağız

    graph = ox.graph_from_point(center_point, dist=dist, network_type='drive')
    nodes, _ = ox.graph_to_gdfs(graph)

    # Düğümleri listeye çeviriyoruz
    all_nodes = nodes[['y', 'x']].copy()
    all_nodes.columns = ['lat', 'lon']

    filtered_nodes = []

    # --- FİLTRELEME MANTIĞI ---
    for _, node in all_nodes.iterrows():
        keep_node = False

        for loc_name, loc_coords in LOCATIONS.items():
            # Filtre mesafesi:
            # Balkan ve Delta geniş bir alan olduğu için oralarda 1.5 km'lik alanı koru
            # Şehir merkezinde ise 1.0 km yeterli.
            limit = 1.5 if "BALKAN" in loc_name or "DELTA" in loc_name else 1.2
            # Eğer bir sokak düğümü, bizim 7 noktamızdan herhangi birine 1.2 km'den yakınsa tut
            # Bu sayede 'ıssız' ve 'ilgisiz' yerlerdeki düğümler silinecek
            if haversine(node['lat'], node['lon'], loc_coords[0], loc_coords[1]) < limit:
                keep_node = True
                break

        if keep_node:
            filtered_nodes.append(node)

    nodes_df = pd.DataFrame(filtered_nodes)

    # Klasör kontrolü
    if not os.path.exists('../data'):
        os.makedirs('../data')

    nodes_df.to_csv('../data/edirne_nodes.csv', index=False)

    print(f"✅ Temizlik Tamamlandı! Gereksiz noktalar elendi.")
    print(f"📍 Toplam {len(nodes_df)} adet stratejik nokta 'edirne_nodes.csv' dosyasına yazıldı.")


if __name__ == "__main__":
    extract_urban_nodes()