"""
utils/som_depots.py
SOM modeli tarafından belirlenen 3 optimum depo konumu.
Gerçek projenizde train_som.py çıktısı olan optimized_hubs.csv'den okunabilir.
"""

import math
import os
import pandas as pd


# ─────────────────────────────────────────────
# SOM DEPO KONUMLARI (train_som.py çıktısı)
# ─────────────────────────────────────────────
# Eğer optimized_hubs.csv varsa oradan oku, yoksa sabit değerleri kullan.

def _load_depots():
    csv_candidates = [
        "data/optimized_hubs.csv",
        "../data/optimized_hubs.csv",
        "optimized_hubs.csv",
    ]
    for path in csv_candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                depots = []
                depot_names = ["Merkez Depo", "Karaağaç Depo", "Doğu Depo"]
                for i, row in df.iterrows():
                    depots.append({
                        "id":    i + 1,
                        "name":  depot_names[i] if i < len(depot_names) else f"Depo {i+1}",
                        "lat":   float(row["lat"]),
                        "lon":   float(row["lon"]),
                    })
                return depots
            except Exception:
                pass

    # CSV yoksa Edirne için varsayılan SOM konumları
    return [
        {
            "id":   1,
            "name": "Merkez Depo",
            "lat":  41.6772,
            "lon":  26.5567,
            "note": "Selimiye yakını — ticari merkez",
        },
        {
            "id":   2,
            "name": "Karaağaç Depo",
            "lat":  41.6634,
            "lon":  26.5321,
            "note": "Meriç köprüsü yakını — batı güzergahı",
        },
        {
            "id":   3,
            "name": "Delta Depo",
            "lat":  41.6430,
            "lon":  26.6100,
            "note": "Delta yurtları yakını — doğu bölgesi",
        },
    ]


DEPOT_LOCATIONS = _load_depots()


# ─────────────────────────────────────────────
# EN YAKIN DEPO BULUCU
# ─────────────────────────────────────────────
def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """İki koordinat arası mesafeyi km cinsinden hesaplar."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def find_nearest_depot(order_lat: float, order_lon: float) -> dict:
    """Verilen sipariş koordinatına en yakın depoyu bulur."""
    nearest = None
    min_dist = float("inf")

    for depot in DEPOT_LOCATIONS:
        dist = haversine(order_lat, order_lon, depot["lat"], depot["lon"])
        if dist < min_dist:
            min_dist = dist
            nearest = depot.copy()
            nearest["distance_km"] = round(dist, 3)

    return nearest


def get_all_depot_distances(order_lat: float, order_lon: float) -> list[dict]:
    """Tüm depolara mesafe listesini sıralı döndürür."""
    result = []
    for depot in DEPOT_LOCATIONS:
        dist = haversine(order_lat, order_lon, depot["lat"], depot["lon"])
        result.append({**depot, "distance_km": round(dist, 3)})
    return sorted(result, key=lambda x: x["distance_km"])