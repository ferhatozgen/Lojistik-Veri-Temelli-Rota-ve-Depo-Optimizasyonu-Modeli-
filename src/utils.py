import os
import numpy as np

def haversine(lat1, lon1, lat2, lon2, unit="km"):
    """
    İki koordinat arası mesafeyi hesaplar.
    unit: "km" (varsayılan) veya "m" (metre)
    """
    R = 6371.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance_km = R * c
    return distance_km * 1000 if unit == "m" else distance_km

def get_path(*parts):
    """ROOT_DIR'e göre güvenli path birleştirir."""
    return os.path.join(*parts)

def ensure_dir(path):
    """Klasör yoksa oluşturur."""
    os.makedirs(path, exist_ok=True)

ROAD_CURVATURE_FACTOR = 1.3  # Edirne sokak simülasyonu katsayısı
def haversine_road_meters(lat1, lon1, lat2, lon2):
    """Yol eğrilik katsayısı uygulanmış mesafe (metre, OR-Tools için int döner)."""
    return int(haversine(lat1, lon1, lat2, lon2, unit="m") * ROAD_CURVATURE_FACTOR)