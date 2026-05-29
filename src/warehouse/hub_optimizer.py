import os
import sys
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import joblib
from tensorflow.keras.models import load_model

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)

from src.forecasting.inference import get_next_day_predictions
from simulation.extract_edirne_nodes import bridge_data_gap_system

def optimize_temporary_hubs_flexible(target_date_str, user_hub_capacity):
    """
    Kullanıcının panelden seçtiği dinamik HUB kapasitesine göre
    K-Means çalıştırır. Koordinat düzeltmesi (Projection) uygulanmıştır.
    """
    print(f"🧩 {target_date_str} Günü İçin Dinamik Hub Optimizasyonu Başladı...")
    print(f"🎛️ Yönetici Paneli Girişi -> Kullanıcı Tarafından Seçilen Hub Kapasitesi: {user_hub_capacity} Paket")

    # 0. Kayan zaman penceresi: hedef tarih veri setinde yoksa eksik günleri üret.
    # Bu adım hem hourly_demand.csv hem simulated_orders.csv dosyasını hedef güne kadar tamamlar.
    bridge_data_gap_system(target_date_str)

    # 1. LSTM Tahmin Adımı
    predicted_demands = get_next_day_predictions(target_date_str)
    total_predicted_packages = sum(predicted_demands.values())

    # Dinamik K Değeri Hesaplama
    calculated_k = int(np.ceil(total_predicted_packages / user_hub_capacity))
    calculated_k = max(1, calculated_k)  # En az 1 hub açılmalı

    print(f" LSTM Öngörülen Toplam Kargo: {total_predicted_packages} Paket -> Gerekli Optimum Hub Sayısı (K): {calculated_k}")

    # 2. Gerçek Sipariş Koordinatlarını Yükleme
    orders_path = os.path.join(ROOT_DIR, "data", "simulated_orders.csv")
    df_orders = pd.read_csv(orders_path)
    df_orders['timestamp'] = pd.to_datetime(df_orders['timestamp'])

    target_date = pd.to_datetime(target_date_str).date()
    day_orders = df_orders[df_orders['timestamp'].dt.date == target_date].copy()

    if len(day_orders) == 0:
        print(f"⚠️ {target_date_str} için kesinleşmiş sipariş bulunamadı.")
        return {
            "target_date": target_date_str,
            "total_predicted_packages": int(total_predicted_packages),
            "hub_count": 0,
            "silhouette_score": 0.0,
            "predicted_demands": predicted_demands,
            "hubs": [],
            "message": "Bu tarih için üretilebilir sipariş bulunamadı."
        }

    calculated_k = min(calculated_k, len(day_orders))
    coords = day_orders[['lat', 'lon']].values

    # --- COĞRAFİ PROJEKSİYON (EQUIRECTANGULAR DÜZELTME) ---
    # K-Means'in Öklid kısıtlamasını aşmak için Lat/Lon değerlerini metre (X, Y) cinsine çeviriyoruz
    R = 6371000.0  # Dünya yarıçapı (metre)
    mean_lat_rad = np.radians(np.mean(coords[:, 0]))

    # Lat/Lon -> X/Y (Metre) Dönüşümü
    x_coords = np.radians(coords[:, 1]) * R * np.cos(mean_lat_rad)
    y_coords = np.radians(coords[:, 0]) * R
    cartesian_coords = np.column_stack((x_coords, y_coords))

    # 3. K-Means Kümeleme (Artık metre cinsinden gerçek fiziksel mesafelerle çalışıyor)
    kmeans = KMeans(n_clusters=calculated_k, init='k-means++', n_init=10, random_state=42)
    day_orders['assigned_hub'] = kmeans.fit_predict(cartesian_coords)
    cartesian_centers = kmeans.cluster_centers_

    # Centroid'leri (X, Y Ağırlık Merkezleri) tekrar Lat/Lon formatına geri çevirme (Ters Projeksiyon)
    center_lons = np.degrees(cartesian_centers[:, 0] / (R * np.cos(mean_lat_rad)))
    center_lats = np.degrees(cartesian_centers[:, 1] / R)
    hub_centers = np.column_stack((center_lats, center_lons))

    # 4. Matematiksel Doğrulama (Silhouette)
    score = 0.0
    if 1 < calculated_k < len(day_orders):
        # Silhouette skoru da metre bazlı gerçek koordinatlar üzerinden hesaplanıyor
        score = silhouette_score(cartesian_coords, day_orders['assigned_hub'])
        print(f"📐 Sistem Kümeleme Kalitesi (Silhouette): {score:.4f}")

    # Sonuçları Kaydetme
    hubs_df = pd.DataFrame(hub_centers, columns=['lat', 'lon'])
    hubs_df.index.name = 'hub_id'
    hubs_df.to_csv(os.path.join(ROOT_DIR, "data", "active_hubs.csv"), index=True)
    day_orders.to_csv(os.path.join(ROOT_DIR, "data", "orders_with_hubs.csv"), index=False)

    print(f"✅ Dağıtım Haritası Güncellendi. {calculated_k} adet geçici hub Edirne sokaklarına konumlandırıldı.")

    result = {
        "target_date": target_date_str,
        "total_predicted_packages": int(total_predicted_packages),
        "hub_count": calculated_k,
        "silhouette_score": round(score, 4),
        "predicted_demands": predicted_demands,
        "hubs": hubs_df.reset_index().to_dict(orient="records")
    }

    return result

if __name__ == "__main__":
    optimize_temporary_hubs_flexible("2026-05-06", user_hub_capacity=120)