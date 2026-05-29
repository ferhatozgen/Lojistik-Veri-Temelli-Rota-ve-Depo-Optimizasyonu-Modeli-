import os
import sys
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import joblib
from tensorflow.keras.models import load_model
from src.forecasting.inference import get_next_day_predictions


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)

def optimize_temporary_hubs_flexible(target_date_str, user_hub_capacity):
    """
    Kullanıcının panelden seçtiği dinamik HUB kapasitesine göre
    K-Means çalıştırır. Üst sınır kısıtlaması (max 5 hub) kaldırılmıştır!
    """
    print(f"🧩 {target_date_str} Günü İçin Dinamik Hub Optimizasyonu Başladı...")
    print(f"🎛️ Yönetici Paneli Girişi -> Kullanıcı Tarafından Seçilen Hub Kapasitesi: {user_hub_capacity} Paket")

    # 1. LSTM Tahmin Adımı
    predicted_demands = get_next_day_predictions(target_date_str)
    total_predicted_packages = sum(predicted_demands.values())

    # Dinamik K Değeri Hesaplama (Üst sınır kaldırıldı)
    calculated_k = int(np.ceil(total_predicted_packages / user_hub_capacity))
    calculated_k = max(1, calculated_k)  # En az 1 hub açılmalı

    print(
        f" LSTM Öngörülen Toplam Kargo: {total_predicted_packages} Paket -> Gerekli Optimum Hub Sayısı (K): {calculated_k}")

    # 2. Gerçek Sipariş Koordinatlarını Yükleme
    orders_path = os.path.join(ROOT_DIR, "data", "simulated_orders.csv")
    df_orders = pd.read_csv(orders_path)
    df_orders['timestamp'] = pd.to_datetime(df_orders['timestamp'])

    target_date = pd.to_datetime(target_date_str).date()
    day_orders = df_orders[df_orders['timestamp'].dt.date == target_date].copy()

    if len(day_orders) == 0:
        print(f"⚠️ {target_date_str} için kesinleşmiş sipariş bulunamadı.")
        return None, 0

    coords = day_orders[['lat', 'lon']].values

    # 3. K-Means Kümeleme (K değeri artık tamamen sipariş yoğunluğuna ve kapasiteye bağlı)
    kmeans = KMeans(n_clusters=calculated_k, init='k-means++', n_init=10, random_state=42)
    day_orders['assigned_hub'] = kmeans.fit_predict(coords)  #her bir sipariş kordinatını inceler ve ona en yakın hub merkezini bulur ve o kargoya ornegin 1 numaralı huba baglı şekilde etiket basar
    hub_centers = kmeans.cluster_centers_ #oluşan kümelerin centroidlerini verir(sabah kurulacak o gecici alanları yani)

    # 4. Matematiksel Doğrulama (Silhouette) => +1 0 -1 değerlerini alır +1 en iyi, 0 civarı kümeler birbirinin sınırında ,-1 yanlış atanmış noktalr
    score = 0.0
    if calculated_k > 1:  #tek kümede silhoutte skoru doğal olarak hesaplanamaz
        score = silhouette_score(coords, day_orders['assigned_hub'])
        print(f"📐 Sistem Kümeleme Kalitesi (Silhouette): {score:.4f}")

    # Sonuçları Kaydetme
    hubs_df = pd.DataFrame(hub_centers, columns=['lat', 'lon'])
    hubs_df.index.name = 'hub_id'
    hubs_df.to_csv(os.path.join(ROOT_DIR, "data", "active_hubs.csv"), index=True)
    day_orders.to_csv(os.path.join(ROOT_DIR, "data", "orders_with_hubs.csv"), index=False)

    print(f"✅ Dağıtım Haritası Güncellendi. {calculated_k} adet geçici hub Edirne sokaklarına konumlandırıldı."),

    result = {
        "target_date": target_date_str,
        "total_predicted_packages": total_predicted_packages,
        "hub_count": calculated_k,
        "silhouette_score": round(score, 4),
        "predicted_demands": predicted_demands,
        "hubs": hubs_df.reset_index().to_dict(orient="records")
    }

    return result


if __name__ == "__main__":
    # Test: Kapasiteyi panelden 120 paket gibi küçük seçersek ne olur simülasyonu
    optimize_temporary_hubs_flexible("2026-05-06", user_hub_capacity=120)