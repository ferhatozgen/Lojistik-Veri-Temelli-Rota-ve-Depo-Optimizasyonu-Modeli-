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


def get_next_day_predictions_dynamic(target_date_str):
    """
    Belirlenen hedef tarihten önceki 24 saatin verilerine bakarak
    LSTM modeliyle o günün mahalle bazlı paket tahminlerini üretir.
    """
    model_path = os.path.join(ROOT_DIR, "models", "saved", "delivery_demand_lstm.h5")
    scaler_path = os.path.join(ROOT_DIR, "models", "saved", "lstm_scaler.pkl")
    demand_path = os.path.join(ROOT_DIR, "data", "hourly_demand.csv")

    model = load_model(model_path)
    scaler = joblib.load(scaler_path)
    df = pd.read_csv(demand_path)

    df['datetime'] = pd.to_datetime(df['datetime'])
    target_dt = pd.to_datetime(target_date_str)

    # Hedef tarihten önceki son 24 saatin verisini filtrele
    past_24h_df = df[df['datetime'] < target_dt].tail(120)  # 5 bölge x 24 saat = 120 satır

    df_pivot = past_24h_df.pivot(index='datetime', columns='district', values='demand').fillna(0)
    df_meta = past_24h_df.groupby('datetime').agg({
        'weather_label': 'max', 'is_weekend': 'max', 'is_special_day': 'max',
        'is_semester_break': 'max', 'is_summer_break': 'max', 'is_prep_week': 'max',
        'exam_engineering': 'max', 'exam_medicine': 'max', 'exam_dentistry': 'max'
    })
    final_df = df_pivot.join(df_meta).fillna(0)

    district_cols = sorted(list(df_pivot.columns))
    feature_cols = district_cols + list(df_meta.columns)

    last_24h_matrix = final_df[feature_cols].values
    last_24h_scaled = scaler.transform(last_24h_matrix)
    X_input = np.expand_dims(last_24h_scaled, axis=0)

    pred_scaled = model.predict(X_input)


    # GERİ ÖLÇEKLEME(INVERSE TRANSFORM) YAPILIYOR
    #ölçekleme işlemi 14 sütun için yapıldı o yüzden biz ona doğrudan bu district_col verirsek(5 sutun) hata verir ondan dummy matris olusturduk.
    dummy_matrix = np.zeros((1, len(feature_cols)))   #tamamen sıfırlardan olusan 1,14 luk bir matris olusturuyoruz
    dummy_matrix[0, :len(district_cols)] = pred_scaled[0] #bu 0 elemanlarının bulundupu matrise dağılım bolge sayısı kadar(5) kısmına modellerin bolgesel paket tahminlerini ekliyoruz
    pred_actual = scaler.inverse_transform(dummy_matrix)[0, :len(district_cols)]  # bu 5 bolge için yazılan paket tahmin değerleri ölçekli şekildeydi o yüzdem geri ölçekleme ile asıl değerlere ulaştık

    # Negatif değerleri temizle ve bölge ve paket sayılarını sözlük olarak dön
    return {district_cols[i]: max(0, round(pred_actual[i])) for i in range(len(district_cols))}


def optimize_temporary_hubs_flexible(target_date_str, user_hub_capacity):
    """
    Kullanıcının panelden seçtiği dinamik HUB kapasitesine göre
    K-Means çalıştırır. Üst sınır kısıtlaması (max 5 hub) kaldırılmıştır!
    """
    print(f"🧩 {target_date_str} Günü İçin Dinamik Hub Optimizasyonu Başladı...")
    print(f"🎛️ Yönetici Paneli Girişi -> Kullanıcı Tarafından Seçilen Hub Kapasitesi: {user_hub_capacity} Paket")

    # 1. LSTM Tahmin Adımı
    predicted_demands = get_next_day_predictions_dynamic(target_date_str)
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

    print(f"✅ Dağıtım Haritası Güncellendi. {calculated_k} adet geçici hub Edirne sokaklarına konumlandırıldı.")
    return calculated_k, score


if __name__ == "__main__":
    # Test: Kapasiteyi panelden 120 paket gibi küçük seçersek ne olur simülasyonu
    optimize_temporary_hubs_flexible("2026-05-06", user_hub_capacity=120)