import pandas as pd
import numpy as np
from minisom import MiniSom
import matplotlib.pyplot as plt
import os
import sys

# Proje dizin ayarı
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ROOT_DIR)


def train_som_hubs(n_hubs=3):
    print(f"🧩 SOM Algoritması {n_hubs} adet ana depo (hub) için çalıştırılıyor...")

    # 1. Veriyi Yükle
    df = pd.read_csv(os.path.join(ROOT_DIR, 'data', 'orders_history.csv'))

    # Sadece lokasyon verilerini alıyoruz
    # İstersen burada 'order_volume' ağırlıklı seçim de yapabiliriz
    data = df[['lat', 'lon']].values

    # 2. SOM Modelini Kur
    # 1x3'lük bir harita oluşturuyoruz (3 adet Hub için)
    som = MiniSom(1, n_hubs, input_len=2, sigma=0.5, learning_rate=0.5)

    # Başlangıç ağırlıklarını veriye göre belirle
    som.random_weights_init(data)

    # 3. Eğitim
    print("🔄 Sinir ağı Edirne haritasına göre şekilleniyor...")
    som.train_random(data, 1000)  # 1000 iterasyon yeterli olacaktır

    # 4. Hub Koordinatlarını Al
    hubs = som.get_weights()
    hubs_reshaped = hubs.reshape(n_hubs, 2)

    # 5. Sonuçları Kaydet ve Yazdır
    hubs_df = pd.DataFrame(hubs_reshaped, columns=['lat', 'lon'])
    hubs_df.to_csv(os.path.join(ROOT_DIR, 'data', 'optimized_hubs.csv'), index=False)

    print("\n✅ SOM Tarafından Belirlenen Optimum Hub Konumları:")
    for i, row in hubs_df.iterrows():
        print(f"📍 Hub {i + 1}: {row['lat']:.5f}, {row['lon']:.5f}")

    return hubs_df


if __name__ == "__main__":
    train_som_hubs(n_hubs=3)  # Edirne için 3 depo başlangıçta idealdir