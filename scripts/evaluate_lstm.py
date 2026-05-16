# scripts/evaluate_lstm.py

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from tensorflow.keras.models import load_model
import os
import sys

# Proje kök dizinini ekle
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ROOT_DIR)

from src.forecasting.preprocess import prepare_lstm_data


def evaluate():
    print("📈 Model performansı görselleştiriliyor...")

    # 1. Dosya Yollarını Tanımla
    csv_path = os.path.join(ROOT_DIR, 'data', 'orders_history.csv')
    model_path = os.path.join(ROOT_DIR, 'models', 'saved', 'demand_lstm_model.h5')

    if not os.path.exists(model_path):
        print(f"❌ Hata: Model dosyası bulunamadı: {model_path}")
        return

    # 2. Veriyi Hazırla
    # Not: preprocess.py içinde 'H' yerine 'h' yazarsan uyarı gider.
    X, y, scaler = prepare_lstm_data(csv_path, n_steps=24)

    # Test setini ayır
    train_size = int(len(X) * 0.8)
    X_test, y_test = X[train_size:], y[train_size:]

    # 3. Modeli Yükle
    model = load_model(model_path)

    # 4. Tahmin Yap
    predictions = model.predict(X_test)

    # 5. Görselleştirme
    plt.figure(figsize=(15, 7))

    # Son 120 saati (5 gün) görelim ki grafik çok sıkışmasın
    view_limit = 120
    plt.plot(y_test[:view_limit], label='Gerçek Edirne Sipariş Hacmi', color='#1f77b4', linewidth=2)
    plt.plot(predictions[:view_limit], label='LSTM Tahmin Edilen', color='#ff7f0e', linestyle='--', linewidth=2)

    plt.title('Edirne Lojistik Talep Analizi (LSTM)', fontsize=14)
    plt.xlabel('Zaman (Son 120 Saat)', fontsize=12)
    plt.ylabel('Normalize Sipariş Yoğunluğu', fontsize=12)
    plt.legend()
    plt.grid(True, alpha=0.3)

    # Sonuçları kaydet
    results_dir = os.path.join(ROOT_DIR, 'results')
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    plt.savefig(os.path.join(results_dir, 'lstm_performance.png'))
    print(f"✅ Başarılı! Grafik '{results_dir}/lstm_performance.png' altına kaydedildi.")
    plt.show()


if __name__ == "__main__":
    evaluate()