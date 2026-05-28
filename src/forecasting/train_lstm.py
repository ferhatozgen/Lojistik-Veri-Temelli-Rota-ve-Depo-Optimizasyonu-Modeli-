import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, Input
from tensorflow.keras.callbacks import EarlyStopping

# Proje kök dizinini dinamik olarak sisteme tanıtıyoruz
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.append(ROOT_DIR)

from src.forecasting.preprocess import prepare_multi_output_lstm_data


def train_forecasting_brain():
    print(" Çok Değişkenli Kargo Hacmi Tahmin Motoru Eğitiliyor...")
    print("-" * 50)

    demand_path = os.path.join(ROOT_DIR, "data", "hourly_demand.csv")
    X, y, feature_cols, district_cols = prepare_multi_output_lstm_data(demand_path, n_steps=24)

    # Veriyi Kronolojik Olarak Bölme (%80 Eğitme - %20 Test)
    # Zaman serilerinde random split yapılmaz, geçmişle eğitip gelecekle test etmeliyiz!
    train_size = int(len(X) * 0.8)
    X_train, y_train = X[:train_size], y[:train_size]
    X_test, y_test = X[train_size:], y[train_size:]

    # MIMO (Multi-Input Multi-Output) Derin Öğrenme Mimarisi
    # Giriş: (24 saat, 14 özellik) -> Çıkış: (5 mahalle talebi)
    model = Sequential([
        Input(shape=(X_train.shape[1], X_train.shape[2])),

        # İlk LSTM Katmanı (Dizilim döndürmeli ki sonraki LSTM katmanı beslensin)
        LSTM(units=64, return_sequences=True),      #1 tane daha katman olduğu için bir sonraki katmana bu çıktıyı iletmesi için return_sequences=True şeklinde ayırdık
        Dropout(0.2),  # %20 nöron sönümleme ile ezber engelleme

        # İkinci LSTM Katmanı (Dizilim döndürmeyi kapatıyoruz, artık yoğun katmana geçiş)
        LSTM(units=32, return_sequences=False),   #Bundan sonra baska lstm katmanı gelmeyeceği için rs kısmı False olarak ayarlandı
        Dropout(0.2),

        # Yoğun Katmanlar (Doğrusal olmayan ilişkileri çözmek için)
        Dense(units=16, activation='relu'),

        # Çıkış Katmanı: 5 mahallenin sonraki saatteki paket hacmini aynı anda üretir
        Dense(units=len(district_cols))
    ])

    # loss='mean_squared_error' (MSE) lojistik hacim tahminlerinde cezalandırma gücü yüksek olduğu için idealdir
    model.compile(optimizer='adam', loss='mean_squared_error', metrics=['mae'])

    #  SAHTE LOSS ENGELEYİCİ: Erken Durdurma (Early Stopping)
    # Eğer test kaybı (val_loss) 4 epoch boyunca iyileşmezse eğitimi keser ve en iyi ağırlıklara döner
    early_stop = EarlyStopping(
        monitor='val_loss',
        patience=4,
        restore_best_weights=True,
        verbose=1
    )

    # 3. Model Eğitimi
    print(f"🏋 Model Edirne'nin dönemsel kargo ritmini öğreniyor...")
    history = model.fit(
        X_train, y_train,
        epochs=40,
        batch_size=64,
        validation_data=(X_test, y_test),
        callbacks=[early_stop],
        verbose=1
    )

    # 4. Model ve Performans Grafiklerini Kaydetme
    models_dir = os.path.join(ROOT_DIR, "models", "saved")
    results_dir = os.path.join(ROOT_DIR, "results")
    os.makedirs(models_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    # Modeli H5 formatında kalıcı olarak sakla
    model_save_path = os.path.join(models_dir, "delivery_demand_lstm.h5")
    model.save(model_save_path)
    print(f"\n Başarılı! Model '{model_save_path}' konumuna kaydedildi.")

    # Eğitim Kayıp (Loss) Grafiğini Çizdirme ve Kaydetme
    # Bu grafik sunum slaytlarında "Model Başarısı" olarak göstereceğimiz ilk görsel olacak
    plt.figure(figsize=(10, 5))
    plt.plot(history.history['loss'], label='Eğitim Kaybı (Train Loss)', color='#1f77b4', linewidth=2)
    plt.plot(history.history['val_loss'], label='Doğrulama Kaybı (Val Loss)', color='#ff7f0e', linestyle='--',
             linewidth=2)
    plt.title('Edirne Lojistik LSTM Tahmin Motoru - Eğitim Süreci', fontsize=12)
    plt.xlabel('Epoch Sayısı', fontsize=10)
    plt.ylabel('Kayıf Değeri (MSE)', fontsize=10)
    plt.legend()
    plt.grid(True, alpha=0.3)

    graph_save_path = os.path.join(results_dir, "lstm_training_loss.png")
    plt.savefig(graph_save_path)
    plt.close()
    print(f" Eğitim performans grafiği '{graph_save_path}' olarak kaydedildi.")


if __name__ == "__main__":
    train_forecasting_brain()