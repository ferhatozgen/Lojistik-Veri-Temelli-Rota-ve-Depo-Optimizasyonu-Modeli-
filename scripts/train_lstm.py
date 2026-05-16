import sys
import os
import joblib  # Scaler'ı kaydetmek için
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout

from src.forecasting.preprocess import prepare_lstm_data


def train_model():
    # 1. VERİYİ HAZIRLA (Aktif hale getirdiğimiz yer burası)
    print("📊 Veriler ön işlemeden geçiriliyor...")
    X, y, scaler = prepare_lstm_data('../data/orders_history.csv', n_steps=24)

    # Scaler'ı kaydetmemiz ŞART.
    # Çünkü yarın canlı veri geldiğinde aynı ölçekle (0-1 arası) küçültmemiz gerekecek.
    if not os.path.exists('../models/saved'):
        os.makedirs('../models/saved')
    joblib.dump(scaler, '../models/saved/lstm_scaler.pkl')

    # Train/Test Ayrımı
    train_size = int(len(X) * 0.8)
    X_train, y_train = X[:train_size], y[:train_size]
    X_test, y_test = X[train_size:], y[train_size:]

    # 2. LSTM MODEL MİMARİSİ
    model = Sequential([
        LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])),
        Dropout(0.2),  # Ezberlemeyi (overfitting) önlemek için %20'sini unut
        LSTM(units=50, return_sequences=False),
        Dropout(0.2),
        Dense(units=25),
        Dense(units=1)  # Çıkış: Bir sonraki saatin toplam sipariş hacmi
    ])

    model.compile(optimizer='adam', loss='mean_squared_error')

    # 3. EĞİTİM
    print("🧠 LSTM Modeli eğitiliyor... Lütfen bekleyin.")
    model.fit(X_train, y_train, epochs=20, batch_size=32, validation_data=(X_test, y_test))

    # 4. MODELİ KAYDET
    model.save('../models/saved/demand_lstm_model.h5')
    print("✅ Model ve Scaler 'models/saved/' klasörüne kaydedildi!")


if __name__ == "__main__":
    train_model()