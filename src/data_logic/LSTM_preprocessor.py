import pandas as pd
import numpy as np
import joblib  # Scaler'ı kaydetmek için
from sklearn.preprocessing import MinMaxScaler, LabelEncoder
import os

class AdvancedLSTMPreprocessor:
    def __init__(self, window_size=24, target_col='toplam_siparis_sayisi'):
        self.window_size = window_size
        self.target_col = target_col
        self.scaler = MinMaxScaler()
        self.label_encoder = LabelEncoder()

    def _encode_cyclical(self, df):
        """Saat ve gün verilerini dairesel (sin/cos) formatına sokar."""
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24.0)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24.0)
        df['day_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7.0)
        df['day_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7.0)
        return df

    def transform_data(self, csv_path):
        print(f"📂 Veri okunuyor: {csv_path}")
        df = pd.read_csv(csv_path)

        # 1. Kategorik Verileri Sayısallaştırma
        df['grid_numeric'] = self.label_encoder.fit_transform(df['grid_id'])
        df['weather_numeric'] = df['weather'].map({'Clear': 0, 'Rain/Snow': 1}).fillna(0)

        # 2. Döngüsel Zaman Özellikleri
        df = self._encode_cyclical(df)

        # 3. Normalizasyon (Min-Max Scaling)
        # Sadece modelin girdi olarak kullanacağı özellikleri ölçekliyoruz
        feature_cols = [
            'toplam_siparis_sayisi', 'traffic_index', 'is_special_event',
            'weather_numeric', 'hour_sin', 'hour_cos', 'day_sin', 'day_cos', 'grid_numeric'
        ]

        print("⚖️ Veriler normalize ediliyor...")
        df[feature_cols] = self.scaler.fit_transform(df[feature_cols])

        return df, feature_cols

    def create_sequences(self, df, feature_cols):
        """
        Veriyi LSTM için 3B Tensor formatına sokar.
        KRİTİK: Her grid için ayrı pencereleme yapılır (Gridler arası veri karışmaz).
        """
        print("🌀 Zaman pencereleri (Sliding Windows) oluşturuluyor...")
        X, y = [], []

        # Her bir grid için veriyi ayırıp pencereleme yapıyoruz
        for grid in df['grid_numeric'].unique():
            grid_df = df[df['grid_numeric'] == grid][feature_cols].values

            for i in range(len(grid_df) - self.window_size):
                X.append(grid_df[i : i + self.window_size]) # Geçmiş 24 saat
                y.append(grid_df[i + self.window_size, 0])  # Hedef: toplam_siparis_sayisi (index 0)

        X = np.array(X)
        y = np.array(y)

        print(f"✅ İşlem Tamamlandı! Girdi Şekli (X): {X.shape}, Hedef Şekli (y): {y.shape}")
        return X, y

    def save_assets(self, path='../../models/saved/'):
        """Eğitimden sonra scaler ve label_encoder'ı saklarız."""
        if not os.path.exists(path):
            os.makedirs(path)
        joblib.dump(self.scaler, os.path.join(path, 'scaler.gz'))
        joblib.dump(self.label_encoder, os.path.join(path, 'label_encoder.gz'))
        print(f"💾 Scaler ve Label Encoder '{path}' klasörüne kaydedildi.")

# --- KULLANIM ÖRNEĞİ ---
if __name__ == "__main__":
    # Dosya yolları
    raw_data_path = '../../data/lstm_icin_hazir_veri.csv'

    preprocessor = AdvancedLSTMPreprocessor(window_size=24) # Son 24 saate bak

    # 1. Adım: Veriyi temizle ve ölçekle
    processed_df, features = preprocessor.transform_data(raw_data_path)

    # 2. Adım: LSTM formatına (3B) sok
    X, y = preprocessor.create_sequences(processed_df, features)

    # 3. Adım: Kaydet (Model eğitimi için hazır!)
    np.save('../../data/X_train.npy', X)
    np.save('../../data/y_train.npy', y)
    print("\n🚀 Veriler 'npy' formatında kaydedildi. Model eğitimine geçebiliriz!")