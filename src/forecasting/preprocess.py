import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler

def prepare_multi_output_lstm_data(demand_csv_path, n_steps=24, train_ratio=0.8):
    if not os.path.exists(demand_csv_path):
        raise FileNotFoundError(f"Hata: Gerekli zaman serisi verisi bulunamadı: {demand_csv_path}")

    df = pd.read_csv(demand_csv_path)
    df['datetime'] = pd.to_datetime(df['datetime'])

    df_pivot = df.pivot(index='datetime', columns='district', values='demand').fillna(0)
    df_meta = df.groupby('datetime').agg({
        'weather_label': 'max', 'is_weekend': 'max', 'is_special_day': 'max',
        'is_semester_break': 'max', 'is_summer_break': 'max', 'is_prep_week': 'max',
        'exam_engineering': 'max', 'exam_medicine': 'max', 'exam_dentistry': 'max'
    })

    final_df = df_pivot.join(df_meta).fillna(0)

    target_cols = sorted(list(df_pivot.columns))
    meta_cols = [
        'weather_label', 'is_weekend', 'is_special_day', 'is_semester_break',
        'is_summer_break', 'is_prep_week', 'exam_engineering', 'exam_medicine', 'exam_dentistry'
    ]
    feature_cols = target_cols + meta_cols

    # VERİ SIZINTISI (DATA LEAKAGE) ÇÖZÜMÜ
    split_index = int(len(final_df) * train_ratio)
    scaler = MinMaxScaler(feature_range=(0, 1))

    # Scaler SADECE eğitim (train) verisine fit ediliyor
    scaler.fit(final_df[feature_cols].iloc[:split_index])

    # Dönüşüm (Transform) tüm veriye uygulanıyor
    scaled_data = scaler.transform(final_df[feature_cols])

    X, y = [], []
    n_districts = len(target_cols)

    for i in range(len(scaled_data) - n_steps):
        X.append(scaled_data[i: (i + n_steps)])
        y.append(scaled_data[i + n_steps, :n_districts])

    # Sliding window kaydırmasından dolayı index'i geri çekiyoruz
    windowed_split_index = split_index - n_steps

    return np.array(X), np.array(y), feature_cols, target_cols, scaler, windowed_split_index