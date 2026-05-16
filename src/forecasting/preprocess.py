import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler


def prepare_lstm_data(csv_path, n_steps=24):
    """
    n_steps: LSTM'in geçmiş kaç saate bakacağını belirler (Örn: Son 24 saat).
    """
    # 1. Veriyi Yükle
    df = pd.read_csv(csv_path)
    df['delivery_timestamp'] = pd.to_datetime(df['delivery_timestamp'])

    # VERİ TOPLULAŞTIRMA: RESAMPLING=> SAATLİK TOPLULAŞTIRMA (Resampling)
    """ Model eğitilirken bu saatte ne kadar sipariş geliri öğreniyoruz bu kodda veriyi o formata getiriyor."""
    # Veriyi saate göre gruplayıp önemli özellikleri topluyoruz
    df_hourly = df.set_index('delivery_timestamp').resample('H').agg({     # resample için timestamp sutununun(Zaman sutununun) index olması gerekiyormuş,   agg ise veriyi tekrardan H:saatlik dilimlere böler
        'order_volume': 'sum',                                             #alttakilerde o saate yuvarlanan saatlerin ilgili sutun verisine ne yapması gerektiğini söylüuot
        'traffic_index': 'mean',
        'weather_label': 'max',  # O saatteki en kötü hava durumu
        'is_special_event': 'max'
    }).fillna(0)  # Sipariş olmayan saatleri 0 ile doldur

    # 3. NORMALİZASYON (Scaling)
    # LSTM 0-1 arasındaki verilerle çok daha hızlı ve tutarlı öğrenir
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(df_hourly)

    # 4. PENCERELEME (Sliding Window)
    # X: Geçmiş veriler, y: Tahmin edilecek gelecek veri
    X, y = [], []
    for i in range(len(scaled_data) - n_steps):
        X.append(scaled_data[i: (i + n_steps)])  # Geçmiş n_steps kadar saat
        y.append(scaled_data[i + n_steps, 0])  # Bir sonraki saatin order_volume'u

    return np.array(X), np.array(y), scaler