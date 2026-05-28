import os
import sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import joblib


def prepare_multi_output_lstm_data(demand_csv_path, n_steps=24):
    """
    demand_engine.py'dan çıkan kümülatif saatlik veriyi okur,
    mahalleleri yan yana sütunlara pivotlar (Multi-Output için) ve
    LSTM'in ihtiyaç duyduğu 3D (X) ve 2D (y) matris formatına getirir.
    """
    if not os.path.exists(demand_csv_path):
        raise FileNotFoundError(f" Hata: Gerekli zaman serisi verisi bulunamadı: {demand_csv_path}")

    # 1. Veriyi Yükleme
    df = pd.read_csv(demand_csv_path)
    df['datetime'] = pd.to_datetime(df['datetime'])

    # 2. Bölgeleri Sütunlara Çevirme (Pivot Tablo)
    # Satırlar: Zaman Damgası, Sütunlar: 5 Mahalle, Değerler: O saatteki Kargo Hacmi
    df_pivot = df.pivot(index='datetime', columns='district', values='demand').fillna(0)

    # Her saat diliminde bu değerler tüm mahalleler için aynı olduğundan 'max' toplulaştırması güvenlidir
    # bu işleme meta verileri tek bir satıra güvenle sıkıştırma(collapse) etmek deniyor.
    df_meta = df.groupby('datetime').agg({    #group by işlemi yaptığımızda otomatik index datetime oluyor
        'weather_label': 'max',
        'is_weekend': 'max',
        'is_special_day': 'max',
        'is_semester_break': 'max',
        'is_summer_break': 'max',
        'is_prep_week': 'max',
        'exam_engineering': 'max',
        'exam_medicine': 'max',
        'exam_dentistry': 'max'
    })

    # İki tabloyu zaman indeksine göre birleştiriyoruz
    final_df = df_pivot.join(df_meta).fillna(0)

    #  KRİTİK MÜHENDİSLİK: Sütun sıralamasını alfabetik olarak sabitliyoruz.
    # Bu sıralama yarın canlı simülasyonda modelin şaşırmasını engelleyecek.
    target_cols = sorted(list(df_pivot.columns))  # ['AYSEKADIN', 'BALKAN', 'KARAAGAC', 'SARACLAR', 'SUKRUPASA']
    meta_cols = [
        'weather_label', 'is_weekend', 'is_special_day',
        'is_semester_break', 'is_summer_break', 'is_prep_week',
        'exam_engineering', 'exam_medicine', 'exam_dentistry'
    ]
    feature_cols = target_cols + meta_cols  # bu sayede yapı üzerindeki columns düzeni kontrol altına alınmış oluyor

    # 4. Veriyi Ölçeklendirme (MinMax Scaling)
    # LSTM 0-1 arasındaki verilerle çok daha kararlı ve sızıntısız öğrenir
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(final_df[feature_cols])

    # Ölçekleyiciyi saklıyoruz (Aşama 3 ve 4'te tahmin edilen veriyi gerçeğe dönüştürmek için gerekecek)
    models_dir = ("../../models/saved")
    os.makedirs(models_dir, exist_ok=True)
    joblib.dump(scaler, "../../models/saved/lstm_scaler.pkl")

    # 5. Kayan Zaman Penceresi (Sliding Window) Oluşturma: DÜZ TABLOYU YSA NIN ANLAYACAĞI 3D Küpe çevirir
    X, y = [], []
    n_districts = len(target_cols)

    #Bu kısımı toplam veri  - pencere boyutu şeklinde yazıyoruz ki taşma gibi bir durum olmasın örneğin dizi1 5 elemanı var biz dizi[6]  gibi bir ifade olmasının önüne geçiyoruz
    for i in range(len(scaled_data) - n_steps):    #n_steps bakacağı örneklem sayısı model girişi için
        # Giriş (X): Geçmiş 24 saatin TÜM verileri (Geçmiş Talepler + Hava + Sınav Durumları)
        X.append(scaled_data[i: (i + n_steps)])
        # Çıkış (y): Bir sonraki saatin (25. saat) SADECE 5 mahalledeki kargo talep sayıları
        y.append(scaled_data[i + n_steps, :n_districts])

    return np.array(X), np.array(y), feature_cols, target_cols


if __name__ == "__main__":
    demand_file_path = ("../../data/hourly_demand.csv")
    try:
        X, y, feats, targets = prepare_multi_output_lstm_data(demand_file_path, n_steps=24)
        print(" Veri Ön İşleme (Preprocess) Başarıyla Tamamlandı!")
        print("-" * 50)
        print(f" Giriş Matrisi (X) Şekli [Örnek Sayısı, Zaman Adımı, Feature Sayısı]: {X.shape}")
        print(f" Çıkış Matrisi (y) Şekli [Örnek Sayısı, Tahmin Edilen Mahalle]: {y.shape}")
        print("-" * 50)
        print(f" Model Girişindeki Sütun Düzeni:\n {feats}")
        print(f" Model Çıkışındaki Mahalle Düzeni:\n {targets}")
    except Exception as e:
        print(e)