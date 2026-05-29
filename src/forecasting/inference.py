import os
import numpy as np
import pandas as pd
import joblib
from tensorflow.keras.models import load_model

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

def get_next_day_predictions(target_date_str):
    """
    Hedef tarihten önceki 24 saatin verisine bakarak
    LSTM modeliyle mahalle bazlı paket tahminlerini üretir.
    """
    model_path = os.path.join(ROOT_DIR, "models", "saved", "delivery_demand_lstm.h5")
    scaler_path = os.path.join(ROOT_DIR, "models", "saved", "lstm_scaler.pkl")
    demand_path = os.path.join(ROOT_DIR, "data", "hourly_demand.csv")

    model = load_model(model_path)
    scaler = joblib.load(scaler_path)

    df = pd.read_csv(demand_path)
    df['datetime'] = pd.to_datetime(df['datetime'])
    target_dt = pd.to_datetime(target_date_str)

    past_24h_df = df[df['datetime'] < target_dt].tail(120)  # 5 bölge x 24 saat

    df_pivot = past_24h_df.pivot(index='datetime', columns='district', values='demand').fillna(0)
    df_meta = past_24h_df.groupby('datetime').agg({
        'weather_label': 'max', 'is_weekend': 'max', 'is_special_day': 'max',
        'is_semester_break': 'max', 'is_summer_break': 'max', 'is_prep_week': 'max',
        'exam_engineering': 'max', 'exam_medicine': 'max', 'exam_dentistry': 'max'
    })

    final_df = df_pivot.join(df_meta).fillna(0)
    district_cols = sorted(list(df_pivot.columns))
    feature_cols = district_cols + list(df_meta.columns)

    last_24h_scaled = scaler.transform(final_df[feature_cols].values)
    X_input = np.expand_dims(last_24h_scaled, axis=0)
    pred_scaled = model.predict(X_input)

    dummy_matrix = np.zeros((1, len(feature_cols)))
    dummy_matrix[0, :len(district_cols)] = pred_scaled[0]
    pred_actual = scaler.inverse_transform(dummy_matrix)[0, :len(district_cols)]

    return {district_cols[i]: max(0, round(pred_actual[i])) for i in range(len(district_cols))}