"""
utils/order_generator.py
Edirne lojistik sistemi için gerçekçi sipariş verisi üretici.
orders_history.csv varsa oradan okur, yoksa sentetik üretir.
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta


# Edirne kritik bölgeleri (feature_engine.py'den)
EDIRNE_HOTSPOTS = {
    "Selimiye Merkezi":     (41.6772, 26.5567, 0.8),
    "Ayşekadın/Zübeyde":   (41.6676, 26.5776, 0.9),
    "Erasta AVM":           (41.6660, 26.5705, 0.7),
    "Delta Yurtları":       (41.6439, 26.6159, 1.2),
    "Balkan Yurtları":      (41.6382, 26.6122, 1.0),
    "Fatih Mahallesi":      (41.6592, 26.5997, 0.6),
    "Şükrüpaşa":            (41.6677, 26.5975, 0.5),
    "Sanayi Sitesi":        (41.6579, 26.5805, 0.4),
    "Karaağaç":             (41.6520, 26.5310, 0.3),
}

ORDER_COUNTER = [1000]


def _new_id():
    ORDER_COUNTER[0] += 1
    return f"ORD-{ORDER_COUNTER[0]}"


def _generate_order(order_type: str) -> dict:
    """Edirne koordinat aralığında tek bir sipariş üretir."""
    # Hotspot'lara ağırlıklı olarak yakın noktalar üret
    spot_name, (spot_lat, spot_lon, weight) = random.choices(
        list(EDIRNE_HOTSPOTS.items()),
        weights=[v[2] for v in EDIRNE_HOTSPOTS.values()],
        k=1,
    )[0]

    lat = spot_lat + random.uniform(-0.008, 0.008)
    lon = spot_lon + random.uniform(-0.008, 0.008)

    # Edirne sınırları içine kırp
    lat = max(41.630, min(41.710, lat))
    lon = max(26.500, min(26.635, lon))

    return {
        "id":      _new_id(),
        "lat":     round(lat, 6),
        "lon":     round(lon, 6),
        "type":    order_type,           # "today" | "tomorrow"
        "address": f"{spot_name} Bölgesi",
        "volume":  random.randint(3, 60),
        "hour":    random.randint(8, 22),
    }


def generate_live_orders(n_today: int = 25, n_tomorrow: int = 15) -> list[dict]:
    """
    Uygulama başlangıcında sipariş listesi oluşturur.
    Önce orders_history.csv'den okumayı dener; yoksa sentetik üretir.
    """
    # CSV'den okuma denemesi
    csv_candidates = [
        "data/orders_history.csv",
        "../data/orders_history.csv",
        "orders_history.csv",
    ]
    for path in csv_candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, nrows=500)
                df["delivery_timestamp"] = pd.to_datetime(df["delivery_timestamp"])

                # Bugün ve yarın için son satırları al
                today_rows = df.tail(n_today).copy()
                tomorrow_rows = df.sample(min(n_tomorrow, len(df))).copy()

                orders = []
                for i, row in today_rows.iterrows():
                    orders.append({
                        "id":      f"ORD-{i}",
                        "lat":     float(row["lat"]),
                        "lon":     float(row["lon"]),
                        "type":    "today",
                        "address": "CSV Siparişi",
                        "volume":  int(row.get("order_volume", random.randint(5, 40))),
                        "hour":    int(row.get("hour", 12)),
                    })
                for i, row in tomorrow_rows.iterrows():
                    orders.append({
                        "id":      f"ORD-T{i}",
                        "lat":     float(row["lat"]),
                        "lon":     float(row["lon"]),
                        "type":    "tomorrow",
                        "address": "CSV Siparişi (Yarın)",
                        "volume":  int(row.get("order_volume", random.randint(5, 40))),
                        "hour":    int(row.get("hour", 14)),
                    })
                return orders
            except Exception:
                pass

    # CSV yoksa → sentetik üret
    orders = []
    for _ in range(n_today):
        orders.append(_generate_order("today"))
    for _ in range(n_tomorrow):
        orders.append(_generate_order("tomorrow"))

    return orders


def get_orders_for_hour(orders: list[dict], hour: int) -> list[dict]:
    """Belirli saate ait siparişleri filtreler."""
    return [o for o in orders if abs(o.get("hour", 12) - hour) <= 2]


def apply_midnight_transform(orders: list[dict]) -> list[dict]:
    """
    Gece yarısı dönüşümü:
    Saat 00:00 olduğunda tüm 'tomorrow' siparişleri 'today'e dönüşür.
    """
    return [
        {**o, "type": "today"} if o["type"] == "tomorrow" else o
        for o in orders
    ]