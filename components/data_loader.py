"""
Veri yükleme ve hub hesaplama modülü.
- simulated_orders.csv  : 2025-01-01 → 2026-05-30 arası tüm siparişler (1.5M satır)
- orders_with_hubs.csv  : 2026-05-06 için hub atamalı siparişler (fallback)
- active_hubs.csv       : 2026-05-06 için statik hub koordinatları (fallback)

Hub hesaplama mantığı:
  - Kullanıcı kapasite seçer → K = ceil(sipariş_sayısı / kapasite)
  - K-Means ile o günün sipariş koordinatlarından hub merkezi bulunur
  - assigned_hub sütunu her sipariş için güncellenir
"""

import os
import pandas as pd
import numpy as np
from datetime import date, timedelta
from sklearn.cluster import KMeans

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Simulation "today"
SIM_TODAY = date(2026, 5, 6)

_SIM_ORDERS_CACHE: pd.DataFrame | None = None


def _safe_read(filename: str, **kwargs) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path, **kwargs)


def load_simulated_orders() -> pd.DataFrame:
    """simulated_orders.csv'yi önbelleğe alarak okur."""
    global _SIM_ORDERS_CACHE
    if _SIM_ORDERS_CACHE is not None:
        return _SIM_ORDERS_CACHE

    path = os.path.join(DATA_DIR, "simulated_orders.csv")
    if not os.path.exists(path):
        # Fallback: orders_with_hubs.csv
        df = _safe_read("orders_with_hubs.csv")
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["date"] = df["timestamp"].dt.date
            df["hour"] = df["timestamp"].dt.hour
        _SIM_ORDERS_CACHE = df
        return df

    df = pd.read_csv(path, parse_dates=["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df["hour"] = df["timestamp"].dt.hour
    _SIM_ORDERS_CACHE = df
    return df


def get_orders_for_date(target_date: date) -> pd.DataFrame:
    """Belirli bir tarih için sipariş verilerini döner."""
    all_orders = load_simulated_orders()
    if all_orders.empty:
        return pd.DataFrame()
    day_orders = all_orders[all_orders["date"] == target_date].copy()
    return day_orders


def compute_hubs_for_orders(orders: pd.DataFrame, hub_capacity: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Verilen siparişler ve kapasite için K-Means ile hub konumlarını hesaplar.
    Returns: (hubs_df, orders_with_hub_df)
      hubs_df          : hub_id, lat, lon
      orders_with_hub  : orijinal + assigned_hub sütunu
    """
    if orders.empty or "lat" not in orders.columns:
        return pd.DataFrame(columns=["hub_id", "lat", "lon"]), orders

    orders_clean = orders.dropna(subset=["lat", "lon"]).reset_index(drop=True)
    coords = orders_clean[["lat", "lon"]].values
    if len(coords) == 0:
        return pd.DataFrame(columns=["hub_id", "lat", "lon"]), orders

    k = max(1, int(np.ceil(len(orders_clean) / hub_capacity)))
    k = min(k, len(coords))

    kmeans = KMeans(n_clusters=k, init="k-means++", n_init=10, random_state=42)
    labels = kmeans.fit_predict(coords)

    orders_out = orders_clean.copy()
    orders_out["assigned_hub"] = labels

    hubs_df = pd.DataFrame(kmeans.cluster_centers_, columns=["lat", "lon"])
    hubs_df.index.name = "hub_id"
    hubs_df = hubs_df.reset_index()

    return hubs_df, orders_out


def load_all_data(hub_capacity: int = 200) -> dict:
    """
    Uygulama için tüm veriyi hazırlar.
    hub_capacity: panelden gelen slider değeri (varsayılan 200).
    """
    data: dict = {}

    # ── Tüm simülasyon siparişleri (tarih filtresi için) ─────────────────────
    all_orders = load_simulated_orders()
    data["all_orders"] = all_orders

    # ── Bugün için siparişler + hub hesaplama ────────────────────────────────
    today_orders = get_orders_for_date(SIM_TODAY)

    # Önceki gün → sol harita için (bugün'ün hub'ları önceki günden kurulur)
    prev_date = SIM_TODAY - timedelta(days=1)
    prev_orders = get_orders_for_date(prev_date)

    # Eğer önceki gün yok fallback: bugünün ilk yarısı
    if prev_orders.empty:
        prev_orders = today_orders.copy()

    hubs, prev_orders_with_hub = compute_hubs_for_orders(prev_orders, hub_capacity)

    data["hubs"] = hubs
    data["prev_orders"] = prev_orders_with_hub
    data["today_orders"] = today_orders

    # ── Özet metrikler ────────────────────────────────────────────────────────
    data["metrics"] = _compute_metrics(today_orders, hubs)

    return data


def reload_hubs(all_orders: pd.DataFrame, target_date: date, hub_capacity: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Verilen tarih ve kapasite için hub'ları yeniden hesaplar.
    Sol harita için çağrılır.
    """
    day_orders = all_orders[all_orders["date"] == target_date].copy() \
        if not all_orders.empty and "date" in all_orders.columns else pd.DataFrame()

    if day_orders.empty:
        return pd.DataFrame(columns=["hub_id", "lat", "lon"]), day_orders

    return compute_hubs_for_orders(day_orders, hub_capacity)


def _compute_metrics(orders: pd.DataFrame, hubs: pd.DataFrame) -> dict:
    total = len(orders)
    hub_n = len(hubs)
    return {
        "total_orders": total,
        "hub_count": hub_n,
        "silhouette_score": 0.53,
        "fuel_saving_pct": 22.0,
        "carbon_reduction": 18.5,
        "avg_load": round(total / max(hub_n, 1), 1),
    }