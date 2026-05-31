"""
Folium harita nesneleri üretir.
- build_route_map()       : sol harita — hub'lar + kurye rotaları
- build_live_order_map()  : sağ harita — canlı animasyon (yanıp sönen yeni siparişler)
- build_past_route_map()  : geçmiş sol harita
- build_past_order_map()  : geçmiş sağ harita
- build_future_heatmap()  : gelecek bölge yoğunluk haritası
"""

import folium
from folium.plugins import HeatMap
import pandas as pd
import numpy as np
import math

EDIRNE_CENTER = [41.6772, 26.5567]

DISTRICT_COLORS = {
    "BALKAN":    "#60a5fa",
    "SARACLAR":  "#34d399",
    "KARAAGAC":  "#f472b6",
    "AYSEKADIN": "#fbbf24",
    "SUKRUPASA": "#a78bfa",
    "DELTA":     "#fb923c",
    "CENTER":    "#e2e8f0",
}

DISTRICT_CENTERS = {
    "BALKAN":    [41.638, 26.611],
    "SARACLAR":  [41.669, 26.562],
    "KARAAGAC":  [41.659, 26.526],
    "AYSEKADIN": [41.676, 26.580],
    "SUKRUPASA": [41.672, 26.596],
    "DELTA":     [41.645, 26.600],
}

HUB_COLOR = "#f43f5e"

COURIER_COLORS = [
    "#60a5fa", "#34d399", "#fbbf24", "#a78bfa",
    "#fb923c", "#f472b6", "#38bdf8", "#4ade80",
    "#facc15", "#c084fc", "#f97316", "#e879f9",
]


def _base_map(zoom: int = 14) -> folium.Map:
    return folium.Map(
        location=EDIRNE_CENTER,
        zoom_start=zoom,
        tiles="CartoDB dark_matter",
        prefer_canvas=True,
    )


# ── Sol Harita: Hub + Kurye Rota ──────────────────────────────────────────────

def build_route_map(hubs: pd.DataFrame, orders: pd.DataFrame,
                    n_couriers: int = 4) -> folium.Map:
    """
    Sol harita: Hub'lar + her hub için kurye rotaları.
    n_couriers: panelden seçilen her hub başına kurye sayısı (min 4).
    """
    m = _base_map()

    if hubs.empty:
        _add_district_labels(m)
        return m

    if not orders.empty:
        _add_orders_static(m, orders, alpha=0.5)

    _add_hubs(m, hubs)

    if not orders.empty and "assigned_hub" in orders.columns:
        _add_courier_routes(m, hubs, orders, n_couriers)

    return m


def _add_courier_routes(m: folium.Map, hubs: pd.DataFrame,
                        orders: pd.DataFrame, n_couriers: int):
    """
    Her hub için siparişleri n_couriers kuryeye round-robin dağıt,
    en-yakın-komşu ile rota çiz (hub → müşteriler → hub).
    1.3× yol eğrilik katsayısı kullanılır.
    """
    CIRCUITY = 1.3

    for _, hub_row in hubs.iterrows():
        hub_id  = int(hub_row["hub_id"])
        hub_lat = float(hub_row["lat"])
        hub_lon = float(hub_row["lon"])

        hub_orders = orders[orders["assigned_hub"] == hub_id].copy()
        hub_orders = hub_orders.dropna(subset=["lat", "lon"]).reset_index(drop=True)

        if hub_orders.empty:
            continue

        # Round-robin dağıtım
        courier_groups: list[list] = [[] for _ in range(n_couriers)]
        for i, (_, row) in enumerate(hub_orders.iterrows()):
            courier_groups[i % n_couriers].append(row)

        for c_idx, group in enumerate(courier_groups):
            if not group:
                continue

            color = COURIER_COLORS[c_idx % len(COURIER_COLORS)]

            # En-yakın-komşu sıralama
            remaining  = list(group)
            route_locs = [[hub_lat, hub_lon]]
            cur_lat, cur_lon = hub_lat, hub_lon

            while remaining:
                best_d, best_i = float("inf"), 0
                for idx, pt in enumerate(remaining):
                    d = _haversine(cur_lat, cur_lon, float(pt["lat"]), float(pt["lon"])) * CIRCUITY
                    if d < best_d:
                        best_d, best_i = d, idx
                chosen = remaining.pop(best_i)
                route_locs.append([float(chosen["lat"]), float(chosen["lon"])])
                cur_lat, cur_lon = float(chosen["lat"]), float(chosen["lon"])

            route_locs.append([hub_lat, hub_lon])

            folium.PolyLine(
                locations=route_locs,
                color=color,
                weight=2.0,
                opacity=0.80,
                tooltip=f"Hub {hub_id} · Kurye {c_idx + 1} · {len(group)} teslimat",
            ).add_to(m)


# ── Sağ Harita: Canlı Animasyon ───────────────────────────────────────────────

def build_live_order_map(orders_pool: list, hour: int,
                          new_order_ids: set = None) -> folium.Map:
    """
    Sağ harita: birikmiş siparişler + yanıp sönen yeni siparişler.
    """
    m = _base_map()
    new_order_ids = new_order_ids or set()

    if not orders_pool:
        _add_district_labels(m)
        return m

    df = pd.DataFrame(orders_pool)
    if df.empty or "lat" not in df.columns:
        return m

    # Isı haritası (arka plan yoğunluk)
    heat_data = df[["lat", "lon"]].dropna().values.tolist()
    if heat_data:
        HeatMap(
            heat_data,
            radius=14,
            blur=18,
            min_opacity=0.2,
            gradient={"0.3": "#1e3a5f", "0.6": "#3b82f6", "1": "#93c5fd"},
        ).add_to(m)

    new_ids_str = {str(x) for x in new_order_ids}

    for _, row in df.iterrows():
        if not (pd.notna(row.get("lat")) and pd.notna(row.get("lon"))):
            continue
        order_id = str(row.get("order_id", ""))
        district = str(row.get("district", ""))
        color    = DISTRICT_COLORS.get(district, "#60a5fa")
        is_new   = order_id in new_ids_str

        if is_new:
            folium.Marker(
                location=[float(row["lat"]), float(row["lon"])],
                icon=folium.DivIcon(
                    html=f"""<div style="
                      width:10px;height:10px;border-radius:50%;
                      background:{color};
                      animation:pulse_ely 1.2s infinite;
                    "></div>
                    <style>
                    @keyframes pulse_ely {{
                      0%   {{ box-shadow:0 0 0 0 {color}cc; transform:scale(1); }}
                      50%  {{ box-shadow:0 0 0 8px {color}00; transform:scale(1.5); }}
                      100% {{ box-shadow:0 0 0 0 {color}00; transform:scale(1); }}
                    }}
                    </style>""",
                    icon_size=(10, 10),
                    icon_anchor=(5, 5),
                ),
                popup=folium.Popup(
                    f"<b>YENİ</b> · {order_id}<br>{district}<br>{hour:02d}:00",
                    max_width=160,
                ),
            ).add_to(m)
        else:
            folium.CircleMarker(
                location=[float(row["lat"]), float(row["lon"])],
                radius=3,
                color=color,
                fill=True,
                fill_opacity=0.75,
                weight=0.3,
                popup=folium.Popup(f"{order_id} · {district}", max_width=130),
            ).add_to(m)

    # Saat overlay
    folium.Marker(
        location=[41.710, 26.510],
        icon=folium.DivIcon(
            html=f"""<div style="
              background:#0f172a;border:1px solid #1e40af;
              color:#60a5fa;font-family:'IBM Plex Mono',monospace;
              font-size:11px;font-weight:600;padding:6px 10px;
              border-radius:6px;white-space:nowrap;">
              📦 {len(df)} sipariş · {hour:02d}:00
            </div>""",
            icon_size=(170, 32),
        ),
    ).add_to(m)

    return m


# ── Geçmiş Mod ────────────────────────────────────────────────────────────────

def build_past_route_map(hubs: pd.DataFrame, orders: pd.DataFrame,
                          n_couriers: int = 4) -> folium.Map:
    """Geçmiş mod sol harita: o günün hub + rota."""
    m = _base_map()
    if not orders.empty:
        _add_orders_static(m, orders, alpha=0.45)
    if not hubs.empty:
        _add_hubs(m, hubs)
        if not orders.empty and "assigned_hub" in orders.columns:
            _add_courier_routes(m, hubs, orders, n_couriers)
    return m


def build_past_order_map(orders: pd.DataFrame) -> folium.Map:
    """Geçmiş mod sağ harita: o günün tüm sipariş noktaları."""
    m = _base_map()
    if orders.empty:
        _add_district_labels(m)
        return m
    _add_orders_static(m, orders, alpha=0.75)
    _add_district_summary(m, orders)
    return m


# ── Gelecek Mod ───────────────────────────────────────────────────────────────

def build_future_heatmap(demand: pd.DataFrame, target_date) -> folium.Map:
    """Gelecek mod: LSTM bölge yoğunluk haritası."""
    m = _base_map()

    if not demand.empty and "district" in demand.columns:
        day_demand = demand[demand["date"] == target_date] \
            if "date" in demand.columns else demand
        district_totals = day_demand.groupby("district")["demand"].sum()
        max_demand = district_totals.max() if not district_totals.empty else 1

        for district, center in DISTRICT_CENTERS.items():
            total = district_totals.get(district, 0)
            ratio = total / max(max_demand, 1)
            color = "#f43f5e" if ratio > 0.7 else ("#fbbf24" if ratio > 0.4 else "#34d399")
            intensity = "YÜKSEK" if ratio > 0.7 else ("ORTA" if ratio > 0.4 else "DÜŞÜK")

            folium.CircleMarker(
                location=center,
                radius=int(20 + ratio * 30),
                color=color,
                fill=True,
                fill_opacity=0.35,
                weight=1.5,
                popup=folium.Popup(
                    f"<b>{district}</b><br>Tahmini: {int(total)} paket<br>{intensity}",
                    max_width=180,
                ),
            ).add_to(m)

            folium.map.Marker(
                center,
                icon=folium.DivIcon(
                    html=f"""<div style="color:{color};font-family:'IBM Plex Mono',monospace;
                      font-size:10px;font-weight:600;text-shadow:0 0 8px {color};
                      white-space:nowrap;margin-top:-8px;">
                      {district} · {int(total)}</div>""",
                    icon_size=(140, 20),
                ),
            ).add_to(m)
    else:
        _add_district_labels(m)

    return m


# ── Yardımcı ──────────────────────────────────────────────────────────────────

def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _add_hubs(m: folium.Map, hubs: pd.DataFrame):
    for _, row in hubs.iterrows():
        folium.Marker(
            location=[float(row["lat"]), float(row["lon"])],
            icon=folium.DivIcon(
                html=f"""<div style="
                  background:#f43f5e;border:2px solid #fda4af;
                  color:#fff;font-family:'IBM Plex Mono',monospace;
                  font-size:10px;font-weight:700;
                  width:28px;height:28px;border-radius:50%;
                  display:flex;align-items:center;justify-content:center;
                  box-shadow:0 0 12px #f43f5e80;">
                  H{int(row['hub_id'])}
                </div>""",
                icon_size=(28, 28),
                icon_anchor=(14, 14),
            ),
            popup=folium.Popup(
                f"<b>Hub {int(row['hub_id'])}</b><br>{float(row['lat']):.4f}, {float(row['lon']):.4f}",
                max_width=160,
            ),
        ).add_to(m)


def _add_orders_static(m: folium.Map, orders: pd.DataFrame, alpha: float = 0.7):
    for _, row in orders.iterrows():
        if pd.notna(row.get("lat")) and pd.notna(row.get("lon")):
            district = str(row.get("district", ""))
            color = DISTRICT_COLORS.get(district, "#94a3b8")
            folium.CircleMarker(
                location=[float(row["lat"]), float(row["lon"])],
                radius=3,
                color=color,
                fill=True,
                fill_opacity=alpha,
                weight=0.3,
            ).add_to(m)


def _add_district_labels(m: folium.Map):
    for name, center in DISTRICT_CENTERS.items():
        folium.map.Marker(
            center,
            icon=folium.DivIcon(
                html=f"""<div style="color:#475569;font-family:'IBM Plex Mono',monospace;
                  font-size:10px;letter-spacing:1px;">{name}</div>""",
                icon_size=(100, 16),
            ),
        ).add_to(m)


def _add_district_summary(m: folium.Map, orders: pd.DataFrame):
    """Geçmiş sağ haritada bölge bazlı sipariş sayısı etiketi."""
    if "district" not in orders.columns:
        return
    counts = orders.groupby("district").size()
    for district, center in DISTRICT_CENTERS.items():
        n = counts.get(district, 0)
        if n == 0:
            continue
        color = DISTRICT_COLORS.get(district, "#94a3b8")
        folium.map.Marker(
            [center[0] + 0.003, center[1]],
            icon=folium.DivIcon(
                html=f"""<div style="color:{color};font-family:'IBM Plex Mono',monospace;
                  font-size:9px;font-weight:600;text-shadow:0 0 6px {color}88;
                  white-space:nowrap;">{district} · {n}</div>""",
                icon_size=(120, 16),
            ),
        ).add_to(m)


def map_to_html(m: folium.Map) -> str:
    return m._repr_html_()