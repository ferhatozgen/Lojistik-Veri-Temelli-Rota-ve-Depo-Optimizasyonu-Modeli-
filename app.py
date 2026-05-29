"""
EDİRNE LOJİSTİK OPERASYON MERKEZİ — Streamlit UI
Dinamik K-Means Hub Optimizasyonu & Google OR-Tools CVRP Rota Motoru
"""
import os
import sys
import math
from datetime import datetime, timedelta

import folium
import numpy as np
import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(ROOT_DIR)

try:
    from src.warehouse.hub_optimizer import optimize_temporary_hubs_flexible
    from src.warehouse.route_optimizer import run_route_optimization_engine
except Exception as e:
    optimize_temporary_hubs_flexible = None
    run_route_optimization_engine = None
    BACKEND_IMPORT_ERROR = e
else:
    BACKEND_IMPORT_ERROR = None

EDIRNE_CENTER = [41.6772, 26.5567]
DATA_DIR = os.path.join(ROOT_DIR, "data")

st.set_page_config(
    page_title="Edirne Lojistik Operasyon Merkezi",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');
:root { --bg-dark:#0a0e17; --bg-card:#111827; --border:#2a3a55; --blue:#3b82f6; --green:#22c55e; --orange:#f97316; --red:#ef4444; --muted:#94a3b8; --text:#f8fafc; }
.stApp { background:var(--bg-dark) !important; font-family:'Syne',sans-serif !important; }
[data-testid="stSidebar"] { background:linear-gradient(180deg,#0d1424 0%,#111827 100%) !important; border-right:1px solid var(--border) !important; }
[data-testid="stSidebar"] * { color:#e2e8f0 !important; }
.ops-header { background:linear-gradient(135deg,#0f1f3d 0%,#1a2d4a 50%,#0f1f3d 100%); border:1px solid var(--border); border-radius:14px; padding:16px 22px; margin-bottom:14px; display:flex; align-items:center; gap:14px; }
.ops-header h1 { font-size:1.35rem !important; font-weight:800 !important; color:var(--text) !important; margin:0 !important; }
.ops-sub { font-family:'JetBrains Mono',monospace; font-size:.62rem; color:var(--blue); letter-spacing:2px; text-transform:uppercase; }
.section-title { font-family:'JetBrains Mono',monospace; font-size:.64rem; color:var(--blue); letter-spacing:2px; text-transform:uppercase; border-bottom:1px solid var(--border); padding-bottom:6px; margin:16px 0 10px; }
.metric-card { background:var(--bg-card); border:1px solid var(--border); border-radius:12px; padding:12px 14px; margin-bottom:9px; }
.metric-label { font-family:'JetBrains Mono',monospace; font-size:.62rem; color:var(--muted); letter-spacing:1.4px; text-transform:uppercase; margin-bottom:4px; }
.metric-value { font-size:1.5rem; font-weight:800; color:var(--text); line-height:1; }
.info-box { background:#111827; border:1px dashed #2a3a55; border-radius:12px; padding:18px; color:#94a3b8; font-size:.82rem; line-height:1.55; }
.depot-item { display:flex; gap:8px; padding:8px 10px; background:rgba(59,130,246,.08); border:1px solid rgba(59,130,246,.25); border-radius:8px; margin:4px 0; font-size:.78rem; color:#e2e8f0; }
.live-pill { display:inline-block; padding:3px 8px; border-radius:999px; background:rgba(34,197,94,.12); border:1px solid rgba(34,197,94,.35); color:#86efac; font-family:'JetBrains Mono',monospace; font-size:.65rem; }
#MainMenu, footer { visibility:hidden; }
.stDeployButton { display:none; }
</style>
""", unsafe_allow_html=True)


def read_csv_safe(filename: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_csv(path)


def get_day_orders(day) -> pd.DataFrame:
    df = read_csv_safe("simulated_orders.csv")
    if df.empty or "timestamp" not in df.columns:
        return pd.DataFrame()
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df[df["timestamp"].dt.date == day].copy()


def estimate_min_couriers(route_results, orders_df, capacity: int) -> int:
    if route_results:
        return len(pd.DataFrame(route_results)[["hub_id", "vehicle_id"]].drop_duplicates())
    if orders_df.empty:
        return 0
    return int(np.ceil(len(orders_df) / max(1, capacity)))


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def route_distance_km(route_results) -> float:
    if not route_results:
        return 0.0
    total = 0.0
    df = pd.DataFrame(route_results).sort_values(["hub_id", "vehicle_id", "sequence_no"])
    for _, g in df.groupby(["hub_id", "vehicle_id"]):
        pts = g[["lat", "lon"]].values.tolist()
        for a, b in zip(pts, pts[1:]):
            total += haversine_km(a[0], a[1], b[0], b[1]) * 1.3
    return round(total, 1)


def build_active_distribution_map(hub_data, route_data):
    m = folium.Map(location=EDIRNE_CENTER, zoom_start=12, tiles="CartoDB dark_matter", prefer_canvas=True)

    if hub_data and hub_data.get("hubs"):
        for hub in hub_data["hubs"]:
            folium.Marker(
                location=[hub["lat"], hub["lon"]],
                popup=f"<b>Transit Dağıtım Noktası {hub['hub_id']}</b><br>K-Means ağırlık merkezi",
                tooltip=f"Hub {hub['hub_id']}",
                icon=folium.Icon(color="red", icon="truck", prefix="fa"),
            ).add_to(m)

    if route_data:
        df_routes = pd.DataFrame(route_data)
        colors = ["blue", "green", "orange", "purple", "red", "cadetblue", "darkred", "darkblue"]
        for (hub_id, vehicle_id), group in df_routes.groupby(["hub_id", "vehicle_id"]):
            group = group.sort_values("sequence_no")
            coords = group[["lat", "lon"]].values.tolist()
            color = colors[int(vehicle_id) % len(colors)]
            if len(coords) > 1:
                folium.PolyLine(coords, color=color, weight=3, opacity=.85, tooltip=f"Hub {hub_id} · Kurye {vehicle_id}").add_to(m)
            for _, row in group.iterrows():
                if row["order_id"] != "DEPOT":
                    folium.CircleMarker(
                        location=[row["lat"], row["lon"]], radius=3, color=color,
                        fill=True, fill_opacity=.75, popup=f"Sipariş: {row['order_id']}"
                    ).add_to(m)
    return m


def build_live_pool_map(day, visible_until_hour: int):
    m = folium.Map(location=EDIRNE_CENTER, zoom_start=12, tiles="CartoDB dark_matter", prefer_canvas=True)
    orders = get_day_orders(day)
    if orders.empty:
        return m, orders
    orders = orders[orders["timestamp"].dt.hour <= visible_until_hour]
    district_colors = {
        "SARACLAR": "blue", "BALKAN": "cadetblue", "AYSEKADIN": "green",
        "SUKRUPASA": "purple", "ZUBEYDE": "purple", "KARAAGAC": "orange"
    }
    for _, row in orders.iterrows():
        color = district_colors.get(str(row.get("district", "")).upper(), "blue")
        folium.CircleMarker(
            location=[row["lat"], row["lon"]], radius=3, color=color,
            fill=True, fill_opacity=.65,
            popup=f"Yarın havuzu: {row.get('order_id', '')}<br>{row.get('district', '')}"
        ).add_to(m)
    return m, orders


for key, default in {
    "optimization_results": None,
    "route_results": None,
    "last_params": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:14px 0 6px 0;">
        <div style="font-size:1.8rem;">🚚</div>
        <div style="font-size:1.05rem;font-weight:800;color:#f8fafc;margin-top:5px;">EDİRNE LOJİSTİK</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:.58rem;color:#3b82f6;letter-spacing:3px;margin-top:2px;">OPERASYON SİSTEMİ</div>
    </div>
    <hr style="border-color:#2a3a55;margin:10px 0;">
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">⚙️ Yönetici Kontrol Paneli</div>', unsafe_allow_html=True)
    target_date = st.date_input("Dağıtım Günü", datetime(2026, 5, 29))
    hub_capacity = st.slider("Hub İşleme Kapasitesi", min_value=50, max_value=500, value=150, step=10, help="Bir geçici transit noktanın günlük kaldırabileceği maksimum paket.")
    vehicle_capacity = st.slider("Araç Taşıma Kapasitesi", min_value=10, max_value=100, value=25, step=5, help="Bir kuryenin tek turda taşıyacağı paket sınırı.")

    todays_orders_preview = get_day_orders(target_date)
    min_courier_preview = estimate_min_couriers(None, todays_orders_preview, vehicle_capacity)
    manual_courier_extra = st.slider(
        "Ekstra Kurye Desteği",
        min_value=0,
        max_value=10,
        value=0,
        step=1,
        help=f"Bu kapasiteyle teorik minimum yaklaşık {min_courier_preview} kurye. Slider bu minimumun üstüne ek destek verir.",
    )

    st.caption(f"Tahmini minimum kurye: **{min_courier_preview}** · Sahaya çıkarılacak: **{min_courier_preview + manual_courier_extra}**")

    st.markdown('<div class="section-title">🚀 Operasyon</div>', unsafe_allow_html=True)
    if BACKEND_IMPORT_ERROR:
        st.error(f"Backend modülleri yüklenemedi: {BACKEND_IMPORT_ERROR}")

    if st.button("Hub ve Rotaları Optimize Et", use_container_width=True, type="primary", disabled=BACKEND_IMPORT_ERROR is not None):
        progress = st.progress(0, text="Hazırlık başlatılıyor...")
        try:
            progress.progress(15, text="Veri hattı ve eksik tarih kontrolü yapılıyor...")
            hub_results = optimize_temporary_hubs_flexible(
                target_date_str=target_date.strftime("%Y-%m-%d"),
                user_hub_capacity=hub_capacity,
            )
            st.session_state.optimization_results = hub_results

            progress.progress(55, text="K-Means hub noktaları üretildi. OR-Tools CVRP motoru çalışıyor...")
            if hub_results and hub_results.get("hub_count", 0) > 0:
                st.session_state.route_results = run_route_optimization_engine(
                    user_vehicle_capacity=vehicle_capacity,
                    manual_courier_extra=manual_courier_extra,
                )
            else:
                st.session_state.route_results = []

            progress.progress(90, text="Harita katmanları hazırlanıyor...")
            st.session_state.last_params = {
                "target_date": target_date.strftime("%Y-%m-%d"),
                "hub_capacity": hub_capacity,
                "vehicle_capacity": vehicle_capacity,
                "manual_courier_extra": manual_courier_extra,
            }
            progress.progress(100, text="Optimizasyon tamamlandı.")
            st.success("Optimizasyon tamamlandı. Haritalar ve metrikler güncellendi.")
        except Exception as e:
            st.session_state.route_results = []
            st.error(f"Sistem hatası: {e}")
            with st.expander("Hata detayını göster"):
                st.exception(e)

now = datetime.now()
st.markdown(f"""
<div class="ops-header">
  <span style="font-size:1.7rem;">🗺️</span>
  <div>
    <h1>Canlı Operasyon Haritası</h1>
    <div class="ops-sub">EDİRNE MERKEZ · {target_date.strftime('%d.%m.%Y')} · K-MEANS + OR-TOOLS CVRP</div>
  </div>
</div>
""", unsafe_allow_html=True)

hub_data = st.session_state.optimization_results
route_data = st.session_state.route_results

map_col, pool_col = st.columns(2)
with map_col:
    st.markdown("<span class='live-pill'>08:00 Aktif Dağıtım</span>", unsafe_allow_html=True)
    st.markdown("#### Bugünün optimize edilmiş rota ağı")
    st_folium(build_active_distribution_map(hub_data, route_data), width="100%", height=520, key="active_distribution_map")

with pool_col:
    st.markdown("<span class='live-pill'>Canlı Sipariş Havuzu</span>", unsafe_allow_html=True)
    st.markdown("#### Yarın teslim edilecek siparişlerin akışı")
    live_hour = st.slider("Canlı akış saati", min_value=0, max_value=23, value=min(23, now.hour), step=1)
    tomorrow_date = target_date + timedelta(days=1)
    live_map, live_orders = build_live_pool_map(tomorrow_date, live_hour)
    st_folium(live_map, width="100%", height=470, key="live_pool_map")
    st.caption(f"{tomorrow_date.strftime('%d.%m.%Y')} havuzunda saat {live_hour:02d}:59'a kadar görünen sipariş: {len(live_orders)}")

summary_col, detail_col = st.columns([1, 1])
with summary_col:
    st.markdown('<div class="section-title">📊 Optimizasyon Özeti</div>', unsafe_allow_html=True)
    if hub_data:
        total_vehicles = estimate_min_couriers(route_data, get_day_orders(target_date), vehicle_capacity)
        total_distance = route_distance_km(route_data)
        st.markdown(f"""
        <div class="metric-card"><div class="metric-label">Planlanan Geçici Hub</div><div class="metric-value" style="color:var(--blue);">{hub_data.get('hub_count', 0)}</div></div>
        <div class="metric-card"><div class="metric-label">Toplam Kargo Tahmini</div><div class="metric-value">{hub_data.get('total_predicted_packages', 0)}</div></div>
        <div class="metric-card"><div class="metric-label">Aktif Kurye Sayısı</div><div class="metric-value" style="color:var(--orange);">{total_vehicles}</div></div>
        <div class="metric-card"><div class="metric-label">Yaklaşık Toplam Rota</div><div class="metric-value">{total_distance} km</div></div>
        <div class="metric-card"><div class="metric-label">Silhouette Skoru</div><div class="metric-value" style="color:var(--green);">{hub_data.get('silhouette_score', 0)}</div></div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="info-box">
        Soldaki panelden tarih, hub kapasitesi ve araç kapasitesini seçip optimizasyonu başlat. Sistem önce eksik tarih verisini tamamlar, sonra LSTM tahmini, K-Means hub yerleşimi ve OR-Tools CVRP rota çözümünü çalıştırır.
        </div>
        """, unsafe_allow_html=True)

with detail_col:
    st.markdown('<div class="section-title">🏭 Aktif Transit Noktaları</div>', unsafe_allow_html=True)
    if hub_data and hub_data.get("hubs"):
        for hub in hub_data["hubs"]:
            st.markdown(f"""
            <div class="depot-item">
              <span>🚚</span>
              <div><b>Hub {hub['hub_id']}</b><br><span style="font-family:'JetBrains Mono',monospace;color:#94a3b8;font-size:.68rem;">{hub['lat']:.5f}, {hub['lon']:.5f}</span></div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("Henüz aktif hub üretimi yapılmadı.")

with st.expander("Jüriye anlatım notu"):
    st.markdown("""
    Bu panel iki yönlü çalışıyor: solda saat 08:00'de kapanan dağıtım havuzunun K-Means ile belirlenen geçici transit noktaları ve Google OR-Tools CVRP rotaları gösteriliyor. Sağda ise gün içinde düşen yeni siparişler yarının teslimat havuzuna akıyor. Böylece sistem hem bugünkü operasyonu yürütüyor hem de yarının kaynak planlamasını besliyor.
    """)
