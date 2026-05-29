"""
╔══════════════════════════════════════════════════════════╗
║   EDİRNE LOJİSTİK OPERASYON MERKEZİ — Streamlit UI       ║
║   Dinamik K-Means Hub Optimizasyonu & Canlı Simülasyon   ║
╚══════════════════════════════════════════════════════════╝
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
from datetime import datetime
import time
import math
import random
import numpy as np
from sklearn.cluster import KMeans

from utils.traffic import get_traffic_level, get_route_color, check_special_closure
from utils.routing import draw_route_straight
from utils.order_generator import generate_live_orders

# ─── Yardımcı ───────────────────────────────
def _haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def find_nearest_dynamic_hub(order_lat, order_lon, hubs):
    nearest = None
    min_dist = float("inf")
    for hub in hubs:
        dist = _haversine(order_lat, order_lon, hub["lat"], hub["lon"])
        if dist < min_dist:
            min_dist = dist
            nearest = hub.copy()
            nearest["distance_km"] = round(dist, 3)
    return nearest

# ─── Sayfa Yapılandırması ───────────────────
st.set_page_config(
    page_title="Edirne Lojistik Operasyon Merkezi",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS ────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@400;600;800&display=swap');

:root {
    --bg-dark:#0a0e17; --bg-panel:#111827; --bg-card:#1a2235;
    --border:#2a3a55; --accent-blue:#3b82f6; --accent-green:#22c55e;
    --accent-orange:#f97316; --accent-red:#ef4444; --accent-purple:#a855f7;
    --text-primary:#e2e8f0; --text-muted:#64748b; --text-bright:#f8fafc;
}
.stApp { background-color:var(--bg-dark) !important; font-family:'Syne',sans-serif !important; }
[data-testid="stSidebar"] {
    background:linear-gradient(180deg,#0d1424 0%,#111827 100%) !important;
    border-right:1px solid var(--border) !important;
}
[data-testid="stSidebar"] * { color:var(--text-primary) !important; }
.ops-header {
    background:linear-gradient(135deg,#0f1f3d 0%,#1a2d4a 50%,#0f1f3d 100%);
    border:1px solid var(--border); border-radius:12px;
    padding:14px 22px; margin-bottom:14px;
    display:flex; align-items:center; gap:12px;
    box-shadow:0 4px 24px rgba(59,130,246,0.1);
}
.ops-header h1 {
    font-family:'Syne',sans-serif !important; font-size:1.3rem !important;
    font-weight:800 !important; color:var(--text-bright) !important; margin:0 !important;
}
.section-title {
    font-family:'JetBrains Mono',monospace; font-size:0.62rem; color:var(--accent-blue);
    letter-spacing:2px; text-transform:uppercase; border-bottom:1px solid var(--border);
    padding-bottom:5px; margin-bottom:10px; margin-top:18px;
}
.metric-card {
    background:var(--bg-card); border:1px solid var(--border);
    border-radius:10px; padding:10px 14px; margin-bottom:8px;
}
.metric-label { font-family:'JetBrains Mono',monospace; font-size:0.62rem;
    color:var(--text-muted); letter-spacing:1.5px; text-transform:uppercase; margin-bottom:3px; }
.metric-value { font-family:'Syne',sans-serif; font-size:1.5rem;
    font-weight:800; color:var(--text-bright); line-height:1; }
.alert-high { background:rgba(239,68,68,0.1); border:1px solid var(--accent-red);
    border-left:4px solid var(--accent-red); border-radius:8px;
    padding:9px 12px; margin:6px 0; font-size:0.8rem; color:#fca5a5; }
.alert-medium { background:rgba(249,115,22,0.1); border:1px solid var(--accent-orange);
    border-left:4px solid var(--accent-orange); border-radius:8px;
    padding:9px 12px; margin:6px 0; font-size:0.8rem; color:#fdba74; }
.alert-low { background:rgba(34,197,94,0.1); border:1px solid var(--accent-green);
    border-left:4px solid var(--accent-green); border-radius:8px;
    padding:9px 12px; margin:6px 0; font-size:0.8rem; color:#86efac; }
.order-item { display:flex; align-items:center; gap:8px; padding:7px 9px;
    background:var(--bg-card); border:1px solid var(--border); border-radius:6px;
    margin:3px 0; font-family:'JetBrains Mono',monospace; font-size:0.7rem; color:var(--text-primary); }
.dot-blue  { width:9px; height:9px; border-radius:50%; background:#3b82f6; flex-shrink:0; }
.dot-green { width:9px; height:9px; border-radius:50%; background:#22c55e; flex-shrink:0; }
.depot-item { display:flex; align-items:center; gap:8px; padding:7px 10px;
    background:rgba(59,130,246,0.07); border:1px solid rgba(59,130,246,0.25);
    border-radius:6px; margin:3px 0; font-size:0.78rem; color:var(--text-primary); }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.35} }
.live-dot { width:8px; height:8px; border-radius:50%; background:var(--accent-green);
    animation:pulse 1.5s ease-in-out infinite; display:inline-block; }
#MainMenu,footer,header{visibility:hidden}
.stDeployButton{display:none}
</style>
""", unsafe_allow_html=True)

# ─── Session State ───────────────────────────
if "orders"           not in st.session_state:
    st.session_state.orders = generate_live_orders(n_today=35, n_tomorrow=15)
if "selected_order"   not in st.session_state:
    st.session_state.selected_order = None

# ════════════════════════════════════════════════════════
#  SOL PANEL (KONTROL MERKEZİ)
# ════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="text-align:center;padding:14px 0 6px 0;">
        <div style="font-size:1.7rem;">🚚</div>
        <div style="font-family:'Syne',sans-serif;font-size:1.05rem;font-weight:800;
                    color:#f8fafc;margin-top:5px;">EDİRNE LOJİSTİK</div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.58rem;
                    color:#3b82f6;letter-spacing:3px;margin-top:2px;">OPERASYON SİSTEMİ v2.1</div>
    </div>
    <hr style="border-color:#2a3a55;margin:10px 0;">
    """, unsafe_allow_html=True)

    st.markdown('<div class="section-title">⚙️ Optimizasyon Modu</div>', unsafe_allow_html=True)
    opt_mode = st.radio(
        "mod", ["💰 Minimum Maliyet", "⚡ Maksimum Hız", "🌿 Minimum Karbon"],
        index=0, label_visibility="collapsed",
    )

    st.markdown('<div class="section-title">🕐 Zaman Kontrolü</div>', unsafe_allow_html=True)
    live_mode = st.toggle("🔴 Canlı Mod", value=True)
    now = datetime.now()

    if live_mode:
        current_hour   = now.hour
        current_minute = now.minute
        st.markdown(f"""
        <div style="display:flex;align-items:center;gap:6px;margin-top:5px;
                    font-family:'JetBrains Mono',monospace;font-size:0.68rem;color:#22c55e;">
            <div class="live-dot"></div>
            CANLI — {now.strftime('%H:%M:%S')}
        </div>""", unsafe_allow_html=True)
    else:
        total_min = st.slider("Saat", 0, 23*60+59, now.hour*60+now.minute, step=15, label_visibility="collapsed")
        current_hour   = total_min // 60
        current_minute = total_min % 60

    st.markdown('<div class="section-title">🧠 YZ Geçici Hub Planlama</div>', unsafe_allow_html=True)
    hub_capacity = st.slider("Bir Hub'ın Max Kapasitesi (Paket)", min_value=50, max_value=300, value=150, step=10)

    orders = st.session_state.orders
    today_orders = [o for o in orders if o["type"] == "today" or (current_hour == 0 and o["type"] == "tomorrow")]
    tomorrow_orders = [o for o in orders if o["type"] == "tomorrow" and current_hour != 0]
    
    total_today_volume = sum(o["volume"] for o in today_orders)
    k_clusters = max(1, math.ceil(total_today_volume / hub_capacity)) if total_today_volume > 0 else 1

    st.markdown(f"""
    <div style="background:rgba(34,197,94,0.1);border:1px solid #22c55e; border-radius:6px; padding:10px; margin-top:5px; text-align:center;">
        <div style="font-size:0.7rem; color:#86efac;">Atanan Geçici Dağıtım Noktası</div>
        <div style="font-size:1.5rem; font-weight:bold; color:#22c55e;">{k_clusters} Adet</div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">🔵 Bugün</div>'
                    f'<div class="metric-value">{len(today_orders)}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">🟢 Yarın</div>'
                    f'<div class="metric-value">{len(tomorrow_orders)}</div></div>', unsafe_allow_html=True)

    st.markdown('<div class="section-title">➕ Canlı Sipariş Simülasyonu</div>', unsafe_allow_html=True)
    if st.button("🆕 Yeni Sipariş Ekle", use_container_width=True, type="primary"):
        new_order = {
            "id":      f"ORD-{random.randint(10000,99999)}",
            "lat":     round(random.uniform(41.635, 41.700), 6),
            "lon":     round(random.uniform(26.510, 26.625), 6),
            "type":    "tomorrow",
            "address": "Anlık Sipariş",
            "volume":  random.randint(5, 50),
            "hour":    current_hour,
        }
        st.session_state.orders.append(new_order)
        st.success(f"✅ {new_order['id']} eklendi!")
        st.rerun()

# ════════════════════════════════════════════════════════
#  DİNAMİK K-MEANS HESAPLAMASI
# ════════════════════════════════════════════════════════
dynamic_depots = []
if today_orders:
    coords = np.array([[o["lon"], o["lat"]] for o in today_orders])
    kmeans = KMeans(n_clusters=k_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(coords)
    centers = kmeans.cluster_centers_
    
    for idx, (lon, lat) in enumerate(centers):
        dynamic_depots.append({
            "id": idx + 1,
            "name": f"Geçici Hub {idx + 1}",
            "lat": lat,
            "lon": lon
        })
        
    for i, o in enumerate(today_orders):
        o["assigned_hub_idx"] = labels[i]

# ════════════════════════════════════════════════════════
#  ANA ALAN (Alt Alta Harita + Sağ Panel)
# ════════════════════════════════════════════════════════
traffic_level = get_traffic_level(current_hour)
date_str = now.strftime("%m-%d")
closure  = check_special_closure(date_str, current_hour)

t_map = {
    "high":   (85, "#ef4444", "🔴 YOĞUN TRAFİK"),
    "medium": (55, "#f97316", "🟠 ORTA YOĞUNLUK"),
    "low":    (18, "#22c55e", "🟢 AKIŞKAN TRAFİK"),
}
t_pct, t_clr, t_label = t_map[traffic_level]

st.markdown(f"""
<div class="ops-header">
    <span style="font-size:1.7rem;">🗺️</span>
    <div>
        <h1>Canlı Operasyon Haritası</h1>
        <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                    color:#3b82f6;letter-spacing:2px;text-transform:uppercase;">
            EDİRNE LOJİSTİK · {now.strftime('%d %B %Y')} · {current_hour:02d}:{current_minute:02d}
        </div>
    </div>
    <div style="margin-left:auto;display:flex;gap:8px;align-items:center;">
        <div style="background:#1a2235;border:1px solid {t_clr};border-radius:6px;
                    padding:5px 12px;font-family:'JetBrains Mono',monospace;
                    font-size:0.68rem;color:{t_clr};">
            {t_label}
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Ekranı ikiye bölüyoruz: %75 Ana Haritalar (Alt Alta), %25 Bilgi Paneli
main_col, info_col = st.columns([2.5, 1])

# ─── SOL TARAF: ALT ALTA HARİTALAR ────────────────
with main_col:
    # 1. ÜST HARİTA (BUGÜN)
    st.markdown("<h4 style='color:#3b82f6; font-size:1.1rem; margin-bottom: 0px;'>🔵 Bugün (Aktif Dağıtım Ağı)</h4>", unsafe_allow_html=True)
    m_left = folium.Map(location=[41.6772, 26.5567], zoom_start=12, tiles="CartoDB dark_matter", prefer_canvas=True)

    for depot in dynamic_depots:
        folium.Marker(
            location=[depot["lat"], depot["lon"]],
            popup=f"<b style='color:#3b82f6;'>🚚 {depot['name']}</b><br><small>Dinamik K-Means Noktası</small>",
            tooltip=f"{depot['name']}",
            icon=folium.Icon(color="blue", icon="truck", prefix="fa"),
        ).add_to(m_left)

    for order in today_orders:
        icon_html = ("<div style='width:10px;height:10px;background:#3b82f6;"
                     "border-radius:50%;border:1px solid #3b82f6;"
                     "box-shadow:0 0 5px #3b82f688;cursor:pointer;'></div>")
        folium.Marker(
            location=[order["lat"], order["lon"]],
            popup=f"<b>📦 {order.get('id','?')}</b><br><small>Hacim: {order.get('volume','?')} birim</small>",
            tooltip=f"🔵 {order.get('id','SIP')} — Tıkla",
            icon=folium.DivIcon(html=icon_html, icon_size=(10,10), icon_anchor=(5,5)),
        ).add_to(m_left)

    if st.session_state.selected_order and st.session_state.selected_order["type"] == "today":
        sel = st.session_state.selected_order
        depot = find_nearest_dynamic_hub(sel["lat"], sel["lon"], dynamic_depots)
        rstyle = get_route_color(traffic_level, opt_mode)
        draw_route_straight(m_left, depot=depot, order=sel, style=rstyle, traffic_level=traffic_level, closure=closure)

    map_data_left = st_folium(m_left, width="100%", height=380, returned_objects=["last_object_clicked_tooltip"], key="map_left")

    if map_data_left and map_data_left.get("last_object_clicked_tooltip"):
        tt = str(map_data_left["last_object_clicked_tooltip"])
        for order in today_orders:
            if order.get("id") in tt and st.session_state.selected_order != order:
                st.session_state.selected_order = order
                st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # 2. ALT HARİTA (YARIN)
    st.markdown("<h4 style='color:#22c55e; font-size:1.1rem; margin-bottom: 0px;'>🟢 Yarın (Canlı Sipariş Havuzu)</h4>", unsafe_allow_html=True)
    m_right = folium.Map(location=[41.6772, 26.5567], zoom_start=12, tiles="CartoDB dark_matter", prefer_canvas=True)

    for order in tomorrow_orders:
        icon_html = ("<div style='width:10px;height:10px;background:#22c55e;"
                     "border-radius:50%;border:1px solid #22c55e;"
                     "box-shadow:0 0 5px #22c55e88;cursor:pointer;'></div>")
        folium.Marker(
            location=[order["lat"], order["lon"]],
            popup=f"<b>📦 {order.get('id','?')}</b><br><small>Hacim: {order.get('volume','?')} birim</small>",
            tooltip=f"🟢 {order.get('id','SIP')} — Yarına Aktarılacak",
            icon=folium.DivIcon(html=icon_html, icon_size=(10,10), icon_anchor=(5,5)),
        ).add_to(m_right)

    st_folium(m_right, width="100%", height=380, key="map_right")

# ─── SAĞ BİLGİ PANELİ ────────────────
with info_col:
    if st.session_state.selected_order and st.session_state.selected_order["type"] == "today":
        sel    = st.session_state.selected_order
        depot  = find_nearest_dynamic_hub(sel["lat"], sel["lon"], dynamic_depots)
        rstyle = get_route_color(traffic_level, opt_mode)
        dist   = _haversine(sel["lat"], sel["lon"], depot["lat"], depot["lon"])

        st.markdown(f"""
        <div style="background:#1a2235;border:1px solid {rstyle['color']}44;
                    border-left:4px solid {rstyle['color']};border-radius:10px;
                    padding:13px;margin-bottom:10px;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:0.6rem;
                         color:{rstyle['color']};letter-spacing:1.5px;margin-bottom:7px;">ROTA ANALİZİ</div>
            <div style="font-size:0.82rem;font-weight:700;color:#f8fafc;margin-bottom:5px;">
                📦 {sel.get('id','?')}</div>
            <div style="font-size:0.73rem;color:#94a3b8;margin-bottom:3px;">
                📍 {sel['lat']:.4f}, {sel['lon']:.4f}</div>
            <hr style="border-color:#2a3a55;margin:7px 0;">
            <div style="font-size:0.73rem;color:#94a3b8;margin-bottom:3px;">
                🚚 <b style="color:#f8fafc;">{depot['name']}</b></div>
            <div style="font-size:0.73rem;color:#94a3b8;margin-bottom:3px;">
                📏 <b style="color:#f8fafc;">{dist:.2f} km</b></div>
            <div style="font-size:0.73rem;color:#94a3b8;margin-bottom:3px;">
                🚦 Trafik: <b style="color:{rstyle['color']};">{traffic_level.upper()}</b></div>
            <div style="font-size:0.73rem;color:#94a3b8;">
                ⚡ <b style="color:#f8fafc;">{opt_mode.split()[0]} {opt_mode.split()[1]}</b></div>
        </div>""", unsafe_allow_html=True)

        if traffic_level == "high":
            st.markdown('<div class="alert-high">⚠️ Yoğun trafik! Rota kırmızı.</div>', unsafe_allow_html=True)
        elif traffic_level == "medium":
            st.markdown('<div class="alert-medium">ℹ️ Orta yoğunluk. Gecikme olabilir.</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="alert-low">✅ İdeal teslimat koşulları.</div>', unsafe_allow_html=True)

        if st.button("✕ Rotayı Kapat", use_container_width=True):
            st.session_state.selected_order = None
            st.rerun()
    else:
        st.markdown("""
        <div style="background:#111827;border:1px dashed #2a3a55;border-radius:10px;
                    padding:22px;text-align:center;color:#374151;margin-bottom:12px;">
            <div style="font-size:1.4rem;margin-bottom:8px;">👆</div>
            <div style="font-size:0.76rem;line-height:1.6;color:#64748b;">
                Üst haritada bir sipariş noktasına tıklayarak rota analizini görün.
            </div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-title">🏭 Aktif Hub Noktaları</div>', unsafe_allow_html=True)
    for d in dynamic_depots:
        st.markdown(f"""
        <div class="depot-item">
            <span>🚚</span>
            <div>
                <div style="font-weight:600;font-size:0.78rem;">{d['name']}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.62rem;color:#64748b;">
                    {d['lat']:.4f}, {d['lon']:.4f}</div>
            </div>
        </div>""", unsafe_allow_html=True)

if live_mode:
    time.sleep(30)
    st.rerun()