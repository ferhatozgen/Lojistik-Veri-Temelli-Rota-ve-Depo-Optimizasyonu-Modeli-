"""
╔══════════════════════════════════════════════════════════╗
║   EDİRNE LOJİSTİK OPERASYON MERKEZİ — Streamlit UI      ║
║   SOM Depo Optimizasyonu + LSTM Talep Tahmini            ║
╚══════════════════════════════════════════════════════════╝
"""

import streamlit as st
import folium
from streamlit_folium import st_folium
from datetime import datetime
import time
import math
import random

from utils.traffic import get_traffic_level, get_route_color, check_special_closure
from utils.som_depots import DEPOT_LOCATIONS, find_nearest_depot
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
    background:rgba(239,68,68,0.07); border:1px solid rgba(239,68,68,0.25);
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
    st.session_state.orders = generate_live_orders(n_today=28, n_tomorrow=18)
if "selected_order"   not in st.session_state:
    st.session_state.selected_order = None

# ════════════════════════════════════════════════════════
#  SOL PANEL
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

    # — Optimizasyon Modu —
    st.markdown('<div class="section-title">⚙️ Optimizasyon Modu</div>', unsafe_allow_html=True)
    opt_mode = st.radio(
        "mod", ["💰 Minimum Maliyet", "⚡ Maksimum Hız", "🌿 Minimum Karbon"],
        index=0, label_visibility="collapsed",
    )
    mode_desc = {
        "💰 Minimum Maliyet": ("Mesafe optimize edilir. En kısa güzergah.", "#3b82f6"),
        "⚡ Maksimum Hız":    ("Trafik en düşük yollar seçilir.",           "#a855f7"),
        "🌿 Minimum Karbon":  ("Optimum hız & az duruş noktaları.",         "#22c55e"),
    }
    desc, clr = mode_desc[opt_mode]
    st.markdown(f"""
    <div style="background:rgba(59,130,246,0.06);border:1px solid {clr}33;
                border-left:3px solid {clr};border-radius:6px;
                padding:7px 10px;font-size:0.73rem;color:#94a3b8;margin-top:4px;">
        {desc}
    </div>""", unsafe_allow_html=True)

    # — Zaman Kontrolü —
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
        total_min = st.slider("Saat", 0, 23*60+59,
                              now.hour*60+now.minute, step=15,
                              label_visibility="collapsed")
        current_hour   = total_min // 60
        current_minute = total_min % 60
        st.markdown(f"""
        <div style="text-align:center;font-family:'JetBrains Mono',monospace;
                    font-size:1.4rem;font-weight:700;color:#f8fafc;
                    background:#1a2235;border:1px solid #2a3a55;
                    border-radius:8px;padding:9px;margin:5px 0;">
            🕐 {current_hour:02d}:{current_minute:02d}
        </div>""", unsafe_allow_html=True)

    # — Trafik Durumu —
    st.markdown('<div class="section-title">🚦 Trafik Durumu</div>', unsafe_allow_html=True)
    traffic_level = get_traffic_level(current_hour)
    t_map = {
        "high":   (85, "#ef4444", "🔴 YOĞUN TRAFİK"),
        "medium": (55, "#f97316", "🟠 ORTA YOĞUNLUK"),
        "low":    (18, "#22c55e", "🟢 AKIŞKAN TRAFİK"),
    }
    t_pct, t_clr, t_label = t_map[traffic_level]
    st.markdown(f"""
    <div style="background:#1a2235;border:1px solid #2a3a55;border-radius:8px;padding:10px;margin:5px 0;">
        <div style="display:flex;justify-content:space-between;margin-bottom:5px;">
            <span style="font-size:0.73rem;color:#94a3b8;">{t_label}</span>
            <span style="font-family:'JetBrains Mono',monospace;font-size:0.73rem;
                         color:{t_clr};font-weight:700;">{t_pct}%</span>
        </div>
        <div style="background:#0a0e17;border-radius:4px;height:7px;overflow:hidden;">
            <div style="width:{t_pct}%;height:7px;background:{t_clr};border-radius:4px;"></div>
        </div>
    </div>""", unsafe_allow_html=True)

    date_str = now.strftime("%m-%d")
    closure  = check_special_closure(date_str, current_hour)
    if closure:
        st.markdown(f'<div class="alert-high">⚠️ <b>Özel Etkinlik:</b> {closure}</div>', unsafe_allow_html=True)
    elif traffic_level == "high":
        st.markdown(f'<div class="alert-high">⚠️ <b>Pik Saat</b> — {current_hour:02d}:00</div>', unsafe_allow_html=True)
    elif traffic_level == "medium":
        st.markdown('<div class="alert-medium">ℹ️ Orta yoğunluk. Dikkatli sürüş.</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-low">✅ Trafik akışkan. İdeal koşullar.</div>', unsafe_allow_html=True)

    # — Depo Konumları —
    st.markdown('<div class="section-title">🏭 SOM Depo Konumları</div>', unsafe_allow_html=True)
    for d in DEPOT_LOCATIONS:
        st.markdown(f"""
        <div class="depot-item">
            <span>🏠</span>
            <div>
                <div style="font-weight:600;font-size:0.78rem;">{d['name']}</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:0.62rem;color:#64748b;">
                    {d['lat']:.4f}, {d['lon']:.4f}</div>
            </div>
        </div>""", unsafe_allow_html=True)

    # — Sipariş Özeti —
    st.markdown('<div class="section-title">📦 Sipariş Özeti</div>', unsafe_allow_html=True)
    orders       = st.session_state.orders
    today_cnt    = sum(1 for o in orders if o["type"] == "today")
    tomorrow_cnt = sum(1 for o in orders if o["type"] == "tomorrow")
    # Gece yarısı ise yarınki siparişler bugüne geçer
    if current_hour == 0:
        today_cnt    += tomorrow_cnt
        tomorrow_cnt  = 0

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f'<div class="metric-card"><div class="metric-label">🔵 Bugün</div>'
                    f'<div class="metric-value">{today_cnt}</div></div>', unsafe_allow_html=True)
    with c2:
        st.markdown(f'<div class="metric-card"><div class="metric-label">🟢 Yarın</div>'
                    f'<div class="metric-value">{tomorrow_cnt}</div></div>', unsafe_allow_html=True)

    # — Canlı Sipariş Ekleme —
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

    if current_hour == 0:
        st.markdown("""
        <div style="background:rgba(168,85,247,0.1);border:1px solid #a855f7;
                    border-radius:8px;padding:9px;text-align:center;
                    font-size:0.78rem;color:#d8b4fe;margin-top:8px;">
            🌙 <b>Gece Yarısı Dönüşümü</b><br>Yeşil → Mavi dönüşüm aktif
        </div>""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════
#  ANA ALAN
# ════════════════════════════════════════════════════════
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
        <div style="background:#1a2235;border:1px solid #2a3a55;border-radius:6px;
                    padding:5px 12px;font-family:'JetBrains Mono',monospace;font-size:0.68rem;">
            <span style="color:#64748b;">MOD:</span>
            <span style="color:#f8fafc;font-weight:700;margin-left:5px;">{opt_mode}</span>
        </div>
        <div style="background:#1a2235;border:1px solid {t_clr};border-radius:6px;
                    padding:5px 12px;font-family:'JetBrains Mono',monospace;
                    font-size:0.68rem;color:{t_clr};">
            {t_label}
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

map_col, info_col = st.columns([3, 1])

# ─── FOLİUM HARİTASI ──────────────────────────────────
with map_col:
    m = folium.Map(
        location=[41.6772, 26.5567],
        zoom_start=13,
        tiles="CartoDB dark_matter",
        prefer_canvas=True,
    )

    # Gece yarısı dönüşümü
    display_orders = []
    for o in st.session_state.orders:
        oc = o.copy()
        if current_hour == 0 and oc["type"] == "tomorrow":
            oc["type"] = "today"
        display_orders.append(oc)

    # Depo simgeleri
    for depot in DEPOT_LOCATIONS:
        folium.Marker(
            location=[depot["lat"], depot["lon"]],
            popup=folium.Popup(
                f"<b style='color:#ef4444;'>🏠 {depot['name']}</b><br>"
                f"<small>SOM Optimizasyon Noktası</small><br>"
                f"<small>📍 {depot['lat']:.5f}, {depot['lon']:.5f}</small>",
                max_width=200,
            ),
            tooltip=f"🏠 {depot['name']}",
            icon=folium.Icon(color="red", icon="home", prefix="fa"),
        ).add_to(m)

    # Sipariş noktaları
    for order in display_orders:
        is_today  = order["type"] == "today"
        dot_clr   = "#3b82f6" if is_today else "#22c55e"
        dot_emoji = "🔵" if is_today else "🟢"
        dot_label = "Bugün" if is_today else "Yarın"

        icon_html = (f"<div style='width:13px;height:13px;background:{dot_clr};"
                     f"border-radius:50%;border:2px solid {dot_clr};"
                     f"box-shadow:0 0 7px {dot_clr}88;cursor:pointer;'></div>")

        folium.Marker(
            location=[order["lat"], order["lon"]],
            popup=folium.Popup(
                f"<b>📦 {order.get('id','?')}</b><br>"
                f"<span style='color:{dot_clr};'>● {dot_label}</span><br>"
                f"<small>📊 Hacim: {order.get('volume','?')} birim</small>",
                max_width=200,
            ),
            tooltip=f"{dot_emoji} {order.get('id','SIP')} — Tıkla",
            icon=folium.DivIcon(html=icon_html, icon_size=(13,13), icon_anchor=(6,6)),
        ).add_to(m)

    # Seçili sipariş rotası
    if st.session_state.selected_order is not None:
        sel    = st.session_state.selected_order
        depot  = find_nearest_depot(sel["lat"], sel["lon"])
        rstyle = get_route_color(traffic_level, opt_mode)

        draw_route_straight(
            m, depot=depot, order=sel, style=rstyle,
            traffic_level=traffic_level, closure=closure,
        )
        # Seçili nokta vurgu halkası
        folium.CircleMarker(
            location=[sel["lat"], sel["lon"]],
            radius=16, color=rstyle["color"],
            fill=True, fill_color=rstyle["color"],
            fill_opacity=0.25, weight=3,
        ).add_to(m)

    # Haritayı render et
    map_data = st_folium(
        m, width="100%", height=555,
        returned_objects=["last_object_clicked_tooltip"],
        key="main_map",
    )

    # Tıklama → sipariş seçimi
    if map_data and map_data.get("last_object_clicked_tooltip"):
        tt = str(map_data["last_object_clicked_tooltip"])
        for order in display_orders:
            oid = order.get("id", "")
            if oid and oid in tt:
                if st.session_state.selected_order != order:
                    st.session_state.selected_order = order
                    st.rerun()

    # Alt açıklama
    st.markdown("""
    <div style="display:flex;gap:18px;padding:8px 2px;
                font-family:'JetBrains Mono',monospace;font-size:0.68rem;color:#64748b;">
        <span><span style="color:#ef4444;">🏠</span> SOM Depo</span>
        <span><span style="color:#3b82f6;">●</span> Bugünkü Sipariş</span>
        <span><span style="color:#22c55e;">●</span> Yarınki Sipariş</span>
        <span><span style="color:#f97316;">━</span> Yoğun Güzergah</span>
        <span><span style="color:#3b82f6;">━</span> Normal Güzergah</span>
    </div>""", unsafe_allow_html=True)


# ─── SAĞ BİLGİ PANELİ ────────────────────────────────
with info_col:
    if st.session_state.selected_order:
        sel    = st.session_state.selected_order
        depot  = find_nearest_depot(sel["lat"], sel["lon"])
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
                🏠 <b style="color:#f8fafc;">{depot['name']}</b></div>
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

        if closure:
            st.markdown(f'<div class="alert-high">🎪 <b>{closure}</b> — Etkinlik uyarısı!</div>', unsafe_allow_html=True)

        if st.button("✕ Rotayı Kapat", use_container_width=True):
            st.session_state.selected_order = None
            st.rerun()
    else:
        st.markdown("""
        <div style="background:#111827;border:1px dashed #2a3a55;border-radius:10px;
                    padding:22px;text-align:center;color:#374151;margin-bottom:12px;">
            <div style="font-size:1.4rem;margin-bottom:8px;">👆</div>
            <div style="font-size:0.76rem;line-height:1.6;color:#64748b;">
                Haritada bir sipariş noktasına tıklayın rota analizini görmek için
            </div>
        </div>""", unsafe_allow_html=True)

    # Son siparişler listesi
    st.markdown('<div class="section-title">📋 Son Siparişler</div>', unsafe_allow_html=True)
    for o in display_orders[-9:][::-1]:
        is_today = o["type"] == "today"
        st.markdown(f"""
        <div class="order-item">
            <div class="{'dot-blue' if is_today else 'dot-green'}"></div>
            <div>
                <div style="font-weight:600;">{o.get('id','?')[:14]}</div>
                <div style="color:#64748b;font-size:0.62rem;">{o['lat']:.3f}, {o['lon']:.3f}</div>
            </div>
        </div>""", unsafe_allow_html=True)

# Canlı mod otomatik yenileme
if live_mode:
    time.sleep(30)
    st.rerun()