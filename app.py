"""
Edirne Lojistik Yönetim Sistemi — Ana Uygulama
================================================
Simülasyon mantığı:

SAĞ HARİTA (Canlı Sipariş Akışı):
  - 00:00'dan başlar, her adım 1 saati temsil eder
  - Hız panelden seçilir (1/2/3 saniye = 1 saat)
  - Yeni düşen siparişler yanıp söner, eskiler sabit kalır
  - 24 saat tamamlanınca simülasyon durur

SOL HARİTA (Hub + Rotalar):
  - today/future modunda: panelden seçilen KAPASİTE + KURYE değerine göre
    önceki günün siparişlerinden K-Means ile hub hesaplanır
  - Kapasite değişince hub'lar otomatik yeniden hesaplanır
  - 08:00'dan önce kuryeler BEKLEMEDE gösterilir
  - Simülasyon bittikten sonra (23:59) kullanıcı "Güncelle" butonuyla
    sol haritayı sağdan gelen o günün siparişlerine güncelleyebilir

GEÇMİŞ MOD:
  - Sadece gözlem: o günün siparişleri statik, K-Means 200 sabit kapasite
  - Hiçbir slider/kontrol gösterilmez

GELECEk MOD:
  - LSTM ısı haritası (demand.csv gerekir) ya da bölge yoğunluk gösterimi
"""

import streamlit as st
import pandas as pd
import time
from datetime import date, timedelta

# Komponent importları
from components.sidebar       import render_sidebar
from components.left_map      import render_left_map
from components.right_map     import render_right_map
from components.metrics_panel import render_metrics
from components.lstm_chart    import render_lstm_chart
from components.data_loader   import load_all_data, get_orders_for_date

# ── Sayfa Ayarları ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Edirne Lojistik Yönetim Sistemi",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

  html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
  .stApp { background-color: #0d1117; }

  [data-testid="stSidebar"] {
    background: #111827;
    border-right: 1px solid #1f2937;
  }

  .main-header {
    background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
    border: 1px solid #1e3a4a;
    border-radius: 12px;
    padding: 20px 28px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  .main-header h1 {
    font-family: 'IBM Plex Mono', monospace;
    color: #e2e8f0;
    font-size: 1.4rem;
    font-weight: 600;
    margin: 0;
    letter-spacing: -0.5px;
  }
  .main-header .subtitle {
    color: #64748b;
    font-size: 0.8rem;
    font-family: 'IBM Plex Mono', monospace;
    margin-top: 4px;
  }

  .mode-badge {
    display: inline-block;
    padding: 5px 14px;
    border-radius: 20px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-transform: uppercase;
  }
  .mode-past   { background: #1a2e1a; color: #4ade80; border: 1px solid #166534; }
  .mode-today  { background: #1a1a2e; color: #60a5fa; border: 1px solid #1e40af; }
  .mode-future { background: #2e1a2e; color: #c084fc; border: 1px solid #7e22ce; }

  .map-header {
    background: #111827;
    border: 1px solid #1f2937;
    border-radius: 8px 8px 0 0;
    padding: 10px 16px;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .map-header .dot { width: 8px; height: 8px; border-radius: 50%; }
  .dot-green  { background: #4ade80; box-shadow: 0 0 6px #4ade80; }
  .dot-blue   { background: #60a5fa; box-shadow: 0 0 6px #60a5fa; }
  .dot-purple { background: #c084fc; box-shadow: 0 0 6px #c084fc; }
  .dot-red    { background: #f43f5e; box-shadow: 0 0 6px #f43f5e; }
  .map-header span {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    color: #94a3b8;
    font-weight: 600;
  }
  .map-header .map-info {
    margin-left: auto;
    font-size: 0.7rem;
    color: #475569;
  }

  .sim-clock {
    background: #0d1117;
    border: 1px solid #1f2937;
    border-radius: 8px;
    padding: 8px 14px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.1rem;
    font-weight: 600;
    color: #60a5fa;
    text-align: center;
    letter-spacing: 2px;
  }

  .map-container {
    border: 1px solid #1f2937;
    border-top: none;
    border-radius: 0 0 8px 8px;
    overflow: hidden;
  }

  ::-webkit-scrollbar { width: 4px; }
  ::-webkit-scrollbar-track { background: #0d1117; }
  ::-webkit-scrollbar-thumb { background: #1f2937; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# ── Session State ──────────────────────────────────────────────────────────────
def init_state():
    defaults = {
        "hub_capacity":      200,
        "vehicle_capacity":  20,
        "chosen_couriers":   4,
        "selected_date":     date.today(),
        "mode":              "today",
        "sim_hour":          0,
        "sim_running":       False,
        "sim_speed":         2,          # saniye (panelden değiştirilir)
        "sim_done":          False,       # 23:59 tamamlandı mı
        "orders_pool":       [],
        "new_order_ids":     set(),
        "prev_pool_ids":     set(),
        "_last_hub_cap":     None,
        "_computed_hubs":    None,
        "_computed_prev_orders": None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

SIM_TODAY = date.today()

# ── Veri Yükleme ───────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Veriler yükleniyor...")
def get_data(hub_capacity: int) -> dict:
    return load_all_data(hub_capacity=hub_capacity)

data = get_data(st.session_state.hub_capacity)

# ── Mod Belirleme ──────────────────────────────────────────────────────────────
sel_date = st.session_state.selected_date
if sel_date < SIM_TODAY:
    mode = "past"
elif sel_date == SIM_TODAY:
    mode = "today"
else:
    mode = "future"
st.session_state.mode = mode

# ── Sidebar ────────────────────────────────────────────────────────────────────
render_sidebar(data)
# sidebar selected_date'i güncelleyebilir, mode'u yeniden belirle
sel_date = st.session_state.selected_date
if sel_date < SIM_TODAY:
    mode = "past"
elif sel_date == SIM_TODAY:
    mode = "today"
else:
    mode = "future"
st.session_state.mode = mode

# ── Başlık ─────────────────────────────────────────────────────────────────────
mode_labels = {
    "past":   ("GEÇMİŞ ANALİZ MODU",     "mode-past"),
    "today":  ("CANLI OPERASYON MODU",    "mode-today"),
    "future": ("STRATEJİK PLANLAMA MODU", "mode-future"),
}
mode_label, mode_class = mode_labels[mode]
selected_str = sel_date.strftime("%d %B %Y")

st.markdown(f"""
<div class="main-header">
  <div>
    <h1>📦 Edirne Lojistik Yönetim Sistemi</h1>
    <div class="subtitle">
      Dijital İkiz Simülasyonu · Cross-Docking & Dinamik Mikro-Dağıtım · {selected_str}
    </div>
  </div>
  <span class="mode-badge {mode_class}">{mode_label}</span>
</div>
""", unsafe_allow_html=True)

# ── Üst Metrikler ──────────────────────────────────────────────────────────────
render_metrics(data, mode)
st.markdown("<hr style='margin: 18px 0; border-color:#1f2937'>", unsafe_allow_html=True)

# ── Çift Harita Paneli ─────────────────────────────────────────────────────────
col_left, col_right = st.columns(2, gap="medium")

with col_left:
    # Sol başlık
    if mode == "past":
        dot_cls = "dot-green"; title = "TARİHSEL DAĞITIM ANALİZİ"
        info = f"{selected_str} · Tamamlanan rotalar"
    elif mode == "today":
        dot_cls = "dot-red";   title = "AKTİF DAĞITIM — KURYE ROTALARI"
        _ch = st.session_state.get("_computed_hubs")
        hub_n = len(_ch) if _ch is not None and not (hasattr(_ch, "empty") and _ch.empty) else 0
        info = f"Önceki gün siparişleri · {st.session_state.get('chosen_couriers',4)} kurye/hub"
    else:
        dot_cls = "dot-purple"; title = "LSTM TAHMİN HARİTASI"
        info = "Bölgesel yoğunluk tahmini"

    st.markdown(f"""
    <div class="map-header">
      <div class="dot {dot_cls}"></div>
      <span>{title}</span>
      <span class="map-info">{info}</span>
    </div>""", unsafe_allow_html=True)

    render_left_map(data, mode)

with col_right:
    # Sağ başlık
    if mode == "today":
        sim_h = st.session_state.sim_hour
        dot_cls2 = "dot-blue"; title2 = "CANLI SİPARİŞ HAVUZU"
        info2 = f"Simülasyon saati: {sim_h:02d}:00"
    elif mode == "past":
        dot_cls2 = "dot-green"; title2 = "O GÜN SİPARİŞ DAĞILIMI"
        info2 = "Kesinleşmiş sipariş noktaları"
    else:
        dot_cls2 = "dot-purple"; title2 = "TAHMİN YOĞUNLUK HARİTASI"
        info2 = "LSTM çıktısı · Bölge bazlı"

    st.markdown(f"""
    <div class="map-header">
      <div class="dot {dot_cls2}"></div>
      <span>{title2}</span>
      <span class="map-info">{info2}</span>
    </div>""", unsafe_allow_html=True)

    render_right_map(data, mode)

st.markdown("<hr style='margin: 18px 0; border-color:#1f2937'>", unsafe_allow_html=True)

# ── Simülasyon Kontrolleri (today ve future modunda) ───────────────────────────
if mode in ("today", "future"):
    ctrl1, ctrl2, ctrl3, ctrl4, ctrl5 = st.columns([1, 1, 1, 1, 3])

    with ctrl1:
        if st.button("▶ Başlat", width="stretch",
                     disabled=st.session_state.sim_running):
            # Seçilen günün siparişlerini yükle
            day_orders_df = get_orders_for_date(sel_date)
            # Bugün için veri yoksa (simülasyon verisi geçmişe kadar) son mevcut güne fallback
            if day_orders_df.empty:
                all_o = data.get("all_orders", pd.DataFrame())
                if not all_o.empty and "date" in all_o.columns:
                    last_date = all_o["date"].max()
                    day_orders_df = all_o[all_o["date"] == last_date].copy()
            st.session_state._sim_day_orders = day_orders_df.to_dict("records") \
                if not day_orders_df.empty else []
            st.session_state.sim_running    = True
            st.session_state.sim_done       = False
            st.session_state.sim_hour       = 0
            st.session_state.orders_pool    = []
            st.session_state.new_order_ids  = set()
            st.session_state.prev_pool_ids  = set()
            st.rerun()

    with ctrl2:
        if st.button("⏸ Durdur", width="stretch",
                     disabled=not st.session_state.sim_running):
            st.session_state.sim_running = False
            st.rerun()

    with ctrl3:
        if st.button("↺ Sıfırla", width="stretch"):
            st.session_state.sim_running    = False
            st.session_state.sim_done       = False
            st.session_state.sim_hour       = 0
            st.session_state.orders_pool    = []
            st.session_state.new_order_ids  = set()
            st.session_state.prev_pool_ids  = set()
            st.session_state._sim_day_orders = []
            st.rerun()

    with ctrl4:
        # Sol haritayı güncelle (simülasyon bittikten sonra aktif)
        sim_done = st.session_state.get("sim_done", False)
        if st.button("🔄 Sol Haritayı Güncelle", width="stretch",
                     disabled=not sim_done,
                     help="Günün siparişleri tamamlandıktan sonra sol haritayı günceller"):
            # Bugünün siparişlerine göre hub'ları yeniden hesapla → sol haritaya aktar
            hub_cap  = st.session_state.hub_capacity
            pool_df  = pd.DataFrame(st.session_state.orders_pool)
            if not pool_df.empty:
                from components.data_loader import compute_hubs_for_orders
                new_hubs, new_orders = compute_hubs_for_orders(pool_df, hub_cap)
                st.session_state["_computed_hubs"]         = new_hubs
                st.session_state["_computed_prev_orders"]  = new_orders
                st.session_state["_last_hub_cap"]          = hub_cap
                st.success(f"Sol harita güncellendi! {len(new_hubs)} hub hesaplandı.")

    with ctrl5:
        progress_pct = int((st.session_state.sim_hour / 23) * 100) \
            if st.session_state.sim_hour > 0 else 0
        status_txt = "▶ ÇALIŞIYOR" if st.session_state.sim_running else \
                     ("✓ TAMAMLANDI" if st.session_state.sim_done else "⏸ BEKLIYOR")
        # Hangi günün simüle edildiğini göster
        sim_orders = st.session_state.get("_sim_day_orders", [])
        if sim_orders:
            import pandas as _pd2
            _df_s = _pd2.DataFrame(sim_orders[:1])
            sim_date_str = str(_df_s["timestamp"].iloc[0])[:10] if "timestamp" in _df_s.columns else str(sel_date)
        else:
            sim_date_str = str(sel_date)
        st.markdown(f"""
        <div class="sim-clock">
          ⏱ {st.session_state.sim_hour:02d}:00 — {progress_pct}%&nbsp; {status_txt}&nbsp;·&nbsp;{sim_date_str}
        </div>
        """, unsafe_allow_html=True)

    # ── Animasyon Döngüsü ──────────────────────────────────────────────────────
    if st.session_state.sim_running and st.session_state.sim_hour < 24:
        speed = st.session_state.get("sim_speed", 2)
        time.sleep(speed)

        next_hour = st.session_state.sim_hour + 1

        # Günün tüm siparişleri session'da (başlat butonunda yüklendi)
        all_day = st.session_state.get("_sim_day_orders", [])

        if all_day:
            filtered = [r for r in all_day if r.get("hour", 0) <= next_hour]
            prev_ids = st.session_state.prev_pool_ids
            all_ids  = {str(r.get("order_id", "")) for r in filtered}
            new_ids  = all_ids - prev_ids

            st.session_state.orders_pool    = filtered
            st.session_state.new_order_ids  = new_ids
            st.session_state.prev_pool_ids  = all_ids

        st.session_state.sim_hour = next_hour

        if next_hour >= 24:
            st.session_state.sim_running   = False
            st.session_state.sim_done      = True
            st.session_state.new_order_ids = set()
            st.success("✓ 24 saatlik simülasyon tamamlandı! '🔄 Sol Haritayı Güncelle' butonu ile sol haritayı güncelleyebilirsiniz.")

        st.rerun()

st.markdown("<hr style='margin: 18px 0; border-color:#1f2937'>", unsafe_allow_html=True)

# ── Alt Panel: LSTM Grafiği ────────────────────────────────────────────────────
st.markdown("""
<div style="font-family:'IBM Plex Mono',monospace; font-size:0.78rem; color:#475569;
            text-transform:uppercase; letter-spacing:1px; margin-bottom:14px;">
  📊 Yapay Zeka Doğrulama — Gerçek vs LSTM Tahmini
</div>
""", unsafe_allow_html=True)

render_lstm_chart(data)