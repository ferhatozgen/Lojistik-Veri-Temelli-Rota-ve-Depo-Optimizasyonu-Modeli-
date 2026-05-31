"""
Sağ harita bileşeni.
- today/future: 00:00→23:59 sipariş animasyonu (sn=saat)
  → 08:00'dan önce hublar bekleme modunda (sol harita sabit kalır)
  → 23:59 sonrası sol haritaya aktarım hazır
- past: o günün tüm siparişleri statik (bölge bazlı istatistikler)
"""

import streamlit as st
import pandas as pd
from components.map_builder import (
    build_live_order_map,
    build_past_order_map,
    build_future_heatmap,
    map_to_html,
)

DELIVERY_START_HOUR = 8  # Kuryeler bu saatten itibaren aktif


def render_right_map(data: dict, mode: str):
    all_orders    = data.get("all_orders", pd.DataFrame())
    demand        = data.get("demand", pd.DataFrame())
    selected_date = st.session_state.selected_date

    if mode in ("today", "future"):
        orders_pool   = st.session_state.get("orders_pool", [])
        sim_hour      = st.session_state.get("sim_hour", 0)
        new_order_ids = st.session_state.get("new_order_ids", set())

        m    = build_live_order_map(orders_pool, sim_hour, new_order_ids)
        html = map_to_html(m)
        st.markdown('<div class="map-container">', unsafe_allow_html=True)
        st.iframe(html, height=430)
        st.markdown('</div>', unsafe_allow_html=True)

        if orders_pool:
            df_pool = pd.DataFrame(orders_pool)
            _render_live_stats(df_pool, sim_hour)
        else:
            _render_waiting_banner()

    elif mode == "past":
        if not all_orders.empty and "date" in all_orders.columns:
            day_orders = all_orders[all_orders["date"] == selected_date].copy()
        else:
            day_orders = pd.DataFrame()

        if day_orders.empty:
            st.markdown(_no_data_card(str(selected_date)), unsafe_allow_html=True)
            return

        m    = build_past_order_map(day_orders)
        html = map_to_html(m)
        st.markdown('<div class="map-container">', unsafe_allow_html=True)
        st.iframe(html, height=430)
        st.markdown('</div>', unsafe_allow_html=True)

        _render_past_stats(day_orders)


def _render_live_stats(df: pd.DataFrame, hour: int):
    """Canlı istatistik: toplam sipariş, en aktif bölge, kalan saat."""
    district_counts = df.groupby("district").size().reset_index(name="count") \
        if "district" in df.columns else pd.DataFrame()

    most_active = district_counts.nlargest(1, "count")["district"].values[0] \
        if not district_counts.empty else "—"
    remaining = max(0, 23 - hour)

    # Kurye durum bilgisi
    courier_status = "⏳ Hublar bekliyor (08:00)" if hour < DELIVERY_START_HOUR else "🚐 Kuryeler aktif"
    courier_color  = "#fbbf24" if hour < DELIVERY_START_HOUR else "#34d399"

    st.markdown("<div style='margin-top:8px'>", unsafe_allow_html=True)
    cols = st.columns(4)

    _stat_card(cols[0], "Toplam Sipariş", str(len(df)), "#1e40af", "#60a5fa")
    _stat_card(cols[1], "En Aktif Bölge", most_active, "#1e40af", "#60a5fa")
    _stat_card(cols[2], "Kalan Saat", str(remaining), "#1e40af", "#fbbf24")
    _stat_card(cols[3], "Kurye Durumu", courier_status.split(" ", 1)[1],
               "#1e1e1e", courier_color)

    st.markdown("</div>", unsafe_allow_html=True)


def _render_past_stats(df: pd.DataFrame):
    """Geçmiş mod: bölge sipariş dağılımı + saatlik yoğunluk."""
    if "district" not in df.columns:
        return

    with st.expander("📊 Bölge Sipariş Dağılımı", expanded=False):
        counts = df.groupby("district").size().reset_index(name="Sipariş Sayısı")
        counts.columns = ["Bölge", "Sipariş Sayısı"]
        counts = counts.sort_values("Sipariş Sayısı", ascending=False)
        st.dataframe(counts, width="stretch", hide_index=True)

    if "hour" in df.columns:
        with st.expander("⏰ Saatlik Sipariş Dağılımı", expanded=False):
            hourly = df.groupby("hour").size().reset_index(name="Sipariş")
            hourly.columns = ["Saat", "Sipariş"]
            peak = hourly.loc[hourly["Sipariş"].idxmax()]
            st.markdown(f"""
            <div style="background:#111827;border:1px solid #1f2937;border-radius:6px;
                        padding:8px 12px;margin-bottom:8px;font-size:0.72rem;
                        color:#fbbf24;font-family:'IBM Plex Mono',monospace;">
              🔝 Pik saat: {int(peak['Saat']):02d}:00 — {int(peak['Sipariş'])} sipariş
            </div>
            """, unsafe_allow_html=True)
            st.dataframe(hourly, width="stretch", hide_index=True)


def _render_waiting_banner():
    st.markdown("""
    <div style="background:#111827;border:1px solid #1f2937;border-radius:8px;
                padding:12px 16px;margin-top:8px;font-size:0.75rem;
                color:#475569;font-family:'IBM Plex Mono',monospace;text-align:center;">
      ▶ Simülasyonu başlatmak için sol paneldeki butonu kullanın
    </div>
    """, unsafe_allow_html=True)


def _stat_card(col, label: str, value: str, border_color: str, val_color: str):
    with col:
        st.markdown(f"""
        <div style="background:#111827;border:1px solid {border_color};border-radius:8px;
                    padding:10px;text-align:center;">
          <div style="font-size:0.62rem;color:#475569;font-family:'IBM Plex Mono',monospace;
                      text-transform:uppercase;">{label}</div>
          <div style="font-size:1.1rem;font-weight:600;color:{val_color};
                      font-family:'IBM Plex Mono',monospace;">{value}</div>
        </div>
        """, unsafe_allow_html=True)


def _no_data_card(date_str: str) -> str:
    return f"""
    <div class="map-container" style="height:430px;display:flex;align-items:center;
         justify-content:center;background:#0d1117;border:1px solid #1f2937;border-radius:8px;">
      <div style="text-align:center;color:#475569;font-family:'IBM Plex Mono',monospace;">
        <div style="font-size:2rem;margin-bottom:12px;">📭</div>
        <div style="font-size:0.85rem;">{date_str} için veri bulunamadı</div>
      </div>
    </div>
    """