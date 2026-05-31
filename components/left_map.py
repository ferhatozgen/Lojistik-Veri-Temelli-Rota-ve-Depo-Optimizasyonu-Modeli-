"""
Sol harita bileşeni.
- past   : o günün siparişlerine göre K-Means hub + kurye rotaları (salt-okunur)
- today  : önceki günün siparişlerine göre hesaplanan hub + rotalar (slider'a duyarlı)
- future : LSTM bölge ısı haritası
"""

import streamlit as st
import pandas as pd
from components.map_builder import (
    build_route_map,
    build_past_route_map,
    build_future_heatmap,
    map_to_html,
    _base_map,
    _add_hubs,
    _add_orders_static,
    _add_district_labels,
)
from components.data_loader import compute_hubs_for_orders, reload_hubs


def render_left_map(data: dict, mode: str):
    demand      = data.get("demand", pd.DataFrame())
    all_orders  = data.get("all_orders", pd.DataFrame())
    selected_date = st.session_state.selected_date
    hub_capacity  = st.session_state.get("hub_capacity", 200)
    n_couriers    = st.session_state.get("chosen_couriers", 4)

    if mode == "future":
        m = build_future_heatmap(demand, selected_date)

    elif mode == "past":
        # O günün siparişleri → K-Means hub → rotalar
        if not all_orders.empty and "date" in all_orders.columns:
            day_orders = all_orders[all_orders["date"] == selected_date].copy()
        else:
            day_orders = pd.DataFrame()

        if day_orders.empty:
            st.markdown(_no_data_card(str(selected_date)), unsafe_allow_html=True)
            return

        hubs, day_orders_with_hub = compute_hubs_for_orders(day_orders, hub_capacity=200)
        m = build_past_route_map(hubs, day_orders_with_hub, n_couriers=4)

    else:
        # Today: önceki günün siparişlerine göre hub → slider'dan alınan kapasite
        hubs  = data.get("hubs", pd.DataFrame())
        prev_orders = data.get("prev_orders", pd.DataFrame())

        # Kapasite değişince hub'ları yeniden hesapla
        cached_cap = st.session_state.get("_last_hub_cap", None)
        if cached_cap != hub_capacity or hubs.empty:
            from datetime import timedelta
            prev_date = selected_date - timedelta(days=1)
            hubs, prev_orders = reload_hubs(all_orders, prev_date, hub_capacity)
            # prev_orders boşsa today'in kendisini kullan
            if prev_orders.empty:
                hubs, prev_orders = compute_hubs_for_orders(
                    all_orders[all_orders["date"] == selected_date].copy() if not all_orders.empty else pd.DataFrame(),
                    hub_capacity
                )
            st.session_state["_last_hub_cap"] = hub_capacity
            st.session_state["_computed_hubs"] = hubs
            st.session_state["_computed_prev_orders"] = prev_orders
        else:
            hubs        = st.session_state.get("_computed_hubs", hubs)
            prev_orders = st.session_state.get("_computed_prev_orders", prev_orders)

        sim_hour    = st.session_state.get("sim_hour", 0)
        sim_started = st.session_state.get("sim_running", False) or sim_hour > 0

        if not sim_started:
            # Simülasyon başlamadı: sadece hub marker'ları
            m = _base_map()
            if not hubs.empty:
                _add_hubs(m, hubs)
            else:
                _add_district_labels(m)
        elif sim_hour < 8:
            # 00:00–07:59: hub görünür, kuryeler henüz yolda değil
            m = _base_map()
            if not hubs.empty:
                _add_hubs(m, hubs)
            if not prev_orders.empty:
                _add_orders_static(m, prev_orders, alpha=0.4)
        else:
            # 08:00+: hub + kurye rotaları
            m = build_route_map(hubs, prev_orders, n_couriers=max(4, n_couriers))

    html = map_to_html(m)
    st.markdown('<div class="map-container">', unsafe_allow_html=True)
    st.iframe(html, height=430)
    st.markdown('</div>', unsafe_allow_html=True)

    # Hub tablosu
    hubs_to_show = st.session_state.get("_computed_hubs", data.get("hubs", pd.DataFrame())) \
        if mode == "today" else \
        (_get_past_hubs(all_orders, selected_date) if mode == "past" else pd.DataFrame())

    if not hubs_to_show.empty and mode != "future":
        with st.expander(f"🏭 Hub Koordinatları ({len(hubs_to_show)} adet)", expanded=False):
            display = hubs_to_show.copy()
            display.columns = ["Hub ID", "Enlem", "Boylam"]
            display["Enlem"]  = display["Enlem"].round(5)
            display["Boylam"] = display["Boylam"].round(5)
            st.dataframe(display, width="stretch", hide_index=True)

    if mode == "today":
        n_c = st.session_state.get("chosen_couriers", 4)
        hub_n = len(st.session_state.get("_computed_hubs", data.get("hubs", pd.DataFrame())))
        st.markdown(f"""
        <div style="background:#111827;border:1px solid #1f2937;border-radius:6px;
                    padding:8px 12px;margin-top:6px;font-size:0.7rem;
                    color:#475569;font-family:'IBM Plex Mono',monospace;">
          ⚡ {hub_n} hub · {n_c} kurye/hub · 1.3× yol eğrilik katsayısı · En-yakın-komşu rota
        </div>
        """, unsafe_allow_html=True)


def _get_past_hubs(all_orders: pd.DataFrame, target_date) -> pd.DataFrame:
    """Geçmiş mod için o günün hub'larını hesaplar (200 sabit kapasite)."""
    from components.data_loader import compute_hubs_for_orders
    if all_orders.empty or "date" not in all_orders.columns:
        return pd.DataFrame(columns=["hub_id", "lat", "lon"])
    day = all_orders[all_orders["date"] == target_date]
    if day.empty:
        return pd.DataFrame(columns=["hub_id", "lat", "lon"])
    hubs, _ = compute_hubs_for_orders(day, hub_capacity=200)
    return hubs


def _no_data_card(date_str: str) -> str:
    return f"""
    <div class="map-container" style="height:430px;display:flex;align-items:center;
         justify-content:center;background:#0d1117;border:1px solid #1f2937;border-radius:8px;">
      <div style="text-align:center;color:#475569;font-family:'IBM Plex Mono',monospace;">
        <div style="font-size:2rem;margin-bottom:12px;">📭</div>
        <div style="font-size:0.85rem;">{date_str} için veri bulunamadı</div>
        <div style="font-size:0.7rem;margin-top:6px;color:#334155;">
          Mevcut aralık: 2025-01-01 → 2026-05-30
        </div>
      </div>
    </div>
    """