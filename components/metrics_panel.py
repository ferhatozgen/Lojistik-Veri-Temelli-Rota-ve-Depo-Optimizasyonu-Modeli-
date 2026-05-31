"""
Üst metrik şeridi — 6 kart.
Hub sayısı artık K-Means ile dinamik hesaplanmış değer.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date


def render_metrics(data: dict, mode: str):
    metrics    = data.get("metrics", {})
    hub_cap    = st.session_state.get("hub_capacity", 200)
    n_couriers = st.session_state.get("chosen_couriers", 4)
    all_orders = data.get("all_orders", pd.DataFrame())
    selected_date = st.session_state.selected_date

    silhouette  = metrics.get("silhouette_score", 0.53)
    fuel_saving = metrics.get("fuel_saving_pct", 22.0)
    carbon_red  = metrics.get("carbon_reduction", 18.5)

    cols = st.columns(6)

    if mode == "past":
        day_orders = all_orders[all_orders["date"] == selected_date] \
            if not all_orders.empty and "date" in all_orders.columns else pd.DataFrame()
        day_total = len(day_orders)

        # K-Means hub sayısı (200 sabit kapasite)
        hub_n = max(1, int(np.ceil(day_total / 200))) if day_total > 0 else 0

        _metric(cols[0], "O Günün Siparişi",  f"{day_total:,}",            "Gerçekleşen veri",    "#e2e8f0")
        _metric(cols[1], "Aktif Hub",          str(hub_n),                  "K-Means hesabı",      "#f43f5e")
        _metric(cols[2], "Ortalama Yük",       f"{round(day_total/max(hub_n,1),1)}", "Sipariş/hub","#34d399")
        _metric(cols[3], "Silhouette Score",   f"{silhouette:.2f}",         "K-Means kalitesi",    "#60a5fa")
        _metric(cols[4], "Yakıt Tasarrufu",    f"%{fuel_saving:.0f}",       "Optimize rota",       "#fbbf24")
        _metric(cols[5], "Karbon Azaltımı",    f"%{carbon_red:.0f}",        "Emisyon düşüşü",      "#a78bfa")

    else:
        total_orders = metrics.get("total_orders", 0)
        # Gerçek hub sayısı: _computed_hubs varsa ondan al, yoksa formülle hesapla
        _ch = st.session_state.get("_computed_hubs")
        if _ch is not None and not _ch.empty:
            n_hubs = len(_ch)
        else:
            n_hubs = max(1, int(np.ceil(total_orders / hub_cap)))
        total_couriers = n_couriers * n_hubs

        _metric(cols[0], "Toplam Sipariş",  f"{total_orders:,}",     "simulated_orders.csv", "#e2e8f0")
        _metric(cols[1], "Aktif Hub",        str(n_hubs),             f"Kapasite: {hub_cap}", "#f43f5e")
        _metric(cols[2], "Sahada Kurye",     str(total_couriers),     f"{n_couriers}/hub",    "#34d399")
        _metric(cols[3], "Silhouette Score", f"{silhouette:.2f}",     "K-Means kalitesi",     "#60a5fa")
        _metric(cols[4], "Yakıt Tasarrufu",  f"%{fuel_saving:.0f}",   "Optimize rota kazanımı","#fbbf24")
        _metric(cols[5], "Karbon Azaltımı",  f"%{carbon_red:.0f}",    "Emisyon düşüşü",       "#a78bfa")


def _metric(col, label: str, value: str, sublabel: str, color: str):
    with col:
        st.markdown(f"""
        <div style="background:#111827;border:1px solid #1f2937;border-radius:10px;
                    padding:14px 16px;">
          <div style="font-size:0.65rem;color:#475569;font-family:'IBM Plex Mono',monospace;
                      text-transform:uppercase;letter-spacing:0.8px;margin-bottom:6px;">
            {label}
          </div>
          <div style="font-size:1.65rem;font-weight:600;color:{color};
                      font-family:'IBM Plex Mono',monospace;line-height:1;">
            {value}
          </div>
          <div style="font-size:0.65rem;color:#334155;margin-top:5px;
                      font-family:'IBM Plex Sans',sans-serif;">
            {sublabel}
          </div>
        </div>
        """, unsafe_allow_html=True)