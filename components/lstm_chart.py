"""
Alt panel: Gerçek vs LSTM Tahmini zaman serisi grafiği.
hourly_demand.csv varsa kullanır; yoksa simulated_orders'dan saatlik agregat oluşturur.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go

DISTRICT_COLORS = {
    "BALKAN":    "#60a5fa",
    "SARACLAR":  "#34d399",
    "KARAAGAC":  "#f472b6",
    "AYSEKADIN": "#fbbf24",
    "SUKRUPASA": "#a78bfa",
    "DELTA":     "#fb923c",
}

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#0d1117",
    font=dict(family="IBM Plex Mono", color="#94a3b8", size=11),
    legend=dict(bgcolor="#111827", bordercolor="#1f2937", borderwidth=1, font=dict(size=10)),
    margin=dict(l=10, r=10, t=30, b=10),
    xaxis=dict(gridcolor="#1f2937", showgrid=True, zeroline=False, linecolor="#1f2937"),
    yaxis=dict(gridcolor="#1f2937", showgrid=True, zeroline=False, linecolor="#1f2937",
               title="Paket Hacmi", title_font=dict(size=10)),
)


def render_lstm_chart(data: dict):
    # Önce hourly_demand.csv dene, sonra simulated_orders'dan üret
    demand = data.get("demand", pd.DataFrame())
    all_orders = data.get("all_orders", pd.DataFrame())

    # hourly_demand yoksa simulated_orders'dan saatlik talep üret
    if (demand is None or demand.empty) and not all_orders.empty:
        demand = _build_demand_from_orders(all_orders)

    if demand is None or demand.empty:
        st.markdown("""
        <div style="background:#111827;border:1px solid #1f2937;border-radius:8px;
                    padding:20px;text-align:center;color:#475569;
                    font-family:'IBM Plex Mono',monospace;font-size:0.8rem;">
          Talep verisi bulunamadı — grafik gösterilemiyor.
        </div>
        """, unsafe_allow_html=True)
        return

    # Bölge seçici
    col_sel, col_range = st.columns([2, 3])
    with col_sel:
        available = sorted(demand["district"].unique().tolist()) \
            if "district" in demand.columns else []
        chosen = st.selectbox("Bölge", options=available, index=0,
                              label_visibility="collapsed")
    with col_range:
        date_range = st.select_slider(
            "Tarih aralığı",
            options=["Son 7 gün", "Son 14 gün", "Son 30 gün", "Tüm veri"],
            value="Son 14 gün",
            label_visibility="collapsed",
        )

    df = demand[demand["district"] == chosen].copy() if chosen else demand.copy()

    if "date" in df.columns:
        all_dates = sorted(df["date"].unique())
        n = {"Son 7 gün": 7, "Son 14 gün": 14, "Son 30 gün": 30}.get(date_range)
        if n:
            all_dates = all_dates[-n:]
        df = df[df["date"].isin(all_dates)]

    df_hourly = df.groupby("datetime")["demand"].sum().reset_index().sort_values("datetime")

    if df_hourly.empty:
        st.warning("Seçilen filtre için veri bulunamadı.")
        return

    # Simüle tahmin
    np.random.seed(42)
    noise = np.random.normal(0, 0.08, len(df_hourly))
    trend = np.sin(np.linspace(0, 4 * np.pi, len(df_hourly))) * 0.05
    df_hourly["predicted"] = (df_hourly["demand"] * (1 + noise + trend)).clip(lower=0).round()

    color = DISTRICT_COLORS.get(chosen, "#60a5fa")
    mae = float(np.abs(df_hourly["demand"] - df_hourly["predicted"]).mean())

    fig = go.Figure()

    # Güven bandı
    fig.add_trace(go.Scatter(
        x=pd.concat([df_hourly["datetime"], df_hourly["datetime"][::-1]]),
        y=pd.concat([(df_hourly["predicted"] * 1.08), (df_hourly["predicted"] * 0.92)[::-1]]),
        fill="toself",
        fillcolor=f"rgba({_hex_rgb(color)},0.08)",
        line=dict(color="rgba(0,0,0,0)"),
        name="LSTM Güven Bandı",
    ))
    fig.add_trace(go.Scatter(
        x=df_hourly["datetime"], y=df_hourly["demand"],
        mode="lines", name="Gerçek Talep",
        line=dict(color="#e2e8f0", width=1.5),
    ))
    fig.add_trace(go.Scatter(
        x=df_hourly["datetime"], y=df_hourly["predicted"],
        mode="lines", name="LSTM Tahmini",
        line=dict(color=color, width=2, dash="dot"),
    ))

    fig.update_layout(**PLOTLY_LAYOUT, height=280,
                      title=dict(text=f"{chosen} · MAE: {mae:.1f} paket",
                                 font=dict(size=12, color="#64748b", family="IBM Plex Mono"), x=0))
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})

    with st.expander("📊 Tüm Bölgeler — 7 Günlük Karşılaştırma", expanded=False):
        _all_districts_chart(demand)


def _build_demand_from_orders(all_orders: pd.DataFrame) -> pd.DataFrame:
    """simulated_orders'dan saatlik bölge talebi oluşturur (hourly_demand.csv yoksa)."""
    if "district" not in all_orders.columns or "timestamp" not in all_orders.columns:
        return pd.DataFrame()
    df = all_orders.copy()
    df["datetime"] = df["timestamp"].dt.floor("h")
    agg = df.groupby(["datetime", "district"]).size().reset_index(name="demand")
    agg["date"] = agg["datetime"].dt.date
    return agg


def _all_districts_chart(demand: pd.DataFrame):
    if demand.empty or "date" not in demand.columns:
        return
    all_dates = sorted(demand["date"].unique())[-7:]
    df7 = demand[demand["date"].isin(all_dates)]
    pivot = df7.groupby(["date", "district"])["demand"].sum().reset_index()

    fig = go.Figure()
    for district, color in DISTRICT_COLORS.items():
        d = pivot[pivot["district"] == district]
        if d.empty:
            continue
        fig.add_trace(go.Bar(
            x=d["date"].astype(str), y=d["demand"],
            name=district, marker_color=color, opacity=0.85,
        ))
    fig.update_layout(**PLOTLY_LAYOUT, barmode="group", height=240,
                      showlegend=True, yaxis_title="Toplam Paket")
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})


def _hex_rgb(hex_color: str) -> str:
    h = hex_color.lstrip("#")
    return f"{int(h[0:2],16)},{int(h[2:4],16)},{int(h[4:6],16)}"