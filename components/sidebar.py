"""
Sol yönetici kontrol paneli.
- Tarih seçici (past / today / future modu)
- Hub kapasitesi + kurye sayısı (SADECE today/future modunda)
- Geçmiş modda: salt-okunur özet kart
- Bölge filtresi KALDIRILDI
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta

SIM_TODAY = date(2026, 5, 6)
DATA_MIN  = date(2025, 1, 1)
DATA_MAX  = date(2026, 5, 30)


def render_sidebar(data: dict):
    with st.sidebar:
        # ── Logo ──────────────────────────────────────────────────────────────
        st.markdown("""
        <div style="padding:16px 0 24px 0;border-bottom:1px solid #1f2937;margin-bottom:20px;">
          <div style="font-family:'IBM Plex Mono',monospace;font-size:1.1rem;
                      font-weight:600;color:#e2e8f0;letter-spacing:-0.5px;">
            📦 ELY Sistemi
          </div>
          <div style="font-size:0.7rem;color:#475569;margin-top:3px;
                      font-family:'IBM Plex Mono',monospace;">
            Edirne Lojistik Yönetim
          </div>
        </div>
        """, unsafe_allow_html=True)

        # ── Tarih Seçici ──────────────────────────────────────────────────────
        st.markdown(_label("📅 Simülasyon Tarihi"), unsafe_allow_html=True)

        selected = st.date_input(
            label="Tarih",
            value=st.session_state.selected_date,
            min_value=DATA_MIN,
            max_value=DATA_MAX,
            label_visibility="collapsed",
        )
        st.session_state.selected_date = selected

        # Mod belirleme
        if selected < SIM_TODAY:
            mode = "past"
        elif selected == SIM_TODAY:
            mode = "today"
        else:
            mode = "future"

        st.session_state.mode = mode

        # Mod etiketi
        if mode == "past":
            st.markdown("""
            <div style="background:#1a2e1a;border:1px solid #166534;border-radius:6px;
                        padding:8px 12px;margin:8px 0 20px 0;font-size:0.72rem;
                        color:#4ade80;font-family:'IBM Plex Mono',monospace;">
              ✓ Raporlama Modu — Gerçek veriler
            </div>""", unsafe_allow_html=True)
        elif mode == "today":
            st.markdown("""
            <div style="background:#1a1a2e;border:1px solid #1e40af;border-radius:6px;
                        padding:8px 12px;margin:8px 0 20px 0;font-size:0.72rem;
                        color:#60a5fa;font-family:'IBM Plex Mono',monospace;">
              ● Canlı Operasyon Modu
            </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="background:#2e1a2e;border:1px solid #7e22ce;border-radius:6px;
                        padding:8px 12px;margin:8px 0 20px 0;font-size:0.72rem;
                        color:#c084fc;font-family:'IBM Plex Mono',monospace;">
              ◆ Stratejik Planlama Modu — LSTM tahmini
            </div>""", unsafe_allow_html=True)

        st.markdown("<hr style='border-color:#1f2937;margin:4px 0 16px'>", unsafe_allow_html=True)

        # ── Kapasite / Kurye: sadece today ve future ─────────────────────────
        if mode != "past":
            _render_capacity_controls(data)
        else:
            _render_past_info(data, selected)

        st.markdown("<hr style='border-color:#1f2937;margin:4px 0 16px'>", unsafe_allow_html=True)

        # ── Hız Kontrolü (sadece today/future modunda simülasyon için) ────────
        if mode != "past":
            st.markdown(_label("⚡ Simülasyon Hızı"), unsafe_allow_html=True)
            speed_label = st.select_slider(
                "Hız",
                options=["Yavaş (3sn)", "Normal (2sn)", "Hızlı (1sn)"],
                value="Normal (2sn)",
                label_visibility="collapsed",
            )
            speed_map = {"Yavaş (3sn)": 3, "Normal (2sn)": 2, "Hızlı (1sn)": 1}
            st.session_state.sim_speed = speed_map[speed_label]
            st.markdown("<hr style='border-color:#1f2937;margin:4px 0 16px'>", unsafe_allow_html=True)

        # ── Silhouette Badge ──────────────────────────────────────────────────
        st.markdown("""
        <div style="background:linear-gradient(135deg,#1a1a2e,#16213e);
                    border:1px solid #1e40af;border-radius:8px;padding:12px;margin-bottom:8px;">
          <div style="font-size:0.65rem;color:#475569;font-family:'IBM Plex Mono',monospace;
                      text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">
            K-Means Silhouette Score
          </div>
          <div style="font-size:1.6rem;font-weight:600;color:#60a5fa;
                      font-family:'IBM Plex Mono',monospace;">0.53</div>
          <div style="font-size:0.65rem;color:#3b82f6;margin-top:2px;
                      font-family:'IBM Plex Mono',monospace;">
            ✓ Lojistik literatürü için başarılı
          </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div style="margin-top:24px;font-size:0.65rem;color:#334155;
                    font-family:'IBM Plex Mono',monospace;text-align:center;">
          ELY v2.0 · LSTM + K-Means + OR-Tools<br>
          Trakya Üniversitesi · 2026
        </div>
        """, unsafe_allow_html=True)


def _render_capacity_controls(data: dict):
    """Hub kapasitesi + araç kapasitesi + kurye sliderları."""
    metrics = data.get("metrics", {})
    total_orders = metrics.get("total_orders", 0)

    # Hub Kapasitesi
    st.markdown(_label("🏭 Hub İşleme Kapasitesi"), unsafe_allow_html=True)
    hub_cap = st.slider(
        "Hub kapasitesi",
        min_value=100, max_value=500,
        value=st.session_state.hub_capacity,
        step=50, format="%d paket",
        label_visibility="collapsed",
    )
    st.session_state.hub_capacity = hub_cap

    n_hubs = max(1, int(np.ceil(total_orders / hub_cap)))

    col_a, col_b = st.columns(2)
    with col_a:
        _mini_card("Hub Kapasite", str(hub_cap), "#f43f5e")
    with col_b:
        _mini_card("Açılan Hub", str(n_hubs), "#fbbf24")

    st.markdown("<div style='margin-bottom:16px'></div>", unsafe_allow_html=True)

    # Araç Kapasitesi
    st.markdown(_label("🚐 Araç Taşıma Kapasitesi"), unsafe_allow_html=True)
    vehicle_cap = st.slider(
        "Araç kapasitesi",
        min_value=10, max_value=60,
        value=st.session_state.vehicle_capacity,
        step=5, format="%d paket",
        label_visibility="collapsed",
    )
    st.session_state.vehicle_capacity = vehicle_cap

    # Kurye Sayısı (min 4)
    st.markdown(_label("🧑‍💼 Kurye Sayısı (Hub Başına)"), unsafe_allow_html=True)
    min_c = max(4, int(np.ceil(hub_cap / vehicle_cap)))

    chosen_couriers = st.slider(
        "Kurye sayısı",
        min_value=min_c,
        max_value=min_c + 6,
        value=max(st.session_state.get("chosen_couriers", min_c), min_c),
        step=1, format="%d kurye",
        label_visibility="collapsed",
    )
    st.session_state.chosen_couriers = chosen_couriers

    # Dinamik hesaplar
    avg_load      = round(hub_cap / max(chosen_couriers, 1), 1)
    base_time     = 180
    delivery_time = max(30, min(int(base_time * (min_c / chosen_couriers)), 240))
    fuel_idx      = round(min(30.0, (chosen_couriers - min_c) * 3.5 + 22.0), 1)

    st.markdown(f"""
    <div style="background:#111827;border:1px solid #1f2937;border-radius:8px;
                padding:12px;margin-bottom:4px;">
      {_row("Min. Kurye (Hesaplanan)", str(min_c), "#f43f5e")}
      {_row("Araç Başı Yük", f"{avg_load} pkt", "#e2e8f0")}
      {_row("Tahmini Dağıtım Süresi", f"{delivery_time} dk", "#fbbf24")}
      {_row("Yakıt Tasarrufu", f"%{fuel_idx}", "#34d399")}
    </div>
    """, unsafe_allow_html=True)


def _render_past_info(data: dict, selected_date: date):
    """Geçmiş mod: salt-okunur özet kart."""
    all_orders = data.get("all_orders", pd.DataFrame())
    day_orders = pd.DataFrame()
    if not all_orders.empty and "date" in all_orders.columns:
        day_orders = all_orders[all_orders["date"] == selected_date]

    total = len(day_orders)

    st.markdown(f"""
    <div style="background:#111827;border:1px solid #1f2937;border-radius:8px;
                padding:14px;margin-bottom:16px;">
      <div style="font-size:0.65rem;color:#475569;font-family:'IBM Plex Mono',monospace;
                  text-transform:uppercase;letter-spacing:1px;margin-bottom:10px;">
        📋 O Günün Özeti
      </div>
      {_row("Toplam Sipariş", f"{total:,}", "#e2e8f0")}
      {_row("Tarih", str(selected_date), "#60a5fa")}
      <div style="margin-top:10px;padding-top:10px;border-top:1px solid #1f2937;
                  font-size:0.65rem;color:#334155;font-family:'IBM Plex Mono',monospace;">
        ℹ️ Geçmiş modda kapasite ayarları geçerli değildir.
      </div>
    </div>
    """, unsafe_allow_html=True)


def _label(text: str) -> str:
    return f"""<div style="font-size:0.7rem;color:#475569;font-family:'IBM Plex Mono',monospace;
        text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">{text}</div>"""


def _mini_card(label: str, value: str, color: str) -> None:
    st.markdown(f"""
    <div style="background:#111827;border:1px solid #1f2937;border-radius:8px;
                padding:10px;text-align:center;margin-bottom:4px;">
      <div style="font-size:0.65rem;color:#475569;font-family:'IBM Plex Mono',monospace;
                  text-transform:uppercase;letter-spacing:0.5px;">{label}</div>
      <div style="font-size:1.4rem;font-weight:600;color:{color};
                  font-family:'IBM Plex Mono',monospace;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def _row(label: str, value: str, color: str) -> str:
    return f"""<div style="display:flex;justify-content:space-between;margin-bottom:8px;">
      <span style="font-size:0.7rem;color:#475569;font-family:'IBM Plex Mono',monospace;">{label}</span>
      <span style="font-size:0.85rem;font-weight:600;color:{color};
                   font-family:'IBM Plex Mono',monospace;">{value}</span>
    </div>"""