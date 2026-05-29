"""
utils/routing.py
Folium haritası üzerine rota çizimi.
OSRM / OpenRouteService entegrasyonu için hazır iskelet dahil.
"""

import folium
import math
import random


# ─────────────────────────────────────────────
# DÜZÇIZGI ROTA (Temel — hızlı)
# ─────────────────────────────────────────────
def draw_route_straight(
    folium_map,
    depot: dict,
    order: dict,
    style: dict,
    traffic_level: str,
    closure: str | None = None,
):
    """
    Depodan sipariş noktasına düz çizgi + simüle edilmiş ara noktalar ile rota çizer.
    Gerçek projede bu fonksiyon OSRM/ORS API'siyle değiştirilebilir.
    """
    depot_coords = [depot["lat"], depot["lon"]]
    order_coords = [order["lat"], order["lon"]]

    # Orta nokta biraz kaydırarak "doğal yol etkisi" yaratıyoruz
    mid_lat = (depot["lat"] + order["lat"]) / 2 + random.uniform(-0.003, 0.003)
    mid_lon = (depot["lon"] + order["lon"]) / 2 + random.uniform(-0.003, 0.003)

    route_points = [depot_coords, [mid_lat, mid_lon], order_coords]

    # Ana rota çizgisi
    folium.PolyLine(
        locations=route_points,
        color=style["color"],
        weight=style["weight"],
        opacity=style["opacity"],
        dash_array=style.get("dash", "none") if style.get("dash") != "none" else None,
        tooltip=f"🚚 {style.get('label','Rota')} — Trafik: {traffic_level.upper()}",
    ).add_to(folium_map)

    # Yoğun trafik → ek titreşim halkası (glow efekti)
    if traffic_level in ("high", "medium"):
        folium.PolyLine(
            locations=route_points,
            color=style["color"],
            weight=style["weight"] + 6,
            opacity=0.18,
            tooltip="Yoğunluk uyarısı",
        ).add_to(folium_map)

    # Yön oku (depodan siparişe)
    _draw_arrow(folium_map, mid_lat, mid_lon, order["lat"], order["lon"], style["color"])

    # Özel etkinlik uyarı balonu
    if closure:
        folium.Marker(
            location=[mid_lat, mid_lon],
            popup=folium.Popup(
                f"<b>⚠️ {closure}</b><br>Bu güzergahta etkinlik nedeniyle yoğunluk var!",
                max_width=220,
            ),
            icon=folium.Icon(color="orange", icon="warning-sign", prefix="glyphicon"),
        ).add_to(folium_map)

    # Depo başlangıç noktası vurgu
    folium.CircleMarker(
        location=depot_coords,
        radius=10,
        color=style["color"],
        fill=True,
        fill_color=style["color"],
        fill_opacity=0.6,
        weight=3,
        tooltip=f"🏠 Başlangıç: {depot['name']}",
    ).add_to(folium_map)


def _draw_arrow(folium_map, lat1, lon1, lat2, lon2, color):
    """Basit yön oku (küçük üçgen marker)."""
    angle = math.degrees(math.atan2(lon2 - lon1, lat2 - lat1))
    arrow_html = f"""
    <div style='
        width:0; height:0;
        border-left:6px solid transparent;
        border-right:6px solid transparent;
        border-bottom:12px solid {color};
        transform:rotate({angle}deg);
        opacity:0.9;
    '></div>"""
    folium.Marker(
        location=[(lat1 + lat2) / 2, (lon1 + lon2) / 2],
        icon=folium.DivIcon(html=arrow_html, icon_size=(12, 12), icon_anchor=(6, 6)),
    ).add_to(folium_map)


# ─────────────────────────────────────────────
# GELECEKTEKİ OSRM ENTEGRASYONU (İSKELET)
# ─────────────────────────────────────────────
def draw_route_osrm(folium_map, depot, order, style, traffic_level, closure=None):
    """
    OSRM (Open Source Routing Machine) ile gerçek yol ağı üzerinde rota çizer.
    Kullanım:
        1. OSRM sunucusu kur: docker run -p 5000:5000 osrm/osrm-backend
        2. Bu fonksiyonu draw_route_straight yerine çağır.

    API endpoint: http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}
    """
    import requests

    try:
        url = (
            f"http://router.project-osrm.org/route/v1/driving/"
            f"{depot['lon']},{depot['lat']};{order['lon']},{order['lat']}"
            f"?overview=full&geometries=geojson"
        )
        resp = requests.get(url, timeout=5)
        data = resp.json()

        if data.get("code") == "Ok":
            coords = data["routes"][0]["geometry"]["coordinates"]
            # OSRM: [lon, lat] → Folium: [lat, lon]
            route_points = [[c[1], c[0]] for c in coords]

            folium.PolyLine(
                locations=route_points,
                color=style["color"],
                weight=style["weight"],
                opacity=style["opacity"],
                tooltip=f"🚚 OSRM Rota — {style.get('label','')}",
            ).add_to(folium_map)

            if traffic_level in ("high", "medium"):
                folium.PolyLine(
                    locations=route_points,
                    color=style["color"],
                    weight=style["weight"] + 6,
                    opacity=0.15,
                ).add_to(folium_map)
        else:
            # OSRM başarısız → düz çizgiye düş
            draw_route_straight(folium_map, depot, order, style, traffic_level, closure)

    except Exception:
        draw_route_straight(folium_map, depot, order, style, traffic_level, closure)


# Ana export: app.py bu ismi kullanıyor
draw_route = draw_route_straight