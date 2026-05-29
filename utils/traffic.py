"""
utils/traffic.py
Saate ve koşullara göre trafik yoğunluğu + rota rengi hesaplama.
Edirne'ye özgü pik saatler ve özel etkinlikler dahil.
"""

from datetime import datetime


# ─────────────────────────────────────────────
# TRAFİK SEVİYESİ
# ─────────────────────────────────────────────
def get_traffic_level(hour: int) -> str:
    """
    Saate göre Edirne trafik yoğunluğunu döndürür.
    Edirne sabah piki 07-09, akşam piki 17-19.
    Öğle arası 12-13 hafif yoğunluk.
    Returns: 'low' | 'medium' | 'high'
    """
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        return "high"
    elif (10 <= hour <= 11) or (12 <= hour <= 14) or (15 <= hour <= 16):
        return "medium"
    else:
        return "low"


def get_traffic_percentage(hour: int) -> int:
    """Trafik yoğunluğunu 0-100 arasında sayı olarak döndürür."""
    traffic_map = {
        0: 8,  1: 5,  2: 4,  3: 4,  4: 6,  5: 12,
        6: 22, 7: 65, 8: 82, 9: 70, 10: 48, 11: 55,
        12: 60, 13: 58, 14: 50, 15: 52, 16: 61,
        17: 88, 18: 90, 19: 75, 20: 45, 21: 30,
        22: 20, 23: 12,
    }
    return traffic_map.get(hour, 20)


# ─────────────────────────────────────────────
# ROTA RENGİ & STİLİ
# ─────────────────────────────────────────────
def get_route_color(traffic_level: str, mode: str) -> dict:
    """
    Trafik seviyesi ve optimizasyon moduna göre
    Folium PolyLine için renk ve kalınlık döndürür.
    """
    # Trafik yoğunsa → uyarı rengi (mod fark etmez)
    if traffic_level == "high":
        return {
            "color":   "#ef4444",   # Parlak kırmızı
            "weight":  9,
            "opacity": 0.92,
            "dash":    "none",
            "label":   "Yoğun Trafik",
        }
    elif traffic_level == "medium":
        return {
            "color":   "#f97316",   # Turuncu
            "weight":  7,
            "opacity": 0.85,
            "dash":    "none",
            "label":   "Orta Yoğunluk",
        }
    else:
        # Trafik düşükse → moda göre renk
        mode_styles = {
            "💰 Minimum Maliyet": {
                "color":   "#3b82f6",   # Mavi
                "weight":  5,
                "opacity": 0.85,
                "dash":    "none",
                "label":   "Optimum Maliyet Rotası",
            },
            "⚡ Maksimum Hız": {
                "color":   "#a855f7",   # Mor
                "weight":  5,
                "opacity": 0.85,
                "dash":    "none",
                "label":   "Hız Rotası",
            },
            "🌿 Minimum Karbon": {
                "color":   "#22c55e",   # Yeşil
                "weight":  5,
                "opacity": 0.85,
                "dash":    "8 4",
                "label":   "Karbon Optimum Rotası",
            },
        }
        return mode_styles.get(mode, mode_styles["💰 Minimum Maliyet"])


# ─────────────────────────────────────────────
# ÖZEL ETKİNLİK / YOL KAPANMASI
# ─────────────────────────────────────────────
SPECIAL_CLOSURES = {
    # format: "MM-DD" : { name, affected_hours, severity }
    "01-01": {"name": "Yılbaşı",          "affected_hours": list(range(0, 4)) + list(range(22, 24)),  "severity": "medium"},
    "04-23": {"name": "23 Nisan",          "affected_hours": list(range(9, 18)),  "severity": "high"},
    "05-01": {"name": "1 Mayıs İşçi Bayramı", "affected_hours": list(range(8, 20)), "severity": "high"},
    "05-19": {"name": "19 Mayıs",          "affected_hours": list(range(9, 18)),  "severity": "medium"},
    "06-28": {"name": "Kırkpınar Haftası", "affected_hours": list(range(10, 22)), "severity": "high"},
    "06-29": {"name": "Kırkpınar Haftası", "affected_hours": list(range(10, 22)), "severity": "high"},
    "06-30": {"name": "Kırkpınar Haftası", "affected_hours": list(range(10, 22)), "severity": "high"},
    "07-15": {"name": "15 Temmuz",         "affected_hours": list(range(18, 24)), "severity": "high"},
    "08-30": {"name": "30 Ağustos",        "affected_hours": list(range(9, 17)),  "severity": "medium"},
    "10-29": {"name": "29 Ekim Cumhuriyet", "affected_hours": list(range(9, 18)), "severity": "high"},
}


def check_special_closure(date_str: str, hour: int) -> str | None:
    """
    Verilen tarihe (MM-DD) ve saate özel etkinlik varsa adını döndürür.
    Yoksa None döner.
    """
    if date_str in SPECIAL_CLOSURES:
        closure = SPECIAL_CLOSURES[date_str]
        if hour in closure["affected_hours"]:
            return closure["name"]
    return None


def get_estimated_delay(traffic_level: str, closure: str | None) -> str:
    """Tahmini gecikme süresini döndürür."""
    base = {"low": 0, "medium": 8, "high": 20}.get(traffic_level, 0)
    if closure:
        base += 15
    if base == 0:
        return "Gecikme beklenmez"
    return f"+{base} dakika tahmini gecikme"