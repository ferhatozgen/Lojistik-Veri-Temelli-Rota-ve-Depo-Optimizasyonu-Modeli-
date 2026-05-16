import numpy as np
import math
from src.data_logic.locations import EDIRNE_LOCATIONS

"""
Kırkpınar Haftası: Edirne'nin en yoğun zamanı (Haziran sonu - Temmuz başı).

Resmi Tatiller: 29 Ekim, 23 Nisan, 19 Mayıs, 30 Ağustos, 1 Ocak.

Dini Bayramlar (2025-2026): Bu tarihler lojistikte "backlog" (yığılma) yaratır.

Efsane Cuma (Black Friday): Kasım ayının son haftası (Lojistik patlaması).
"""
def check_special_event(dt):
    """Tarihe göre özel gün olup olmadığını kontrol eder."""
    month, day = dt.month, dt.day

    # Sabit Tarihli Özel Günler
    fixed_events = [
        (1, 1),  # Yılbaşı
        (4, 23),  # 23 Nisan
        (5, 19),  # 19 Mayıs
        (7, 15),  # 15 Temmuz
        (8, 30),  # 30 Ağustos
        (10, 29),  # 29 Ekim
        (11, 28)  # Black Friday civarı (Kasım sonu için dinamikleştirilebilir)
    ]

    # Edirne Özel: Kırkpınar (Haziran son haftası varsayalım)
    if month == 6 and 23 <= day <= 30:
        return 1

    # Bayramlar (2025 ve 2026 tahmini aralıklar)
    # 2025 Ramazan: 30 Mart - 1 Nisan | Kurban: 6-9 Haziran
    # 2026 Ramazan: 20-22 Mart | Kurban: 27-30 Mayıs
    if (dt.year == 2025 and ((month == 3 and day >= 30) or (month == 4 and day <= 1))): return 1
    if (dt.year == 2025 and month == 6 and 6 <= day <= 9): return 1
    if (dt.year == 2026 and month == 3 and 20 <= day <= 22): return 1

    if (month, day) in fixed_events:
        return 1

    return 0


def haversine(lat1, lon1, lat2, lon2):
    """İki nokta arası mesafeyi (km) hesaplar."""
    R = 6371    #dünyanın yarı capı
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def calculate_logistics_metrics(lat, lon, dt, weather_label):
    """Koordinat, zaman ve havaya göre trafik ve sipariş hacmi üretir."""

    is_event=check_special_event(dt)

    # 2. Mesafe Kontrolleri
    dists = {name: haversine(lat, lon, *coords) for name, coords in EDIRNE_LOCATIONS.items()}

    # 3. Trafik İndeksi (0-1)
    # Baz trafik (Pik saatler: 08-10 ve 17-20)
    hour = dt.hour
    base_traffic = 0.6 if (11 <= hour <= 13 or 17 <= hour <= 20) else 0.3

    if is_event: base_traffic += 0.2   #özel gün trafiği

    # Mekansal Cezalar (Darboğazlar)
    penalties = 0
    if dists["MERIC_BRIDGE"] < 0.4: penalties += 0.35  # Köprü kilit
    if dists["TUNCA_BRIDGE"] < 0.4: penalties += 0.35  # Köprü kilit
    if dists["AYSEKADIN_ZUBEYDE"] < 0.6: penalties += 0.25  # Ayşekadın trafik
    if dists["ERASTA_AVM"] < 0.5: penalties += 0.20  # Yoğunluk
    if dists["CENTER_SELIMIYE"] < 0.8: penalties += 0.15  # Merkez yoğunluk

    # Hava Durumu Etkisi (Meteostat'tan gelen wl: 1=Yağmur, 2=Kar)
    weather_penalty = 0.15 if weather_label == 1 else (0.4 if weather_label == 2 else 0)

    traffic_index = np.clip(base_traffic + penalties + weather_penalty + np.random.normal(0, 0.05), 0, 1)

    # 3. Sipariş Hacmi (Order Volume) konuma göre tetiklenir.
    volume_base = 25 if (13 <= hour <= 15 or 18 <= hour <= 22) else 12  # Akşam patlamasını yansıtma
    if is_event: volume_base *= 2.0


    # Mekansal Bonuslar (Siparişin çok geldiği yerler) BU NOKTALARLA UZAKLIGINA GORE ORDERCOUNT BELİRLİYOR
    if dists["DELTA_DORMS"] < 0.7: volume_base *= 1.8  # Yurtlar bölgesi
    if dists["ERASTA_AVM"] < 0.5: volume_base *= 1.5  # Alışveriş bölgesi
    if dists["AYSEKADIN_ZUBEYDE"] < 0.9: volume_base *= 1.5
    if dists["CENTER_SELIMIYE"] < 0.9: volume_base *= 1.3
    if dists["SANAYI_SITESI"] < 0.6: volume_base *= 1.3

    # Hava kötüyse insanlar eve sipariş verir
    if weather_label > 0: volume_base *= 1.25

    order_volume = np.random.poisson(volume_base)

    return round(traffic_index, 3), order_volume, is_event