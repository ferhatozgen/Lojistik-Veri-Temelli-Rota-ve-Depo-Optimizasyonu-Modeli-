import os
import random
import numpy as np
import pandas as pd
from simulation.district_profiles import DISTRICT_PROFILES

WEATHER_EFFECTS = {0: 1.0, 1: 1.15, 2: 1.35}

def cluster_nodes_by_district(nodes_df):
    """GERÇEK EDİRNE KOORDİNATLARINI AKILLI BİR ŞEKİLDE TEMİZLER VE KULLANIR."""
    if nodes_df is None:
        raise ValueError("Kritik Hata: edirne_nodes.csv verisi eksik veya okunamadı!")

    # 1. Sütun isimlerini küçük harfe çevir ve boşlukları temizle (Büyük/Küçük harf duyarlılığını çözer)
    nodes_df.columns = nodes_df.columns.str.strip().str.lower()

    # 2. Eğer sütun adı Türkçe 'mahalle' olarak kalmışsa, sisteme uyması için 'district' olarak değiştir
    if 'mahalle' in nodes_df.columns:
        nodes_df.rename(columns={'mahalle': 'district'}, inplace=True)

    # Hâlâ district yoksa içindeki sütunları yazdırarak nerede hata olduğunu bize söylesin
    if 'district' not in nodes_df.columns:
        raise ValueError(f"Kritik Hata: edirne_nodes.csv içinde mahalleleri belirten sütun bulunamadı! Mevcut sütunlar: {nodes_df.columns.tolist()}")

    # 3. İçerikteki mahalle isimlerini standartlaştır (Boşlukları sil, tamamen büyük harf yap)
    nodes_df['district'] = nodes_df['district'].astype(str).str.strip().str.upper()

    # 4. Türkçe Karakterleri İngilizceye çevir (AYŞEKADIN -> AYSEKADIN uyuşmazlığını çözer)
    replacements = {'Ş': 'S', 'Ç': 'C', 'Ğ': 'G', 'Ü': 'U', 'Ö': 'O', 'İ': 'I'}
    for tr_char, en_char in replacements.items():
        nodes_df['district'] = nodes_df['district'].str.replace(tr_char, en_char)

    pools = {}
    for dist in DISTRICT_PROFILES.keys():
        # İlgili mahalleye ait koordinatları liste olarak al
        pools[dist] = nodes_df[nodes_df['district'] == dist][['lat', 'lon']].values.tolist()
        
        # Eğer o mahalleye ait hiç koordinat bulamadıysa uyar (Veri kalitesi kontrolü)
        if not pools[dist]:
            print(f"⚠️ Uyarı: {dist} mahallesi için edirne_nodes.csv içinde hiç koordinat eşleşmedi!")

    return pools

def get_calendar_features(dt):
    month, day, year = dt.month, dt.day, dt.year
    cal = {"is_special_day": 0, "is_semester_break": 0, "is_summer_break": 0, 
           "exam_engineering": 0, "exam_medicine": 0, "exam_dentistry": 0, "is_prep_week": 0}
    
    fixed_specials = [(1, 1), (4, 23), (5, 19), (7, 15), (8, 30), (10, 29), (11, 27)]
    if (month, day) in fixed_specials or (month == 6 and 23 <= day <= 30): cal["is_special_day"] = 1
    
    if (month == 1 and day >= 16 and year == 2026) or (month == 2 and day <= 16 and year == 2026):
        cal["is_semester_break"] = 1; return cal
    if (month == 6 and day >= 6 and year == 2025) or (month in [7, 8]) or (month == 9 and day <= 15):
        cal["is_summer_break"] = 1; return cal
        
    # Mühendislik
    if month == 11 and 8 <= day <= 16: cal["exam_engineering"] = 1
    elif month == 11 and 1 <= day <= 7: cal["is_prep_week"] = 1
    if month == 1 and 1 <= day <= 10 and year == 2025: cal["exam_engineering"] = 1
    elif month == 12 and 24 <= day <= 31 and year == 2024: cal["is_prep_week"] = 1
    if month == 3 and 24 <= day <= 28 and year == 2025: cal["exam_engineering"] = 1
    elif month == 3 and 17 <= day <= 23 and year == 2025: cal["is_prep_week"] = 1
    if year == 2025 and ((month == 5 and day > 25) or (month == 6 and day < 6)): cal["exam_engineering"] = 1
    elif year == 2025 and (month == 5 and 18 <= day <= 25): cal["is_prep_week"] = 1
    
    # Tıp
    if month == 10 and 21 <= day <= 27 and year == 2025: cal["exam_medicine"] = 1
    elif month == 10 and 14 <= day <= 20 and year == 2025: cal["is_prep_week"] = 1
    if month == 12 and 15 <= day <= 21 and year == 2025: cal["exam_medicine"] = 1
    elif month == 12 and 8 <= day <= 14 and year == 2025: cal["is_prep_week"] = 1
    if month == 3 and 23 <= day <= 29 and year == 2026: cal["exam_medicine"] = 1
    elif month == 3 and 16 <= day <= 22 and year == 2026: cal["is_prep_week"] = 1
    if month == 5 and 12 <= day <= 18 and year == 2026: cal["exam_medicine"] = 1
    elif month == 5 and 5 <= day <= 11 and year == 2026: cal["is_prep_week"] = 1
    
    # Diş Hekimliği
    if month == 11 and 1 <= day <= 7 and year == 2025: cal["exam_dentistry"] = 1
    elif month == 10 and 25 <= day <= 31 and year == 2025: cal["is_prep_week"] = 1
    if month == 1 and 13 <= day <= 19 and year == 2026: cal["exam_dentistry"] = 1
    elif month == 1 and 6 <= day <= 12 and year == 2026: cal["is_prep_week"] = 1
    if month == 4 and 1 <= day <= 7 and year == 2026: cal["exam_dentistry"] = 1
    elif month == 3 and 25 <= day <= 31 and year == 2026: cal["is_prep_week"] = 1
    if month == 6 and 13 <= day <= 19 and year == 2026: cal["exam_dentistry"] = 1
    elif month == 6 and 6 <= day <= 12 and year == 2026: cal["is_prep_week"] = 1
    
    return cal

def calculate_demand(profile, district_name, hour, weather_label, is_weekend, cal):
    base_demand = profile["base_demand"]
    hour_multiplier = profile["activity_curve"][hour]
    weekend_multiplier = profile["weekend_multiplier"] if is_weekend else 1.0
    weather_multiplier = WEATHER_EFFECTS[weather_label]
    special_day_multiplier = 1.9 if cal["is_special_day"] else 1.0
    academic_multiplier = 1.0

    if cal["is_semester_break"] or cal["is_summer_break"]:
        academic_multiplier = 0.25 if district_name in ["BALKAN", "AYSEKADIN"] else 0.85
    else:
        if district_name in ["BALKAN", "AYSEKADIN"]:
            if cal["exam_engineering"]: academic_multiplier += 0.6
            if cal["exam_medicine"]:    academic_multiplier += 0.5
            if cal["exam_dentistry"]:   academic_multiplier += 0.4
            if cal["is_prep_week"]:     academic_multiplier += 0.25
            if hour in [22, 23, 0, 1, 2] and (cal["exam_engineering"] or cal["exam_medicine"] or cal["exam_dentistry"] or cal["is_prep_week"]):
                academic_multiplier *= 1.35
        elif district_name == "SARACLAR" and any([cal["exam_engineering"], cal["exam_medicine"], cal["exam_dentistry"], cal["is_prep_week"]]):
            academic_multiplier = 0.75
        elif district_name == "KARAAGAC" and any([cal["exam_engineering"], cal["exam_medicine"], cal["exam_dentistry"], cal["is_prep_week"]]):
            academic_multiplier = 0.50

    spike_multiplier = random.uniform(1.5, 2.2) if random.random() < 0.03 else 1.0
    demand = (base_demand * hour_multiplier * weekend_multiplier * weather_multiplier * special_day_multiplier * academic_multiplier * spike_multiplier) + np.random.normal(0, 3)
    return max(0, round(demand))

def generate_hourly_demand(weather_df):
    rows = []
    print("Gerçek hava durumu verisi üzerinden Edirne Lojistik Zaman Serisi işleniyor...")
    for _, weather_row in weather_df.iterrows():
        dt = pd.to_datetime(weather_row["time"])
        hour = dt.hour
        weather_label = weather_row["weather_label"]
        is_weekend = dt.weekday() >= 5
        cal = get_calendar_features(dt)

        for district_name, profile in DISTRICT_PROFILES.items():
            demand = calculate_demand(profile, district_name, hour, weather_label, is_weekend, cal)
            rows.append({
                "datetime": dt, "district": district_name, "weather_label": weather_label,
                "is_weekend": int(is_weekend), "is_special_day": cal["is_special_day"],
                "is_semester_break": cal["is_semester_break"], "is_summer_break": cal["is_summer_break"],
                "is_prep_week": cal["is_prep_week"], "exam_engineering": cal["exam_engineering"],
                "exam_medicine": cal["exam_medicine"], "exam_dentistry": cal["exam_dentistry"],
                "demand": demand
            })
    return pd.DataFrame(rows)