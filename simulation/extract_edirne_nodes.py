import os
import sys
import pandas as pd
import random
from datetime import timedelta

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from simulation.order_sampler import get_calendar_features, calculate_demand, cluster_nodes_by_district
from simulation.district_profiles import DISTRICT_PROFILES

def get_weather_for_date(dt, today_date):
    """
    Simülasyon zaman çizgisine göre akıllı hava durumu ataması yapar.
    - Bugünün öncesi (6 Mayıs - 27 Mayıs arası): Yaşanmış Mayıs normları rastsallığı.
    - Bugünün sonrası (Gelecek planlaması): 7 günlük lojistik forecast tahmini.
    """
    if dt.date() < today_date:
        return 1 if random.random() < 0.25 else 0
    else:
        return 1 if random.random() < 0.20 else 0

def bridge_data_gap_system(target_date_str, today_date_str='2026-05-28'):
    """
    Frontend'den gelen hedef tarihe göre veri boşluklarını tespit eder.
    demand_engine'deki kuralları kullanarak on-the-fly (canlı) onarım yapar.
    """
    print("🔄 Kayan Zaman Pencereli Veri Hattı (Data Pipeline) Kontrol Ediliyor...")

    demand_path = os.path.join(ROOT_DIR, "data", "hourly_demand.csv")
    orders_path = os.path.join(ROOT_DIR, "data", "simulated_orders.csv")
    nodes_path = os.path.join(ROOT_DIR, "data", "edirne_nodes.csv")

    if not os.path.exists(demand_path) or not os.path.exists(orders_path):
        print(" Hata: Başlangıç veri setleri (hourly_demand veya simulated_orders) bulunamadı!")
        print(" Önce ana veri setlerinin oluşturulması gerekiyor.")
        return

    # 1. Veritabanındaki En Son Tarihi Oku
    df_demand = pd.read_csv(demand_path)
    df_demand['datetime'] = pd.to_datetime(df_demand['datetime'])
    last_recorded_date = df_demand['datetime'].max().date()

    target_date = pd.to_datetime(target_date_str).date()
    today_date = pd.to_datetime(today_date_str).date()

    # Hedef tarih zaten mevcutsa sistemi yormaya gerek yok
    if target_date <= last_recorded_date:
        print(f"✅ Talep edilen tarih ({target_date_str}) veri tabanında mevcut. Doğrudan yükleniyor.")
        return

    print(f"⚠ Veride Boşluk Saptandı! Son Kayıt: {last_recorded_date} -> Hedef: {target_date}")
    print(f"⚙ Aradaki günler kurallara göre tıkır tıkır dolduruluyor...")

    # Sokak havuzunu ve sipariş ID sayacını hazırla
    nodes_df = pd.read_csv(nodes_path)
    district_pools = cluster_nodes_by_district(nodes_df)

    df_orders = pd.read_csv(orders_path)
    order_id_counter = len(df_orders) + 300000

    new_demand_rows = []
    new_order_rows = []

    current_processing_date = last_recorded_date + timedelta(days=1)

    while current_processing_date <= target_date:
        for hour in range(24):
            dt = pd.to_datetime(f"{current_processing_date} {hour:02d}:00:00")
            weather_label = get_weather_for_date(dt, today_date)
            is_weekend = dt.weekday() >= 5
            cal = get_calendar_features(dt)

            for district_name, profile in DISTRICT_PROFILES.items():
                demand = calculate_demand(profile, district_name, hour, weather_label, is_weekend, cal)

                new_demand_rows.append({
                    "datetime": dt, "district": district_name, "weather_label": weather_label,
                    "is_weekend": int(is_weekend), "is_special_day": cal["is_special_day"],
                    "is_semester_break": cal["is_semester_break"], "is_summer_break": cal["is_summer_break"],
                    "is_prep_week": cal["is_prep_week"], "exam_engineering": cal["exam_engineering"],
                    "exam_medicine": cal["exam_medicine"], "exam_dentistry": cal["exam_dentistry"],
                    "demand": demand
                })

                pool = district_pools[district_name]
                if pool and demand > 0:
                    chosen_nodes = random.choices(pool, k=int(demand))
                    for node in chosen_nodes:
                        exact_time = dt + timedelta(minutes=random.randint(0, 59), seconds=random.randint(0, 59))
                        new_order_rows.append({
                            "order_id": f"ORD_{order_id_counter}", "timestamp": exact_time,
                            "district": district_name, "lat": node[0], "lon": node[1],
                            "weather_label": weather_label, "is_weekend": int(is_weekend),
                            "is_special_day": cal["is_special_day"], "is_semester_break": cal["is_semester_break"],
                            "is_summer_break": cal["is_summer_break"], "is_prep_week": cal["is_prep_week"],
                            "exam_engineering": cal["exam_engineering"], "exam_medicine": cal["exam_medicine"],
                            "exam_dentistry": cal["exam_dentistry"]
                        })
                        order_id_counter += 1

        current_processing_date += timedelta(days=1)

    if new_demand_rows:
        pd.DataFrame(new_demand_rows).to_csv(demand_path, mode='a', header=False, index=False)
        pd.DataFrame(new_order_rows).to_csv(orders_path, mode='a', header=False, index=False)
        print(f"✅ Başarılı! Dosyalar güncellendi. Sistem {target_date_str} tarihine hazır.")

if __name__ == "__main__":
    bridge_data_gap_system("2026-05-30")