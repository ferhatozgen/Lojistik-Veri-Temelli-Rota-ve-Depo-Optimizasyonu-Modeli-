import os
import sys
import pandas as pd

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from simulation.order_sampler import generate_hourly_demand
from simulation.extract_edirne_nodes import bridge_data_gap_system

def run_pipeline():
    print("🚀 Ana Veri Hattı Başlatılıyor...")
    
    weather_path = os.path.join(ROOT_DIR, "data", "edirne_weather_2025_2026.csv")
    demand_path = os.path.join(ROOT_DIR, "data", "hourly_demand.csv")
    orders_path = os.path.join(ROOT_DIR, "data", "simulated_orders.csv")

    if not os.path.exists(weather_path):
        print(f"Hata: {weather_path} eksik. Gerçek hava durumu verisi gerekiyor.")
        return

    # Eğer ana demand dosyası yoksa önce onu üret
    if not os.path.exists(demand_path):
        print("Temel saatlik talep verisi (hourly_demand) sıfırdan oluşturuluyor...")
        weather_df = pd.read_csv(weather_path)
        demand_df = generate_hourly_demand(weather_df)
        demand_df.to_csv(demand_path, index=False)
        
        # simulated_orders.csv boş bir iskelet olarak oluşturulsun ki bridge_data_gap_system üstüne yazabilsin
        pd.DataFrame(columns=["order_id", "timestamp", "district", "lat", "lon", "weather_label", "is_weekend", "is_special_day", "is_semester_break", "is_summer_break", "is_prep_week", "exam_engineering", "exam_medicine", "exam_dentistry"]).to_csv(orders_path, index=False)
        print("Temel veri başarıyla oluşturuldu.")

    # 30 Mayıs'a kadar eksik günleri senin yazdığın köprü sistemiyle gerçek koordinatlardan doldur
    bridge_data_gap_system("2026-05-30")

if __name__ == "__main__":
    run_pipeline()