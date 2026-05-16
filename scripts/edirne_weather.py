import pandas as pd
from meteostat import Point, Hourly
from datetime import datetime
import os

# 1. Klasörü Oluştur (Yoksa hata almamak için)
if not os.path.exists('../data'):
    os.makedirs('data')

# 2. Edirne Verisi
edirne = Point(41.6772, 26.5567, 41)
start = datetime(2025, 1, 1)
end = datetime(2026, 5, 6, 23, 59)

data = Hourly(edirne, start, end)
data = data.fetch().reset_index()

# 3. İhtiyacımız olanları seçttik
weather_df = data[['time', 'temp', 'prcp', 'coco', 'wspd']].copy()

# 4. Senin istediğin "Hava Durumu Gruplaması" (Weather Label - wl)
# 0: İyi, 1: Riskli (Yağmur/Sis), 2: Kritik (Kar/Fırtına)
def group_weather(coco):
    if coco <= 4:
        return 0 # Açık
    elif 5 <= coco <= 9:
        return 1 # Yağmur / Sis
    else:
        return 2 # Kar / Fırtına

weather_df['weather_label'] = weather_df['coco'].apply(group_weather)

# 5. DOSYAYI KAYDET
weather_df.to_csv('../data/edirne_weather_2025_2026.csv', index=False)

print("✅ edirne_weather_2025_2026.csv başarıyla data/ klasörüne kaydedildi.")
print(weather_df.head())