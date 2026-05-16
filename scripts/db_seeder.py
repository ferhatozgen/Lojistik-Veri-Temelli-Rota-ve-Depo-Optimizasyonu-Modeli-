import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def seed_database(csv_path):
    print("🚀 Veritabanına aktarım başlıyor...")

    load_dotenv()
    db_url=os.getenv('DB_URL')
    if not db_url:
        print("hata, .env dosyasında DB_URL adında dosya yok!")

    engine = create_engine(db_url)

    # CSV'yi oku
    df = pd.read_csv(csv_path)

    # Veriyi tabloya bas (if_exists='append' sayesinde üzerine ekler)
    df.to_sql('orders', engine, if_exists='append', index=False)
    print(f"✅ {len(df)} satır veritabanına başarıyla aktarıldı!")


if __name__ == "__main__":
    seed_database('../data/orders_history.csv')