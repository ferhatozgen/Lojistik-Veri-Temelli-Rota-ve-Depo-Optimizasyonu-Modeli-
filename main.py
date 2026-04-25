import os
import sys
from dotenv import load_dotenv
from src.database.db_handler import DatabaseHandler
from src.data_logic.data_generator import DataGenerator
from src.analysis.eda import LogisticEDA

# .env dosyasını yükle
load_dotenv()


def run_pipeline():
    print("\n--- [START] Lojistik Operasyonu Başlatıldı ---")

    DB_URI = os.getenv("DB_URL")
    db = DatabaseHandler(DB_URI)

    # --- AKILLI KONTROL ---
    # Tablo adı "orders". Eğer veri varsa True dönecek.
    if db.is_table_populated("orders"):
        print("✅ Veritabanında mevcut kayıtlar bulundu. Üretim adımı atlanıyor.")
    else:
        print("⚠️ Veritabanı boş veya tablo yok. Yeni veri üretiliyor...")
        generator = DataGenerator()
        df_raw = generator.generate_batch(n_samples=25000)

        print("📥 Yeni veriler PostgreSQL'e yükleniyor...")
        db.upload_dataframe(df_raw, "orders")

    # --- ANALİZ AŞAMASI (Her durumda çalışır) ---
    print("📋 Veriler analiz ediliyor...")
    df = db.fetch_query("SELECT * FROM orders")

    if df is not None and not df.empty:
        eda = LogisticEDA(df)
        eda.save_visuals()
        eda.save_map()
        print(f"📊 {len(df)} satır üzerinde analiz tamamlandı. Raporlar 'results/' klasöründe.")
    else:
        print(" Kritik Hata: Veri çekilemedi!")

    print("--- [FINISH] ---")


# KRİTİK NOKTA: Bu satır olmazsa kod asla çalışmaz!
if __name__ == "__main__":
    run_pipeline()