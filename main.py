import os
import sys
from dotenv import load_dotenv
from src.database.db_handler import DatabaseHandler
from src.data_logic.data_generator import DataGenerator
from src.analysis.eda import LogisticEDA

# .env dosyasını yükle
load_dotenv()

def run_pipeline():
    print("\n---- [START] Lojistik Operasyonu Başlatıldı ----")

    # Veritabanı bağlantısı
    DB_URI = os.getenv("DB_URL")
    if not DB_URI:
        print("❌ Hata: .env dosyasında DB_URL bulunamadı.")
        print("   .env dosyanıza şunu ekleyin: DB_URL=postgresql://kullanici:sifre@localhost:5432/db_adi")
        sys.exit(1)

    try:
        db = DatabaseHandler(DB_URI)
        print("✅ Veritabanı bağlantısı kuruldu.")
    except Exception as e:
        print(f"❌ Veritabanı bağlantısı kurulamadı: {e}")
        sys.exit(1)

    # Akıllı kontrol
    try:
        if db.is_table_populated("orders"):
            print("✅ Veritabanında mevcut kayıtlar bulundu. Üretim adımı atlanıyor.")
        else:
            print("⚠️ Veritabanı boş. Yeni veri üretiliyor...")
            generator = DataGenerator()
            df_raw = generator.generate_batch(n_samples=25000)
            print("🚀 Yeni veriler PostgreSQL'e yükleniyor...")
            db.upload_dataframe(df_raw, "orders")
    except Exception as e:
        print(f"❌ Veri üretimi sırasında hata: {e}")
        sys.exit(1)

    # Analiz aşaması
    try:
        print("📊 Veriler analiz ediliyor...")
        df = db.fetch_query("SELECT * FROM orders")
        if df is not None and not df.empty:
            eda = LogisticEDA(df)
            eda.save_visuals()
            eda.save_map()
            print(f"📍 {len(df)} satır üzerinde analiz tamamlandı. Raporlar 'results/' klasöründe.")
        else:
            print("❌ Kritik Hata: Veri çekilemedi!")
    except Exception as e:
        print(f"❌ Analiz sırasında hata: {e}")
        sys.exit(1)

    print("---- [FINISH] ----")

if __name__ == "__main__":
    run_pipeline()