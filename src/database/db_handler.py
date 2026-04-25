import pandas as pd
from sqlalchemy import create_engine
import logging
from sqlalchemy import text, inspect # text ve inspect ekledik

class DatabaseHandler:
    """Veritabanı erişim katmanı (DAL). Sadece veri transferinden sorumludur."""

    def __init__(self, db_uri):
        self.engine = create_engine(db_uri)
        logging.basicConfig(level=logging.INFO)  #loglama mekanizması var
        self.logger = logging.getLogger("DB_Handler")
#level=logging.INFO: Bu ayar, "Bilgilendirme" seviyesindeki ve üzerindeki (Warning, Error, Critical) tüm mesajları göster demektir.

    def upload_dataframe(self, df, table_name):
        """Transaction başlatır ve DataFrame'i DB'ye yazar."""
        try:
            with self.engine.begin() as connection:   #transaction yapısı
                df.to_sql(table_name, con=connection, if_exists='replace', index=False)
                self.logger.info(f" {table_name} tablosuna {len(df)} satır yüklendi.")
        except Exception as e:
            self.logger.error(f" Veri yükleme hatası: {e}")
            raise

    def fetch_query(self, query):
        """SQL sorgusuyla veri çeker ve DataFrame döner."""
        try:
            return pd.read_sql(query, self.engine)
        except Exception as e:
            self.logger.error(f" Veri çekme hatası: {e}")
            return None

    def is_table_populated(self, table_name):
        """Tablonun var olup olmadığını ve içinde veri olup olmadığını kontrol eder."""
        try:
            # Önce tablo var mı kontrol et
            ins = inspect(self.engine)
            if not ins.has_table(table_name):
                return False

            # Tablo varsa, içinde satır var mı kontrol et
            with self.engine.connect() as connection:
                query = text(f"SELECT COUNT(*) FROM {table_name}")
                result = connection.execute(query).scalar()
                return result > 0
        except Exception as e:
            self.logger.error(f"⚠️ Kontrol sırasında hata: {e}")
            return False