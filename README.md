# 🚚 Lojistik Veri Temelli Rota ve Depo Optimizasyonu Modeli

Yapay sinir ağları ve kümeleme algoritmaları kullanarak dinamik depo konumu önerisi ve rota optimizasyonu yapan bir veri bilimi projesi.

---

## 📋 Proje Hakkında

Bu proje, lojistik teslimat verilerini analiz ederek şu soruları yanıtlamayı hedefler:

- Mevcut müşteri dağılımına göre **optimal depo konumları** neresi olmalı?
- Hava durumu, trafik ve saat gibi faktörler göz önüne alındığında **en verimli rotalar** nasıl belirlenir?
- Talep tahminleri için **LSTM tabanlı zaman serisi modeli** nasıl kurulur?

---

## 👥 Ekip ve Görev Dağılımı

| İsim | Rol |
|------|-----|
| Ferhat ÖZGEN | Veri Toplama ve Analiz |
| Pınar KARABULUT | Veri Ön İşleme |
| Furkan EROĞLU | Yapay Sinir Ağı & Kümeleme Modeli |
| Kerem Salih TURGAY | Sistem Entegrasyonu ve Raporlama |
| Umut HATA | Test, Performans Analizi ve Görselleştirme |

---

## 📁 Proje Yapısı

```
├── data/
│   ├── raw_logistic_data.csv       # Ham lojistik verisi
│   ├── dijital_ikiz_veri.csv       # Dijital ikiz simülasyon verisi
│   ├── lstm_icin_hazir_veri.csv    # LSTM modeli için hazırlanmış veri
│   └── yeni_lojistik_veri.csv      # İşlenmiş lojistik verisi
├── src/
│   ├── analysis/                   # EDA ve görselleştirme modülleri
│   ├── data_logic/                 # Veri üretimi ve ön işleme
│   └── database/                   # Veritabanı bağlantı katmanı
├── results/                        # Analiz çıktıları ve görseller
├── main.py                         # Ana pipeline
├── requirements.txt                # Python bağımlılıkları
└── .env                            # Ortam değişkenleri (GitHub'a yüklenmez!)
```

---

## ⚙️ Kurulum

### 1. Repoyu klonla

```bash
git clone https://github.com/ferhatozgen/Lojistik-Veri-Temelli-Rota-ve-Depo-Optimizasyonu-Modeli-.git
cd Lojistik-Veri-Temelli-Rota-ve-Depo-Optimizasyonu-Modeli-
```

### 2. Sanal ortam oluştur ve aktif et

```bash
python -m venv .venv

# macOS / Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Bağımlılıkları kur

```bash
pip install -r requirements.txt
```

### 4. `.env` dosyasını oluştur

Proje kök dizininde `.env` adında boş bir dosya oluştur. Veritabanı bağlantı bilgisi ekipten alındıktan sonra aşağıdaki formatta doldurulacaktır:

```
DB_URL=postgresql://kullanici:sifre@host:port/veritabani_adi
```

> ⚠️ `.env` dosyasını kimseyle paylaşma ve GitHub'a yükleme. `.gitignore` tarafından zaten engellenmektedir.
> 
> ℹ️ Veritabanı kurulumu henüz tamamlanmamıştır. Bağlantı bilgisi için ekiple iletişime geçin.

---

## 🚀 Çalıştırma

```bash
python main.py
```

Başarılı çalışma çıktısı:

```
---- [START] Lojistik Operasyonu Başlatıldı ----
✅ Veritabanında mevcut kayıtlar bulundu. Üretim adımı atlanıyor.
📊 Veriler analiz ediliyor...
📍 25000 satır üzerinde analiz tamamlandı. Raporlar 'results/' klasöründe.
---- [FINISH] ----
```

---

## 📊 Kullanılan Teknolojiler

- **Python 3.12**
- **PostgreSQL** — Veritabanı
- **SQLAlchemy** — ORM katmanı
- **Pandas / NumPy** — Veri işleme
- **Scikit-learn** — Kümeleme (K-Means)
- **Matplotlib / Seaborn** — Görselleştirme
- **python-dotenv** — Ortam değişkeni yönetimi

---

## 🗂️ Veri Seti Açıklaması

| Kolon | Açıklama |
|-------|----------|
| `delivery_timestamp` | Teslimat zaman damgası |
| `lat`, `lon` | Enlem ve boylam koordinatları |
| `weather` | Hava durumu (Clear, Rain/Snow) |
| `traffic_index` | Trafik yoğunluk indeksi (0-1) |
| `hour` | Teslimat saati |
| `day_of_week` | Haftanın günü |
| `is_special_event` | Özel gün bayrağı |

---

## 📌 Notlar

- Veritabanı ilk çalıştırmada boşsa sistem otomatik olarak 25.000 satır sentetik veri üretir.
- Sonuçlar `results/` klasörüne kaydedilir.
- Her çalıştırmada mevcut veri kontrol edilir, tekrar üretim yapılmaz.
