# Arayüz ve Motor İnceleme Notları

## Kısa değerlendirme
- Kodda "QR tools" değil, Google **OR-Tools CVRP** rota optimizasyonu kullanılıyor. Arayüzde ve sunumda bu isimle anlatılmalı.
- K-Means hub mantığı genel olarak doğru: LSTM'den gelen toplam talep / hub kapasitesi ile hub sayısı belirleniyor, sonra sipariş koordinatları kümeleniyor.
- Canlı veri hattı fikri doğru; fakat arayüzden seçilen ileri tarih için optimizer içinde veri boşluğu kapatma fonksiyonunun çağrılması gerekiyordu. Eklendi.
- Önceki rota motorunda kilometre cinsinden float mesafe integer matrise yazıldığı için 1 km altındaki mesafeler 0'a düşebiliyordu. OR-Tools maliyeti artık metre cinsinden integer hesaplanıyor.
- Streamlit arayüzü iki haritalı demo mantığına göre düzenlendi: solda aktif dağıtım, sağda yarın havuzu.

## Jüriye söylenecek güvenli açıklama
Bu proje gerçek bir lojistik operasyonunun dijital ikizini simüle eder. Hava durumu ve Edirne koordinatları gerçek veriden; sipariş davranışının önemli bölümü ise akademik takvim, sınav haftaları, bölge profilleri ve gürültü terimleriyle sentetik olarak modellenmiştir. Bu yüzden LSTM sonuçlarının çok iyi çıkması tek başına gerçek dünya başarısı gibi sunulmamalı; modelin, tasarlanan yapısal talep sinyallerini öğrendiği şeklinde açıklanmalıdır.

## Loading önerisi
Butonda tek `time.sleep` kullanmak yerine aşamalı progress kullanıldı:
1. Veri hattı / eksik tarih kontrolü
2. K-Means hub üretimi
3. OR-Tools CVRP çözümü
4. Harita katmanlarının hazırlanması

Bu, bekleme süresini daha profesyonel gösterir ve 3-5 saniyelik hesaplama sırasında kullanıcıya sistemin donmadığını hissettirir.

## Sunumda vurgulanacak akış
1. T günü seçilir.
2. Veri yoksa sistem hedef tarihe kadar eksik saatlik talep ve sipariş koordinatlarını üretir.
3. LSTM mahalle bazlı hacim tahmini verir.
4. Hub kapasitesi slider'ı gerekli hub sayısını değiştirir.
5. K-Means sipariş yoğunluk merkezlerine geçici transit noktaları yerleştirir.
6. Araç kapasitesi slider'ı minimum kurye sayısını değiştirir.
7. OR-Tools CVRP her hub için kapasite kısıtlı en kısa dağıtım rotalarını üretir.
8. Sağ harita yarın teslim edilecek sipariş havuzunun gün içinde nasıl dolduğunu gösterir.
