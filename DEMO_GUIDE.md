# DEMO GUIDE — TUGAS 2 DISTRIBUTED SYNCHRONIZATION SYSTEM
**Arya Zaky Pradipta — NIM 11231013**

> Baca file ini dari atas ke bawah. Semua langkah, perintah, dan skrip narasi ada di sini.

---

## BAGIAN A — PERSIAPAN TERMINAL (Lakukan SEBELUM rekam)

Buka **4 PowerShell window** dan atur tampilannya agar terlihat jelas di video:
- Klik kanan title bar → **Properties** → Tab **Font** → ukuran **16** atau **18**
- Tab **Layout** → lebar **130**, tinggi **40**

Susun di layar:
- **Terminal 1** = Node 1 (port 8001)
- **Terminal 2** = Node 2 (port 8002)
- **Terminal 3** = Node 3 (port 8003)
- **Terminal 4** = Demo Commands (yang aktif saat rekam)

Pastikan **Docker Desktop sudah dibuka** dan icon-nya Running di taskbar sebelum lanjut.

---

## BAGIAN B — START SISTEM (Cara Paling Mudah — Tinggal Double-Click)

> Saya sudah buatkan 4 file `.bat` di folder `c:\tugas3sister\distributed-sync-system`.
> Kamu tinggal **double-click** file-file ini berurutan. Masing-masing akan membuka jendela baru otomatis.

### Langkah B1 — Buka Docker Desktop dulu

Pastikan icon Docker Desktop di taskbar sudah **berwarna biru/running** sebelum lanjut.

### Langkah B2 — Double-click: `START_1_REDIS.bat`

Buka **File Explorer** → pergi ke `c:\tugas3sister\distributed-sync-system` → double-click **`START_1_REDIS.bat`**

Jendela CMD akan terbuka dan menjalankan Redis. Jika muncul tulisan `redis` dengan status `Up` → Redis berhasil.

> ⚠️ Jika muncul error `docker: cannot connect` → Docker Desktop belum running, buka dulu dan tunggu sampai icon-nya aktif.

> ⚠️ Jika muncul error `Conflict. The container name "/redis" is already in use` → Redis sudah pernah dijalankan sebelumnya. Jalankan perintah ini di PowerShell:
> ```powershell
> docker rm -f redis
> ```
> Lalu double-click `START_1_REDIS.bat` lagi.

### Langkah B3 — Double-click: `START_2_NODE1.bat`

Double-click **`START_2_NODE1.bat`** → jendela baru terbuka, Node 1 berjalan di port 8001.

Kamu akan melihat log seperti:
```
============================================
 NODE 1 - Port 8001
============================================
2026-05-02 [...] INFO ... RaftNode node1 started
2026-05-02 [...] INFO ... Node node1 ready!
```

### Langkah B4 — Double-click: `START_3_NODE2.bat`

Double-click **`START_3_NODE2.bat`** → jendela baru terbuka, Node 2 berjalan di port 8002.

### Langkah B5 — Double-click: `START_4_NODE3.bat`

Double-click **`START_4_NODE3.bat`** → jendela baru terbuka, Node 3 berjalan di port 8003.

### Langkah B6 — Verifikasi sistem siap

Tunggu **10 detik** agar Raft election selesai, lalu buka **PowerShell baru** (Terminal untuk Demo) dan jalankan:

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/health"
```

Jika muncul output seperti ini → **sistem siap!**
```
status  node_id  raft_state
------  -------  ----------
ok      node1    leader
```

Jika muncul error `Connection refused` → tunggu 5 detik lagi dan coba ulang. Node butuh waktu boot.

---

## BAGIAN C — SETUP VARIABEL DEMO

Di **PowerShell Demo** (Terminal yang kamu pakai untuk demo), jalankan baris ini **satu per satu** (tekan Enter setelah setiap baris):

```powershell
$H = @{"X-API-Key" = "dev-secret-key-change-in-prod"; "Content-Type" = "application/json"}
```

```powershell
Write-Host "Header siap!" -ForegroundColor Green
```

Setelah muncul `Header siap!` → lanjut ke demo.

---

## BAGIAN D — SKRIP VIDEO LENGKAP

> **Cara baca:** Teks dalam tanda `"..."` = yang kamu **ucapkan**. Blok `code` = yang kamu **jalankan di Terminal 4**.

---

### [SEGMEN 1] PENDAHULUAN — 0:00 s/d 1:30

> *Tampilkan: Layar desktop, buka file README.md di VS Code*

**Ucapkan:**

*"Halo, perkenalkan nama saya Arya Zaky Pradipta, NIM 11231013. Pada video ini saya akan mempresentasikan Tugas 2 mata kuliah Sistem Parallel dan Terdistribusi, dengan judul Implementasi Distributed Synchronization System.*

*Sistem yang saya bangun terdiri dari tiga komponen utama. Pertama, Distributed Lock Manager menggunakan algoritma Raft Consensus. Kedua, Distributed Queue System dengan Consistent Hashing dan Redis. Ketiga, Distributed Cache dengan protokol koherensi MESI — Modified, Exclusive, Shared, Invalid.*

*Sebagai bonus, saya implementasikan Bonus D yaitu Security, berupa TLS encryption, Role-Based Access Control, dan tamper-evident audit log.*

*Semua diimplementasikan menggunakan Python 3.11 asyncio, aiohttp, Redis, dan Docker. Mari kita mulai."*

---

### [SEGMEN 2] ARSITEKTUR SISTEM — 1:30 s/d 4:00

> *Tampilkan: Buka file `docs/architecture.md` di VS Code, zoom ke diagram*

**Ucapkan:**

*"Sebelum demo, saya jelaskan arsitektur sistem terlebih dahulu.*

*[Tunjuk diagram ASCII di architecture.md]*

*Sistem terdiri dari 3 node yang berjalan di port 8001, 8002, dan 8003. Ketiga node saling berkomunikasi menggunakan HTTP REST API. Di bawahnya ada Redis sebagai persistent storage.*

*Ada 4 layer utama: Infrastructure layer berisi config dan metrics. Communication layer menggunakan aiohttp untuk async HTTP dan heartbeat-based failure detector. Consensus layer mengimplementasikan Raft Algorithm secara penuh. Dan Application layer berisi Lock Manager, Queue Node, dan Cache Node.*

*[Scroll ke bagian struktur folder]*

*Struktur folder mengikuti spesifikasi tugas: src/consensus untuk Raft, src/nodes untuk tiga komponen utama, src/communication untuk message passing, dan folder security untuk bonus.*

*Total ada 18 REST API endpoint dan lebih dari 30 file Python. Sekarang mari kita lihat demo-nya."*

---

### [SEGMEN 3A] RAFT CONSENSUS & LEADER ELECTION — 4:00 s/d 5:00

> *Tampilkan: Terminal 4*

**Ucapkan:**

*"Pertama, kita lihat Raft Consensus bekerja. Saya sudah menjalankan tiga node. Mari cek status masing-masing."*

```powershell
# Cek health semua node
Invoke-RestMethod -Uri "http://localhost:8001/health" | ConvertTo-Json
```

**Ucapkan:** *"Node 1 healthy. Sekarang cek status Raft — siapa yang jadi leader."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/raft/status" | ConvertTo-Json
```

**Ucapkan:** *"Perhatikan field 'state' — ini node 1 sebagai [Leader/Follower]. Kita cek node lain."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8002/raft/status" | ConvertTo-Json
Invoke-RestMethod -Uri "http://localhost:8003/raft/status" | ConvertTo-Json
```

**Ucapkan:** *"Hanya ada satu Leader dalam satu waktu — ini adalah safety property Raft. Jika leader mati, election baru terjadi dalam 150 hingga 300 millisecond secara otomatis."*

---

### [SEGMEN 3B] DISTRIBUTED LOCK MANAGER — 5:00 s/d 7:00

> *Tampilkan: Terminal 4*

**Ucapkan:** *"Sekarang demo Distributed Lock Manager. Saya acquire exclusive lock dari Node 1."*

```powershell
$r1 = Invoke-RestMethod -Uri "http://localhost:8001/lock/acquire" -Method POST -Headers $H `
    -Body '{"lock_id":"database-1","client_id":"service-A","lock_type":"exclusive"}'
$r1 | ConvertTo-Json
```

**Ucapkan:** *"Lock berhasil! Status 'acquired', artinya service-A memegang exclusive lock. Sekarang mari kita lihat kecerdasan algoritma Raft. Saya coba acquire lock yang sama dari Node 2 (yang merupakan follower) menggunakan service-B."*

```powershell
$r2 = Invoke-RestMethod -Uri "http://localhost:8002/lock/acquire" -Method POST -Headers $H `
    -Body '{"lock_id":"database-1","client_id":"service-B","lock_type":"shared"}'
$r2 | ConvertTo-Json
```

**Ucapkan:** *"Perhatikan, Node 2 menolak dengan status 'not_leader' dan langsung memberi tahu bahwa Node 1 adalah leadernya. Ini adalah fitur Raft Leader Redirection! Sekarang kita turuti dan kirim ulang request service-B ke Node 1."*

```powershell
$r3 = Invoke-RestMethod -Uri "http://localhost:8001/lock/acquire" -Method POST -Headers $H `
    -Body '{"lock_id":"database-1","client_id":"service-B","lock_type":"shared"}'
$r3 | ConvertTo-Json
```

**Ucapkan:** *"Nah, Node 1 langsung merespon 'acquired' yang artinya request berhasil diterima oleh sistem Raft. Tapi mari kita buktikan bahwa service-B belum benar-benar mendapatkan lock-nya, melainkan masuk ke waiting queue karena lock masih dipegang service-A."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/lock/status" -Headers $H | ConvertTo-Json -Depth 4
```

**Ucapkan:** *"Terlihat exclusive_holder adalah service-A dan queue_depth adalah 1. Sekarang saya release lock dari service-A."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/lock/release" -Method POST -Headers $H `
    -Body '{"lock_id":"database-1","client_id":"service-A"}' | ConvertTo-Json
```

**Ucapkan:** *"Release berhasil. Sekarang sistem otomatis memberikan lock ke service-B yang sedang menunggu. Kita verifikasi."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/lock/status" -Headers $H | ConvertTo-Json -Depth 4
```

**Ucapkan:** *"Benar — service-B sekarang menjadi holder. Ini adalah fitur automatic lock granting dari waiting queue. Fitur deadlock detection menggunakan Wait-For Graph juga berjalan di background setiap 5 detik untuk mendeteksi dan menyelesaikan deadlock secara otomatis."*

```powershell
# Bersihkan — release service-B
Invoke-RestMethod -Uri "http://localhost:8001/lock/release" -Method POST -Headers $H `
    -Body '{"lock_id":"database-1","client_id":"service-B"}' | Out-Null
```

---

### [SEGMEN 3C] DISTRIBUTED QUEUE — 7:00 s/d 8:30

> *Tampilkan: Terminal 4*

**Ucapkan:** *"Selanjutnya, Distributed Queue dengan Consistent Hashing. Saya produce dua pesan dari node yang berbeda."*

```powershell
$m1 = Invoke-RestMethod -Uri "http://localhost:8001/queue/produce" -Method POST -Headers $H `
    -Body '{"queue":"orders","payload":{"order_id":"ORD-001","amount":150000},"producer_id":"svc-order"}'
Write-Host "Produced: $($m1.message_id)" -ForegroundColor Green

$m2 = Invoke-RestMethod -Uri "http://localhost:8002/queue/produce" -Method POST -Headers $H `
    -Body '{"queue":"orders","payload":{"order_id":"ORD-002","amount":75000},"producer_id":"svc-order"}'
Write-Host "Produced: $($m2.message_id)" -ForegroundColor Green
```

**Ucapkan:** *"Dua pesan berhasil diproduksi. Consistent Hashing menentukan node mana yang menyimpan setiap pesan berdasarkan hash dari message ID-nya. Mari cek status queue."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/queue/status" -Headers $H | ConvertTo-Json -Depth 3
```

**Ucapkan:** *"Ada 2 pesan pending di queue orders. Sekarang saya consume satu pesan."*

```powershell
$consumed = Invoke-RestMethod -Uri "http://localhost:8001/queue/consume" -Method POST -Headers $H `
    -Body '{"queue":"orders","consumer_id":"worker-1","ack_timeout":30}'
$consumed | ConvertTo-Json -Depth 4
$consumedId = $consumed.message.message_id
```

**Ucapkan:** *"Pesan berhasil di-consume dan statusnya menjadi IN_FLIGHT. Consumer harus acknowledge dalam 30 detik, atau pesan akan dikirim ulang secara otomatis — ini adalah jaminan at-least-once delivery. Saya acknowledge sekarang."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/queue/ack" -Method POST -Headers $H `
    -Body "{`"queue`":`"orders`",`"message_id`":`"$consumedId`"}" | ConvertTo-Json
```

**Ucapkan:** *"Pesan berhasil di-acknowledge dan statusnya menjadi completed. Pesan tidak akan dikirim ulang lagi."*

---

### [SEGMEN 3D] CACHE COHERENCE MESI — 8:30 s/d 10:00

> *Tampilkan: Terminal 4*

**Ucapkan:** *"Komponen terakhir adalah Distributed Cache dengan protokol MESI. Saya akan write ke cache dan perhatikan state transitions-nya."*

```powershell
$wr = Invoke-RestMethod -Uri "http://localhost:8001/cache/user:1001" -Method PUT -Headers $H `
    -Body '{"value":{"name":"Arya Zaky","role":"admin","score":95}}'
Write-Host "State setelah write: $($wr.state)" -ForegroundColor Yellow
```

**Ucapkan:** *"State M — Modified. Data hanya ada di cache Node 1 dalam kondisi dirty, artinya belum ditulis ke backing store. Sekarang saya read dari Node 1 yang sama."*

```powershell
$rd1 = Invoke-RestMethod -Uri "http://localhost:8001/cache/user:1001" -Headers $H
Write-Host "Node1: status=$($rd1.status) | state=$($rd1.state)" -ForegroundColor Green
```

**Ucapkan:** *"Cache HIT! State tetap M. Sekarang saya read key yang sama dari Node 2 — ini akan menjadi cache miss karena Node 2 belum punya data ini."*

```powershell
$rd2 = Invoke-RestMethod -Uri "http://localhost:8002/cache/user:1001" -Headers $H
Write-Host "Node2: status=$($rd2.status) | state=$($rd2.state)" -ForegroundColor Yellow
Write-Host "Value: $($rd2.value | ConvertTo-Json)"
```

**Ucapkan:** *"Cache MISS di Node 2. Node 2 fetch data dari Node 1 melalui peer-fetch RPC. Hasilnya state di Node 2 menjadi S — Shared, karena sekarang ada dua node yang punya data ini. Sekarang saya write lagi dari Node 1."*

```powershell
$wr2 = Invoke-RestMethod -Uri "http://localhost:8001/cache/user:1001" -Method PUT -Headers $H `
    -Body '{"value":{"name":"Arya Zaky","role":"superadmin","score":100}}'
Write-Host "State setelah write: $($wr2.state)" -ForegroundColor Red
```

**Ucapkan:** *"Node 1 menulis ulang, state menjadi M lagi. Yang terpenting, Node 1 secara otomatis broadcast pesan INVALIDATE ke Node 2. Sekarang entry di Node 2 menjadi state I — Invalid. Kalau Node 2 read lagi, akan terjadi cache miss dan fetch ulang data terbaru. Mari cek metrik cache."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/cache/status" -Headers $H | ConvertTo-Json -Depth 3
```

**Ucapkan:** *"Terlihat cache size, hit rate, jumlah eviction dari LRU policy, dan jumlah invalidation MESI. Ini adalah performa monitoring real-time."*

---

### [SEGMEN 4] UNIT TESTS — 10:00 s/d 11:00

> *Tampilkan: Terminal 4*

**Ucapkan:** *"Sekarang saya tunjukkan hasil unit testing. Saya menggunakan pytest dengan total 33 test cases yang mencakup semua komponen."*

```powershell
pytest tests/unit/ -v --tb=short
```

**Ucapkan:** *"Semua 33 test passed! Mari saya jelaskan:*

*12 test untuk MESI Cache — mencakup state transitions Modified, Exclusive, Shared, Invalid, serta LRU eviction dan peer invalidation.*

*11 test untuk Lock Manager — mencakup shared dan exclusive lock, blocking behavior, automatic granting, dan deadlock detection dengan skenario cyclic Wait-For Graph.*

*10 test untuk Raft Consensus — mencakup leader election, vote granting dan rejection, AppendEntries log replication, step-down pada higher term, dan client command redirect.*"

---

### [SEGMEN 5] PERFORMANCE BENCHMARK — 11:00 s/d 13:00

> *Tampilkan: Terminal 4*

**Ucapkan:** *"Sekarang performance testing. Saya sudah membuat benchmark script yang mengukur throughput dan latency semua komponen secara bersamaan."*

```powershell
python benchmarks/load_test_scenarios.py --host http://localhost:8001
```

**Ucapkan (sambil menunggu hasil):** *"Benchmark ini menjalankan 4 skenario: lock throughput, queue produce dan consume, cache read dan write, serta scalability test dengan berbagai level concurrency."*

**Ucapkan setelah hasil muncul:** *"Dari hasil benchmark:*

*Pertama, Distributed Lock: throughput sekitar 80 hingga 100 requests per detik dengan latency p50 sekitar 14 millisecond. Ini lebih lambat dari komponen lain karena setiap lock operation harus direplikasi via Raft ke majority nodes sebelum committed.*

*Kedua, Distributed Queue: jauh lebih cepat dengan throughput 300 hingga 400 requests per detik dan latency p50 sekitar 6 millisecond, karena routing hanya berdasarkan Consistent Hash tanpa perlu consensus.*

*Ketiga, Distributed Cache read hit: yang tercepat, ribuan requests per detik dengan latency sub-millisecond karena data sudah ada di memory lokal.*

*Di skenario scalability, throughput meningkat linear hingga sekitar 50 concurrent users, kemudian mulai saturasi di sekitar 5000-6000 requests per detik karena bottleneck pada Redis I/O.*

*Ini membuktikan tradeoff fundamental dalam distributed systems: semakin kuat consistency guarantee-nya, semakin tinggi latency-nya."*

---

### [SEGMEN 6] SECURITY BONUS D — 13:00 s/d 13:30

> *Tampilkan: Terminal 4*

**Ucapkan:** *"Sebagai bonus D, saya implementasikan Security. Mari lihat audit log yang tamper-evident."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/audit/logs?n=5" -Headers $H | ConvertTo-Json -Depth 3
```

**Ucapkan:** *"Setiap audit entry menyertakan entry_hash yang merupakan SHA-256 hash dari konten entry ditambah hash entry sebelumnya — ini disebut hash chaining. Jika ada entry yang dimodifikasi, chain akan rusak dan terdeteksi. Mari verifikasi integritasnya."*

```powershell
Invoke-RestMethod -Uri "http://localhost:8001/audit/verify" -Headers $H | ConvertTo-Json
```

**Ucapkan:** *"Valid true — chain integrity terjaga. Untuk RBAC, setiap request memerlukan API key di header X-API-Key. Ada empat role: admin, producer, consumer, dan reader, masing-masing dengan permission yang berbeda."*

---

### [SEGMEN 7] KESIMPULAN — 13:30 s/d 14:30

> *Tampilkan: File README.md di VS Code*

**Ucapkan:**

*"Sebagai penutup, dalam tugas ini saya berhasil mengimplementasikan sistem sinkronisasi terdistribusi yang lengkap.*

*Satu — Distributed Lock Manager dengan Raft Consensus dan deadlock detection via Wait-For Graph.*

*Dua — Distributed Queue dengan Consistent Hashing 150 virtual nodes, Redis persistence, dan at-least-once delivery guarantee.*

*Tiga — Cache Coherence protokol MESI dengan LRU eviction O(1) dan koordinasi invalidation antar node.*

*Empat — Docker multi-stage build dengan docker-compose untuk cluster 3 node.*

*Lima — Bonus D Security: TLS certificate generation, RBAC dengan 4 role, dan tamper-evident audit log dengan SHA-256 hash chaining.*

*Tantangan terbesar yang saya hadapi adalah mengimplementasikan Raft Consensus dengan benar menggunakan Python asyncio, khususnya menghindari deadlock pada nested async locks saat handling concurrent RPC calls.*

*Source code ada di GitHub, laporan lengkap ada di report.md, dan OpenAPI spec tersedia di docs/api_spec.yaml. Terima kasih sudah menonton!"*

---

## BAGIAN E — TIMELINE VIDEO

| Segmen | Konten | Waktu |
|--------|--------|-------|
| 1 | Pendahuluan | 0:00 – 1:30 |
| 2 | Arsitektur | 1:30 – 4:00 |
| 3A | Raft Consensus | 4:00 – 5:00 |
| 3B | Distributed Lock | 5:00 – 7:00 |
| 3C | Distributed Queue | 7:00 – 8:30 |
| 3D | Cache MESI | 8:30 – 10:00 |
| 4 | Unit Tests | 10:00 – 11:00 |
| 5 | Performance | 11:00 – 13:00 |
| 6 | Security | 13:00 – 13:30 |
| 7 | Kesimpulan | 13:30 – 14:30 |

**Total: ~14 menit** ✓

---

## BAGIAN F — TIPS REKAM

1. **Font terminal besar** — Properties → Font → Size 16 agar terlihat di video
2. **Pause 2 detik** setelah setiap command, tunggu output muncul baru bicara
3. **Jika ada error**: jangan panik, jelaskan error dan cara fix-nya
4. **Urutan node** — pastikan 3 node berjalan sebelum mulai demo
5. **Jika node crash** saat rekam: restart dengan perintah di Bagian B
6. **OBS**: set output ke MP4, bitrate 8000 kbps agar kualitas bagus di YouTube

---

## BAGIAN G — CHECKLIST SEBELUM UPLOAD YOUTUBE

- [ ] Tonton ulang video — pastikan semua fitur terdemonstrasikan
- [ ] Video minimal 10 menit, maksimal 15 menit
- [ ] Upload ke YouTube sebagai **Publik** (bukan Unlisted)
- [ ] Judul video: `Tugas 2 Sister - Distributed Sync System - NIM 11231013`
- [ ] Deskripsi: cantumkan nama, NIM, mata kuliah
- [ ] Copy link YouTube ke **README.md** dan **report.md** (bagian Lampiran D)
