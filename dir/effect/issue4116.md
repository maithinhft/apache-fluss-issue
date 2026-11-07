# Issue #4116 — Các uber-jar filesystem (`fluss-fs-*`) chứa jackson và guava chưa đổi tên package, có thể đè lên thư viện của ứng dụng

Link: https://github.com/apache/fluss/issues/4116 — Phiên bản được báo: `main` (người báo: Jackeyzhe, 26/08/2026). Sau đó người báo **đo lại trên artifact 1.0.0** ở Maven Central. Issue rất chi tiết:
- bảng số lớp jackson và guava bị lộ trong 7 plugin filesystem;
- nguyên nhân trong file `pom`.

Có 3 bình luận:
- một người bổ sung rằng `fluss-fs-hadoop-shaded` cũng lộ guava và commons, đã tách ra #4143 và làm PR #4144;
- người báo đồng ý phân chia phạm vi;
- người báo **tự đính chính**: khi đặt jar đúng trong `plugins/` của **server**, các lớp này **không** đè lên thư viện của server, vì plugin classloader cách ly.

Trạng thái: còn mở. Các PR liên quan [#4073](https://github.com/apache/fluss/pull/4073) (s3) và [#4144](https://github.com/apache/fluss/pull/4144) (cả họ filesystem) đều **chưa merge**.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và thấy một điểm **người báo chưa xét**: phía **client** (Flink job, ứng dụng Java), `FlussConnection` khởi tạo filesystem **không có plugin manager**. Plugin filesystem được nạp bằng `ServiceLoader` **từ classpath của ứng dụng**. Nghĩa là ở client, jar `fluss-fs-*` **bắt buộc nằm trên classpath chung**, và kịch bản "đè thư viện" **là kịch bản mặc định**, không phải chuyện cài sai. Mình chưa tái hiện xung đột cụ thể.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Uber-jar (fat jar)** | Một file jar gói luôn các thư viện phụ thuộc (hadoop, AWS SDK...) vào bên trong. |
| **Shade / relocate** | Đổi tên package của thư viện được gói, ví dụ `com.fasterxml.jackson` thành `org.apache.fluss.shaded...jackson`, để **không đụng** với bản jackson mà ứng dụng tự dùng. |
| **Jackson / Guava** | Hai thư viện Java cực phổ biến (JSON, tiện ích). Spring Boot, Flink connector và SDK nào cũng có thể mang một phiên bản riêng. |
| **Plugin filesystem `fluss-fs-*`** | `fluss-fs-s3`, `-hdfs`, `-oss`, `-gs`, `-azure`, `-obs`, `-cos`: để Fluss đọc ghi **remote storage**. |
| **`PluginClassLoader` (server)** | Trên **server**, jar đặt trong `${FLUSS_HOME}/plugins/<scheme>/` được nạp bằng classloader riêng, có parent là platform/bootstrap. Chỉ các package "owner-first" (`java.`, `org.apache.fluss.`, `org.apache.hadoop.`...) là thấy được từ host. |
| **`ServiceLoader` trên classpath (client)** | Cơ chế Java tìm các implementation khai báo trong `META-INF/services` **trên classpath hiện có**. Không có cách ly. |
| **`NoSuchMethodError` / `NoClassDefFoundError`** | Lỗi khi chạy: lớp được nạp là phiên bản khác với phiên bản mà code được biên dịch. |

## Tóm tắt

Cả 7 plugin filesystem của 1.0.0 chứa hàng nghìn lớp `com.fasterxml.*` (jackson-databind **2.15.3**) và `com.google.*` (guava) **không đổi tên package** (số liệu người báo đo trên jar 1.0.0 từ Maven Central). Nguyên nhân: cấu hình `maven-shade-plugin` của từng plugin chỉ relocate `org.apache.commons`.

- **Trên server**, jar đặt đúng trong `plugins/` thì được cách ly: không đè lên jackson hay guava của server (theo đính chính của người báo, khớp với mã `PluginLoader` và `ComponentClassLoader`).
- **Trên client** (theo mã 1.0.0): `FlussConnection` gọi `FileSystem.initialize(conf, null)`, **không có plugin manager**, nên filesystem được tìm bằng `ServiceLoader.load(FileSystemPlugin.class, FileSystem.class.getClassLoader())`, tức **classpath của ứng dụng**. Muốn job Flink hay service Java đọc được dữ liệu trên S3 (remote log, KV snapshot), phải đặt `fluss-fs-s3` **vào classpath chung**, và khi đó jackson và guava 2.15.3 không shade **nằm chung** với thư viện của ứng dụng.

## 1. Lỗi nằm ở thành phần nào

- **Thành phần:** `fluss-filesystems/*/pom.xml` (cấu hình shade). Phía client là `fluss-client` `FlussConnection` và `fluss-common` `fs/FileSystem.initialize`.
- **Đã xác nhận trong mã `v1.0.0` (client):**
  - `FlussConnection(Configuration conf, ...)`:
    ```java
    // init Filesystem with configuration from FlussConnection, only pass options with 'client.fs.' prefix
    FileSystem.initialize(Configuration.fromMap(extractAndRemovePrefix(..., CLIENT_PREFIX + "fs.")), null);
    ```
  - `FileSystem.initialize(config, pluginManager)`: luôn thêm nguồn `ServiceLoader.load(FileSystemPlugin.class, FileSystem.class.getClassLoader())`. **Chỉ khi** `pluginManager != null` mới thêm nguồn từ plugin (`PluginFileSystemWrapper`). Phía client truyền `null`.
  - Không thấy `fluss-flink` khai báo phụ thuộc vào `fluss-fs-*`, tức connector Flink **không đóng gói sẵn** plugin filesystem. Người dùng phải tự thêm jar.
- **Tài liệu 1.0.0** (`website/docs/maintenance/tiered-storage/filesystems/s3.md`): mô tả server dùng `AssumeRole` để *"generate temporary credentials for clients (Flink/Spark connectors) that read tiered data from S3"*. Đây xác nhận client **tự đọc S3** (xem thêm #5012 về token).
- **Phía server (theo đính chính của người báo, khớp với mã):** plugin trong `plugins/` được nạp bằng `PluginClassLoader` (component-only). `com.fasterxml` và `com.google` không nằm trong danh sách parent-first mặc định, nên không đè lên host.

## 2. Khi nào lỗi xảy ra

- **Client (mặc định):** job Flink hoặc ứng dụng Java dùng Fluss client và **cần đọc remote storage**. Ví dụ:
  - job đọc log **cũ** đã tier lên S3;
  - Flink batch scan bảng PK (đọc **KV snapshot** từ remote);
  - union read với lake.

  Khi đó jar `fluss-fs-*` phải nằm trên classpath (Flink `lib/` hoặc fat jar của job). Xung đột chỉ lộ ra nếu **ứng dụng hoặc connector khác** cũng mang jackson hay guava **khác phiên bản** và dùng API không tương thích.
- **Server:** chỉ khi đặt jar sai chỗ (vào `lib/` thay vì `plugins/`).

**Phiên bản:** báo trên `main`, và người báo đã xác nhận có trong artifact `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** chủ yếu **phía client**, gồm job Flink và service Java (ví dụ service AI agent viết bằng Spring Boot) cần đọc dữ liệu trên remote storage.

**Hậu quả:**
1. **Lỗi khi chạy** (`NoSuchMethodError`, `NoClassDefFoundError`) hoặc **sai lệch phiên bản ngầm**, tùy bản nào thắng khi nạp lớp. Spring Boot 3.x dùng jackson 2.15 trở lên, và hành vi phụ thuộc thứ tự classpath.
2. Với Flink: Flink tự shade jackson của nó (`flink-shaded-jackson`), nên bản thân Flink thường không bị. Nhưng **user code và connector khác** trong cùng job thì có thể bị (suy luận).
3. **Không ảnh hưởng dữ liệu.** Lỗi thường lộ ra lúc khởi động hoặc lúc lần đầu dùng API bị đè.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` gom #4116 vào ghi chú cuối mục 4.4 (*"Uber-jar `fluss-fs-*` lộ jackson/guava"*, 🟢). Đối chiếu:
- **Không có fix trong 1.0.0:** đúng.
- **Bổ sung:**
  - Đính chính của người báo ("đặt đúng `plugins/` thì không đè") **chỉ đúng cho server**.
  - Ở **client**, mã 1.0.0 nạp filesystem từ classpath, nên jar buộc phải nằm chung với thư viện ứng dụng. Rủi ro xung đột là **mặc định**, không phải do cài sai.
  - Mức 🟢 vẫn hợp lý nếu ứng dụng không xung đột phiên bản, nhưng cần kiểm tra.

## 5. Mức ảnh hưởng với CDP

- CDP có remote storage (S3 hoặc MinIO) và có **job Flink** đọc dữ liệu cũ hoặc batch scan, và có thể có **service AI agent** đọc dữ liệu cũ qua Java client. Cả hai đều cần `fluss-fs-s3` trên classpath.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Phía server:** đặt `fluss-fs-*` **chỉ trong `plugins/<scheme>/`**.
- **Phía Flink:** đặt `fluss-fs-s3` vào `lib/` của Flink (để dùng chung cho mọi job) thay vì gói vào fat jar của từng job. Kiểm tra job với đúng tổ hợp connector sẽ dùng.
- **Phía service Java (AI agent):**
  - chạy `mvn dependency:tree` và các plugin như `maven-enforcer` (`banDuplicateClasses`) để phát hiện lớp trùng giữa `fluss-fs-s3` và jackson hay guava của ứng dụng;
  - nếu xung đột, **tự relocate** bằng shade plugin trong build của service, hoặc tách việc đọc dữ liệu cũ (cần filesystem) sang một tiến trình riêng.
  - AI agent chỉ **lookup** (đọc KV qua RPC, không đọc remote storage) thì có thể **không cần** `fluss-fs-*` trên classpath (suy luận).
- Xem thêm #4072 (riêng s3) và #3553 (jackson trong chính `fluss-client`).

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện xung đột jackson cụ thể với Spring Boot hoặc Flink.
- Mình chưa kiểm tra chắc chắn rằng lookup thuần **không** chạm tới `FileSystem` (mới đọc đường khởi tạo).
- Mình chưa kiểm tra các engine khác (Spark connector) nạp filesystem thế nào.
- Mình chưa tự đếm số lớp trong jar 1.0.0 (dựa vào số liệu của người báo).
