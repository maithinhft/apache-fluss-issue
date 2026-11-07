# Issue #4072 — Uber-jar `fluss-fs-s3` chứa khoảng 1.114 lớp jackson chưa đổi tên package

Link: https://github.com/apache/fluss/issues/4072 — Phiên bản được báo: `main` (người báo: Jackeyzhe, 22/08/2026). Issue có lệnh tái hiện (build rồi đếm `com/fasterxml` trong jar ra **1114**), bảng nguồn gốc các lớp, và hướng sửa (relocate theo mẫu của `fluss-fs-hadoop-shaded`, lọc lớp MRJ). Không có bình luận. Trạng thái: còn mở. PR [#4073](https://github.com/apache/fluss/pull/4073) (cùng tác giả) **đang mở**. Đây là trường hợp riêng của [#4116](issue4116.md), cùng tác giả, và #4116 đã đo lại trên artifact 1.0.0.

> **Mức chắc chắn:** cao. Số liệu của người báo và đo lại ở #4116 trên artifact 1.0.0 khớp nhau. Mình đã đọc mã client trong `v1.0.0` (xem #4116): client nạp filesystem **từ classpath**, nên với S3 (remote storage phổ biến nhất) đây là rủi ro **phía client mặc định**. Mình chưa tự đếm lớp trong jar.

## 0. Giải thích các thuật ngữ trước

Xem bảng thuật ngữ trong [issue4116.md](issue4116.md) (uber-jar, shade/relocate, plugin classloader của server, `ServiceLoader` trên classpath của client). Bổ sung:

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`fluss-fs-s3`** | Plugin filesystem cho `s3://` (AWS S3, MinIO và các dịch vụ tương thích S3). Theo tài liệu 1.0.0, nó *"bundles Hadoop S3A 3.4.3 together with the required modular AWS SDK for Java 2.x dependencies"*. |
| **MRJ (Multi-Release JAR)** | Jar chứa nhiều phiên bản của cùng một lớp cho các phiên bản Java khác nhau, đặt trong `META-INF/versions/N/`. Maven shade plugin **không relocate** các lớp này, nên chúng "thoát" ra ngoài (cùng loại lỗi với #3553). |
| **jackson-dataformat-cbor** | Module jackson cho định dạng CBOR, đi kèm AWS SDK. |

## Tóm tắt

`fluss-fs-s3` gói `hadoop-common` và AWS SDK **mà không relocate**, nên khoảng **1.114 lớp jackson** nằm ở `com/fasterxml/*`:

| Nhóm | Số lớp (khoảng) | Nguồn |
|---|---|---|
| `jackson-databind` | 800 | hadoop-common |
| `jackson-core` | 200 | hadoop-common |
| `jackson-annotation` | 20 | hadoop-common |
| `jackson-dataformat-cbor` | 50 | aws-java-sdk-core |
| MRJ | 25 | — |

Khi ứng dụng đặt jar này cùng classpath với jackson của chính nó (ví dụ 2.16 trở lên), các lớp này có thể **đè lên**, gây `NoSuchMethodError`. Trong khi đó `fluss-fs-hadoop-shaded` đã relocate đúng các package này.

## 1. Lỗi nằm ở thành phần nào

- **Thành phần:** `fluss-filesystems/fluss-fs-s3/pom.xml` (cấu hình `maven-shade-plugin`).
- **Đường nạp phía client (đã đọc mã `v1.0.0`, xem #4116 mục 1):** `FlussConnection` gọi `FileSystem.initialize(..., null)`, rồi `ServiceLoader` trên classpath ứng dụng. Muốn client đọc `s3://...` thì `fluss-fs-s3` phải nằm trên classpath ứng dụng.
- **Đường nạp phía server:** `plugins/s3/` thì cách ly (xem #4116).
- **Hướng sửa (PR #4073):** relocate `com.fasterxml.jackson.*` (gồm cả `dataformat.cbor`) vào `org.apache.fluss.fs.shaded.s3.*`, và lọc `META-INF/versions/*/com/fasterxml/**`. Theo bình luận ở #4116, PR đã đổi sang relocate **theo đúng package có trong jar** để tránh làm hỏng tham chiếu mềm.

## 2. Khi nào lỗi xảy ra

- **Client** (job Flink, service Java) dùng remote storage **S3 hoặc MinIO**, và cần đọc dữ liệu trên đó (log cũ đã tier, KV snapshot khi batch scan, union read), đồng thời trên classpath có **jackson phiên bản khác** được dùng với API không tương thích.
- **Server** chỉ khi đặt jar sai chỗ (vào `lib/`).

**Phiên bản:** báo trên `main`. Theo #4116, có trong artifact `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

- **Lỗi khi chạy** do xung đột jackson ở client, thường lộ ra khi khởi động hoặc khi gọi API jackson mới.
- Với MinIO hoặc S3 là lựa chọn remote storage phổ biến cho CDP on-premise hoặc cloud, đây là **plugin hay dùng nhất**, nên đáng kiểm tra trước go-live.
- **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` gom #4072 cùng #4116 trong ghi chú cuối mục 4.4 (🟢, không có fix). Đối chiếu:
- **Không có fix trong 1.0.0:** đúng.
- **Bổ sung** như ở #4116: phía client, jar phải nằm trên classpath chung, nên rủi ro không chỉ khi "cài sai".

## 5. Mức ảnh hưởng với CDP

- Nếu remote storage là **S3 hoặc MinIO** (khả năng cao): cần kiểm tra classpath của job Flink và service Java.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Server: `fluss-fs-s3` chỉ trong `plugins/s3/`.
- Flink: đặt `fluss-fs-s3` vào `lib/` của Flink và test với đầy đủ connector của job.
- Service Java: kiểm tra trùng lớp (`maven-enforcer` `banDuplicateClasses`, hoặc `mvn dependency:tree`). Nếu xung đột thì tự shade lại `fluss-fs-s3` trong build của service.
- Nếu service AI agent chỉ lookup (không đọc remote storage), cân nhắc **không đưa** `fluss-fs-s3` vào classpath của nó (suy luận, xem #4116 mục 6).

## 6. Những điều chưa kiểm chứng

- Mình chưa tự đếm số lớp jackson trong jar `fluss-fs-s3-1.0.0`.
- Mình chưa tái hiện `NoSuchMethodError` với một ứng dụng thật.
- Mình chưa đọc diff của PR #4073.
