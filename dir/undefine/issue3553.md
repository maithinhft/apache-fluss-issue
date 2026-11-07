# Issue #3553 — Jar của Fluss chứa jackson-core "lọt" ra ngoài, dễ gây `NoSuchMethodError` trong ứng dụng

Link: https://github.com/apache/fluss/issues/3553 — Phiên bản được báo: `main` (development). Trong phần môi trường, người báo (binary-signal) ghi đã gặp ở `0.9.0-incubating` / `0.9.1-incubating` và tái hiện cả trên `1.0-SNAPSHOT` (Flink 2.2, Java 21, jackson của ứng dụng 2.18.2). Tiêu đề: *"fluss-client, fluss-flink-* uber-jars ship unrelocated jackson-core"*. Trạng thái: còn mở. Có 1 bình luận: Jackeyzhe xin nhận làm. PR sửa: [#3884](https://github.com/apache/fluss/pull/3884) *"Fix shaded Jackson classes leaking into uber-jars"*, đang **mở (chưa merge)**.

> **Lưu ý quan trọng (khác với `relate_issue.md` và với mô tả trong issue):** mình tải trực tiếp jar `1.0.0` từ Maven Central và liệt kê nội dung. Kết quả cho thấy **mô tả của issue không đúng với jar `fluss-client` và `fluss-flink-*` đã phát hành**. Các jar này **không** chứa lớp jackson chưa đổi tên. Cái bị lọt thật là các jar plugin hệ thống file như `fluss-fs-s3`. Chi tiết ở mục 1 và 4.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Jackson** | Thư viện Java đọc/ghi JSON, YAML. `jackson-core` là phần nền, `jackson-databind` là phần ánh xạ đối tượng. Hầu như mọi ứng dụng Java/Spring đều có. |
| **Uber-jar (fat jar)** | Một file jar gộp luôn các thư viện phụ thuộc vào trong, để người dùng chỉ cần thêm một jar. |
| **Classpath** | Danh sách jar/thư mục JVM tìm lớp (class). Hai jar cùng chứa một lớp trùng tên thì **jar đứng trước thắng**. |
| **Shading / relocation (đổi tên gói)** | Khi gộp thư viện vào uber-jar, đổi tên gói của nó (ví dụ `com.fasterxml.jackson` → `org.apache.fluss.shaded.jackson2.com.fasterxml.jackson`) để không đụng thư viện cùng tên của ứng dụng. |
| **"Lọt" / "leak"** | Một bản sao chưa đổi tên (còn tên gói gốc `com.fasterxml.jackson...`) bị gộp vào uber-jar, nên đụng với jackson của ứng dụng. |
| **`NoSuchMethodError`** | Lỗi lúc chạy: mã gọi một hàm mà lớp được nạp **không có**. Thường do trộn hai phiên bản của cùng một thư viện (mã biên dịch với bản mới nhưng JVM nạp lớp từ bản cũ). |
| **Fluss plugin (`fluss-fs-*`)** | Jar hỗ trợ hệ thống file từ xa (S3, OSS, HDFS, GCS, Azure, OBS...). Fluss nạp chúng bằng **classloader riêng của plugin** (`PluginClassLoader`), không trộn với classpath chính. |
| **Parent-first** | Quy tắc classloader: lớp khớp tiền tố nào đó thì nạp từ classloader cha trước. Mặc định của Fluss: `java.`, `org.apache.fluss.`, `javax.annotation.`, `org.apache.hadoop.`, `core-site` và các mẫu logging. **Không** có `com.fasterxml.`. |
| **Multi-Release JAR (`META-INF/versions/N/`)** | Jar chứa thêm lớp riêng cho từng phiên bản Java. Chỉ có hiệu lực nếu manifest khai báo `Multi-Release: true`. |

## Tóm tắt

Người báo gặp `NoSuchMethodError` khi dùng jackson 2.18 của ứng dụng cùng với jar Fluss, và kết luận nguyên nhân là jar `fluss-client`/`fluss-flink-*` chứa một bản `jackson-core` 2.15.x **chưa đổi tên gói**, nên đè lên jackson của ứng dụng. Khi mình kiểm tra các jar đã phát hành, điều này **không đúng với `fluss-client` và `fluss-flink-*`** (0 lớp chưa đổi tên, dù ở 0.9.1 hay 1.0.0). Nhưng cùng loại lỗi **có thật ở các jar plugin hệ thống file** (`fluss-fs-s3`, `fluss-fs-oss`, `fluss-fs-hdfs`...) vốn đóng gói jackson-core 2.15.3 chưa đổi tên. Các jar này được Fluss nạp bằng classloader riêng, nên chỉ gây lỗi nếu bạn đưa chúng lên classpath chung của ứng dụng.

## 1. Lỗi nằm ở thành phần nào

- **Module:** quá trình đóng gói (Maven shade) của các module `fluss-client`, `fluss-flink-*`, `fluss-fs-*`... Đây là lỗi **build/đóng gói**, không phải logic chạy.
- **Cách mình kiểm chứng (tải jar `1.0.0` từ Maven Central, đếm lớp trong jar):**

| Jar `1.0.0` | Lớp `com/fasterxml/jackson/...` chưa đổi tên | Lớp đã đổi tên (`org/apache/fluss/shaded/jackson2/...`) |
|---|---|---|
| `fluss-client` | **0** | 1.167 |
| `fluss-flink-1.18 / 1.19 / 1.20 / 2.2` | **0** | 1.167 |
| `fluss-server`, `fluss-spark-3.5_2.12` | **0** | 1.167 |
| `fluss-flink-tiering`, `fluss-kafka`, `fluss-lake-paimon` | 0 | 0 (không đóng gói jackson) |
| **`fluss-fs-s3`** | **1.033** (trong đó 185 lớp `jackson-core`) | 0 |
| **`fluss-fs-oss`, `fluss-fs-hdfs`, `fluss-fs-gs`, `fluss-fs-azure`** | **1.033** | 0 |
| **`fluss-fs-obs`** | **1.113** | 0 |
| **`fluss-lake-lance`** | **1.091** | 0 |

  Riêng `fluss-client` bản `0.9.1-incubating` cũng có 0 lớp chưa đổi tên. Mình đã đọc phiên bản `jackson-core` trong `fluss-fs-s3` từ lớp `PackageVersion`: **2.15.3**, đúng như issue nói về "2.15.x".
- **Kiểm tra bằng JVM thật (Java 17):** chỉ để `fluss-client-1.0.0.jar` lên classpath, nạp `com.fasterxml.jackson.core.JsonToken` → **không tìm thấy**, còn `org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonToken` → nạp được từ jar. Với `fluss-fs-s3-1.0.0.jar`, kết quả ngược lại: `com.fasterxml.jackson.core.JsonToken` nạp được từ chính jar đó. Nghĩa là jar `fluss-fs-s3` **thật sự** làm lộ jackson 2.15.3 ra classpath nếu nó nằm trên classpath.
- **Một chi tiết nhỏ ở `fluss-client`/`fluss-flink-*`:** có 10 file `.class` của jackson (`doubleparser`) nằm trong `META-INF/versions/11|17|19/com/fasterxml/...` chưa đổi tên. Manifest không có `Multi-Release: true`, và thử nạp `com.fasterxml.jackson.core.io.doubleparser.FastDoubleSwar` từ jar này trên Java 17 → **không nạp được**. Nên chúng là file thừa, vô hại. Mình chỉ thử trên Java 17, chưa thử Java 21 như người báo.
- **Hướng sửa theo PR #3884:** (theo tiêu đề) chặn các lớp jackson chưa đổi tên lọt vào uber-jar. Mình **chưa đọc diff** của PR nên không biết nó có xử lý các jar `fluss-fs-*` hay chỉ `fluss-client`/`fluss-flink-*`.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Một jar Fluss chứa jackson chưa đổi tên nằm trên classpath chung với ứng dụng.** Theo kiểm chứng của mình, chỉ có thể là các jar `fluss-fs-*` (hoặc `fluss-lake-lance`), **không** phải `fluss-client`/`fluss-flink-*` của `1.0.0`.
2. **Jackson của ứng dụng có phiên bản mới hơn 2.15.3** (issue: 2.18.2) và dùng tính năng chỉ có ở bản mới (ví dụ `_updateToken` thêm từ jackson 2.16).
3. **Jar Fluss đứng trước jackson của ứng dụng** trên classpath (hoặc cùng cấp, tùy thứ tự nạp), nên lớp 2.15.3 thắng.

**Điều chưa giải thích được:** người báo nói tái hiện với "fluss-client, fluss-flink-*" trên 0.9.x và SNAPSHOT. Mình chỉ kiểm tra bản đã phát hành. Có thể trong tình huống của họ, thứ gây lỗi là một jar `fluss-fs-*` (ví dụ `fluss-fs-s3`, mà chính họ nhắc tên) hoặc bản SNAPSHOT/tự build khác bản release. Đây là suy đoán của mình, chưa xác nhận.

**Phiên bản:** `1.0.0` còn lỗi ở các jar `fluss-fs-*` và `fluss-lake-lance`. Phần `fluss-client`/`fluss-flink-*` của `1.0.0` thì sạch theo kiểm tra của mình.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người đặt jar `fluss-fs-*` lên classpath chung với ứng dụng có jackson mới hơn 2.15.3. Ví dụ: đưa `fluss-fs-s3` vào `lib/` của Flink, hoặc làm dependency Maven của dịch vụ Spring. Với server Fluss, các jar này được cài vào thư mục plugin và nạp bằng classloader riêng; do quy tắc parent-first mặc định không có `com.fasterxml.`, jackson trong plugin **không** đè lên jackson của phần còn lại (suy luận từ mã `PluginLoader` và cấu hình, mình chưa tái hiện).

**Hậu quả:**
1. **Lỗi lúc chạy**, thường là `NoSuchMethodError`/`NoClassDefFoundError` ở phần của ứng dụng dùng jackson (đọc YAML/JSON), có thể xảy ra ngay khi khởi động.
2. **Khó chẩn đoán:** lỗi nằm trong mã của ứng dụng chứ không trong Fluss, và phụ thuộc thứ tự classpath.
3. **Không làm sai dữ liệu**, chỉ làm ứng dụng không chạy hoặc hỏng một chức năng.

## 4. Đính chính so với `relate_issue.md`

Dòng #3553 ghi: *"`fluss-client`, `fluss-flink-*` chứa jackson-core không shade, gây `NoSuchMethodError` — PR #3884 chưa merge — Service AI agent (Spring/Jackson mới) và job Flink có thể lỗi classpath"*. Đối chiếu:
- **Phần "fluss-client, fluss-flink-* chứa jackson-core không shade" không khớp với jar đã phát hành** (0.9.1 và 1.0.0): 0 lớp jackson chưa đổi tên. Đây là lời của issue, mình đã lặp lại trong `relate_issue.md` mà chưa kiểm chứng.
- **Rủi ro "Service AI agent (Spring/Jackson mới)" nếu chỉ dùng `fluss-client`** vì vậy thấp hơn mô tả. Với job Flink dùng `fluss-flink-*` cũng vậy.
- **Rủi ro thật nằm ở `fluss-fs-*` và `fluss-lake-lance`** khi lên classpath chung. Dòng này nên nêu rõ điều đó.
- Trạng thái "❌ PR #3884 chưa merge" đúng (PR đang mở).

## 5. Mức ảnh hưởng với CDP

- **Dịch vụ AI agent (Spring/Jackson mới) chỉ dùng `fluss-client`:** theo kiểm tra jar `1.0.0`, **không** dính lỗi này. Vẫn nên chạy `mvn dependency:tree` để chắc nó không kéo thêm jar `fluss-fs-*`.
- **Job Flink:** `fluss-flink-*` sạch. Nếu cần đọc dữ liệu tầng lưu trữ từ xa (S3...) và bạn đặt `fluss-fs-s3` vào `lib/` của Flink thay vì thư mục `plugins/` của Flink, bạn có thể rơi vào lỗi này. Mình chưa kiểm tra cách client Flink của Fluss cần nạp `fluss-fs-*` và nên đặt ở đâu.
- **Kubernetes server:** jar `fluss-fs-*` cài ở thư mục plugin của Fluss, chạy trong classloader riêng, nên ít khả năng ảnh hưởng.

**Biện pháp giảm thiểu (suy ra từ kiểm chứng ở trên, chưa thử trong hệ thống của bạn):**
- **Đặt `fluss-fs-*` vào thư mục plugin**, không để chung classpath với ứng dụng.
- **Kiểm tra uber-jar của chính bạn:** `unzip -l fluss-*.jar | grep "com/fasterxml/jackson"` để biết jar nào làm lộ lớp jackson; hoặc `mvn dependency:tree -Dincludes=com.fasterxml.jackson.core`.
- **Nếu buộc phải để chung classpath:** đặt jackson của ứng dụng lên trước, hoặc tự relocate (shade) jar Fluss trong bản build của bạn. Đây là giải pháp thông thường, chưa kiểm chứng với Fluss.
- **Thử khởi động ứng dụng thật** (đọc cấu hình YAML) với đúng bộ jar sẽ triển khai trước go-live.
- Theo dõi PR #3884.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện lỗi `NoSuchMethodError` của người báo; chỉ chứng minh được **sự có mặt** của jackson chưa đổi tên trong `fluss-fs-*` và **sự vắng mặt** trong `fluss-client`/`fluss-flink-*` (đối chiếu bằng cách liệt kê jar và nạp lớp bằng JVM Java 17).
- Mình không biết vì sao người báo thấy lỗi với "fluss-client, fluss-flink-*": có thể do bản SNAPSHOT/tự build, hoặc nhầm jar, hoặc do jar khác trên classpath của họ.
- Mình chưa kiểm tra bản `0.9.0-incubating` và chưa đọc diff PR #3884.
- Mình chưa kiểm tra cách client (không phải server) nạp `fluss-fs-*` khi đọc từ tầng lưu trữ từ xa.
- Chưa xem xét các jar khác như `fluss-lake-iceberg` (không có lớp jackson chưa đổi tên ở gốc, nhưng có rất nhiều đường dẫn `fasterxml` khác chưa xác định là gì).
