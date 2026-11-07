# Issue #1314 — `NoClassDefFoundError: org/apache/hadoop/hdfs/HdfsConfiguration` khi dùng lakehouse Paimon

Link: https://github.com/apache/fluss/issues/1314 — Phiên bản được báo: `0.7.0` (người báo: wuchong, issue gốc tạo 02/09/2026 trên repo `apache/fluss` từ báo cáo trên Slack thời 0.7). Nhãn `priority=blocker` và `priority=critical`. Có 13 bình luận, trong đó maintainer phân tích hai nguyên nhân và đề xuất 6 cải tiến, sau đó tách thành các issue con. Bình luận cuối: một người **gặp lại trên 0.9.1** với cấu hình *"never explicitly sets `hdfs://` filepaths"*, không có chi tiết. Trạng thái: còn mở. PR [#4203](https://github.com/apache/fluss/pull/4203) (báo lỗi rõ ràng khi nạp trùng plugin) **đang mở**.
**Nhóm B:** chỉ khi bật lakehouse **Paimon**, hoặc dùng nhiều plugin filesystem.

> **Mức chắc chắn:** cao về những gì đã sửa: mình đã đọc mã `v1.0.0`, file đóng gói, và trạng thái các issue con. Hai nguyên nhân maintainer phân tích **đã được giảm thiểu đáng kể** trong 1.0.0: đóng gói plugin đúng thư mục, bỏ khai báo `HadoopFsPlugin` khỏi jar s3/oss/gs, và có jar HDFS đầy đủ. Còn lại là **báo lỗi rõ ràng** khi cài sai (PR #4203), test end-to-end và hướng dẫn xử lý sự cố. Mình không biết nguyên nhân của báo cáo mới nhất (0.9.1).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`lib/` và `plugins/<tên>/`** | Hai nơi đặt jar trên server Fluss. `lib/` nằm trên classpath chính. Mỗi `plugins/<tên>/` được nạp bằng **classloader riêng**, cách ly (xem #4116). |
| **`fluss-lake-paimon`** | Plugin để Fluss tier và đọc dữ liệu với Paimon. Paimon cần các lớp Hadoop (ví dụ `HdfsConfiguration`) ngay cả khi warehouse là thư mục local. |
| **`HadoopFsPlugin`** | Lớp plugin filesystem HDFS của Fluss (`fluss-fs-hadoop`). Trước đây nó **cũng bị đóng gói kèm khai báo `META-INF/services`** trong jar s3, oss, gs, vốn không có đủ Hadoop. |
| **`NoClassDefFoundError` / `ClassNotFoundException`** | Lớp cần dùng không có trên classpath (của classloader đang dùng) khi chạy. |
| **Uber jar Hadoop** | Jar gói toàn bộ Hadoop, ví dụ `flink-shaded-hadoop-2-uber`. Người dùng hay thêm vào để chữa cháy. |

## Tóm tắt

Hai nguyên nhân theo phân tích của maintainer (thời 0.7):
1. **Đặt `fluss-lake-paimon` vào `lib/`** thay vì `plugins/paimon/`. Classpath chính không có Hadoop, nên Coordinator lỗi `HdfsConfiguration` khi khởi động. Đặt đúng `plugins/` thì chạy.
2. **Nạp nhầm plugin HDFS:** jar `fluss-fs-s3`, `-oss`, `-gs` chứa `HadoopFsPlugin` **kèm khai báo dịch vụ** nhưng không kèm Hadoop. Tùy thứ tự file, Fluss có thể nạp `HadoopFsPlugin` từ jar s3 và gặp `ClassNotFoundException`.

Trong **1.0.0**:
- nguyên nhân 2 **đã sửa**: jar s3, oss, gs loại `META-INF/**` của `fluss-fs-hadoop` khi đóng gói;
- bản phân phối đặt sẵn plugin lake và filesystem vào đúng `plugins/<tên>/`;
- có jar `fluss-fs-hdfs` đi kèm Hadoop.

Nguyên nhân 1 vẫn có thể xảy ra nếu người dùng tự chép jar sai chỗ, và Fluss **vẫn chưa báo lỗi rõ ràng** cho trường hợp đó.

## 1. Lỗi nằm ở thành phần nào

- **Thành phần:** cách đóng gói `fluss-filesystems/*`, `fluss-dist` (assembly), cơ chế nạp plugin, tài liệu cài đặt.
- **Đã xác nhận trong mã và cấu hình `v1.0.0`:**
  - `fluss-filesystems/fluss-fs-s3/pom.xml` (tương tự `fluss-fs-oss`, `fluss-fs-gs`): trong cấu hình shade có
    ```xml
    <filter>
      <artifact>org.apache.fluss:fluss-fs-hadoop</artifact>
      <excludes><exclude>META-INF/**</exclude></excludes>
    </filter>
    ```
    `META-INF/services/org.apache.fluss.fs.FileSystemPlugin` của s3 chỉ còn `S3AFileSystemPlugin` và `S3FileSystemPlugin`.
  - `fluss-dist/src/main/assemblies/plugins.xml`: jar `fluss-fs-hdfs`, `fluss-fs-oss`, `fluss-fs-s3` vào `plugins/hdfs/`, `plugins/oss/`, `plugins/s3/`; `fluss-lake-paimon`, `-iceberg`, `-lance`, `-hudi` vào `plugins/<tên>/`.
  - Thư mục `fluss-filesystems/` của 1.0.0 có cả `fluss-fs-hadoop`, `fluss-fs-hadoop-shaded` và `fluss-fs-hdfs`.
- **Trạng thái các việc con** (theo trang GitHub ngày 07/11/2026):
  - [#1322](https://github.com/apache/fluss/issues/1322) (oss), [#1323](https://github.com/apache/fluss/issues/1323) (obs), [#1324](https://github.com/apache/fluss/issues/1324) (s3): bỏ `HadoopFsPlugin` khỏi `META-INF`. **Đã đóng**, PR #1342, #1351, #1348 đã merge.
  - [#1513](https://github.com/apache/fluss/issues/1513): phát hành artifact HDFS đi kèm phụ thuộc. **Đã đóng** (PR #1522, #1755).
  - [#1514](https://github.com/apache/fluss/issues/1514): test end-to-end. **Đang mở.**
  - [#1515](https://github.com/apache/fluss/issues/1515): hướng dẫn xử lý sự cố. **Đang mở.**
  - PR [#4203](https://github.com/apache/fluss/pull/4203): báo lỗi rõ ràng (fail-fast) khi plugin bị nạp trùng. **Đang mở.**

## 2. Khi nào lỗi xảy ra

- **Trên 1.0.0, chủ yếu do cài đặt sai:**
  - chép `fluss-lake-paimon` (hoặc plugin lake hay filesystem khác) vào `lib/` của server;
  - trộn jar từ các phiên bản khác nhau.
- **Phía Flink** (job tiering, union read): `fluss-lake-paimon` hoặc Paimon trong Flink cũng cần Hadoop trên classpath của Flink. Mình suy ra từ phân tích của issue rằng Paimon cần `HdfsConfiguration`. Chưa kiểm tra cấu hình Flink của 1.0.0.
- **Báo cáo trên 0.9.1:** không đủ thông tin để biết điều kiện.

## 3. Tác động đến ai, làm sai lệch gì

- **Coordinator hoặc TabletServer không khởi động được** (lỗi ngay khi start), hoặc job tiering lỗi. Dễ phát hiện, nhưng **khó hiểu nguyên nhân**, vì thông báo lỗi chỉ là "thiếu lớp Hadoop".
- **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng nhóm Paimon trong `relate_issue.md` ghi *"#1314 (NoClassDefFoundError HDFS) ... ❌ (#1314 có PR #4203 fail-fast, chưa merge)"*. Đối chiếu:
- **Nên là 🟡**: hai nguyên nhân gốc đã được xử lý trong mã hoặc cách đóng gói (#1322, #1323, #1324, #1513). Chỉ còn phần báo lỗi rõ ràng (#4203), test và tài liệu.

## 5. Mức ảnh hưởng với CDP

- Chỉ liên quan nếu bật lakehouse Paimon.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Dùng nguyên cấu trúc `plugins/` của bản phân phối 1.0.0.** Không chép plugin lake hay filesystem vào `lib/`.
- Chỉ giữ các plugin filesystem **thực sự dùng** (ví dụ chỉ `plugins/s3/`).
- Phía Flink: làm theo tài liệu "engine-flink" và "lakehouse" của 1.0.0 về các jar cần đặt vào `FLINK_HOME/lib` (Paimon bundle, Hadoop nếu cần), và **test khởi động job tiering và union read** với đúng tổ hợp jar của production.
- Nếu gặp lỗi `HdfsConfiguration`: kiểm tra `lib/` có jar plugin nào đặt sai chỗ không, và có hai phiên bản Paimon trên classpath Flink không (xem #2045).

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện trên 1.0.0.
- Mình không biết nguyên nhân của báo cáo trên 0.9.1 trong bình luận cuối.
- Mình chưa kiểm tra yêu cầu jar phía Flink cho Paimon trong 1.0.0.
- Mình chưa tự kiểm tra `fluss-fs-obs`, `-cos`, `-azure` có cùng bộ lọc `META-INF` hay không (chỉ xem s3, oss, gs).
