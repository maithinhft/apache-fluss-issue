# Issue #3691 — Job Flink đọc bảng có lakehouse vẫn đọc file Iceberg cũ dù đã đặt `table.log.ttl`, và lỗi `ConnectionClosedException` khi tải file Parquet lớn

Link: https://github.com/apache/fluss/issues/3691 — Phiên bản được báo: `0.9.0` / `0.9.1-incubating` (người báo: Ebaharloo, 18/07/2026). Môi trường: Kubernetes, Flink 2.2, 1 Coordinator, 2 TabletServer, Nessie catalog, Iceberg trên MinIO (S3). Issue mô tả kỹ hiện tượng và 6 quan sát. Không có bình luận hay phản hồi, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bảng bật lakehouse (ở đây là Iceberg).

> **Mức chắc chắn:** cao về **phần "vì sao đọc file cũ"**: mình đã đọc `FlinkTableSource` của `v1.0.0`, và đọc file lake cũ là **đúng thiết kế** của chế độ mặc định. Thấp về **phần "vì sao tải file bị đứt"**, vì chưa ai phân tích. Mình nghi do kết nối S3/MinIO khi tải file lớn và việc reader không thử lại, nhưng chưa kiểm chứng.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`scan.startup.mode`** | Chế độ bắt đầu đọc của Flink source: `full` (**mặc định**), `earliest`, `latest`, `timestamp`. |
| **`full` + lake = union read** | Với bảng bật lake, `full` nghĩa là **đọc toàn bộ lịch sử từ lake** (file Parquet của Iceberg) **rồi nối tiếp** bằng log Fluss mới hơn. |
| **`table.log.ttl`** | Thời gian giữ log **trong Fluss** (local hoặc remote). Không giới hạn dữ liệu **trong lake**. Lake giữ tới khi bạn dọn bằng công cụ của lake. |
| **Parquet** | Định dạng file dữ liệu của Iceberg. Một file có thể hàng trăm MB. |
| **`ConnectionClosedException: Premature end of Content-Length delimited message body`** | Kết nối HTTP tới S3/MinIO bị đóng **giữa chừng** khi đang tải file (đã nhận 135 MB trên 442 MB). |
| **`EXPLAIN PLAN`** | Lệnh Flink hiển thị kế hoạch thực thi. Với Fluss source, nó chỉ hiện `TableSourceScan`, không nói có đọc lake hay không. |

## Tóm tắt

Người dùng có bảng `table.log.ttl = '24 h'`, `table.datalake.enabled = true`, `format = iceberg`. Một job Flink streaming `INSERT INTO ... SELECT ... FROM` bảng đó, tức đọc với chế độ mặc định. Sau vài phút tới vài giờ, job **luôn** lỗi khi tải một file Parquet **cũ vài ngày** (khoảng 442 MB). Restart lại lỗi tương tự. Đọc với `scan.startup.mode = 'latest'` thì không lỗi. Người báo kỳ vọng `table.log.ttl = 24h` nghĩa là không đọc dữ liệu cũ hơn 24 giờ.

Theo mã 1.0.0: chế độ **mặc định `full`** cộng với bảng có lake thì **bật lake source**, tức đọc lịch sử từ Iceberg. **Đọc file cũ là đúng thiết kế.** `table.log.ttl` không liên quan tới lake. Vấn đề thực sự là:
1. tải file lớn từ MinIO bị đứt;
2. reader và job không phục hồi được, nên restart thì lại đọc lại từ đầu và gặp lại lỗi.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink` (`source/FlinkTableSource`, lake split reader), `fluss-lake/fluss-lake-iceberg` (reader Iceberg), và tầng S3 client (Hadoop S3A hoặc Iceberg `S3FileIO`, chưa xác định cái nào).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkConnectorOptions.SCAN_STARTUP_MODE`: `defaultValue(ScanStartupMode.FULL)`.
  - `FlinkTableSource`:
    ```java
    if (isDataLakeEnabled) {
        this.lakeSource = checkNotNull(createLakeSource(tablePath, tableOptions), "LakeSource must not be null if enable datalake");
    }
    ...
    case FULL:
        offsetsInitializer = OffsetsInitializer.full();
        // when it's full mode and lake source is not null, enable lake source as the historical data
        enableLakeSource = lakeSource != null;
        break;
    case LATEST: offsetsInitializer = OffsetsInitializer.latest(); break;   // không bật lake source
    ```
    Điều này giải thích quan sát số 3 của người báo (`latest` thì không lỗi).
- **Phần chưa xác định:** vì sao kết nối bị đóng giữa chừng (timeout của MinIO, load balancer, giới hạn kết nối, tài nguyên pod), và reader Iceberg có thử lại khi đứt hay không.
- Quan sát số 6 (*"bỏ `table.datalake.enabled` thì lỗi ít hơn nhưng vẫn xảy ra"*): mình **không giải thích được**. Có thể giá trị option trong catalog không đổi như người báo nghĩ, hoặc còn đường đọc lake khác. Chưa kiểm chứng.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. Bảng **bật lakehouse**, và lake đã có **dữ liệu lịch sử lớn** (file to).
2. Job Flink đọc bảng bằng **chế độ mặc định** (không đặt `scan.startup.mode`, hoặc đặt `full`).
3. Kết nối tới object store **không ổn định** khi tải file lớn.

**Phiên bản:** báo trên `0.9.x`. Hành vi `full` bật lake source vẫn như vậy ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job Flink đọc bảng có lake mà không chủ động chọn chế độ đọc.

**Hậu quả:**
1. **Job fail lặp lại**, và không phục hồi được nếu vẫn đọc từ đầu.
2. **Tốn tài nguyên và thời gian:** mỗi lần khởi động lại phải đọc lại toàn bộ lịch sử trong lake. Nếu job chỉ cần dữ liệu mới thì việc này lãng phí và chậm.
3. **Hiểu nhầm:** người dùng tưởng `table.log.ttl` giới hạn lượng dữ liệu đọc.
4. **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3691 trong `relate_issue.md` ghi: *"scan đọc file lake lịch sử bất chấp `table.log.ttl` | 🟠 | ❌"*. Đối chiếu:
- **"Bất chấp `table.log.ttl`" không phải lỗi:** đó là thiết kế của `scan.startup.mode = full` (mặc định) cộng lake. `table.log.ttl` chỉ áp cho log Fluss.
- **Lỗi thật** là việc tải file lớn bị đứt và job không phục hồi. Nguyên nhân chưa xác định, và có thể nằm ở hạ tầng object store.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu CDP bật lake và có job Flink **streaming** đọc các bảng đó.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Luôn đặt `scan.startup.mode` rõ ràng** cho job đọc bảng có lake:
  - chỉ cần dữ liệu mới: `'scan.startup.mode' = 'latest'` hoặc `'timestamp'`;
  - cần lịch sử: để `full`, nhưng chuẩn bị hạ tầng object store cho việc tải file lớn.
- **Kiểm tra object store** (MinIO, S3, Ceph): timeout của proxy hoặc load balancer, giới hạn kết nối, băng thông tới pod Flink.
- **Kiểm soát kích thước file** ở lake (compaction của Iceberg hoặc Paimon), để file không quá lớn. Cũng xem #2420 và #4940 về compaction Iceberg.
- **Restart strategy:** nếu job buộc phải đọc full, cấu hình checkpoint để tiến độ đọc lake được lưu, tránh đọc lại từ đầu sau mỗi lần lỗi. Mình chưa kiểm tra split lake có được checkpoint giữa chừng hay không.

## 6. Những điều chưa kiểm chứng

- Nguyên nhân kết nối bị đóng giữa chừng: chưa ai phân tích.
- Mình chưa kiểm tra reader Iceberg của Fluss có retry khi tải file lỗi hay không.
- Mình chưa giải thích được quan sát số 6 (tắt `datalake.enabled` vẫn còn lỗi).
- Mình chưa kiểm tra tiến độ đọc trong một split lake có được checkpoint không.
