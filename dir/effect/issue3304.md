# Issue #3304 — Metric `currentFetchEventTimeLag` của Flink source gây hiểu nhầm khi đọc nhiều partition hoặc bucket có độ trễ chênh nhau

Link: https://github.com/apache/fluss/issues/3304 — Phiên bản được báo: `0.9.0` (người báo: loserwang1024, 21/05/2026). Issue chỉ đúng dòng mã gây lỗi, có ví dụ và hai đề xuất. Có hai bình luận:
- lo ngại rằng nếu đổi sang "lag lớn nhất", partition cũ **không còn dữ liệu mới** sẽ báo lag cao mãi;
- đề xuất làm kiểu Kafka: theo dõi log end offset theo từng bucket, với `recordsLagMax`, `recordsLagSum`, `laggingBuckets`.

Trạng thái: còn mở. PR [#3356](https://github.com/apache/fluss/pull/3356) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã xác nhận đoạn `Math.max` trong `v1.0.0`. Mình cũng thấy 1.0.0 **đã có** một metric khác đo độ trễ **theo offset**: Flink `pendingRecords`, bằng tổng (high watermark − offset đang đọc) qua mọi bucket. Metric này dùng thay được cho mục đích giám sát. Mình chưa kiểm tra nó hiển thị thế nào trên dashboard Flink.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Event time** | Thời điểm sự kiện xảy ra. Ở đây là **timestamp của bản ghi** trong log Fluss (thời điểm ghi). |
| **`currentFetchEventTimeLag`** | Metric chuẩn của Flink source (FLIP-33): *thời điểm fetch* trừ *event time của bản ghi vừa lấy về*. |
| **`currentEmitEventTimeLag`** | Metric chuẩn của Flink source: *thời điểm phát bản ghi cho operator sau* trừ *event time của nó*. Fluss tính **theo từng bản ghi**. |
| **`pendingRecords`** | Metric chuẩn của Flink source: số bản ghi **chưa đọc**. Trong Fluss bằng tổng `recordsLag` của các bucket. |
| **`recordsLag` (Fluss scanner)** | Với mỗi bucket: `max(highWatermark − offset, 0)`. High watermark lấy từ lần fetch gần nhất. |
| **Backpressure** | Operator phía sau xử lý chậm làm source phải chờ. |
| **Một lần fetch** | Một lượt `logScanner.poll()` trả về bản ghi của **nhiều bucket** cùng lúc. |

## Tóm tắt

Khi một Flink source đọc nhiều partition hoặc bucket, có cái **đã bắt kịp** và có cái **trễ 2 giờ** (ví dụ đang backfill partition cũ):
- `currentFetchEventTimeLag` báo gần **0**;
- `currentEmitEventTimeLag` báo khoảng **2 giờ**;
- không có backpressure.

Nguyên nhân: trong mỗi lần fetch, mã lấy timestamp **lớn nhất** của bản ghi cuối cùng mỗi bucket (`Math.max`), rồi báo `fetchTimestamp − max`. Vậy lag được báo là lag **nhỏ nhất** giữa các bucket. Bucket mới (luôn có dữ liệu sẵn) che hoàn toàn bucket đang trễ.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink`, gồm `source/reader/FlinkSourceSplitReader.forLogRecords`, `source/metrics/FlinkSourceReaderMetrics`. Liên quan thêm `fluss-client`: `LogScannerStatus`, `BucketScanStatus`.
- **Đã xác nhận trong mã `v1.0.0`:**
  ```java
  long fetchTimestamp = System.currentTimeMillis();
  long maxConsumerRecordTimestampInFetch = -1;
  ...  // với mỗi bucket trong lần fetch
  maxConsumerRecordTimestampInFetch = Math.max(maxConsumerRecordTimestampInFetch, lastRecord.timestamp());
  ...
  if (maxConsumerRecordTimestampInFetch > 0) {
      flinkSourceReaderMetrics.reportRecordEventTime(fetchTimestamp - maxConsumerRecordTimestampInFetch);
  }
  ```
- **Metric thay thế đã có trong `v1.0.0` (đã đọc mã):**
  - `BucketScanStatus.recordsLag()`: `if (offset < 0 || highWatermark < 0) return 0; return Math.max(highWatermark - offset, 0);`
  - `LogScannerStatus.recordsLag()`: **tổng** `recordsLag` của mọi bucket đang đọc.
  - `AbstractLogScanner` đăng ký gauge `recordsLag` (`MetricNames.SCANNER_RECORDS_LAG`). `FlinkSourceSplitReader` lấy gauge này và gọi `flinkSourceReaderMetrics.maybeAddRecordsLagMetric(...)`, hàm này `sourceReaderMetricGroup.setPendingRecordsGauge(this::getPendingRecords)`. Vậy Flink source của Fluss **xuất `pendingRecords`** là số bản ghi còn phải đọc.

## 2. Khi nào lỗi xảy ra

- Một subtask của Flink source đọc **nhiều bucket hoặc partition** cùng lúc.
- Tiến độ giữa các bucket **chênh lệch** lớn: backfill partition cũ song song partition mới, một bucket bị nghẽn hoặc kẹt (ví dụ #1256, #4676), hoặc partition mới phát hiện được đọc từ earliest (#3548).

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người giám sát độ trễ job Flink đọc Fluss.

**Hậu quả:**
1. **Giám sát sai:** dựa vào `currentFetchEventTimeLag` sẽ tưởng job **đã bắt kịp**, trong khi một phần dữ liệu trễ hàng giờ. Alert "trễ cao" theo metric này **không bắn**.
2. **Chẩn đoán sai:** `fetchLag ≈ 0` mà `emitLag` cao dễ làm người ta nghĩ là backpressure phía sau, trong khi thật ra là bucket chậm.
3. **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3304 trong `relate_issue.md` (phần ghi chú cuối mục 4.4) gom chung với #1037 là *"độ trễ event-time của Flink source sai khi đọc nhiều partition"*. Đối chiếu:
- **Đúng** về bản chất.
- **Bổ sung:**
  - Hai issue nói về **hai metric khác nhau**: #3304 là `currentFetchEventTimeLag` báo **thấp hơn** thực tế; #1037 là `currentEmitEventTimeLag` báo **cao**.
  - 1.0.0 đã có `pendingRecords` (độ trễ theo offset) dùng thay được.

## 5. Mức ảnh hưởng với CDP

- CDP cần **near realtime**, nên độ trễ tiêu thụ của các job Flink đọc Fluss (tính segment, đồng bộ downstream) là chỉ số quan trọng. Metric sai thì phát hiện chậm.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Alert theo `pendingRecords`** (số bản ghi chưa đọc, tính theo offset) của Flink source, thay cho `currentFetchEventTimeLag`.
  - Lưu ý: đây là **tổng** của mọi bucket trong subtask, nên không thấy được bucket nào trễ.
  - High watermark lấy từ **lần fetch gần nhất** của bucket đó, nên bucket không được fetch (kẹt) có thể báo lag cũ (suy luận từ mã).
- Có thể dùng thêm `currentEmitEventTimeLag` (tính theo từng bản ghi, phản ánh bản ghi cũ nhất đang phát), nhưng xem #1037.
- **Theo dõi theo bucket:** so `currentOffset` theo bucket (Fluss source có metric theo bucket, theo mô tả issue) với log end offset lấy từ Admin API.

## 6. Những điều chưa kiểm chứng

- Mình chưa kiểm tra `pendingRecords` hiển thị đúng trên Flink UI và reporter.
- Mình chưa kiểm tra metric `currentOffset` theo bucket có trong 1.0.0 hay không (issue có nhắc).
- Mình chưa đọc diff của PR #3356.
