# Issue #1037 — Khi đọc bảng nhiều partition, `currentEmitEventTimeLag` của job Flink luôn cao

Link: https://github.com/apache/fluss/issues/1037 — Phiên bản được báo: `0.6.0` (người báo: swuferhong, 09/06/2025, nhãn `priority=critical`). Issue **chỉ có một câu và một ảnh biểu đồ** (không đọc được dạng chữ). Không có bình luận, không có PR. Trạng thái: còn mở.

> **Mức chắc chắn:** thấp. Issue không nêu điều kiện, cấu hình hay con số. Mình đã đọc mã `v1.0.0` để biết metric này được tính từ đâu, rồi đưa ra các giả thuyết về khi nào nó "cao" một cách hợp lý và khi nào cao một cách bất thường. Mình **chưa tái hiện**, nên không kết luận được đây có phải lỗi hay không.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`currentEmitEventTimeLag`** | Metric chuẩn của Flink source (FLIP-33): *thời điểm source phát bản ghi cho operator sau* trừ *timestamp của bản ghi đó*. **Flink tự tính** từ timestamp mà source gắn khi `collect(record, timestamp)`. |
| **Timestamp của bản ghi Fluss** | Mỗi bản ghi trong log có timestamp, theo mặc định là **thời điểm batch được ghi vào Fluss**. Đây là `ScanRecord.timestamp()`, không phải cột event time của nghiệp vụ. |
| **`currentFetchEventTimeLag`** | Metric tương tự nhưng tính lúc **lấy về** (xem #3304). |
| **Bảng nhiều partition** | Ví dụ bảng chia theo ngày. Mỗi subtask Flink có thể đọc bucket của nhiều partition cùng lúc. |
| **Partition discovery / earliest** | Từ #3548 (có trong 1.0.0), partition **mới phát hiện** được đọc **từ đầu** (earliest), không theo `scan.startup.mode`. |

## Tóm tắt

`currentEmitEventTimeLag` = "bây giờ" trừ **timestamp ghi vào Fluss** của bản ghi vừa phát ra. Metric này cao khi source **đang phát bản ghi cũ**. Với bảng nhiều partition, có vài lý do hợp lý khiến điều đó xảy ra:
- đang đọc lại partition cũ (job khởi động từ earliest, hoặc backfill);
- partition mới được đọc từ đầu sau khi phát hiện (#3548);
- một bucket hoặc partition bị chậm, và bản ghi cũ của nó cứ xen kẽ được phát ra.

Nếu job **đã bắt kịp mọi partition** mà metric vẫn "luôn cao", đó mới là bất thường. Issue không đủ thông tin để biết trường hợp của người báo thuộc loại nào.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink`, gồm `source/emitter/FlinkRecordEmitter`, `source/reader/FlinkSourceSplitReader`. Phần tính metric nằm trong Flink (`SourceOutput` / `SourceReaderBase`), không phải trong Fluss.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkRecordEmitter.processAndEmitRecord`:
    ```java
    long timestamp = scanRecord.timestamp();
    if (timestamp > 0) { sourceOutput.collect(record, timestamp); }
    else { sourceOutput.collect(record); }
    ```
    Timestamp gắn cho bản ghi là timestamp **trong log Fluss**.
  - Fluss tự báo `currentFetchEventTimeLag` (`FlinkSourceReaderMetrics.reportRecordEventTime`, xem #3304), còn `currentEmitEventTimeLag` thì để Flink tự tính.
- **Giả thuyết về cái "luôn cao" bất thường (chưa kiểm chứng):**
  1. **Partition không còn dữ liệu mới:** không ảnh hưởng, vì không phát bản ghi nào thì không làm metric thay đổi.
  2. **Nhiều partition cùng có dữ liệu, nhưng một partition đọc chậm:** bản ghi cũ của partition chậm liên tục được phát xen kẽ, nên metric (giá trị của bản ghi phát gần nhất) dao động và có lúc rất cao.
  3. **Bảng log nhận dữ liệu với timestamp cũ:** nếu timestamp ghi vào Fluss đến từ dữ liệu nguồn chứ không phải giờ ghi, lag phản ánh độ trễ nguồn. Mình chưa kiểm tra Fluss có cho phép client đặt timestamp bản ghi hay không.

## 2. Khi nào lỗi xảy ra

Theo issue: đọc bảng nhiều partition (không nêu thêm điều kiện).

**Phiên bản:** báo trên `0.6.0`. Không thấy thay đổi nào sửa trực tiếp trong `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

- **Không ảnh hưởng dữ liệu.**
- **Alert hoặc dashboard độ trễ có thể báo cao** (nếu là lỗi), hoặc **phản ánh đúng** việc đang đọc dữ liệu cũ (nếu không phải lỗi). Người vận hành khó phân biệt nếu chỉ nhìn metric này.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng ghi chú trong `relate_issue.md` gom #1037 cùng #3304: *"độ trễ event-time của Flink source sai khi đọc nhiều partition"*. Đối chiếu:
- **Chưa chắc là "sai".** Issue không có đủ thông tin. Theo cơ chế, metric cao khi đang đọc dữ liệu cũ, và đó là đúng.
- #3304 (fetch lag báo **thấp** do lấy max timestamp) là lỗi đã được xác định rõ. #1037 thì **chưa**.

## 5. Mức ảnh hưởng với CDP

- **Thấp**, chủ yếu là diễn giải metric.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi thấy `currentEmitEventTimeLag` cao, kiểm tra **`pendingRecords`** (số bản ghi còn phải đọc theo offset, xem #3304):
  - `pendingRecords` lớn: job thật sự đang trễ hoặc đang đọc dữ liệu cũ;
  - `pendingRecords` gần 0 mà emit lag vẫn cao: đáng nghi, cần điều tra hoặc báo upstream.
- Khi khởi động job đọc bảng partition, dự kiến emit lag cao trong lúc đọc lại dữ liệu cũ (nhất là partition mới đọc từ earliest theo #3548). Đặt alert có điều kiện thời gian chạy hoặc kết hợp với `pendingRecords`.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện, không biết cấu hình và tình huống của người báo.
- Mình chưa kiểm tra cách Flink tính `currentEmitEventTimeLag` khi có nhiều split xen kẽ (giá trị cuối hay giá trị lớn nhất).
- Mình chưa kiểm tra timestamp của bản ghi đến từ **lake split** (union read) được gắn thế nào.
- Mình chưa kiểm tra Fluss có cho client đặt timestamp bản ghi hay không.
