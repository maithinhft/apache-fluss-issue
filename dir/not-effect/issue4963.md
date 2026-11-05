# Issue #4963 — Kafka gateway: Produce mất dữ liệu âm thầm và treo response khi hai tên topic cùng trỏ vào một bucket

Link: https://github.com/apache/fluss/issues/4963 — Phiên bản được báo: `main (development)`

## Tóm tắt

Kafka gateway của Fluss cho phép một bảng Fluss được gọi bằng hai cách viết tên topic: tên ngắn (`t`, được ghép với database mặc định `kafka.default.database`) và tên đầy đủ (`fluss.t`). Nếu **một Produce request chứa cả hai tên** (cùng partition) thì chúng cùng trỏ vào một `TableBucket`. Mã xử lý dùng `TableBucket` làm khóa của hai `HashMap`, nên mục đến sau **ghi đè** mục đến trước. Hậu quả:

1. Dữ liệu của tên topic đến trước **không bao giờ được ghi** nhưng không có lỗi nào được báo (mất dữ liệu âm thầm).
2. Kết quả (future) của tên đến trước **không bao giờ được hoàn tất**, nên cả Produce response bị treo cho tới khi client hết `delivery.timeout.ms`.
3. Client retry request, và dữ liệu của tên đến sau (vốn đã được ghi) bị **ghi lại lần nữa** (trùng dữ liệu).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-kafka` (Kafka protocol gateway chạy trong TabletServer).
- **Lớp lỗi:** `GatewayKafkaProduceBackend` (`fluss-kafka/src/main/java/org/apache/fluss/kafka/backend/produce/GatewayKafkaProduceBackend.java`), cụ thể là lớp nội bộ `AppendBatch` và phương thức `write(...)`.
- **Đoạn mã gây lỗi** (đã đối chiếu trên nhánh `main`):

```java
batch.entriesPerBucket.put(bucket, converted.records());   // Map<TableBucket, MemoryLogRecords>
batch.results.put(bucket, result);                         // Map<TableBucket, CompletableFuture<PartitionResult>>
```

Cả hai map đều dùng `TableBucket` làm khóa. Hai tên topic khác nhau nhưng cùng một bảng và cùng partition thì cho ra cùng một `TableBucket`, nên lệnh `put` thứ hai đè lên lệnh thứ nhất.

- **Vì sao treo:** với mỗi partition, `write(...)` tạo một `CompletableFuture` và đưa vào danh sách `partitions` của topic đó, rồi chờ tất cả bằng `FutureUtils.combineAll`. Khi phản hồi từ `appendRecordsToLog` về, `completeResults` chỉ hoàn tất các future còn trong map `results`. Future của mục bị ghi đè không còn nằm trong map nên không ai hoàn tất nó, và `combineAll` chờ mãi.
- **Vì sao mất dữ liệu:** `entriesPerBucket` (thứ thực sự được ghi) chỉ còn bản ghi của mục đến sau. Bản đã chuyển đổi của mục đến trước vẫn nằm trong `convertedRecords` và bị giải phóng ở `close()`, nên bộ nhớ Arrow được trả đi mà dữ liệu chưa hề được ghi.

## 2. Khi nào lỗi xảy ra

Phải thỏa đồng thời:

1. **Dùng Kafka gateway của Fluss** (Kafka protocol được bật trên TabletServer) và bảng được bật cho Kafka.
2. **Một Produce request chứa cả hai cách viết tên của cùng một bảng**, ví dụ `t` partition 0 và `fluss.t` partition 0 (với `kafka.default.database = fluss`). Theo issue, bước kiểm tra hiện tại chỉ từ chối tên topic trùng nhau và partition trùng nhau trong cùng một topic, nên cặp `t` + `fluss.t` vẫn được chấp nhận.
3. **Cùng một producer dùng cả hai cách viết** với cùng một bucket. Theo issue, điều này có thể xảy ra với producer thật: phần xử lý metadata trả về một topic ID riêng cho tên ngắn để một client có thể cache cả hai tên cùng lúc, và việc dùng cả hai cách viết là cấu hình được hỗ trợ, ví dụ khi đang chuyển dần từ tên ngắn sang tên đầy đủ.
4. Hai mục phải rơi vào **cùng một request**. Nếu hai tên được gửi ở hai request khác nhau thì không bị đè.

Không ảnh hưởng: ứng dụng dùng nhất quán một cách viết tên topic, hoặc không dùng Kafka gateway (client Java/Flink của Fluss không đi qua đường này).

**Về phiên bản:** `GatewayKafkaProduceBackend` **có trên nhánh `main` nhưng không có trong tag `v1.0.0`**. Tại `v1.0.0`, module `fluss-kafka` chỉ có 33 file, gồm bộ khung nhận request (`KafkaRequestHandler`, `KafkaCommandDecoder`, `KafkaProtocolPlugin`, …) và các test, không có lớp backend của Produce. Vì vậy lỗi này **không ảnh hưởng bản phát hành 1.0.0**, chỉ ảnh hưởng nhánh đang phát triển (và các bản sẽ phát hành sau đó nếu không được sửa). Lưu ý: mình mới xác nhận lớp này không tồn tại ở 1.0.0, chưa kiểm tra bộ khung 1.0.0 xử lý Produce ra sao.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** những người chạy bản build từ `main` (hoặc bản phát hành tương lai có Kafka gateway) và dùng ứng dụng Kafka để ghi vào Fluss, đặc biệt khi đang di chuyển từ tên topic ngắn sang tên đầy đủ nên một producer dùng lẫn hai cách viết.

**Sai lệch có thể gặp:**

1. **Mất dữ liệu âm thầm.** Các bản ghi gửi theo tên đến trước không được ghi vào Fluss và không có lỗi nào trả về cho riêng chúng.
2. **Treo request.** Produce response không về cho tới khi `delivery.timeout.ms` của client hết hạn, kéo theo độ trễ lớn và có thể làm kẹt luồng gửi của producer.
3. **Ghi trùng.** Khi client retry, bản ghi theo tên đến sau (đã được ghi ở lần đầu) bị ghi thêm lần nữa. Theo mô tả trong issue; mình chưa kiểm tra liệu cơ chế idempotent producer của Kafka (nếu được hỗ trợ ở gateway) có chặn được việc này hay không.
4. **Khó phát hiện.** Phần mất dữ liệu không để lại log lỗi; chỉ thấy triệu chứng treo và retry.

**Cách tránh tạm thời:** dùng thống nhất **một** cách viết tên topic cho mỗi bảng trong cùng một producer, và không gửi lẫn hai tên trong cùng một request. Hướng sửa đề xuất trong issue: đăng ký future bằng `putIfAbsent` và trả `INVALID_REQUEST` cho mục đến sau, trước bước phân quyền và chuyển đổi dữ liệu.
