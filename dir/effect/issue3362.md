# Issue #3362 — Writer client không bao giờ dọn thông tin partition cũ, CPU tăng dần theo thời gian chạy

Link: https://github.com/apache/fluss/issues/3362 — Phiên bản được báo: `0.9.0` (người báo: loserwang1024, 22/05/2026). Issue **rất ngắn**: không kèm log, số đo, bình luận hay PR. Trạng thái: còn mở. Nó được nhắc trong issue tổng hợp [#4980](https://github.com/apache/fluss/issues/4980) (*"[Umbrella][client] Java client robustness and performance improvements"*), mục *"A re-created table leaves stale partition metadata in the client cache"*, cùng với #4741.

> **Mức chắc chắn:** đây là lỗi **thiết kế/bộ nhớ-CPU**, không phải lỗi sai dữ liệu. Mình đã đọc mã `v1.0.0` và xác nhận hai điểm "không bao giờ dọn" mà issue nêu. Mình **không có số đo** về mức CPU tăng, và **chưa chạy thử**, nên độ nghiêm trọng thực tế phụ thuộc vào số partition mà job ghi vào theo thời gian.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Partition** | Bảng chia theo giá trị cột (ví dụ theo ngày/giờ), mỗi giá trị là một partition riêng có tên và id. |
| **Writer client** | Phần của `fluss-client` ghi dữ liệu: gom bản ghi thành batch trong bộ nhớ rồi gửi cho TabletServer. Flink sink dùng nó. |
| **`RecordAccumulator`** | Bộ đệm của writer: giữ các hàng đợi batch theo từng "đường dẫn vật lý" (bảng + partition). |
| **`PhysicalTablePath`** | Định danh một bảng cụ thể hoặc một partition cụ thể của bảng (ví dụ `db.events` + `20260610`). |
| **`writeBatches`** | Bản đồ trong `RecordAccumulator`: `PhysicalTablePath` → hàng đợi batch. Mỗi partition từng được ghi có một mục. |
| **`Cluster` (metadata của client)** | Bản sao metadata client giữ trong bộ nhớ: server nào sống, bucket nào có leader ở đâu, partition nào có id gì. |
| **`Sender`** | Luồng nền của writer, lặp liên tục: kiểm tra batch nào sẵn sàng gửi, gom theo server, gửi đi. |
| **Drain (rút batch)** | Bước `Sender` lấy các batch sẵn sàng ra khỏi bộ đệm để gửi. Nó duyệt qua các bucket của một server. |
| **Auto-partition expiration** | Fluss tự xóa partition cũ khi vượt số partition giữ lại. Sau khi xóa, partition không còn tồn tại trên server. |
| **Metadata cache "không bao giờ dọn"** | Mục dữ liệu được thêm vào nhưng không có đoạn mã nào xóa khi nó hết dùng, nên bộ nhớ và công việc duyệt tăng dần. |

## Tóm tắt

Khi một job ghi chạy rất lâu vào bảng có partition, mỗi partition mới được ghi tạo ra một mục trong bộ đệm của writer và trong metadata của client. Sau khi partition đó hết hạn và bị xóa trên server, **các mục này vẫn ở lại**. `Sender` thì ở mỗi vòng lặp lại **duyệt qua toàn bộ các mục** (kể cả mục của partition đã chết), nên công việc mỗi vòng tăng dần theo số partition từng xuất hiện. Cách khắc phục duy nhất hiện nay theo issue: **khởi động lại job**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, phần ghi: `RecordAccumulator`, `Sender`, `MetadataUpdater`/`Cluster`.
- **Đã xác nhận trong mã `v1.0.0`:**
  1. **`RecordAccumulator.writeBatches`** (`ConcurrentMap<PhysicalTablePath, BucketAndWriteBatches>`): mình tìm các lệnh `remove`/`clear` trên map này và **không có**. Chỉ có `computeIfAbsent`, `putIfAbsent`, `get` và thao tác trên hàng đợi bên trong. Nghĩa là mục của mỗi partition từng ghi sẽ ở lại tới khi writer bị đóng.
  2. **Metadata của client (`Cluster`)**: khi cập nhật từng phần (`sendMetadataRequestAndRebuildCluster` với `partialUpdate = true`), mã sao chép toàn bộ `bucketLocationsByPath`, `partitionIdByPath`, `bucketCountByTableOrPartition` cũ rồi `putAll` các mục mới; **không xóa mục nào**. Đường duy nhất làm mất mục là `invalidPhysicalTableBucketMeta` / `...AndPartitionMeta`, được gọi khi `Sender` gặp lỗi metadata (xóa theo đường dẫn bị lỗi) và ở nhánh "historical partition" của `WriterClient`; không có chỗ nào dọn định kỳ các partition đã hết hạn. Cập nhật toàn phần (`partialUpdate = false`) chỉ dùng khi khởi tạo kết nối.
  3. **Nơi tốn công việc, đúng như issue chỉ:**
     - `RecordAccumulator.ready(cluster)` duyệt `writeBatches.values()` mỗi vòng.
     - `drain(...)` gọi `drainBatchesForOneNode` → `getAllBucketsInCurrentNode`, hàm này duyệt **mọi khóa** của `cluster.getBucketLocationsByPath()` và với mỗi khóa gọi `cluster.getAvailableBucketsForPhysicalTablePath(path)`. Trong mã còn comment *"TODO This is a very time-consuming operation, which will be moved to be computed in the Cluster later on."*
  Vậy công việc mỗi vòng gửi tăng tuyến tính theo số partition từng được biết tới.
- **Hướng sửa:** issue không đề xuất cụ thể; chỉ nêu cần dọn `PhysicalTablePath` trong `writeBatches` và thông tin partition trong `Cluster`. Mình không thấy PR nào cho issue này.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Bảng có partition** và job ghi **chạy rất lâu** (ngày, tuần, tháng) mà không khởi động lại.
2. **Số partition mà một writer đã từng ghi hoặc từng biết tăng liên tục**, ví dụ partition theo ngày/giờ, mỗi partition mới thêm vào, còn partition cũ bị xóa dần bởi auto-partition expiration. Với bảng không partition, hoặc partition cố định số lượng, không dính.
3. **Mức độ rõ rệt tùy tốc độ sinh partition:** partition theo ngày chỉ thêm khoảng 365 mục/năm cho mỗi writer; partition theo giờ khoảng 8.760 mục/năm; partition theo phút nhiều hơn rất nhiều. (Các con số này là tính toán số học của mình, không phải đo đạc. Mức CPU thực tế mình không biết.)

**Phiên bản:** báo trên `0.9.0`; mã `v1.0.0` không có đoạn dọn nào nên vẫn còn.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job Flink sink (hoặc dịch vụ dùng `fluss-client`) **chạy liên tục lâu** và ghi vào **bảng nhiều partition sinh ra liên tục**.

**Hậu quả (theo issue; mình chưa đo):**
1. **CPU của luồng `Sender` tăng dần theo thời gian chạy**, vì mỗi vòng duyệt nhiều mục hơn.
2. **Độ trễ gửi tăng, throughput giảm dần**, có thể làm sink tụt lại phía sau (đây là suy luận của mình từ việc `Sender` chậm đi, không phải từ issue).
3. **Bộ nhớ heap tăng nhẹ** theo số mục tồn đọng (suy luận).
4. **Không làm sai hay mất dữ liệu**, chỉ làm chậm và tốn tài nguyên. Hồi phục bằng cách khởi động lại job.
5. **Lỗi khó nhận ra:** suy giảm chậm rãi theo ngày/tuần, không có lỗi hay log cảnh báo.

**Liên hệ với các issue khác:** nếu partition hết hạn vẫn còn bị ghi (dữ liệu đến trễ), nó rơi vào #4726 (batch kẹt, spam log). #3466 là tải ZooKeeper khi partition hết hạn. Ba issue này cùng xoay quanh "client không biết partition đã chết".

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3362 ghi: *"Cache metadata partition của writer không được dọn, CPU tăng dần. ❌. Job sink chạy lâu trên bảng nhiều partition. Restart định kỳ hoặc giới hạn số partition."* Đối chiếu:
- **Đúng về cơ chế** (mục 1) và **đúng trạng thái ❌**: không thấy PR sửa riêng cho #3362. Issue nằm trong umbrella #4980 như một việc cần làm.
- **"Nhiều partition"**: cụ thể hơn là *"nhiều partition **được tạo mới liên tục** trong suốt đời của job"*. Một bảng có sẵn vài nghìn partition mà job chỉ ghi vào vài partition gần đây thì writer chỉ giữ các mục mà nó từng chạm tới (suy luận từ cách `writeBatches` được tạo theo yêu cầu); mình chưa kiểm tra việc `Cluster` có tải sẵn toàn bộ partition của bảng khi khởi tạo hay không. Đây là điểm cần kiểm chứng.
- **Mức độ ảnh hưởng chưa được đo**; issue không đưa số liệu.

## 5. Mức ảnh hưởng với CDP

- **Bảng profile PK không partition** (đường lookup và cập nhật chính): **không dính**.
- **Bảng sự kiện/context cho AI agent có partition theo ngày hoặc giờ**, với job sink Flink chạy mãi: có dính. Partition theo **ngày** thì mức tăng rất chậm (hàng trăm mục/năm), nên thường chấp nhận được nếu bạn khởi động lại job định kỳ (ví dụ khi triển khai). Partition theo **giờ** trở xuống thì rủi ro cao hơn, đặc biệt nếu job chạy nhiều tháng.
- Triệu chứng cần theo dõi: CPU của TaskManager chứa sink tăng dần, độ trễ ghi tăng, trong khi lưu lượng dữ liệu không đổi.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Khởi động lại sink định kỳ** (khi có savepoint/stateful upgrade), đây là cách duy nhất issue nêu. Với Flink, dùng savepoint rồi khởi động lại job.
- **Chọn độ mịn partition thô hơn** (ngày thay vì giờ) nếu nghiệp vụ cho phép, để số mục tăng chậm.
- **Giới hạn thời gian sống của job** theo chu kỳ triển khai (ví dụ hàng tuần) thay vì chạy mãi.
- **Theo dõi CPU của luồng Sender / TaskManager** và đặt cảnh báo khi tăng đều theo thời gian.
- **Thử tải dài ngày** (soak test) ở môi trường thử với đúng độ mịn partition và TTL sẽ dùng, trước go-live.
- Theo dõi #3362 và umbrella #4980.

## 6. Những điều chưa kiểm chứng

- Mình chưa đo CPU/bộ nhớ thực tế. Mức tăng "đáng kể" phụ thuộc số partition, mình không có số liệu.
- Mình chưa kiểm tra liệu `Cluster` của client có nạp sẵn toàn bộ partition của bảng (khi cập nhật metadata theo bảng) làm cho kích thước ban đầu đã lớn hay không.
- Mình chưa đọc cách `bucketReady` xử lý các mục có hàng đợi rỗng (có tốn công không).
- Mình chưa xác nhận ảnh hưởng đến heap (chỉ suy luận).
- Mình chưa kiểm tra bản `main` xem đã có thay đổi nào dọn cache chưa (umbrella #4980 liệt kê việc này là chưa làm).
