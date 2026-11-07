# Issue #709 — Khi tắt nhiều TabletServer, client nhận hàng loạt `OutOfOrderSequenceException` và job Flink failover

Link: https://github.com/apache/fluss/issues/709 — Phiên bản được báo: `main` thời 0.6 (người báo: swuferhong, 02/04/2025, repo cũ `alibaba/fluss`). Issue có một ảnh chụp (không đọc được dạng chữ) và **một đoạn phân tích nguyên nhân 6 bước** của chính người báo. Không có bình luận, không có PR. Trạng thái: còn mở. Issue tham chiếu tới [#676](https://github.com/apache/fluss/pull/676), là thay đổi tạo ra hiện tượng này.

> **Mức chắc chắn:** trung bình. Mình đã đọc mã `v1.0.0` và xác nhận **gốc rễ mà issue chỉ ra vẫn còn nguyên**: follower tự đặt HW bằng LEO của chính nó, kèm TODO trỏ về #673. Phía client, 1.0.0 đã thêm nhiều nhánh xử lý `OutOfOrderSequence` "giả", nhưng trường hợp không có tiến triển vẫn **reset writer và báo lỗi về Flink**. Mình **chưa tái hiện**, và có một điểm mình chưa lý giải được (xem mục 1, bước 5 của issue).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Batch sequence** | Số thứ tự (0, 1, 2…) của từng batch mà một writer gửi cho **một bucket**. Server dùng nó để phát hiện batch trùng hoặc bị thiếu. |
| **`lastAckedBatchSequence`** | Sequence lớn nhất mà **client đã nhận được xác nhận (ack)** từ server cho bucket đó. |
| **In-flight batch** | Batch đã gửi mà chưa nhận ack. Mặc định tối đa 5 batch mỗi bucket (`client.writer.max-inflight-requests-per-bucket = 5`). |
| **High watermark (HW)** | Offset tới đó dữ liệu được coi là đã cam kết. Leader chỉ ack cho writer khi HW đã vượt qua batch (với `acks=-1`). |
| **Writer state** | Server lưu, với mỗi writer, metadata của **tối đa 5 batch gần nhất** (`WriterStateEntry.NUM_BATCHES_TO_RETAIN = 5`) để nhận ra batch gửi lại. |
| **`OutOfOrderSequenceException` / `DuplicateSequenceException`** | Lỗi server trả khi batch "nhảy cóc" hoặc khi batch đã có. |
| **Reset writer ID** | Client bỏ writer ID hiện tại và đánh số lại từ đầu cho mọi bucket. Batch đang lỗi bị coi là **thất bại**. |
| **Clean shutdown** | Tắt TabletServer có trật tự. Issue nói "kill nhiều TabletServer", còn tiêu đề nói "clean shutdown". |
| **KIP-101** | Đề xuất của Kafka: dùng **leader epoch** thay cho HW để quyết định follower cắt log tới đâu. Fluss theo dõi ý tưởng này ở #673. |

## Tóm tắt

Để tránh mất dữ liệu khi đổi leader (#674), #676 đổi cách cập nhật HW: follower **tự đặt HW bằng LEO của nó ngay sau khi ghi** dữ liệu sao chép, không chờ leader. Hệ quả là follower có thể **"biết" nhiều batch hơn client**. Theo issue:
1. Follower đã ghi batch 3 và 4 rồi cập nhật HW.
2. Client chưa nhận ack cho 3 và 4, nên vẫn nghĩ batch cuối được xác nhận là 2.
3. Leader và follower cùng chết, nên client coi việc ghi đã thất bại.
4. Follower lên làm leader mới.
5. Khi xử lý request mới cho batch 3, leader mới phát hiện không khớp và trả `OutOfOrderSequenceException`.
6. Client coi đây là lỗi **không phục hồi được**, không retry, và job failover.

Trong `1.0.0`, gốc rễ (follower HW = LEO) vẫn còn. Client đã thông minh hơn: coi một số `OutOfOrderSequence` là đã thành công hoặc đáng retry. Nhưng nếu không có dấu hiệu tiến triển, client vẫn reset writer và batch thất bại, nên **Flink job vẫn failover**.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-server`: `ReplicaFetcherThread.processFetchResultFromLocalStorage`, `LogTablet`, `WriterStateEntry`;
  - `fluss-client`: `Sender`, `IdempotenceManager`;
  - `fluss-flink`: `FlinkSinkWriter`.
- **Đã xác nhận trong mã `v1.0.0` (server):** sau khi follower ghi dữ liệu fetch về:
  ```java
  // TODO, to avoid lose data in case of leader change, we now change to update highWatermark
  // first for follower instead of first for leader. ... However, this is just a temporary solution,
  // if we want to have a strong consistency guarantee, we should do as KIP-101 do, trace by #673
  logTablet.updateHighWatermark(logTablet.localLogEndOffset());
  ```
  Follower **không** dùng `replicaData.getHighWatermark()` của leader, mà dùng LEO của chính nó. Đúng như issue mô tả. Leader epoch cache (#673) vẫn chưa có.
- **Đã xác nhận trong mã `v1.0.0` (client): ba nhánh xử lý `OUT_OF_ORDER_SEQUENCE_EXCEPTION`:**
  1. `Sender`: nếu `idempotenceManager.isAlreadyCommitted(...)` (sequence của batch ≤ `lastAckedBatchSequence`), client log WARN `"... received OUT_OF_ORDER_SEQUENCE_EXCEPTION but has already been committed ... Treating as success due to lost response."` và **coi là thành công**. Đây là #2827.
  2. `IdempotenceManager.canRetry`: client **retry** nếu batch không phải batch kế tiếp, nếu sequence đã bị điều chỉnh, hoặc nếu `lastAckedBatchSequence > batch.lastAckedSequenceAtSend()` (ack đã tiến trong lúc batch đang bay, tức phản hồi cũ đến muộn). Đây là #4125.
  3. Còn lại: `handleFailedBatch` gặp `OutOfOrderSequenceException` thì `LOG.error(...)` rồi **`resetWriterId()`**, kèm comment *"we have hit an irrecoverable exception and cannot make any guarantees about the previously committed message"*. Batch bị `failBatch(...)`, exception được trả qua callback. `FlinkSinkWriter` lưu nó vào `asyncWriterException`, và lần `write`/`flush` sau `checkAsyncException()` ném lỗi, làm **task Flink failover**.
- **Phía server khi nhận batch gửi lại (đã đọc mã):**
  - `analyzeAndValidateWriterState` trước tiên tìm **batch trùng** trong 5 batch gần nhất (`findDuplicateBatch`). Nếu thấy, server trả metadata batch cũ (client nhận `DUPLICATE_SEQUENCE` và coi là thành công).
  - Chỉ khi không trùng mới kiểm tra thứ tự. Vì vậy trong ví dụ của issue (leader mới đã có 3 và 4, client gửi lại 3), mình **dự đoán server trả "trùng"**, không phải "sai thứ tự", và client sẽ coi là thành công. Mình **chưa lý giải được** vì sao issue thấy `OutOfOrderSequence` ở bước 5. Có thể code thời 0.6 chưa có các nhánh này, hoặc kịch bản thực tế khác ví dụ (ví dụ follower bị cắt log khi làm follower, xem #596). Đây là điểm chưa chắc.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Ghi bật idempotence** (mặc định) với `acks=-1`.
2. **Nhiều TabletServer bị tắt hoặc chết cùng lúc**, gồm cả leader và một follower của cùng bucket. Hoặc leader chết khi đang có batch in-flight mà follower đã nhận.
3. Tại thời điểm đó **có batch đã tới follower mà client chưa nhận ack**. Với tối đa 5 batch in-flight mỗi bucket và tải cao, điều này gần như luôn đúng.
4. Server mới làm leader có writer state **không khớp** với cái client biết, theo kiểu không rơi vào các nhánh "trùng" hoặc "retry được" ở mục 1.

**Phiên bản:** báo trên `main` thời 0.6. Gốc rễ (follower HW = LEO, chưa có leader epoch) vẫn ở `1.0.0`. Phần client đã được cải thiện bởi #2827 (03/2026), #2953 và #4125 (08/2026).

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job Flink (hoặc ứng dụng Java client) ghi vào Fluss khi cluster bị tắt hoặc nâng cấp nhiều node cùng lúc.

**Hậu quả:**
1. **Job Flink failover** và **checkpoint thất bại** (issue #1163 mô tả cùng triệu chứng khi nâng cấp cluster). Sau failover, Flink phát lại từ checkpoint gần nhất.
2. **Với bảng PK:** ghi lại là upsert theo khóa, nên giá trị cuối thường đúng. Một số thay đổi có thể sinh **changelog trùng** (thêm `-U/+U`) cho các consumer downstream (suy luận).
3. **Với bảng log (append-only):** dữ liệu đã nằm trên server nhưng client tưởng thất bại, nên sau khi Flink phát lại có thể **trùng bản ghi**. Mức độ phụ thuộc vào cách sink hoạt động với checkpoint, mình chưa kiểm tra.
4. **Không mất dữ liệu đã ack.** Lỗi này là "client không biết chắc", không phải "server làm mất".

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #709 trong `relate_issue.md` ghi: *"Shutdown sạch nhiều TabletServer thì client nhận hàng loạt `OutOfOrderSequenceException` | main (0.6) | 🟡 Như #1163 | Rolling restart từng node"*. Đối chiếu:
- **🟡 hợp lý.** Client 1.0.0 đã xử lý được nhiều trường hợp (#2827, #4125). Nhưng gốc rễ phía server vẫn còn nguyên (đã xác nhận TODO #673), và trường hợp "lỗi thật" vẫn làm job failover.
- **"Shutdown sạch":** tiêu đề issue nói clean shutdown, nhưng nội dung nói "kill multiple TabletServers". Rủi ro thực tế cao hơn với kill hoặc crash. Với clean shutdown, Fluss có controlled shutdown (chuyển leader trước khi tắt, #3654 batch LeaderAndIsr), nên ít xảy ra hơn (suy luận).

## 5. Mức ảnh hưởng với CDP

- Ảnh hưởng tới **job Flink ghi profile, feature, sự kiện** khi nâng cấp hoặc restart cluster, hoặc khi có sự cố làm nhiều node chết cùng lúc.
- Với bảng profile (PK), hậu quả chính là **gián đoạn vài giây tới vài phút** do failover, cộng thêm changelog trùng nếu có job đọc changelog.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Rolling restart từng TabletServer một**, dùng tắt có trật tự (SIGTERM, để controlled shutdown chuyển leader trước), và **chờ ISR đầy đủ** rồi mới sang node tiếp.
- **Replication factor 3** và phân bố replica qua nhiều node hoặc zone, để giảm khả năng leader và follower cùng chết.
- **Restart strategy của Flink** cho phép nhiều lần failover trong cửa sổ bảo trì. Alert khi checkpoint thất bại lặp lại.
- **Downstream đọc changelog nên khử trùng** theo khóa và phiên bản, hoặc chịu được `-U/+U` lặp lại.
- Theo dõi log client `"The server returned ... OutOfOrderSequenceException ... "` (ERROR, khi reset writer) và `"Treating as success due to lost response"` (WARN).

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện "kill nhiều TabletServer" trên 1.0.0.
- Mình chưa lý giải được vì sao kịch bản trong issue cho `OutOfOrderSequence` thay vì `DuplicateSequence` (mục 1).
- Mình chưa kiểm tra mức trùng dữ liệu thực tế với bảng log sau failover.
- Mình chưa kiểm tra controlled shutdown của 1.0.0 có loại hẳn trường hợp clean shutdown khỏi lỗi này hay không.
