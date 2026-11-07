# Issue #777 — Bảng chỉ có 1 replica: yêu cầu fetch "đang chờ dữ liệu" chỉ hoàn tất khi hết thời gian chờ

Link: https://github.com/apache/fluss/issues/777 — Phiên bản được báo: `0.6.0` (người báo: luoyuxia, 21/04/2025, repo cũ `alibaba/fluss`). Issue chỉ ra rằng [#768](https://github.com/apache/fluss/pull/768) (sửa deadlock [#762](https://github.com/apache/fluss/issues/762)) đã gỡ lời gọi `tryCompleteDelayedOperations` khỏi `Replica#appendRecordsToLeader()`. Có một bình luận gần đây, phân tích chi tiết, xác nhận lỗi **vẫn còn trên `main`**, đề xuất cách sửa và test. Không có PR. Trạng thái: còn mở.

> **Mức chắc chắn:** cao. Mình đã đọc `Replica` trong `v1.0.0` và xác nhận: sau khi ghi, `maybeIncrementLeaderHW` được gọi nhưng **kết quả bị bỏ**, và **không** có `tryCompleteDelayedOperations()` ở đó. Mình đã liệt kê đúng 5 nơi còn gọi hàm này. Mình chưa đo độ trễ.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Delayed fetch (`DelayedFetchLogOperation`)** | Khi consumer hỏi dữ liệu mới mà chưa có, server **giữ yêu cầu lại** tối đa `maxWaitMs`. Có dữ liệu mới thì trả ngay; hết giờ thì trả rỗng. Cách này giúp đọc gần thời gian thực mà không phải hỏi liên tục. |
| **`tryCompleteDelayedOperations()`** | Hàm "đánh thức" các delayed fetch và delayed write của bucket, để chúng kiểm tra lại xem đã đủ điều kiện hoàn tất chưa. |
| **High watermark (HW)** | Consumer (với isolation `HIGH_WATERMARK`) chỉ đọc được dữ liệu **dưới HW**. Delayed fetch của consumer chỉ hoàn tất khi HW đã tăng. |
| **Replication factor = 1** | Bucket chỉ có một bản, không có follower. |
| **`client.scanner.log.fetch.wait-max-time`** | Thời gian tối đa server giữ yêu cầu fetch của **client**. Mặc định **500 ms**. Với follower là `log.replica.fetch.wait-max-time` (cũng 500 ms). |
| **#762 / #768** | #762 là một deadlock do gọi `tryCompleteDelayedOperations()` **trong khi giữ** `leaderIsrUpdateLock`. #768 sửa bằng cách gỡ lời gọi đó. |

## Tóm tắt

Sau #768, khi leader ghi dữ liệu mới:
- HW có thể tăng ngay (`maybeIncrementLeaderHW` bên trong read lock), nhưng **không ai đánh thức** các delayed fetch đang chờ.
- Delayed fetch chỉ được đánh thức từ: khi follower fetch (`updateFollowerFetchState`), khi KV flush xong (`onKvFlushComplete`), khi trở thành leader, và khi callback AdjustIsr chạy.

Với bảng **1 replica** thì không có follower fetch. Vì vậy với **bảng log**, consumer chờ dữ liệu mới sẽ **chờ đủ `maxWaitMs`** (mặc định 500 ms) mới nhận được, dù dữ liệu đã sẵn sàng ngay.

Bình luận gần đây đề xuất: ghi nhớ việc HW đã tăng, rồi gọi `tryCompleteDelayedOperations()` **sau khi nhả khóa** (không gọi trong khóa để tránh lại #762).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `replica/Replica` (`appendRecordsToLeader`, `putRecordsToLeader`, `putHistoricalRecordsToLeader`, `tryCompleteDelayedOperations`).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `appendRecordsToLeader` chạy trong `inReadLock(leaderIsrUpdateLock, ...)`, ghi log rồi gọi `maybeIncrementLeaderHW(logTablet, clock.milliseconds());` và `return appendInfo;`. Kết quả `boolean` **bị bỏ**.
  - Hai đường ghi KV (`putRecordsToLeader` và đường historical partition) cũng gọi `maybeIncrementLeaderHW(...)` rồi bỏ kết quả. Kèm comment `// we may need to increment high watermark.`
  - `tryCompleteDelayedOperations()` (gọi `delayedWriteManager.checkAndComplete` và `delayedFetchLogManager.checkAndComplete`) chỉ được gọi ở 5 chỗ:
    1. sau khi trở thành leader;
    2. `onKvFlushComplete` (KV flush xong, có thể tăng HW);
    3. `updateFollowerFetchState` (follower fetch làm HW tăng);
    4. callback AdjustIsr;
    5. một chỗ khác quanh dòng 551, cũng sau `maybeIncrementLeaderHW`.
- **Vì sao bảng PK ít bị hơn (theo bình luận và mã):** với bảng PK, HW bị giới hạn bởi KV đã flush (xem #513), nên HW thường chỉ tăng **trong** `onKvFlushComplete`, mà hàm này **có** đánh thức delayed fetch.

## 2. Khi nào lỗi xảy ra

- Bảng có **replication factor 1**.
- Rõ nhất với **bảng log** (append-only).
- Consumer đọc **ở đuôi log** (đã bắt kịp) và gửi fetch trong lúc chưa có dữ liệu mới, tức chờ dữ liệu tiếp theo.

**Phiên bản:** báo trên `0.6.0`. Theo bình luận gần đây và mã `v1.0.0`, vẫn còn.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** consumer của bảng 1 replica.

**Hậu quả:**
1. **Độ trễ đọc tăng** lên tới khoảng `client.scanner.log.fetch.wait-max-time` (mặc định 500 ms) cho mỗi lượt dữ liệu mới, dù dữ liệu đã có ngay sau khi ghi.
2. Với `acks=-1` trên bảng 1 replica: delayed write hoàn tất ngay khi đăng ký nếu HW đã đủ (suy luận từ cách `DelayedOperationManager` kiểm tra khi đăng ký), nên writer không bị ảnh hưởng. Mình chưa kiểm chứng.
3. **Không ảnh hưởng đúng sai dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #777 trong `relate_issue.md` ghi: *"❔ | Production dùng replica ≥ 3 nên không gặp"*. Đối chiếu:
- **Nên là ❌** (đã xác nhận trong mã 1.0.0, và bình luận nói vẫn còn trên `main`), không phải ❔.
- **"Không gặp với replica ≥ 3" đúng phần lớn**, vì follower fetch sẽ đánh thức. Nhưng thời điểm đánh thức phụ thuộc vào lúc follower fetch tiếp, nên consumer vẫn có thể trễ thêm một chút so với lý tưởng (suy luận).

## 5. Mức ảnh hưởng với CDP

- **Production (replication factor 3):** không đáng kể.
- **Môi trường dev, test, PoC với 1 TabletServer hoặc replication 1:** độ trễ đọc log đo được sẽ **cao hơn thực tế** (thêm tới khoảng 500 ms). **Đừng dùng số đo độ trễ ở môi trường này để đánh giá production.**

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi đo hiệu năng hoặc độ trễ near realtime, dùng **replication factor ≥ 2** giống production.
- Nếu buộc phải dùng 1 replica và cần độ trễ thấp, có thể giảm `client.scanner.log.fetch.wait-max-time`. Cái giá là nhiều request rỗng hơn.

## 6. Những điều chưa kiểm chứng

- Mình chưa đo độ trễ thực tế với 1 replica.
- Mình chưa xác định ngữ cảnh chính xác của lời gọi `tryCompleteDelayedOperations()` quanh dòng 551.
- Mình chưa kiểm tra delayed write với `acks=-1` trên bảng 1 replica có hoàn tất ngay hay không.
