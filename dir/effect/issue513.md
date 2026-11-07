# Issue #513 — `flushKv` và `updateHighWatermark` không phải một thao tác nguyên tử, nên có thể có khe hở "thấy log trước khi thấy KV"

Link: https://github.com/apache/fluss/issues/513 — Phiên bản được báo: `main` thời 0.6 (người báo: swuferhong, 03/03/2025, repo cũ `alibaba/fluss`). Issue rất ngắn và mang tính thiết kế. Không có bình luận, không có PR. Trạng thái: còn mở. Issue là phần tiếp theo của [#179](https://github.com/apache/fluss/pull/179) ("The KV data should be visible before CDC log", 06/03/2025, có trong 1.0.0).

> **Mức chắc chắn:** trung bình–cao. Issue chỉ nêu mối lo, không có kịch bản cụ thể. Mình đã đọc mã `v1.0.0` và thấy thiết kế hiện tại **giữ được bất biến mà issue cần**: high watermark không bao giờ vượt phần KV đã flush, và lookup từ bên ngoài chỉ đọc phần đã flush. Mình **chưa chạy test** để xác nhận, và chưa rà mọi đường cập nhật HW.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Bảng Primary Key (PK)** | Bảng có khóa chính. Fluss giữ **giá trị mới nhất của từng key** trong **KV store** (RocksDB), và ghi mọi thay đổi vào **changelog** (log CDC). Changelog đồng thời là WAL để phục hồi KV. |
| **Changelog / CDC log** | Dòng các thay đổi (`+I`, `-U`, `+U`, `-D`) mà người khác đọc theo thời gian thực (Flink đọc changelog, delta join...). |
| **KV pre-write buffer** | Bộ đệm trong bộ nhớ chứa các thay đổi **đã ghi vào changelog nhưng chưa ghi xuống RocksDB**. Việc ghi xuống RocksDB (**flush KV**) chạy **bất đồng bộ**. |
| **`flushedLogOffset`** | Offset changelog mà tới đó KV **đã nằm trong RocksDB**. |
| **High watermark (HW)** | Offset changelog tới đó dữ liệu được coi là đã cam kết. **Người đọc changelog chỉ thấy dữ liệu dưới HW.** |
| **Lookup** | Đọc theo khóa chính từ KV. Đây là đường AI agent và Flink lookup join dùng. |
| **Nguyên tử (atomic)** | Hai việc diễn ra "như một" với người quan sát: không ai thấy được trạng thái nằm giữa. |
| **Delta join** | Tính năng Flink: đọc changelog bảng A, rồi lookup bảng B (hoặc chính A) theo khóa. Yêu cầu: **thấy thay đổi trong changelog thì lookup chắc chắn thấy giá trị tương ứng**. |
| **`acks`** | Số replica phải xác nhận trước khi writer nhận "ghi thành công". `client.writer.acks` mặc định là `all` (tương đương -1). |

## Tóm tắt

Yêu cầu của #179: **KV phải hiển thị trước changelog**. Ai đọc changelog thấy bản ghi X thì lookup key của X phải thấy giá trị tương ứng. #179 đảm bảo `flushKv` chạy **trước** `updateHighWatermark`. #513 lo rằng hai bước này **không nguyên tử**, nên vẫn có khe hở.

Trong `1.0.0`, mã giữ bất biến **"HW ≤ `flushedLogOffset`"** khi tăng HW. HW chỉ được đẩy tới offset mà KV **đã flush xong**. Phần chưa flush thì kích hoạt flush bất đồng bộ và đợi lần sau. Cộng với việc lookup từ bên ngoài **chỉ đọc RocksDB**, người đọc changelog không thể thấy bản ghi mà lookup chưa thấy. Theo mã, khe hở mà issue lo **không còn**.

Hệ quả phụ có ích cho CDP: với `acks=all` (mặc định), writer chỉ nhận ack khi HW đã vượt qua bản ghi, nghĩa là **khi Flink nhận ack thì AI agent lookup đã thấy giá trị mới**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `replica/Replica.maybeIncrementLeaderHW`, `replica/Replica.checkEnoughReplicasReachOffset`, `kv/KvTablet` (flush và lookup).
- **Đã xác nhận trong mã `v1.0.0`: HW bị giới hạn bởi `flushedLogOffset`.** Trong `maybeIncrementLeaderHW`:
  ```java
  KvTablet currentKv = this.kvTablet;
  if (currentKv != null) {
      long flushedOffset = currentKv.getFlushedLogOffset();
      if (flushedOffset < newHighWatermark.getMessageOffset()) {
          // The KV view must be flushed before the log high watermark becomes visible. Kick
          // the asynchronous flush towards the full candidate ... and clamp this advance to the
          // already-flushed offset ...
          mayFlushKv(newHighWatermark.getMessageOffset());
          if (flushedOffset <= leaderLog.getHighWatermark()) return false;
          newHighWatermark = new LogOffsetMetadata(flushedOffset);
      }
  }
  leaderLog.maybeIncrementHighWatermark(newHighWatermark);
  ```
- **Đã xác nhận trong mã `v1.0.0`: `flushedLogOffset` chỉ tăng sau khi dữ liệu đã vào RocksDB.** `KvTablet.completeFlushedSegment` (`@GuardedBy("kvLock")`) gỡ segment khỏi pre-write buffer và nâng `flushedLogOffset`, kèm comment *"advances flushedLogOffset/rowCount to cover exactly the data now in RocksDB"*. `flushedLogOffset` là `volatile` và chỉ tăng.
- **Đã xác nhận trong mã `v1.0.0`: lookup từ bên ngoài chỉ đọc phần đã flush.**
  - `KvTablet.multiGet(keys)` và `prefixLookup(...)` đọc `rocksDBKv`, dưới `kvLock` read lock.
  - Có `multiGetFromBufferOrKv` đọc cả pre-write buffer, nhưng comment ghi rõ: *"Only for internal server-side reads ... External lookups must keep using multiGet so that clients only observe flushed data."*
- **Đã xác nhận trong mã `v1.0.0`: ack của `acks=-1` đợi HW.**
  - `ReplicaManager.delayedWriteRequired` chỉ tạo delayed write khi `requiredAcks == -1`.
  - `Replica.checkEnoughReplicasReachOffset` hoàn tất khi `logTablet.getHighWatermark() >= requiredOffset` (và đủ `log.replica.min-in-sync-replicas-number`).
  - `client.writer.acks` mặc định `"all"`.
- **Các thay đổi liên quan trong 1.0.0** (theo lịch sử git): #179 (03/2025), loạt "async KV flush" như `bf25beee` (#3853, 08/2026, "Align prefix lookup test with async flush semantics") và `b4b1fb19` (09/2026, "Keep async KV flushes on WAL batch boundaries").

## 2. Khi nào lỗi xảy ra

Theo issue: ứng dụng **đọc changelog** rồi **lookup ngay** key vừa thấy (mẫu delta join). Theo mã 1.0.0, bất biến đã được giữ, nên mình **không tìm thấy** kịch bản lỗi trên leader.

Những chỗ mình chưa rà (mục 6): chuyển leader (leader mới có HW và `flushedLogOffset` ra sao khi mới lên), và partition lịch sử (historical partition).

## 3. Tác động đến ai, làm sai lệch gì

**Nếu khe hở còn** (theo mối lo của issue): read-your-write bị vỡ trong một cửa sổ rất ngắn. Lookup trả giá trị **cũ** hoặc **không thấy key**, dù changelog đã có thay đổi. Delta join sẽ ra kết quả sai.

**Theo mã 1.0.0:** không thấy khe hở này. Có hai điều cần hiểu đúng:
1. Với `acks=all` (mặc định), **sau khi writer nhận ack**, lookup sẽ thấy giá trị.
2. Với `acks=1` hoặc `0`, writer nhận ack **ngay sau khi leader ghi changelog local**, có thể **trước** khi KV flush. Lúc đó lookup có thể **chưa** thấy giá trị (suy luận từ `delayedWriteRequired` chỉ chờ khi `acks == -1`).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #513 trong `relate_issue.md` ghi: *"🟡 #179 (KV hiển thị trước CDC log) đã có, issue vẫn mở | Ảnh hưởng read-your-write khi AI agent đọc ngay sau khi Flink ghi (cửa sổ ngắn)"*. Đối chiếu:
- **🟡 chấp nhận được**, nhưng theo mã có thể coi gần như ✅ cho đường leader: HW bị giới hạn bởi `flushedLogOffset`, và lookup chỉ đọc phần đã flush. Mình giữ 🟡 vì issue chưa đóng và chưa có test xác nhận.
- **"Read-your-write khi AI agent đọc ngay sau khi Flink ghi" cần nói rõ hơn:**
  - Nếu "sau khi ghi" nghĩa là **sau khi Flink nhận ack với `acks=all`**, thì lookup **thấy** (theo mã).
  - Nếu là "ngay khi Flink vừa gửi, chưa có ack" thì tất nhiên có thể chưa thấy. Đây là hành vi bình thường, không phải lỗi.
  - Nếu dùng `acks=1` thì có cửa sổ chưa thấy.

## 5. Mức ảnh hưởng với CDP

- **Thấp**, nếu giữ `client.writer.acks = all` (mặc định).
- Đây là thuộc tính tốt cho CDP: AI agent lookup sau khi pipeline Flink đã commit (ack) sẽ thấy dữ liệu mới nhất.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Giữ `client.writer.acks = all`** cho các job ghi profile và context. Đừng hạ xuống `1` để tăng tốc nếu cần read-after-write.
- Nếu có delta join hoặc pipeline "đọc changelog rồi lookup", viết một **test tải cao** kiểm tra không bao giờ gặp "không thấy key" hoặc "giá trị cũ", để xác nhận trên chính môi trường của bạn.
- Lưu ý khác biệt: độ trễ từ lúc ghi tới lúc thấy (cả ở changelog lẫn lookup) tăng theo thời gian **flush KV**. Theo dõi metric `kvFlushLatencyMs` (lưu ý #4139 về histogram).

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test để xác nhận bất biến dưới tải.
- Mình chưa rà khi **chuyển leader**: leader mới khởi tạo HW và `flushedLogOffset` thế nào, và có giai đoạn nào HW > flushed trên leader mới không.
- Mình chưa kiểm tra đường **historical partition** và **KV snapshot recovery**.
- Các chỗ khác cập nhật HW trong `LogTablet` mà mình đã xem đều không làm hỏng bất biến:
  - `updateHighWatermarkWithLogEndOffset` chỉ **kéo HW xuống** khi HW ≥ LEO (sau khi cắt log hoặc roll segment).
  - `loadWriterSnapshot` (đặt HW = LEO) chỉ được gọi từ `ReplicaFetcherThread`, tức trên **follower**, mà follower không phục vụ lookup.
  - Mình chưa rà hết các nơi khác.
