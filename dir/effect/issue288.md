# Issue #288 — Partition mới được tạo làm job Flink đang đọc bị lỗi "Leader not found"

Link: https://github.com/apache/fluss/issues/288 — Phiên bản được báo: `0.5.0` (người báo: luoyuxia, 30/12/2024, repo cũ `alibaba/fluss`). Issue có stack trace dạng chữ và 6 bình luận (thời 0.5–0.6): người báo thử lại trên nhánh mới và vẫn gặp `Leader not found after retry 5 times`, rồi đề nghị retry vô hạn và có backoff như Kafka (KAFKA-14821). Trạng thái: còn mở. PR [#479](https://github.com/apache/fluss/pull/479) đã **bị đóng, không merge**.

> **Mức chắc chắn:** cao về hành vi trong `1.0.0` (đã đọc mã). Có **hai điều trái chiều**:
> - Phía client, logic tìm leader **vẫn chỉ thử 3 lần liên tiếp, không chờ**, nên lỗi "Leader not found" **vẫn xảy ra được**.
> - Phía Flink enumerator, với chế độ streaming có partition discovery, lỗi này **không còn làm job failover**: enumerator chỉ log WARN và thử lại ở lần discovery sau. Mã có comment `TODO: fix me in #288`.
>
> Mình **chưa tái hiện**.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Bảng partition** | Bảng chia theo giá trị một cột, thường là ngày. Mỗi partition có các bucket riêng, và mỗi bucket cần một leader. |
| **Auto-partition** | Coordinator tự tạo partition mới theo lịch (ví dụ mỗi ngày). Ngay sau khi tạo, bucket của partition mới **cần một khoảng thời gian để bầu leader**. |
| **Partition discovery** | Flink source định kỳ (`scan.partition.discovery.interval`, mặc định **1 phút**) hỏi Fluss danh sách partition để phát hiện partition mới. |
| **Enumerator** | Thành phần của Flink source chạy trên **JobManager**. Nó lập danh sách việc đọc (**split**, mỗi bucket một split) rồi giao cho các task. Lỗi ném ra từ enumerator làm **cả job** failover. |
| **`listOffsets`** | API Admin để hỏi offset đầu, cuối hoặc theo thời gian của bucket. Phải gửi tới **leader** của bucket. Enumerator dùng nó để biết đọc partition mới từ đâu. |
| **`MetadataUpdater.leaderFor`** | Hàm client tìm leader của bucket. Nếu cache chưa có, nó hỏi lại metadata. |
| **Backoff** | Chờ một khoảng trước khi thử lại. |
| **Bounded / streaming** | Đọc có điểm dừng (batch) và đọc liên tục. |

## Tóm tắt

Khi có partition mới, enumerator phát hiện ra rồi gọi `listOffsets` cho các bucket của partition đó. Nếu leader **chưa được bầu xong**, `MetadataUpdater.leaderFor` hỏi metadata **3 lần liên tiếp không chờ**, vẫn không thấy leader, rồi ném `FlussRuntimeException("Leader not found after retry 3 times ...")`.

Ở `0.5.0`, lỗi này làm **job Flink failover**, và lặp lại ở mỗi partition mới. Ở `1.0.0`, với job **streaming có partition discovery**, enumerator **nuốt lỗi** (log WARN `Failed to list splits for ...`) và vì partition đó chưa được ghi nhận, lần discovery sau sẽ **thử lại**. Hậu quả còn lại là **partition mới bị đọc trễ ít nhất một chu kỳ discovery**, cộng log WARN. Với job **bounded** (batch) thì lỗi vẫn làm job thất bại.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-client`: `MetadataUpdater.leaderFor`, `FlussAdmin.listOffsets`, `BucketOffsetsRetrieverImpl`;
  - `fluss-flink`: `FlinkSourceEnumerator`.
- **Đã xác nhận trong mã `v1.0.0` (client):**
  ```java
  public int leaderFor(TablePath tablePath, TableBucket tableBucket) {
      Integer serverNode = cluster.leaderFor(tableBucket);
      if (serverNode == null) {
          for (int i = 0; i < MAX_RETRY_TIMES; i++) {   // MAX_RETRY_TIMES = 3
              updateMetadata(...);                         // không có sleep/backoff
              serverNode = cluster.leaderFor(tableBucket);
              if (serverNode != null) break;
          }
          if (serverNode == null)
              throw new FlussRuntimeException("Leader not found after retry  3 times for table bucket: ...");
      }
  ```
  - Số lần thử ít hơn bản người báo thử lại (5 lần), và vẫn không có thời gian chờ.
  - `FlussAdmin` gọi `metadataUpdater.leaderFor(...)` khi chuẩn bị request `listOffsets`. `LookupSender` và `KvBatchScanner` cũng dùng `leaderFor`.
- **Đã xác nhận trong mã `v1.0.0` (Flink enumerator):**
  - Discovery chạy theo `workerExecutor.callAsyncAtFixedDelay(this::listPartitions, this::checkPartitionChanges, 0, scanPartitionDiscoveryIntervalMs)`.
  - `checkPartitionChanges` tính partition mới bằng cách so danh sách từ server với các partition **đã giao** (`assignedPartitions`) hoặc **đang chờ giao** (`pendingSplitAssignment`). Sau đó nó gọi bất đồng bộ `initPartitionedSplits(partitionChange)` (trong đó có `listOffsets`) rồi `handleSplitsAdd`.
  - `handleSplitsAdd` khi có lỗi:
    ```java
    if (isPeriodicPartitionDiscoveryEnabled()) {
        // it means continuously read new partition splits, not throw exception, temporally
        // warn it to avoid job fail. TODO: fix me in #288
        LOG.warn("Failed to list splits for {}.", tablePath, t);
        return;
    } else {
        throw new FlinkRuntimeException(String.format("Failed to list splits for %s to read due to ", tablePath), t);
    }
    ```
  - Vì split của partition bị lỗi **không được thêm** vào danh sách đã giao hoặc đang chờ, lần discovery sau partition đó lại được coi là "mới" và được thử lại (suy ra từ `getPartitionChange`).
  - Lịch sử git: đoạn `TODO: fix me in #288` đã có trong commit đổi tên package `org.apache.fluss` (25/08/2025), nên cách giảm thiểu này có từ trước 0.8. Mình không truy tiếp commit gốc.
- **1.0.0 có thêm** [#1633](https://github.com/apache/fluss/pull/1633) (discovery chạy theo fixed-delay) và #3548 (partition mới phát hiện được đọc từ **earliest** thay vì theo `scan.startup.mode`). Hai thay đổi này **không** sửa vấn đề "chưa có leader".

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. Job Flink **đọc** bảng partition (log table, hoặc changelog của bảng PK partition).
2. Có partition mới được tạo, do auto-partition sang ngày hoặc giờ mới, hoặc tạo bằng tay hay tạo động khi ghi.
3. Discovery chạy **đúng lúc** bucket mới chưa có leader. Khả năng cao hơn khi:
   - cluster lớn, nhiều bucket;
   - Coordinator bận;
   - ZooKeeper follower chậm làm việc tạo replica thất bại tới 60 giây (xem #4522);
   - replica bị đánh dấu offline (xem #3011).

**Phiên bản:** báo trên `0.5.0`. Ở `1.0.0`, phía client vẫn còn lỗi. Phía Flink streaming chỉ còn hậu quả nhẹ.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job Flink đọc bảng partition sinh partition mới liên tục.

**Hậu quả trên `1.0.0`:**
1. **Streaming có discovery:** **không failover**. Partition mới bị **đọc trễ** thêm ít nhất `scan.partition.discovery.interval` (mặc định 1 phút) mỗi lần lỗi, kèm log WARN có stack trace. Nếu bucket mất leader lâu (ví dụ 60 giây như #4522), độ trễ có thể là vài phút.
2. **Bounded/batch** hoặc khi tắt periodic discovery: lỗi vẫn **làm job thất bại** (`FlinkRuntimeException("Failed to list splits ...")`).
3. **Không mất dữ liệu.** Partition mới được đọc từ earliest khi được phát hiện lại (#3548), nên dữ liệu ghi vào trong lúc trễ vẫn được đọc (suy luận từ mô tả #3548).
4. **Ngoài Flink:** ứng dụng dùng `Admin.listOffsets`, lookup hoặc batch scan trên bucket vừa tạo cũng có thể nhận `Leader not found after retry 3 times`, vì cùng dùng `leaderFor`.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #288 trong `relate_issue.md` ghi: *"🟡 #1633 (discovery fixed-delay) đã vào 1.0.0, chưa chắc khắc phục hoàn toàn | Job đọc bảng partition theo ngày. Test lúc chuyển ngày"*. Đối chiếu:
- **🟡 đúng**, nhưng **vì lý do khác**: thứ giảm thiểu lỗi là **enumerator nuốt lỗi khi có periodic discovery** (comment `TODO: fix me in #288`), không phải #1633.
- **Phần chưa sửa:** `leaderFor` chỉ thử 3 lần, không chờ. Đề nghị của người báo (retry vô hạn có backoff) chưa được làm.
- (Đính chính chính mình: một bản trước của file này kết luận ❌ dựa trên phía client. Sai, vì chưa đọc `handleSplitsAdd`.)

## 5. Mức ảnh hưởng với CDP

- Job **streaming** đọc bảng sự kiện partition theo ngày: ảnh hưởng **nhẹ**, chỉ trễ khoảng 1 phút hoặc hơn vào lúc chuyển ngày.
- Job **batch** chạy ngay sau khi tạo partition (backfill, export theo ngày): có thể **thất bại**.
- AI agent lookup vào partition vừa tạo (nếu bảng context partition theo ngày): lookup có thể lỗi trong vài giây đầu.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Tạo partition trước:** đặt `table.auto-partition.num-precreate` ≥ 2, để partition ngày mai đã tồn tại và có leader từ trước khi dữ liệu đến.
- **Job batch:** chạy sau khi partition đã tồn tại đủ lâu, hoặc bọc job bằng cơ chế retry ở tầng điều phối (Airflow...).
- **Ứng dụng gọi Admin hoặc lookup:** tự retry có backoff khi gặp `Leader not found`.
- **Giám sát:** log WARN `Failed to list splits for` trên JobManager, và độ trễ đọc partition mới.
- Có thể giảm `scan.partition.discovery.interval` để rút ngắn độ trễ khi lỗi. Cái giá là hỏi danh sách partition thường xuyên hơn.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện, và chưa đo độ trễ thực tế khi chuyển ngày.
- Mình chưa xác nhận partition lỗi chắc chắn được thử lại ở lần discovery sau (chỉ suy ra từ `getPartitionChange`).
- Mình chưa kiểm tra đường hybrid lake + Fluss (`pendingHybridLakeFlussSplits`) khi gặp lỗi.
- Mình chưa truy commit gốc đã thêm nhánh `LOG.warn` trong `handleSplitsAdd`.
