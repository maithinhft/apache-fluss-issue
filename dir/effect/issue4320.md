# Issue #4320 — Action `remove_orphan_files` không tôn trọng giới hạn tốc độ thấp của filesystem

Link: https://github.com/apache/fluss/issues/4320 — Phiên bản được báo: `0.9.0` (người báo: platinumhamburg, 14/09/2026). Issue ngắn, nêu lỗi tính toán và cách sửa. Không có bình luận. Trạng thái: còn mở. PR [#4321](https://github.com/apache/fluss/pull/4321) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0`: lỗi đúng như mô tả, và chính comment trong mã đã thừa nhận giới hạn này. Có một **đính chính phân loại**: `remove_orphan_files` **không phải** công cụ của Paimon. Đây là một **Flink action của Fluss**, dọn file mồ côi trên **remote storage của Fluss** (remote log, manifest, KV snapshot). Vì vậy nó liên quan tới mọi cluster có `remote.data.dir`, không chỉ khi dùng lake.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`remove_orphan_files`** | Một **Flink action** của Fluss (`fluss-flink` `action/orphan`), chạy như một job Flink batch. Nó tìm và xóa **file mồ côi** trên remote storage của Fluss: segment log, manifest, file KV snapshot không còn được bảng hay partition nào tham chiếu. |
| **File mồ côi** | File còn trên S3 hoặc HDFS nhưng metadata đã không trỏ tới, do xóa bảng dở dang, lỗi giữa chừng khi upload... (liên quan #4516, #3357). |
| **Rate limit (giới hạn tốc độ)** | Số thao tác filesystem tối đa mỗi giây (list, stat, delete), để không làm quá tải object store. S3 có giới hạn request mỗi prefix, và vượt quá thì bị throttle. Option `--remote-fs-op-rate-limit-per-second`, mặc định **100**. |
| **Parallelism** | Số subtask chạy song song của job (`--parallelism`). |
| **`RateLimiter` (Guava)** | Bộ giới hạn tốc độ trong **một JVM**. Không phối hợp giữa các subtask. |

## Tóm tắt

Job chia tổng rate cho các subtask: `quota = total / parallelism` (cộng 1 cho vài subtask đầu nếu chia dư), rồi **ép tối thiểu 1 thao tác/giây** (`Math.max(1.0, quota)`). Nếu **parallelism lớn hơn tổng rate**, mỗi subtask vẫn được 1/s, nên tổng thực tế bằng **parallelism**, vượt giới hạn đặt ra. Ví dụ trong issue: rate 3/s, parallelism 8, tổng thực tế 8/s. Đề xuất sửa: cho phép **số lẻ** (3/8 = 0,375/s mỗi subtask), vì Guava `RateLimiter` hỗ trợ rate thập phân.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink`, gồm `action/orphan/OrphanFilesCleanActionFactory` (`identifier() = "remove_orphan_files"`), `action/orphan/job/ScanAndCleanFunction`, `action/orphan/config/OrphanCleanConfig`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `ScanAndCleanFunction.open(...)`:
    ```java
    // Distribute the configured rate as base + 1 extra for the first `remainder` subtasks.
    // Flink does not provide a cross-JVM limiter here, so this is a best-effort job-level
    // target. Each subtask gets at least 1/s; if parallelism exceeds the configured rate, the
    // effective aggregate can exceed the target by that floor.
    remoteFsOpRateLimiter = RateLimiter.create(perSubtaskRate(remoteFsOpRateLimitPerSecond, parallelism, subtaskIndex));
    ```
  - `perSubtaskRate(...)`:
    ```java
    long base = totalRate / parallelism;
    long remainder = totalRate % parallelism;
    long quota = base + (subtaskIndex < remainder ? 1L : 0L);
    return Math.max(1.0, (double) quota);
    ```
  - Mọi thao tác filesystem (scan, delete trong `SafeDeleter`) gọi `remoteFsOpRateLimiter.acquire()`.
  - `OrphanCleanConfig`: `DEFAULT_REMOTE_FS_OP_RATE_LIMIT_PER_SECOND = 100L`, `DEFAULT_OLDER_THAN = 3 ngày`.
  - **Phạm vi quét:** các rule `LogSegmentRule`, `LogManifestRule`, `KvSnapshotFileRule`, `KvSharedSstRule`, `OrphanDirDetector`, cùng `ActiveRefsFetcher` gọi RPC `ListRemoteLogManifests` và `ListKvSnapshots` (#3480). Đây là **remote storage của Fluss**, không phải dữ liệu Paimon.

## 2. Khi nào lỗi xảy ra

Chạy `remove_orphan_files` với **`--parallelism` lớn hơn `--remote-fs-op-rate-limit-per-second`**. Với mặc định rate 100, chỉ gặp khi parallelism > 100.

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

- **Gửi nhiều request lên object store hơn dự định.** Có thể bị throttle (S3 trả 503 SlowDown), làm chậm **chính cluster Fluss** đang dùng cùng bucket hay prefix (upload remote log, KV snapshot), và tốn chi phí request.
- **Không ảnh hưởng đúng sai** của việc dọn file.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #4320 trong **nhóm Paimon**. Đối chiếu:
- **Sai phân loại:** đây là công cụ dọn **remote storage của Fluss**, áp dụng cho mọi cluster có remote storage. Nên chuyển sang nhóm "vận hành và công cụ" (🟢), không gắn với Paimon.
- **❌ đúng** về trạng thái.

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Chỉ khi chạy công cụ dọn file mồ côi với parallelism cao.
- Đây lại là công cụ **hữu ích** để dọn rác do #4516, #3357, #1629 để lại trên remote storage.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi chạy `remove_orphan_files`, đặt **`--parallelism` ≤ `--remote-fs-op-rate-limit-per-second`**.
- Chạy **`--dry-run`** trước, xem log audit, rồi mới chạy thật.
- Chạy vào **giờ thấp điểm** để không cạnh tranh request S3 với cluster.
- Giữ `--older-than` mặc định (3 ngày) hoặc lâu hơn, để không xóa nhầm file đang được ghi.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy công cụ này trên 1.0.0.
- Mình chưa kiểm tra các cờ `--allow-delete-manifest`, `--allow-clean-orphan-tables`, `--allow-clean-orphan-partitions` hoạt động thế nào và rủi ro ra sao.
- Mình chưa đọc diff của PR #4321.
