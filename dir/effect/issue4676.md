# Issue #4676 — Đọc log từ `EARLIEST` có thể kẹt vĩnh viễn khi offset sớm nhất chỉ còn nằm trong lake

Link: https://github.com/apache/fluss/issues/4676 — Phiên bản được báo: `1.0.0` (người báo: yangshangqing95, 16/10/2026). Issue rất chi tiết:
- ví dụ cụ thể về các khoảng offset;
- trình tự từng bước cho cả đọc không giới hạn lẫn **bounded scan**. Bounded scan là tính năng của #4545, đã bị **revert** trên `main` (#4876);
- ghi chú riêng về changelog của bảng PK.

Không có bình luận, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bảng **bật lakehouse** (`table.datalake.enabled = true`).

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` ở `LogTablet` (cách tính log start offset, điều kiện đọc được từ lake) và `ReplicaManager.handleFetchOutOfRangeException`. Ba điểm của issue khớp với mã. Mình chưa tái hiện, và chưa kiểm tra Coordinator có luôn gửi "lake log start offset" cho TabletServer hay không (mục 6).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Ba tầng lưu log** | **Local** (đĩa TabletServer), **remote** (S3/HDFS, do remote log tiering đẩy lên) và **lake** (Paimon, Iceberg, do lakehouse tiering đẩy lên). Dữ liệu cũ dần dần chỉ còn ở tầng sau. |
| **Log start offset** | Offset sớm nhất của bucket. Fluss 1.0.0 lấy **nhỏ nhất qua cả ba tầng**. |
| **Log Fetch API** | API đọc log của Fluss. API này đọc được **local** và **remote** (trả danh sách segment remote để client tự tải). **Không** trả dữ liệu từ lake. |
| **`EARLIEST_OFFSET` (-2)** | Giá trị đặc biệt khi subscribe: "đọc từ bản ghi sớm nhất". Server tự đổi sang log start offset. |
| **Out-of-range** | Offset yêu cầu không có ở local. Server xem có ở remote hay lake không. |
| **Union read** | Cách đọc của Flink: phần cũ đọc **trực tiếp từ lake**, phần mới đọc từ log Fluss. Đây là cách đúng để đọc lịch sử khi bảng có lake. |
| **Bounded scan** | Đọc tới một offset dừng (#4545), đã bị revert trên `main`. |

## Tóm tắt

Ví dụ trong issue:
- offset 100–300 chỉ còn trong lake;
- offset 300–400 còn ở local;
- remote **không có** 100–300.

Khi đó `EARLIEST` được đổi thành **100**, vì là nhỏ nhất qua ba tầng. Nhưng Fetch API không đọc được 100 từ lake, nên server trả **danh sách rỗng, không lỗi, không cho biết nên nhảy tới đâu** (comment trong mã: *"currently, we just return empty records directly"*). Client bỏ phản hồi rỗng, giữ vị trí `EARLIEST`, và **hỏi lại mãi**. Job **không bao giờ** tới được dữ liệu 300–400 đang có sẵn.

Với **changelog của bảng PK**, `canFetchFromLakeLog` luôn trả `false`, nên thay vì kẹt, client có thể nhận `LogOffsetOutOfRangeException` (xem #3228).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `log/LogTablet` (`logStartOffset`, `canFetchFromLakeLog`, `canFetchFromRemoteLog`, `updateLakeLogStartOffset`) và `replica/ReplicaManager.handleFetchOutOfRangeException`. Phía client là `fluss-client` `table/scanner/log/LogFetcher`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `LogTablet.logStartOffset()`:
    ```java
    return Math.min(Math.min(localLogStartOffset(), remoteLogStartOffset), lakeLogStartOffset);
    ```
  - `LogTablet.canFetchFromLakeLog(fetchOffset)`:
    ```java
    if (isChangeLog) { return false; }   // "we don't support client read changelog directly"
    return lakeLogStartOffset <= fetchOffset && fetchOffset < lakeLogEndOffset;
    ```
  - `ReplicaManager.handleFetchOutOfRangeException(replica, fetchOffset, e)`:
    ```java
    if (fetchOffset == FetchParams.FETCH_FROM_EARLIEST_OFFSET) { fetchOffset = replica.getLogStartOffset(); }
    if (canFetchFromLakeLog(replica, fetchOffset)) {
        // todo: currently, we just return empty records directly
        // need to return the info of datalake to make client can fetch from datalake directly
        return FetchLogResultForBucket.records(tb, MemoryLogRecords.EMPTY, replica.getLogHighWatermark(), -1L, -1L);
    } else if (canFetchFromRemoteLog(replica, fetchOffset)) { ... }
    ```
  - `lakeLogStartOffset` khởi tạo bằng `Long.MAX_VALUE`, và chỉ được cập nhật qua `updateLakeLogStartOffset`, được gọi trong `ReplicaManager` khi nhận `NotifyLakeTableOffset` từ Coordinator, **nếu** thông điệp có trường `logStartOffset`.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. Bảng **log** (không phải changelog bảng PK) **bật lakehouse**.
2. Dữ liệu cũ **đã hết hạn ở local và remote** (theo `table.log.ttl` hoặc `local-ttl`) nhưng **còn trong lake**. Đây là kịch bản bình thường khi giữ log ngắn và dùng lake để lưu lâu.
3. Một client đọc bằng **`EARLIEST`**, ví dụ:
   - `scan.startup.mode = 'earliest'`;
   - `LogScanner.subscribe(bucket, EARLIEST_OFFSET)`;
   - partition mới phát hiện được đọc từ earliest (#3548). Riêng trường hợp này mình nghĩ ít khi trúng, vì partition mới chưa có gì trong lake (suy luận).
4. TabletServer **biết** lake log start offset (Coordinator đã gửi).

**Phiên bản:** báo trên `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job hoặc ứng dụng đọc **log** (không phải union read) của bảng có lake từ `earliest`.

**Hậu quả:**
1. **Kẹt im lặng:** không lỗi, không dữ liệu, throughput bằng 0. Client gửi fetch liên tục và nhận rỗng.
2. **Không bao giờ tới dữ liệu mới**, kể cả phần có sẵn ở local.
3. Với changelog bảng PK: có thể thành `OutOfRange` và job lỗi (xem #3228).
4. **Không ảnh hưởng dữ liệu đã lưu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4676 trong `relate_issue.md` ghi: *"Lakehouse tiering (chung) | scanner kẹt khi `EARLIEST` trỏ offset chỉ còn trong lake | 🟠 | ❌"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** chỉ xảy ra với **bảng log** khi đọc bằng **log scan**. Changelog bảng PK đi đường khác (OutOfRange). Union read của Flink (đọc lake trực tiếp) **không** bị.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu CDP bật lake cho **bảng sự kiện (log)** và có job đọc lại **từ đầu** bằng log scan.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi cần đọc **toàn bộ lịch sử** của bảng có lake: dùng **union read** của Flink (chế độ đọc mặc định cho bảng lake, hoặc `$lake` cộng log) thay cho `scan.startup.mode = 'earliest'`.
- Với job chỉ cần dữ liệu mới: dùng `latest` hoặc `timestamp`.
- Với ứng dụng Java client: không subscribe bằng `EARLIEST_OFFSET` trên bảng có lake. Lấy offset đầu thực tế ở local hoặc remote qua Admin (`listOffsets` earliest), rồi subscribe từ đó. Cần kiểm tra `listOffsets` earliest có cùng vấn đề không.
- **Alert** khi job đọc có throughput bằng 0 nhưng `pendingRecords` (xem #3304) lớn.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa kiểm tra Coordinator có luôn gửi `logStartOffset` của lake trong `NotifyLakeTableOffset` hay không. Nếu không gửi, `lakeLogStartOffset` giữ `Long.MAX_VALUE`, và `EARLIEST` sẽ không trỏ vào lake. Một bình luận ở #3228 (bản 0.9) nói giá trị này *"is never updated"*. Người báo #4676 nói đã thấy trên 1.0.0.
- Mình chưa kiểm tra `Admin.listOffsets(..., earliest)` trả giá trị nào trong kịch bản này.
- Mình chưa kiểm tra client có backoff giữa các lần nhận phản hồi rỗng hay không.
