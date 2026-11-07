# Issue #1256 — Client nhận exception khó hiểu khi đọc partition đang bị xóa

Link: https://github.com/apache/fluss/issues/1256 — Phiên bản được báo: `0.7.0` (người báo: platinumhamburg, 03/07/2025). Issue mang tính định hướng: Fluss chưa có cơ chế khóa đảm bảo tính nguyên tử cho DDL, nên thao tác đọc có thể thấy **trạng thái nửa vời** khi partition đang bị xóa. Issue nêu hai ví dụ:
- `RemoteLogDownloader` ném `No such file or directory`;
- `ReplicaManager` ném `LogOffsetOutOfRangeException` khi đọc partition vừa bị xóa.

Đề xuất trước mắt là **trả loại exception rõ ràng** để code người dùng nhận ra và xử lý được. Không có bình luận. Trạng thái: còn mở. PR [#1257](https://github.com/apache/fluss/pull/1257) đã **bị đóng, không merge**.

> **Mức chắc chắn:** trung bình. Mình đã đọc mã `v1.0.0` ở các đường liên quan:
> - Flink source đã có xử lý riêng cho partition bị xóa (lúc subscribe và qua partition discovery).
> - Log fetcher bỏ qua `PartitionNotExistException` khi cập nhật metadata.
> - Nhưng hai lỗi issue nêu **vẫn không được chuyển thành lỗi rõ ràng**: `LOG_OFFSET_OUT_OF_RANGE` vẫn ném `FetchException`, và lỗi tải file remote được **thử lại mãi**.
>
> Mình chưa tái hiện việc xóa partition giữa lúc đọc.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **DDL** | Lệnh thay đổi cấu trúc: tạo, xóa, sửa bảng hoặc partition. |
| **Tính nguyên tử của DDL** | Lệnh xóa partition diễn ra "trọn vẹn hoặc chưa hề", người đọc không thấy trạng thái giữa chừng. Fluss chưa có cơ chế này. Xóa partition gồm nhiều bước (xóa metadata trong ZooKeeper, dừng replica, xóa file local và remote) chạy không đồng bộ (xem #3357, #4516). |
| **`RemoteLogDownloader`** | Thành phần client tải các segment log **đã đẩy lên remote storage** (S3/HDFS) về đĩa local để đọc. |
| **`LOG_OFFSET_OUT_OF_RANGE` / `LogOffsetOutOfRangeException`** | Server báo "offset bạn hỏi không còn trong log". Ở đây là do log đang bị xóa. |
| **`PartitionNotExistException`** | Lỗi "partition không tồn tại". Đây là loại lỗi rõ ràng mà issue muốn client nhận được. |
| **Partition discovery** | Flink source định kỳ (mặc định 1 phút) so danh sách partition, phát hiện partition mới **và partition đã bị xóa** (xem #288). |
| **Auto-partition expiration** | Coordinator tự xóa partition cũ khi vượt số partition giữ lại (`table.auto-partition.num-retention`). |

## Tóm tắt

Khi một partition **đang bị xóa** trong lúc client đọc nó, lỗi trả về phụ thuộc vào thời điểm:
1. **Lúc subscribe hoặc cập nhật metadata:** nhận `PartitionNotExistException`, là lỗi rõ ràng. 1.0.0 đã xử lý ở Flink source (bỏ split) và ở log fetcher (bỏ qua).
2. **Đang fetch từ local log:** có thể nhận `LOG_OFFSET_OUT_OF_RANGE`. Client ném `FetchException("The fetching offset ... is out of range")`, và **job Flink lỗi**. Không có gì cho biết lý do là partition bị xóa.
3. **Đang tải segment từ remote:** file đã bị xóa, nên lỗi tải. `RemoteLogDownloader` của 1.0.0 **đưa request lại hàng đợi và thử lại không giới hạn**, mỗi lần log ERROR. Bucket đó kẹt cho tới khi partition discovery của Flink nhận ra partition đã bị xóa (nếu có).

Issue muốn trường hợp 2 và 3 trả **lỗi rõ ràng kiểu "partition đã bị xóa"**. 1.0.0 chưa làm điều đó.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-client`: `table/scanner/log/AbstractLogFetchCollector`, `LogFetcher`, `RemoteLogDownloader`;
  - `fluss-flink`: `source/reader/FlinkSourceSplitReader`, `source/enumerator/FlinkSourceEnumerator`;
  - `fluss-server`: `ReplicaManager`.
- **Đã xác nhận trong mã `v1.0.0`, phần đã được xử lý:**
  - `FlinkSourceSplitReader`: khi `logScanner.subscribe(partitionId, ...)` ném lỗi có `PartitionNotExistException` trong chuỗi nguyên nhân, split đó bị bỏ (`removedSplits`, `unsubscribedTableBuckets`) với log WARN *"Partition {} does not exist when subscribing ... Skipping subscription."*. Comment: *"the PartitionNotExistException may still happens when partition is removed but Flink source reader failover before aware of it"*.
  - `FlinkSourceSplitReader.removePartitions(...)` (do enumerator gọi khi discovery thấy partition bị xóa) hủy đăng ký các bucket của partition đó.
  - `LogFetcher`: khi cập nhật metadata gặp `PartitionNotExistException` thì log WARN *"Receive PartitionNotExistException when update metadata, ignore it"*.
  - `AbstractLogFetchCollector.handleInitializeErrors`: `UNKNOWN_TABLE_OR_BUCKET_EXCEPTION` thì log WARN và cập nhật metadata, không ném.
- **Đã xác nhận trong mã `v1.0.0`, phần vẫn còn:**
  - `handleInitializeErrors`: `LOG_OFFSET_OUT_OF_RANGE_EXCEPTION` thì `throw new FetchException("The fetching offset %s is out of range: ...")`. Không phân biệt "do log hết hạn" (#3228) với "do partition đang bị xóa".
  - `RemoteLogDownloader`: khi `downloadFileAsync` lỗi, code `LOG.error("Failed to download remote log segment file {} for table bucket {}.", ...)`, nhả semaphore, rồi **`segmentsToFetch.add(request)`**, tức đưa lại vào hàng đợi. Không có giới hạn số lần, không phân biệt "file không tồn tại" với lỗi tạm thời. Mình không thấy backoff giữa các lần, nhưng chưa đọc vòng lặp lấy request để chắc chắn.
  - Trên `main`, [#1752](https://github.com/apache/fluss/pull/1752) (22/09/2026, sau khi tách 1.0.0): *"Throw Exception after retrying several times when downloading log"*. Thay đổi này **không có trong 1.0.0**.

## 2. Khi nào lỗi xảy ra

- Một job hoặc ứng dụng **đang đọc** một partition **đúng lúc** partition đó bị xóa. Hay gặp nhất:
  - job đọc **chậm** (đang backfill, hoặc vừa restore) trên **partition cũ sắp hết hạn**, đúng lúc auto-partition xóa nó;
  - xóa partition bằng tay khi job đang đọc.
- Trường hợp 3 (remote) chỉ khi partition đó đã có segment **đẩy lên remote storage**, và client đang đọc từ đó (thường là đọc dữ liệu cũ).

**Phiên bản:** báo trên `0.7.0`. Các đường nêu trên vẫn còn ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job Flink hoặc ứng dụng đọc log hay changelog của bảng partition có partition bị xóa (thường là hết hạn tự động).

**Hậu quả:**
1. **Trường hợp 2:** job Flink **failover** với lỗi out-of-range khó hiểu. Sau failover, nếu discovery đã ghi nhận partition bị xóa thì split không được giao lại và job chạy tiếp. Nếu split nằm trong checkpoint thì lúc subscribe sẽ nhận `PartitionNotExistException` và bỏ split (theo mã đã đọc).
2. **Trường hợp 3:** log ERROR lặp lại, bucket đó **không tiến**, và một suất trong số giới hạn tải song song (prefetch semaphore) bị chiếm dần dần (suy luận). Nếu partition discovery tắt (job bounded) thì có thể kẹt lâu.
3. **Không mất dữ liệu cần đọc**, vì partition đó đã bị xóa theo chủ ý. Nhưng nếu job đọc chậm hơn retention, nó **bỏ lỡ** dữ liệu của partition đó. Đây là hậu quả của cấu hình, không phải lỗi này.
4. **Khó chẩn đoán**: thông báo lỗi không nói "partition đã bị xóa".

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #1256 trong `relate_issue.md` ghi: *"Exception khó hiểu khi truy cập partition đang bị xóa | 0.7.0 | ❌ PR #1257 chưa merge"*. Đối chiếu:
- **❌ đúng** cho hai ví dụ trong issue. PR #1257 đã **bị đóng**, không phải "chưa merge".
- **Bổ sung:** 1.0.0 đã xử lý `PartitionNotExistException` ở Flink source và log fetcher, nên trường hợp "phát hiện được là partition không tồn tại" đã ổn. Lỗi tải remote **thử lại vô hạn** là hậu quả đáng chú ý hơn issue mô tả, và chỉ được sửa trên `main` (#1752).

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu có job Flink **đọc** bảng sự kiện partition có TTL partition, đặc biệt job **backfill hoặc đọc lại** dữ liệu cũ gần hạn xóa.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Đặt retention partition** (`table.auto-partition.num-retention`) đủ rộng so với độ trễ tối đa của job đọc, cộng thêm thời gian phục hồi sau sự cố.
- **Không backfill** partition sắp hết hạn. Nếu cần, tăng retention tạm thời trước khi chạy.
- **Theo dõi log client** `"Failed to download remote log segment file"` lặp lại cho cùng một bucket, và metric `remoteFetchErrorCount` của scanner.
- Với job Flink: **giữ partition discovery bật** (mặc định với streaming), để partition bị xóa được bỏ khỏi danh sách đọc.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện việc xóa partition giữa lúc fetch.
- Mình chưa kiểm tra server trả lỗi gì chính xác khi bucket của partition bị dừng giữa lúc đọc (`UNKNOWN_TABLE_OR_BUCKET`, `NOT_LEADER_OR_FOLLOWER` hay `LOG_OFFSET_OUT_OF_RANGE`).
- Mình chưa đọc vòng lặp lấy request của `RemoteLogDownloader` để biết có backoff giữa các lần thử lại hay không.
- Mình chưa kiểm tra bucket bị "kẹt tải remote" có được dọn khi `removePartitions` hủy đăng ký hay không.
