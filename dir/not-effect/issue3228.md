# Issue #3228 — Flink source đọc log table bị `OutOfRangeException` sau khi job dừng lâu, rồi resume

Link: https://github.com/apache/fluss/issues/3228 — Phiên bản được báo: `0.9.0`. Có 1 bình luận (xác nhận cùng lỗi khi backfill dài, nhất là khi bật lake tiering). PR sửa: [#3308](https://github.com/apache/fluss/pull/3308) *"[client] read fallback to lake when encountering OutOfRange exception"*, **chưa merge** (mình kiểm tra qua GitHub API ngày 2026-11-05). Người báo nhận làm PR.

> **Giới hạn khi viết file này:** mình đọc mã `v1.0.0` ở phía client (`AbstractLogFetchCollector`) và phía server (`ReplicaManager`, `LogTablet`, `LocalLog`). Mình **không đọc** phía Flink source (cách xử lý lỗi, cách restore split từ checkpoint) và không chạy tái hiện. Phần đó ghi rõ là suy luận.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Log table / changelog** | Log table là bảng chỉ ghi nối thêm. Bảng khóa chính (PK) cũng có một changelog (chuỗi thay đổi). Flink source đọc các log này theo **offset**. |
| **Offset** | Số thứ tự bản ghi trong log của một bucket. Flink source nhớ "đã đọc tới offset nào" cho từng bucket. |
| **Split (của Flink source)** | Đơn vị công việc của Flink source, ở đây là "đọc bucket X từ offset Y". Trạng thái split (kèm offset) được lưu trong **checkpoint**. |
| **Checkpoint / restore** | Flink định kỳ lưu trạng thái job. Khi job lỗi hoặc dừng rồi chạy lại, nó **restore** từ checkpoint và đọc tiếp từ offset đã lưu. |
| **`OutOfRangeException`** | Lỗi báo offset yêu cầu không còn nằm trong phạm vi log hiện có (nhỏ hơn offset đầu còn lưu, hoặc lớn hơn offset cuối). |
| **Log start offset** | Offset sớm nhất còn đọc được. Trong mã `v1.0.0` có **ba** mốc theo tầng lưu trữ: `localLogStartOffset` (chỉ đĩa local của TabletServer), `remoteLogStartOffset` (các segment đã đẩy lên S3/HDFS) và `lakeLogStartOffset` (bảng lakehouse, khởi tạo `Long.MAX_VALUE`). Hàm `LogTablet.logStartOffset()` trả `min` của ba mốc, tức là mốc tổng thể của cả hệ thống. Riêng bước kiểm tra "ngoài phạm vi" trong `LocalLog.read` chỉ so với mốc local; sau đó server mới thử tiếp lake và remote. Lỗi `OutOfRange` đến client chỉ khi offset nằm trước mốc của mọi tầng áp dụng được. Với changelog của bảng PK, lake không được tính (`canFetchFromLakeLog` trả `false`), nên chỉ còn local + remote. Mình chưa xác minh khi nào remote/lake start offset được cập nhật. |
| **Local log / remote log** | Local log là các segment trên đĩa của TabletServer. Remote log là các segment đã được đẩy lên object store (S3/HDFS) rồi xóa khỏi đĩa local. Client vẫn đọc được remote log. |
| **`table.log.ttl`** | Thời gian giữ log. Quá hạn thì log bị xóa, và log start offset tăng lên. |
| **Lake tiering (đẩy sang lakehouse)** | Dịch vụ riêng đẩy dữ liệu từ Fluss sang bảng lake (Paimon, Iceberg…). Sau đó log cũ có thể bị xóa khỏi Fluss vì đã có bản ở lake. |
| **Lake fallback read** | Cơ chế đề xuất: khi offset không còn trong Fluss nhưng còn ở lake, client đọc từ lake thay vì báo lỗi. |

## Tóm tắt

Flink source lưu offset đã đọc trong checkpoint. Nếu job dừng đủ lâu, đến lúc resume dữ liệu ở offset đó **không còn trong Fluss** (đã bị xóa khỏi local, và không còn ở remote log). Server trả `LOG_OFFSET_OUT_OF_RANGE`. Phía client ném `FetchException`, subtask thất bại, Flink restore lại **cùng offset cũ** và lại lỗi. Vòng lặp này kéo dài đến khi có người can thiệp (người báo viết *"the job will keep failing indefinitely"*).

Người báo mô tả nguyên nhân là log local bị đẩy sang remote storage nên offset đã lưu "cũ". Theo mã `v1.0.0`, **chỉ đẩy sang remote log thì chưa gây lỗi** (xem mục 1); lỗi xảy ra khi offset không còn ở local lẫn remote.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client` (đường fetch log của scanner), `fluss-server` (xử lý fetch), và Flink source dùng scanner này.
- **Phía server (`ReplicaManager.handleFetchOutOfRangeException`, đã đọc ở `v1.0.0`):** khi offset yêu cầu nằm ngoài log local, server thử lần lượt:
  1. **Có thể đọc từ lake không?** (`canFetchFromLakeLog`). Nếu có, trả về **bản ghi rỗng**, kèm chú thích trong mã: *"todo: currently, we just return empty records directly; need to return the info of datalake to make client can fetch from datalake directly"*. Tức là việc chuyển sang lake mới làm một nửa ở server.
  2. **Có thể đọc từ remote log không?** (`canFetchFromRemoteLog`). Nếu có, trả về thông tin các segment remote để client tự tải. **Đây là đường bình thường khi log local đã được offload.**
  3. Nếu không, trả lỗi `LogOffsetOutOfRangeException`.
- **Điều kiện đọc từ lake** (`LogTablet.canFetchFromLakeLog`): bị chặn ngay nếu là **changelog** (`isChangeLog`, comment nói chưa hỗ trợ client đọc changelog trực tiếp từ lake); nếu không, cần `lakeLogStartOffset <= offset < lakeLogEndOffset`. `lakeLogStartOffset` có giá trị khởi tạo `Long.MAX_VALUE`.
- **Phía client (`AbstractLogFetchCollector`, `v1.0.0`):** khi nhận `LOG_OFFSET_OUT_OF_RANGE_EXCEPTION`, ném `FetchException("The fetching offset ... is out of range ...")`. Không có nhánh nào đọc từ lake, không có nhánh nào đặt lại offset.
- **Các chỗ PR #3308 sửa** (theo danh sách file của PR): `LakeCompletedFetch` mới, `LogFetcher` (thêm đường đọc lake khi gặp out-of-range), `ReplicaManager`/`LogTablet`/`DelayedFetchLog`, `FlussApi.proto` và `FetchLogResultForBucket` (truyền thông tin lake qua RPC), cùng các lớp Flink (`FlinkRecordEmitter`, `BoundedSplitReader`, `FlinkSourceSplitReader`). Bình luận trong issue nêu hai khoảng trống: lake start offset không bao giờ được cập nhật (kẹt ở `Long.MAX_VALUE`), và client không có đường đọc lake. Mình **chưa xác minh** khoảng trống đầu còn đúng ở `v1.0.0` không: mình thấy `ReplicaManager` có gọi `updateLakeLogStartOffset` (khoảng dòng 1418), nên có thể điều này đã được xử lý một phần, chưa kiểm tra kỹ.
- **Phạm vi issue:** người báo nói rõ lỗi nghiêm trọng cho **log table dựa trên data lake**, vì ở đó dữ liệu cũ vẫn còn trong lake nhưng client không đọc được.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. Flink job **dừng/lỗi đủ lâu** để offset đã lưu trong checkpoint/savepoint bị bỏ lại phía sau log start offset của bucket.
2. Log ở offset đó **đã bị xóa khỏi Fluss**: hết `table.log.ttl`, hoặc (khi dùng lake tiering) đã được tiering sang lake và bị dọn khỏi Fluss. Với changelog của bảng PK, dữ liệu còn ở lake cũng **không** giúp đọc lại được (xem mục thuật ngữ về log start offset).
3. Offset đó **không còn ở remote log** nữa. Nếu còn ở remote log thì server trả đường dẫn segment remote và client đọc được (theo mã `v1.0.0`).
4. Job resume bằng offset đã lưu thay vì bắt đầu lại từ vị trí khác.

**Hệ quả của việc lặp:** mỗi lần thử lại đều restore đúng offset cũ, nên không tự hết. Đây là suy luận từ cách Flink restore và từ mô tả của người báo; mình chưa đọc code Flink source để xác nhận có đường nào đặt lại offset.

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` mình đọc vẫn ném `FetchException` ở client và vẫn có `todo` ở server, nên lỗi còn trong `1.0.0`. Với PR #3308 chưa merge, `1.0.0` không có cơ chế fallback sang lake.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:**
- Job Flink **đọc log table hoặc changelog của bảng Fluss** (ví dụ tính segment, feature từ bảng profile) và có khả năng bị dừng lâu hơn thời gian giữ log.
- Mức độ nặng hơn nếu bật **lake tiering**: log local càng bị dọn sớm, và issue nói rõ hướng sửa dành cho trường hợp này.
- **Không ảnh hưởng** luồng Flink **ghi** vào Fluss, cũng không ảnh hưởng lookup/KV. Chỉ đường **đọc log theo offset**.

**Hậu quả:**
1. **Job không phục hồi được** sau gián đoạn dài: failover lặp, không tự khỏi.
2. **Muốn chạy lại phải bỏ trạng thái cũ** (khởi động từ vị trí khác), nghĩa là chấp nhận mất đoạn dữ liệu chưa đọc hoặc phải backfill theo cách khác. Cách làm cụ thể phụ thuộc cấu hình job của bạn; mình chưa kiểm chứng trên Fluss.
3. **Không có dữ liệu sai âm thầm:** lỗi dừng rõ ràng. Rủi ro là mất tính liên tục của pipeline downstream.

**Mức ảnh hưởng với CDP:** chỉ gặp nếu có job đọc changelog của bảng PK để tính toán tiếp, và job đó có thể dừng lâu. Nếu bật lake tiering cho bảng đó thì rủi ro cao hơn. Lưu ý thêm: với **changelog của bảng PK**, theo `canFetchFromLakeLog` server không cho đọc từ lake (`isChangeLog`), nên ngay cả khi PR #3308 được merge, hướng sửa của PR có thể không bao phủ changelog; mình chưa đọc PR đủ kỹ để khẳng định.

**Biện pháp giảm thiểu:**
- Đặt `table.log.ttl` **dài hơn thời gian gián đoạn tối đa** bạn chấp nhận (cộng thời gian xử lý sự cố).
- Bật remote log tiering để log cũ vẫn đọc được sau khi rời đĩa local, và nhớ rằng nó vẫn bị giới hạn bởi TTL.
- Giám sát lag của job đọc log và cảnh báo khi job dừng quá một ngưỡng (ví dụ nửa TTL).
- Có runbook "job đọc log bị OutOfRange": quyết định trước sẽ bỏ qua đoạn mất hay backfill từ nguồn khác (ví dụ lake nếu có), và thử trên môi trường thử.
- Theo dõi PR #3308.

## 4. Bổ sung: so với Kafka, và Fluss có chính sách "reset offset" không

> Phần về Kafka là kiến thức chung của mình, **chưa kiểm chứng trong phiên này**. Phần về Fluss là đọc mã `v1.0.0`.

**Kafka (kiến thức chung, chưa kiểm chứng):** consumer có cấu hình `auto.offset.reset` (`earliest` / `latest` / `none`). Khi offset đã lưu không còn, `earliest` nhảy về offset đầu còn giữ, `latest` nhảy về cuối, `none` ném lỗi. Mình nhớ connector Kafka của Flink mặc định `none` (fail), nhưng chưa kiểm tra.

**Fluss `v1.0.0` (đã đọc mã):**
- `FlinkConnectorOptions` chỉ có `scan.startup.mode` (`full` / `earliest` / `latest` / `timestamp`). Tùy chọn này quyết định **vị trí bắt đầu lúc khởi động lần đầu**. Mình không thấy tùy chọn nào kiểu "reset khi gặp out-of-range".
- `FlinkSourceSplitReader.fetch()` gọi `logScanner.poll(...)` mà không bắt `FetchException`; ngoại lệ đi thẳng lên làm subtask lỗi. `catch` duy nhất gần đó dành cho `PartitionNotExistException` khi subscribe.
- `LogScannerImpl` / `AbstractLogFetchCollector` không có đường đặt lại offset.

**Lỗi này sửa ở đâu:** `fluss-flink` là module trong chính repo Fluss, và PR #3308 sửa cả `fluss-client`, `fluss-server`, `fluss-rpc`, `fluss-flink`. Nên đây là việc của Fluss, không phải của Flink. Thiết kế còn bỏ ngỏ: nên đọc tiếp từ lake (hướng PR #3308), hay có chính sách reset kiểu `earliest` (chấp nhận mất đoạn dữ liệu), hay cả hai. Với changelog bảng PK, hướng lake không áp dụng (`isChangeLog`), nên chính sách reset hoặc backfill riêng mới là thứ giúp được CDP; hiện chưa có trong `1.0.0`.

## 5. Bổ sung: điều kiện chính xác để ra lỗi

Hiểu "lỗi chỉ xảy ra khi cả TabletServer và lake đều không còn offset đó" là **đúng ở mức chung, nhưng có ba chỗ cần chính xác hơn** (theo `ReplicaManager.handleFetchOutOfRangeException`, `v1.0.0`):
1. "TabletServer" gồm hai tầng: đĩa local và **remote log** (S3/HDFS do Fluss quản lý). Thiếu ở local chưa phải lỗi nếu còn ở remote; server trả đường dẫn segment để client tự tải.
2. **Lake chỉ được tính với log table thường**, không tính với changelog của bảng PK (`canFetchFromLakeLog` trả `false` nếu `isChangeLog`). Với CDP, điều kiện thực tế là "không còn ở local lẫn remote".
3. Với log table có lake, nếu offset nằm trong phạm vi lake thì ở `v1.0.0` server **không báo lỗi mà trả bản ghi rỗng** (có `todo`). Hậu quả runtime (client có thể không tiến lên được) mình suy ra từ mã, chưa chạy thử.
