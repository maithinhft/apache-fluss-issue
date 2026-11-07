# Issue #3423 — Sau `ADD COLUMN`, đọc log/changelog có chiếu cột (projection) gặp lỗi trên các batch dữ liệu cũ

Link: https://github.com/apache/fluss/issues/3423 — Phiên bản được báo: `0.9.0` (người báo: loserwang1024). Có thêm một bình luận của ymkim1006 xác nhận **gặp lại trên `1.0.0`** và trên `main`, kèm test tái hiện (ngày trong bình luận không hiển thị đầy đủ). PR sửa: [#4927](https://github.com/apache/fluss/pull/4927) *"[log] Fix projection of added columns in historical Arrow batches"*, đang **mở (chưa merge)**. Trong issue còn có một cuộc thảo luận chưa kết luận về hướng sửa (server hay client điền NULL), xem mục 1.

> **Lưu ý quan trọng:** bình luận của ymkim1006 cho thấy lỗi **không chỉ xảy ra khi bạn chọn cột mới thêm**. Nó còn xảy ra khi **không chỉ định projection nào** trong một **batch read của Flink trên bảng khóa chính** (`FlussSource.setBatch()` với `OffsetsInitializer.full()`) ngay sau `ALTER TABLE ... ADD COLUMN`. Mình đã đọc mã `v1.0.0` và xác nhận chuỗi nguyên nhân (mục 1). Đây là kịch bản dễ gặp hơn mô tả ban đầu của issue.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Schema evolution / `ADD COLUMN`** | Thêm cột mới vào bảng đang chạy. Fluss không viết lại dữ liệu cũ; các dòng cũ không có cột mới (đọc ra là NULL hoặc giá trị mặc định). |
| **Batch (lô bản ghi) Arrow** | Log của Fluss lưu theo từng lô, mỗi lô là một khối định dạng Arrow **ghi theo schema tại thời điểm ghi**. Lô cũ có ít cột hơn bảng hiện tại. Mỗi lô ghi kèm `schemaId` của nó. |
| **Projection (chiếu cột)** | Chỉ đọc một số cột (ví dụ `SELECT a, c`). |
| **Projection pushdown (đẩy chiếu cột xuống server)** | Client gửi cho TabletServer danh sách chỉ số cột cần đọc; server chỉ cắt các cột đó ra từ file log rồi gửi về, tiết kiệm băng thông. |
| **`FileLogProjection`** | Lớp trong server cắt cột từ các lô Arrow trên đĩa. Mỗi lô được kiểm tra chỉ số cột đã chọn so với **schema của chính lô đó**. |
| **`InvalidColumnProjectionException` / `INVALID_COLUMN_PROJECTION`** | Lỗi khi chỉ số cột được chiếu không hợp lệ (vượt quá số cột của lô). Client nhận qua `FetchException`. |
| **Batch read bảng khóa chính (Flink)** | Đọc một lần "ảnh chụp" của bảng PK: lấy snapshot KV rồi cộng thêm phần changelog sau snapshot (`KvSnapshotAndLogBatchScanner`). |
| **KV snapshot, `kv.snapshot.interval`** | Fluss định kỳ chụp snapshot dữ liệu KV (mặc định 10 phút, đã đọc trong `ConfigOptions`). Snapshot mới giúp "đẩy" điểm bắt đầu changelog đi qua các lô cũ. |
| **Changelog / log range** | Dãy thay đổi từ offset của snapshot tới offset hiện tại, đọc để hoàn thiện kết quả batch read. |
| **`ProjectionPlan`** | Lớp phía client tính danh sách cột cần quét. Khi không chỉ định cột nào, nó trả về **tất cả cột của schema hiện tại**. |

## Tóm tắt

Lô log trên đĩa được ghi theo schema lúc ghi. Sau `ADD COLUMN`, các lô cũ có ít cột hơn schema hiện tại. Khi client yêu cầu chiếu cột (projection pushdown) với chỉ số cột mà lô cũ không có (cột mới thêm), `FileLogProjection` ở server kiểm tra thấy chỉ số vượt quá số cột của lô và trả lỗi `INVALID_COLUMN_PROJECTION`. Điều đặc biệt: batch read bảng PK của Flink **luôn chiếu toàn bộ cột của schema hiện tại**, kể cả khi người dùng không chọn cột nào, nên vừa `ADD COLUMN` xong là lỗi ngay. Lỗi tự hết khi snapshot KV kế tiếp đã vượt qua các lô cũ (tối đa khoảng một chu kỳ `kv.snapshot.interval`, mặc định 10 phút).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common` (`FileLogProjection`), `fluss-server` (đường đọc log có chiếu cột), `fluss-client` (`ProjectionPlan`, `KvSnapshotAndLogBatchScanner`).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FileLogProjection.createProjectionInfo(schemaId, selectedFieldPositions)` lấy `schemaGetter.getSchema(schemaId)` (schema **của lô**), tạo `toBitSet(arrowSchema.getFields().size(), selectedFieldPositions)`. `toBitSet` ném `InvalidColumnProjectionException("Projected fields [...] is out of bound for schema with N fields.")` khi `i >= length`. Khớp thông báo trong bình luận: `Projected fields [0, 1, 2] is out of bound for schema with 2`.
  - `ProjectionPlan.create(fieldCount, primaryKeyIndexes, projectedFields)`: nếu `projectedFields == null` thì trả về `IntStream.range(0, fieldCount)` tức **mọi cột của schema hiện tại**.
  - `KvSnapshotAndLogBatchScanner` tạo `table.newScan().project(projectionPlan.scanProjectedFields).createLogScanner()` cho phần changelog, nên yêu cầu fetch luôn có chiếu cột dù người dùng không yêu cầu.
- **Đường streaming không chọn cột nào thì không bị**, theo bình luận: client không gửi chiếu cột và `LogRecordReadContext#getOutputProjectedRow` ánh xạ dòng schema cũ ở phía client. Mình chưa đọc nhánh đó.
- **Hai hướng sửa được đề xuất** (thảo luận trong issue, chưa chốt):
  1. *Server điền NULL* (Kaixuan-Duan đề xuất): ở `createProjectionInfo`/`projectSingleBatch`, tách cột có sẵn và cột thiếu, dựng lại batch Arrow với vector NULL ở đúng vị trí. Không cần đổi proto.
  2. *Server chỉ trả cột có sẵn (kèm field id), client điền NULL* (wuchong từng gợi ý theo lời loserwang1024): phải xem có cần đổi RPC proto không.
- **Hướng PR #4927 chọn:** (theo diff mình đọc) làm theo hướng 1, sửa `FileLogProjection` và `LogSegment`: thêm bước `appendNullColumns` dựng các cột NULL cho phần cột thiếu (giả định schema chỉ thêm cột ở cuối), thêm tham số cho phép lô đầu tiên vượt `maxBytes` để không kẹt tiến độ fetch khi lô sau khi thêm cột NULL to hơn dạng lưu. PR chưa merge.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Bảng đã `ALTER TABLE ... ADD COLUMN`** (schema có thêm cột).
2. **Vùng log cần đọc vẫn còn chứa lô ghi theo schema cũ.** Lô cũ chỉ "hết" khỏi vùng đọc khi snapshot KV kế tiếp bắt đầu sau điểm ADD COLUMN (batch read) hoặc khi dữ liệu cũ hết hạn (với đọc log).
3. **Yêu cầu fetch có chiếu cột chứa chỉ số cột mới.** Có hai đường:
   - **Batch read bảng PK của Flink không chỉ định cột:** chắc chắn chạm lỗi nếu vùng changelog có lô cũ (bình luận ymkim1006, `1.0.0`; đã khớp mã).
   - **Truy vấn/đọc log có chọn cột (projection) gồm cột mới** (mô tả gốc của issue, `0.9.0`). Mình suy ra từ mã rằng điều kiện tương tự (chỉ số cột ≥ số cột của lô) cũng chạm; chưa tái hiện riêng nhánh này.

**Phiên bản:** báo trên `0.9.0`; mã `v1.0.0` còn nguyên cơ chế lỗi (đã đọc).

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người thêm cột (`ADD COLUMN`) vào bảng trong khi:
- chạy **batch read** bảng PK bằng Flink (backfill, đối chiếu, export, kiểm tra chất lượng dữ liệu), hoặc
- chạy job đọc log/changelog có projection chọn cột mới, trên dữ liệu ghi trước khi thêm cột.

**Hậu quả:**
1. **Job đọc thất bại** với `FetchException ... INVALID_COLUMN_PROJECTION`, cho tới khi dữ liệu cũ ra khỏi vùng đọc. Với batch read, đợi tối đa khoảng một chu kỳ `kv.snapshot.interval` (mặc định 10 phút) hoặc chủ động chờ snapshot mới.
2. **Không có sai lệch dữ liệu âm thầm:** lỗi hiện rõ. (Mình chưa kiểm tra riêng hậu quả của hướng sửa trong PR, ví dụ NULL so với giá trị mặc định của cột mới.)
3. **Lặp lại với mỗi lần `ADD COLUMN`** nếu quy trình của bạn thường xuyên thêm cột.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3423 ghi: *"Projection pushdown chọn **cột mới thêm** thất bại trên batch dữ liệu cũ. ❌ PR #4927 chưa merge. CDP thường xuyên ADD COLUMN thuộc tính mới. Job đọc log/changelog có thể lỗi."* Đối chiếu:
- **Trạng thái ❌ và "PR chưa merge" đúng** (PR đang mở).
- **Phạm vi lỗi rộng hơn mô tả "chọn cột mới":** theo bình luận ymkim1006 (và mã mình đọc), **batch read bảng PK không chỉ định cột** cũng dính. Câu trong `relate_issue.md` nên thêm kịch bản này, vì đây là kịch bản dễ gặp hơn (người dùng không cần chọn cột nào).
- **"Job đọc log/changelog có thể lỗi":** đúng cho đọc có projection; còn **streaming không projection thì theo bình luận không bị** (mình chưa kiểm chứng nhánh này).

## 5. Mức ảnh hưởng với CDP

- **Lookup KV cho AI agent:** mình không thấy liên hệ trực tiếp, vì lookup đọc dữ liệu KV theo khóa chứ không đi qua đường fetch log có chiếu cột. Mình chưa kiểm chứng riêng đường lookup.
- **Ghi context từ AI agent:** không ảnh hưởng.
- **Job Flink đọc bảng profile:** ảnh hưởng nếu có **batch read bảng PK** (đối soát, export, backfill, đồng bộ sang nơi khác) hoặc **đọc changelog có chọn cột** ngay sau khi `ADD COLUMN`. CDP "thường xuyên ADD COLUMN thuộc tính mới" nên rủi ro có thật cho các job này.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Sau `ADD COLUMN`, tránh chạy batch read bảng PK cho tới khi có snapshot KV mới** (chờ tối đa `kv.snapshot.interval`, mặc định 10 phút, hoặc kiểm tra lại sau một chu kỳ snapshot). Có thể đưa vào quy trình DDL: thêm cột rồi chờ.
- **Lên lịch các job batch read ngoài thời điểm vừa đổi schema.**
- **Với đọc changelog có projection:** nếu job không cần cột mới ngay, giữ danh sách cột cũ cho tới khi dữ liệu cũ hết hạn, hoặc khởi động lại job sau khi vùng log không còn lô cũ. Cân nhắc thử trước.
- **Thử nghiệm quy trình `ADD COLUMN` đầu-cuối** trên môi trường thử, gồm một job batch read và một job streaming có projection (đúng đề xuất của `relate_issue.md`).
- Cherry-pick hoặc theo dõi PR #4927; thay đổi nằm ở `FileLogProjection` và `LogSegment`, khá cô lập, nhưng chưa được review xong.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện lỗi; chuỗi nguyên nhân dựa trên bình luận của ymkim1006 và mã `v1.0.0` (đã khớp các điểm: `ProjectionPlan`, `KvSnapshotAndLogBatchScanner`, `toBitSet`).
- Mình chưa kiểm tra nhánh streaming không chiếu cột (`getOutputProjectedRow`) và đường lookup.
- Mình chưa đọc đầy đủ diff PR #4927 và chưa biết kết quả thảo luận giữa hai hướng sửa.
- Mình chưa xác nhận con số "tối đa một chu kỳ snapshot" cho mọi trường hợp (ví dụ bảng không bật snapshot hoặc bucket chưa có dữ liệu mới).
- Mình chưa kiểm tra giá trị mặc định của cột mới được xử lý thế nào khi điền NULL (PR đang mô tả điền NULL).
