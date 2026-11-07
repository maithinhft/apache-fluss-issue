# Issue #2353 — `LIMIT` trên bảng partition có điều kiện partition có thể trả về dòng của partition khác

Link: https://github.com/apache/fluss/issues/2353 — Phiên bản được báo: `0.8.0` (người báo: luoyuxia, 12/01/2026). Issue kèm test tái hiện trong `FlinkUnionReadPrimaryKeyTableITCase`: câu `select * from <bảng partition> where c16 = 'not_exists' limit 10` (với `c16` là cột partition) trả về **4 dòng**, trong khi đúng ra phải **rỗng**. Chính người báo ghi:
- trên `main` lỗi **không còn xảy ra**, vì sau [#1934](https://github.com/apache/fluss/pull/1934) filter không còn được "nhận", nên `LIMIT` không được đẩy xuống source;
- issue được mở để **theo dõi**, phòng khi sau này filter lại được nhận.

Có 1 bình luận (cc một committer). Không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi chạy **Flink SQL batch** (truy vấn ad-hoc) có `LIMIT` trên **bảng partition**.

> **Mức chắc chắn:** cao về trạng thái "**không xảy ra trong 1.0.0 qua Flink SQL**". Mình đã đọc mã `v1.0.0`: `applyFilters` luôn trả **toàn bộ** filter về cho Flink (#1934, 24/12/2025, có trong tag `v1.0.0`). Đồng thời mình xác nhận **gốc lỗi vẫn còn**: `PushdownUtils.limitScan` **không nhận** điều kiện partition. Phần "Flink không đẩy LIMIT qua một Filter còn lại" là kiến thức về planner của Flink, mình không đọc mã Flink. Mình chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Filter pushdown** (`SupportsFilterPushDown.applyFilters`) | Flink hỏi source: *"bạn tự lọc được điều kiện nào?"*. Source trả hai danh sách: **accepted** (source tự lọc) và **remaining** (Flink vẫn phải tự lọc lại sau). |
| **Limit pushdown** (`SupportsLimitPushDown.applyLimit`) | Flink báo source: *"chỉ cần N dòng"*. Source có thể đọc nhanh N dòng rồi dừng. |
| **Limit scan** | Đường đọc đặc biệt của Fluss khi có `LIMIT`: `PushdownUtils.limitScan` đọc **trực tiếp** N dòng bằng `BatchScanner`, trả về một tập dòng cố định thay vì chạy source bình thường. |
| **Partition filter** | Điều kiện chỉ trên cột partition (ví dụ `dt = '2026-01-01'`). Fluss dùng nó để chọn partition cần đọc (`partitionFilters`). |
| **#1934** | Commit *"Always return all filters to flink when trying to apply filters in FlinkTableSource"* (24/12/2025). Gắn với FLINK-38635: source có thể bị dùng làm lookup source, nơi filter không được áp dụng, nên luôn để Flink lọc lại. |

## Tóm tắt

Ở 0.8.0, chuỗi gây lỗi là:
1. Fluss **nhận** điều kiện partition (`c16 = 'not_exists'`), nên Flink bỏ Filter khỏi kế hoạch.
2. `LIMIT` lúc này nằm **sát** source, nên Flink đẩy xuống (`applyLimit`).
3. Fluss chạy **limit scan**, nhưng limit scan **bỏ qua điều kiện partition** và đọc N dòng **bất kỳ** của bảng.
4. Flink điền giá trị cột partition theo điều kiện đã đẩy (`not_exists`) vào dòng kết quả. Kết quả là **dòng sai**: dữ liệu của partition khác nhưng hiển thị như thuộc partition `not_exists`.

Trong `v1.0.0`, bước 1 đã thay đổi: Fluss vẫn dùng điều kiện partition nội bộ, nhưng **luôn trả mọi filter về là "remaining"**. Flink giữ một Filter giữa `LIMIT` và source, nên `LIMIT` **không được đẩy xuống**, và limit scan không chạy cho truy vấn có `WHERE`. Lỗi **không lộ ra**, nhưng `limitScan` **vẫn không biết** về partition filter. Đây là "lỗi ngủ".

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink/fluss-flink-common`, gồm `source/FlinkTableSource` (`applyFilters`, `applyLimit`, `getScanRuntimeProvider`) và `utils/PushdownUtils.limitScan`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `applyFilters` vẫn tính `partitionFilters` và thêm filter chỉ-trên-cột-partition vào `acceptedFilters`, nhưng **kết thúc bằng**:
    ```java
    // FLINK-38635 ... Always return all original filters as remaining so Flink applies them as a safety net.
    return Result.of(acceptedFilters, filters);
    ```
    Nhánh lookup theo khóa chính (`singleRowFilter`) cũng `return Result.of(acceptedFilters, filters)`.
  - `applyLimit(long limit) { this.limit = limit; }`: không kiểm tra có partition filter hay không.
  - `getScanRuntimeProvider`:
    ```java
    if (singleRowFilter != null || limit > 0 || selectRowCount) {
        ...
        } else if (limit > 0) {
            results = PushdownUtils.limitScan(tablePath, flussConfig, tableOutputType, projectedFields, limit);
    ```
    `partitionFilters` **không được truyền** vào `limitScan`.
  - `PushdownUtils.limitScan`: `table.newScan().project(projectedFields).limit(limit).createBatchScanner()`. Không có tham số partition.
  - `TableScan.createBatchScanner()` với bảng partition: `listPartitionInfos` rồi tạo scanner cho **mọi bucket của mọi partition**, gộp bằng `CompositeBatchScanner(scanners, limit)`. Tức là đọc từ **toàn bảng**.
  - Giới hạn: `limit > MAX_LIMIT_PUSHDOWN` thì ném `UnsupportedOperationException("LIMIT statement doesn't support greater than ...")`.
- **Lịch sử:** `git log` của tag `v1.0.0` có `50cd9ff0 | 2025-12-24 | ... (#1934)`.

## 2. Khi nào lỗi xảy ra

**Trong `v1.0.0` qua Flink SQL:** theo phân tích, **không xảy ra**. `LIMIT` không được đẩy xuống khi có `WHERE`, vì Filter còn lại nằm giữa.

`SELECT * FROM t LIMIT 10` **không có `WHERE`** vẫn đi limit scan. Trường hợp đó đúng: không có điều kiện nào bị bỏ qua.

**Có thể lộ lại nếu:**
- một phiên bản sau thay đổi `applyFilters` để nhận filter partition (như issue lo ngại);
- hoặc một đường khác (Spark connector, client tự viết) dùng limit scan kèm partition. Mình **chưa kiểm tra** Spark: commit #3346 *"Implement SupportsPushDownLimit DSv2 interface"* (01/06/2026) có trong 1.0.0.

**Phiên bản:** báo trên `0.8.0`. Ở `1.0.0` bị che bởi #1934.

## 3. Tác động đến ai, làm sai lệch gì

**Ở 0.8.0 (và nếu lộ lại):**
- Truy vấn ad-hoc `... WHERE <cột partition> = X LIMIT N` trả về **dòng của partition khác**, mang **giá trị partition sai**. Kết quả **sai âm thầm**, không có lỗi.
- Ảnh hưởng tới người hoặc agent dùng truy vấn ad-hoc để xem mẫu dữ liệu. Không ảnh hưởng job streaming.

**Ở `v1.0.0`:** không có tác động sai kết quả qua Flink SQL. Cái giá của #1934: truy vấn `WHERE ... LIMIT N` **không dùng được limit scan nhanh**, nên chạy source batch thông thường (chậm hơn với bảng lớn). Đây là suy luận.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Flink SQL batch / ad-hoc | #2353 ... | 🟡 → 🟢 | #2353: 🟡 #1934 đã vào 1.0.0"*. Đối chiếu:
- **Đúng:** 🟡, lỗi bị che nhờ #1934 có trong 1.0.0.
- **Bổ sung:** gốc lỗi (limit scan bỏ qua partition filter) **vẫn còn trong mã 1.0.0**. Chưa kiểm tra đường Spark.

## 5. Mức ảnh hưởng với CDP

- **Thấp** nếu chỉ truy vấn ad-hoc bằng Flink SQL 1.0.0.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi nâng cấp Fluss lên bản mới, **chạy lại** một test kiểu `WHERE <cột partition> = '<không tồn tại>' LIMIT 10` (phải ra rỗng) trước khi dùng production.
- Nếu dùng **Spark** để truy vấn ad-hoc bảng partition, kiểm tra cùng truy vấn trên Spark trước khi tin kết quả `LIMIT`.
- Không dùng kết quả `LIMIT` (lấy mẫu) làm căn cứ cho logic nghiệp vụ hoặc cho AI agent khi chưa kiểm chứng. Nên dùng lookup theo khóa hoặc truy vấn có điều kiện đầy đủ.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test của issue trên 1.0.0.
- Mình không đọc mã planner Flink. Kết luận "LIMIT không đẩy qua Filter" dựa trên hiểu biết về rule `PushLimitIntoTableSourceScanRule` (khớp `Sort` nằm ngay trên `TableSourceScan`).
- Mình chưa kiểm tra Spark connector (`SupportsPushDownLimit`, #3346) có kết hợp limit với partition filter đúng hay không.
