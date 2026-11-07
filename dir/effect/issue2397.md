# Issue #2397 — `UPDATE` trên bảng PK có cột tính toán (computed column) thất bại: "field count ... Expected: 6, Actual: 7"

Link: https://github.com/apache/fluss/issues/2397 — Phiên bản được báo: `0.8.0` (người báo: campossalex, 15/03/2026). Người báo làm theo quickstart Flink và chạy thêm `UPDATE fluss_order SET order_priority = 'high' WHERE order_key = 14404096;`, nhận lỗi:

`java.lang.IllegalArgumentException: The field count of the row does not match the table schema. Expected: 6, Actual: 7`

Có 2 bình luận: một người xin nhận việc và đã được giao. Trạng thái: còn mở. PR [#2876](https://github.com/apache/fluss/pull/2876) **đang mở**.
**Nhóm B:** chỉ khi chạy **`UPDATE` (Flink SQL batch)** trên bảng PK khai báo trong Flink catalog **có cột tính toán** (ví dụ `ptime AS PROCTIME()`) hoặc cột metadata.

> **Mức chắc chắn:** trung bình đến cao. Mình đã đọc mã `v1.0.0`:
> - bảng `fluss_order` trong quickstart 1.0.0 vẫn có **6 cột vật lý + 1 cột tính toán** `ptime AS PROCTIME()`, khớp đúng "Expected 6, Actual 7";
> - sink được tạo với **kiểu dòng vật lý** (6 cột);
> - `applyRowLevelUpdate` trả `requiredColumns() = Optional.empty()`.
>
> Phần "Flink sinh dòng UPDATE gồm **mọi** cột của schema, kể cả cột tính toán, khi `requiredColumns` rỗng" là **suy luận** từ thông báo lỗi và hành vi của Flink. Mình không đọc mã planner Flink, và chưa chạy lại.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`UPDATE ... SET ... WHERE ...`** | Lệnh cập nhật dòng của Flink SQL (chỉ chạy ở **batch mode**). Fluss hỗ trợ cho bảng PK không dùng merge engine. |
| **Cột vật lý** | Cột thật được lưu trong bảng Fluss. Ở `fluss_order` có 6 cột: `order_key`, `cust_key`, `total_price`, `order_date`, `order_priority`, `clerk`. |
| **Cột tính toán (computed column)** | Cột chỉ tồn tại trong định nghĩa Flink, tính từ biểu thức, ví dụ `ptime AS PROCTIME()` (thời gian xử lý, dùng cho lookup join). Không lưu trong Fluss. |
| **`SupportsRowLevelUpdate.applyRowLevelUpdate`** | API Flink để sink nhận `UPDATE`. Sink trả `RowLevelUpdateInfo`, gồm `requiredColumns()` (sink cần những cột nào) và chế độ (`UPDATED_ROWS`: chỉ gửi dòng bị cập nhật). |
| **`checkFieldCount`** | Kiểm tra trong writer của Fluss client: dòng ghi phải có **đúng số cột** của bảng. |

## Tóm tắt

Khi chạy `UPDATE`, Flink sinh **dòng mới đầy đủ** cho mỗi dòng bị cập nhật và gửi xuống sink Fluss. Vì Fluss trả `requiredColumns() = Optional.empty()`, Flink dùng cấu trúc dòng theo **toàn bộ schema của bảng trong catalog**, gồm cả cột tính toán `ptime`, tức **7 trường** (suy luận). Trong khi đó, sink và writer Fluss được dựng theo **kiểu dòng vật lý** 6 cột. `AbstractTableWriter.checkFieldCount` phát hiện lệch và ném `IllegalArgumentException`, nên lệnh `UPDATE` thất bại.

Với bảng **không có** cột tính toán hay metadata, `UPDATE` hoạt động bình thường (tài liệu `writes.md` có ví dụ).

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-flink/fluss-flink-common`: `sink/FlinkTableSink` (`applyRowLevelUpdate`, `getSinkRuntimeProvider`), `catalog/FlinkTableFactory.createDynamicTableSink`;
  - `fluss-client`: `table/writer/AbstractTableWriter.checkFieldCount`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkTableFactory`: `RowType rowType = (RowType) context.getPhysicalRowDataType().getLogicalType();` rồi `new FlinkTableSink(..., rowType, ...)`. Sink chỉ biết **cột vật lý**.
  - `FlinkTableSink.applyRowLevelUpdate`:
    ```java
    appliedUpdates = true;
    return new RowLevelUpdateInfo() {
        public Optional<List<Column>> requiredColumns() {
            // TODO: return primary-key columns to support partial-updates after FLINK-36735 is resolved.
            return Optional.empty();
        }
        public RowLevelUpdateMode getRowLevelUpdateMode() { return RowLevelUpdateMode.UPDATED_ROWS; }
    };
    ```
  - `getSinkRuntimeProvider`: với `UPDATE`, bỏ qua partial update (*"skip applying partial-updates for UPDATE command as the Context#targetColumns is not correct, see FLINK-36736"*). Tức ghi **cả dòng**.
  - `AbstractTableWriter.checkFieldCount`: `row.getFieldCount() != fieldCount` thì ném đúng thông báo của issue.
  - Quickstart `website/docs/quickstart/flink.md` trong 1.0.0:
    ```sql
    CREATE TABLE fluss_order (
        `order_key` BIGINT, `cust_key` INT NOT NULL, `total_price` DECIMAL(15, 2),
        `order_date` DATE, `order_priority` STRING, `clerk` STRING,
        `ptime` AS PROCTIME(),
        PRIMARY KEY (`order_key`) NOT ENFORCED);
    ```
    6 cột vật lý + 1 cột tính toán.
- **Ràng buộc khác của `UPDATE`** (đọc trong `validateUpdatable` và `applyRowLevelUpdate`): không được cập nhật cột khóa chính; bảng log và bảng có merge engine không hỗ trợ `UPDATE` hay `DELETE`.

## 2. Khi nào lỗi xảy ra

- Flink SQL **batch**, lệnh `UPDATE` trên bảng **PK** của Fluss.
- Bảng có **cột tính toán** trong định nghĩa Flink (thường là `PROCTIME()` để lookup join), hoặc có thể là cột metadata (suy luận, chưa kiểm tra).

**Phiên bản:** báo trên `0.8.0`. Mã `v1.0.0` vẫn như vậy, và quickstart 1.0.0 vẫn dùng bảng có `ptime`.

## 3. Tác động đến ai, làm sai lệch gì

- Lệnh **`UPDATE` thất bại ngay**, rõ ràng. **Không ghi sai, không mất dữ liệu.**
- Ảnh hưởng tới vận hành **sửa dữ liệu thủ công** (data fix) bằng SQL trên bảng có `ptime`. Đây là kiểu bảng rất hay gặp trong CDP, vì bảng dimension cần `PROCTIME()` cho lookup join.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Flink SQL batch / ad-hoc | ... #2397 (`UPDATE` bảng PK) | 🟡 → 🟢 | Còn lại ❌"*. Đối chiếu:
- **Đúng** về trạng thái ❌.
- **Chính xác hơn:** không phải mọi `UPDATE` trên bảng PK đều lỗi, mà chỉ khi bảng có **cột tính toán** (hoặc metadata) trong định nghĩa Flink.

## 5. Mức ảnh hưởng với CDP

- **Thấp đến trung bình:** chỉ ảnh hưởng thao tác sửa dữ liệu thủ công, không ảnh hưởng pipeline.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Không khai báo cột tính toán trong bảng Fluss.** Thay vào đó, thêm `PROCTIME()` ở **phía truy vấn** khi cần lookup join, ví dụ:
  - view `CREATE TEMPORARY VIEW v AS SELECT *, PROCTIME() AS ptime FROM t`;
  - hoặc `CREATE TEMPORARY TABLE ... LIKE` với cột tính toán, chỉ dùng cho truy vấn đọc.

  Mình chưa kiểm tra lookup join qua view hoạt động y hệt.
- Thay `UPDATE` bằng **`INSERT INTO`** (upsert theo khóa chính) với **toàn bộ** giá trị cột vật lý của dòng. Hoặc dùng **partial update** `INSERT INTO t (pk, col) VALUES ...` (chỉ bảng không dùng merge engine hoặc dùng aggregation). Cách này đi qua đường INSERT, chỉ dùng cột vật lý.
- Thử trước trên môi trường test.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy lại lệnh `UPDATE` trên 1.0.0.
- Mình chưa đọc mã planner Flink, nên chưa xác nhận chính xác vì sao dòng có 7 trường.
- Mình chưa kiểm tra cột **metadata** (`METADATA`) có gây cùng lỗi hay không, và chưa kiểm tra `DELETE` (đường `executeDeletion` dựng `GenericRow(tableRowType.getFieldCount())`, có vẻ không bị).
- Mình chưa đọc diff của PR #2876, và chưa kiểm tra `main` có sửa hay chưa.
