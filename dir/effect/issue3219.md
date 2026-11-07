# Issue #3219 — `LIMIT` trên bảng log có cột kiểu phức hợp (ARRAY/MAP/ROW) ném `IndexOutOfBoundsException`

Link: https://github.com/apache/fluss/issues/3219 — Phiên bản được báo: `0.9.0` (người báo: loserwang1024, 28/04/2026). Issue có:
- cách tái hiện: sửa `FlinkComplexTypeITCase#testComplexTypesInLogTable` thành `select * from complex_log_test limit 10`;
- stack trace `IndexOutOfBoundsException: index: 0, length: 1 (expected: range(0, 0))` tại `ArrowBuf.checkIndexD`, gọi từ `ColumnarArray.isNullAt`, `FlussRowToFlinkRowConverter.toFlinkRowData` và `PushdownUtils.limitScan`;
- phân tích nguyên nhân và đề xuất sửa.

Trạng thái: **đã đóng**. PR [#3220](https://github.com/apache/fluss/pull/3220) **đã đóng** (không merge). Bản sửa thực tế là commit [#3221](https://github.com/apache/fluss/pull/3221) *"[client] limit scan use DeepFieldGetter to avoid memory release when lazy complex type initialization"* (28/04/2026), **có trong tag `v1.0.0`**.
**Nhóm B:** chỉ khi chạy **Flink SQL batch** có `LIMIT` trên **bảng log** có cột **ARRAY, MAP hoặc ROW**.

> **Mức chắc chắn:** cao, và kết luận là **đã sửa trong 1.0.0**. Lần phân tích trước, mình thấy commit #3221 nằm trong lịch sử của tag `v1.0.0`. Lần này mình đọc lại mã `v1.0.0` và thấy đường đọc Arrow dùng `InternalRow.createDeepFieldGetter`, hàm **sao chép sâu** ARRAY, MAP và ROW ra `GenericArray`, `GenericMap` và `GenericRow`. Mình chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Limit scan** | Đường đọc nhanh khi Flink đẩy `LIMIT` xuống source: `PushdownUtils.limitScan` dùng `BatchScanner` đọc N dòng (xem #2353). |
| **Arrow / `VectorSchemaRoot`** | Bảng log mặc định lưu theo định dạng cột Arrow. Mỗi lô dữ liệu đọc lên là một `VectorSchemaRoot` với các buffer bộ nhớ. |
| **`ColumnarRow` / `ColumnarArray`** | Dòng (mảng) "ảo" **trỏ thẳng vào** buffer Arrow, không sao chép dữ liệu. |
| **Lazy view** | Đọc trường ARRAY/MAP/ROW chỉ trả về một view vẫn trỏ vào buffer. Giá trị thật được đọc **khi truy cập**. |
| **Giải phóng buffer** | Khi chuyển sang lô tiếp theo, buffer Arrow của lô trước bị `clear()` (dung lượng về 0). Commit #2728 (*"Clear VectorSchemaRoot to release buffer ASAP"*, 11/04/2026) làm việc giải phóng diễn ra **sớm** hơn. |
| **Deep copy** | Sao chép toàn bộ giá trị (kể cả phần tử lồng nhau) ra đối tượng độc lập với buffer. |

## Tóm tắt

`collectRows(batchScanner)` **gom tất cả** dòng của mọi lô vào một danh sách, **sau đó** mới chuyển sang dòng Flink. Kiểu đơn giản (int, string) được sao chép khi đọc nên an toàn. Kiểu **phức hợp** chỉ là view trỏ vào buffer Arrow. Khi scanner chuyển lô, buffer lô cũ bị giải phóng, nên lúc chuyển đổi, view trỏ vào **buffer rỗng** và gây `IndexOutOfBoundsException`.

Bản sửa #3221 (có trong 1.0.0): khi đọc từ Arrow, trường phức hợp được **sao chép sâu ngay** bằng `createDeepFieldGetter`, nên dòng trong danh sách không còn phụ thuộc buffer.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-flink/fluss-flink-common`: `utils/PushdownUtils.limitScan`;
  - `fluss-client`: `table/scanner/batch/BatchScanUtils.collectRows`;
  - `fluss-common`: `record/LogRecordReadContext`, `row/InternalRow.createDeepFieldGetter`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `BatchScanUtils.collectRows` **vẫn** gom hết dòng của mọi lô rồi trả về (`rows.addAll(toList(iterator))`). Cấu trúc gom-rồi-chuyển không đổi.
  - `PushdownUtils.limitScan` vẫn gọi `collectRows(batchScanner)` rồi mới `toFlinkRowData(row)` trong vòng lặp riêng.
  - Điểm khác: `LogRecordReadContext.buildProjectedFieldGetters` dùng
    ```java
    // build deep field getter to support nested types
    fieldGetters[i] = InternalRow.createDeepFieldGetter(dataTypeList.get(selectedFields[i]), selectedFields[i]);
    ```
  - `InternalRow.createDeepFieldGetter`:
    - `ARRAY`: chép từng phần tử ra `new GenericArray(objs)`;
    - `MAP`: chép key và value ra `new GenericMap(javaMap)`;
    - `ROW`: chép từng trường lồng nhau.

    Javadoc: *"Currently, it is only used for deep copying ColumnarRow for Arrow which avoid the arrow buffer is released before accessing elements. It doesn't deep copy STRING and BYTES types, because ColumnarRow already deep copies the bytes."*

## 2. Khi nào lỗi xảy ra

**Ở bản bị lỗi (0.9.0):** Flink SQL batch, `SELECT ... FROM <bảng log có cột ARRAY/MAP/ROW> LIMIT N` (không có `WHERE`, nên `LIMIT` được đẩy xuống), và dữ liệu trải trên **nhiều lô**.

**Ở `v1.0.0`:** không xảy ra theo phân tích mã.

## 3. Tác động đến ai, làm sai lệch gì

**Ở bản bị lỗi:** truy vấn **thất bại** với exception. Không trả kết quả sai, không ảnh hưởng dữ liệu.

**Ở `v1.0.0`:** không còn tác động. Cái giá là có thêm chi phí sao chép cho cột phức hợp ở đường đọc Arrow (không đáng kể với `LIMIT` nhỏ).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Flink SQL batch / ad-hoc | ... #3219 (limit scan kiểu phức hợp) ... | Còn lại ❌"* trong `relate_issue.md`. Đối chiếu:
- **Sai trạng thái:** #3219 **đã đóng** và **đã sửa trong 1.0.0** (#3221), nên phải là **✅**.

## 5. Mức ảnh hưởng với CDP

- **Không còn ảnh hưởng** với Fluss 1.0.0 và connector Flink 1.0.0.
- Nếu còn job hoặc công cụ ad-hoc dùng connector **0.9**, nâng cấp connector lên 1.0.0.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test của issue trên 1.0.0.
- Mình chưa lần theo toàn bộ đường gọi để chắc chắn `BatchScanner` dùng cho `limitScan` trên **bảng log** đi qua `buildProjectedFieldGetters` trong mọi trường hợp (có projection hoặc không, có schema evolution hay không). Bảng **PK** đọc limit từ KV (không phải Arrow), mình chưa kiểm tra.
- Mình chưa kiểm tra đường đọc lake (union read) với kiểu phức hợp.
