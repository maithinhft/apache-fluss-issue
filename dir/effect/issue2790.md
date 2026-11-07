# Issue #2790 — Đọc Paimon có thể không giải phóng reader nếu bước bọc iterator ném lỗi

Link: https://github.com/apache/fluss/issues/2790 — Phiên bản được báo: `0.9.0` (người báo: ThorneANN, 04/03/2026). Issue ngắn, chỉ ra đoạn mã cụ thể và đề xuất cách sửa (`try/catch` quanh constructor để đóng iterator khi lỗi). Không có bình luận, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi đọc dữ liệu từ lake **Paimon** qua Fluss (union read, `$lake` qua đường Fluss, tiering đọc lại...).

> **Mức chắc chắn:** cao về việc **lỗ hổng có trong mã `v1.0.0`**. Về **khả năng xảy ra**, mình đã đọc constructor bị nghi: nó chỉ làm việc nhẹ (kiểm tra kiểu bảng, tạo projection), nên **gần như không ném exception** trong điều kiện bình thường. Rủi ro thực tế thấp. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`PaimonRecordReader`** | Lớp của Fluss đọc một split dữ liệu Paimon và trả bản ghi dạng Fluss. |
| **`RecordReader` / `CloseableIterator` (Paimon)** | Đối tượng đọc file Paimon. Giữ file handle, buffer, và có thể cả kết nối tới object store. **Phải `close()`**. |
| **`PaimonRowAsFlussRecordIterator`** | Lớp bọc iterator của Paimon thành iterator bản ghi Fluss. |
| **Rò rỉ tài nguyên** | Mở mà không đóng. Lâu dần cạn file handle, kết nối hoặc bộ nhớ. |

## Tóm tắt

Trong constructor của `PaimonRecordReader`, mã tạo `RecordReader` của Paimon, gọi `toCloseableIterator()`, rồi **truyền thẳng** vào constructor của `PaimonRowAsFlussRecordIterator`. Nếu constructor đó **ném lỗi**, iterator Paimon đã mở không được gán vào trường nào, nên **không ai đóng nó được**. Đề xuất sửa: tách ra biến cục bộ, `try/catch`, và đóng iterator khi lỗi.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-lake/fluss-lake-paimon`, `source/PaimonRecordReader`.
- **Đã xác nhận trong mã `v1.0.0`:**
  ```java
  org.apache.paimon.reader.RecordReader<InternalRow> recordReader = tableRead.createReader(split.dataSplit());
  iterator = new PaimonRecordReader.PaimonRowAsFlussRecordIterator(recordReader.toCloseableIterator(), paimonRowType);
  ```
  Không có `try/catch` hay `finally`.
- **Constructor bị nghi** (`PaimonRowAsFlussRecordIterator(...)`) trong 1.0.0 chỉ:
  - gán trường;
  - tính `businessFieldCount` (dựa vào `PaimonUtils.isLegacyTable(paimonRowType)`);
  - `ProjectedRow.from(IntStream.range(0, businessFieldCount).toArray())`;
  - `new PaimonRowAsFlussRow()`.

  Không có I/O. Mình không thấy đường nào ném exception có kiểm soát. Chỉ lỗi bất thường (ví dụ `OutOfMemoryError`, hoặc kiểu dữ liệu bất ngờ khiến `businessFieldCount` âm) mới kích hoạt lỗ hổng (suy luận).
- `PaimonSortedRecordReader` (dùng cho union read bảng PK, xem #4979) là lớp khác. Mình chưa kiểm tra nó có cùng mẫu không.

## 2. Khi nào lỗi xảy ra

Chỉ khi constructor `PaimonRowAsFlussRecordIterator` ném lỗi **sau khi** `createReader` và `toCloseableIterator` đã mở tài nguyên. Theo mục 1, trường hợp này rất hiếm.

## 3. Tác động đến ai, làm sai lệch gì

- Nếu xảy ra: rò **một** reader Paimon mỗi lần. Chỉ đáng lo nếu lặp lại nhiều lần trong một process sống lâu (TaskManager).
- **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #2790 trong nhóm Paimon (❌). Đối chiếu:
- **❌ đúng** về mã.
- **Bổ sung:** rủi ro thực tế rất thấp, vì constructor gần như không ném lỗi.

## 5. Mức ảnh hưởng với CDP

- **Rất thấp.**
- Nếu dùng Paimon và thấy TaskManager cạn file handle hoặc kết nối khi job đọc lake hay lỗi, có thể nghĩ tới lỗi này. Nhưng nên xét các nguyên nhân khác trước.

## 6. Những điều chưa kiểm chứng

- Mình chưa kiểm tra `PaimonSortedRecordReader` và reader của Iceberg có cùng mẫu không.
- Mình chưa kiểm tra `PaimonUtils.isLegacyTable` có thể ném lỗi với schema bất thường hay không.
