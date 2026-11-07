# Issue #4979 — Trộn nhiều luồng snapshot của lake bị sai thứ tự, làm bản ghi đã DELETE xuất hiện lại khi union read

Link: https://github.com/apache/fluss/issues/4979 — Phiên bản được báo: `1.0.0` (người báo: slfan1989, 04/11/2026). Issue ngắn, có ví dụ tái hiện tối giản và hướng sửa. Không có bình luận. Trạng thái: còn mở. PR [#5036](https://github.com/apache/fluss/pull/5036) **đang mở**.
**Nhóm B:** chỉ khi **union read bảng PK có lakehouse**. Theo mã 1.0.0, trên thực tế chỉ có **Paimon** (và Hudi), vì Iceberg chưa hỗ trợ union read bảng PK (#3048).

> **Mức chắc chắn:** cao. Mình đã đọc `SortMergeReader.ConcatRecordIterator` trong `v1.0.0`: lỗi đúng như mô tả. Mình cũng lần ra **đường nào bị ảnh hưởng**: chỉ `LakeSnapshotAndLogSplitScanner` (đọc lake cộng log). Đường batch read từ **KV snapshot** (`KvSnapshotAndLogBatchScanner`) chỉ có **một** luồng snapshot nên **không** bị. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Union read bảng PK** | Đọc trạng thái đầy đủ của bảng PK: lấy **snapshot trong lake** (Paimon) của từng bucket, rồi **áp phần changelog mới hơn** từ log Fluss lên trên. |
| **Lake split** | Một phần dữ liệu lake của bucket, thường ứng với một nhóm file. Một bucket có thể có **nhiều** lake split. |
| **`SortedRecordReader`** | Reader của lake trả bản ghi **đã sắp xếp theo khóa chính**. Mỗi lake split cho một luồng đã sắp xếp. |
| **Sort-merge** | Đi song song qua hai luồng **cùng thứ tự khóa** (snapshot và changelog đã gom theo khóa): key có thay đổi thì lấy bản mới, key bị `DELETE` thì bỏ. Thuật toán **chỉ đúng nếu cả hai luồng thật sự đã sắp xếp**. |
| **Priority queue (heap)** | Cấu trúc để trộn nhiều luồng đã sắp xếp thành một: luôn lấy luồng có khóa đầu nhỏ nhất. **Sau khi lấy một phần tử, phải đặt lại luồng đó vào heap** theo khóa kế tiếp. |

## Tóm tắt

Ví dụ trong issue: snapshot có hai luồng `[1, 3]` và `[2, 4]`, changelog có `DELETE key=2`. Kết quả **mong đợi** là `[1, 3, 4]`, nhưng **thực tế** ra `[1, 3, 2, 4]`.

Nguyên nhân: `ConcatRecordIterator` (bộ trộn nhiều luồng snapshot) lấy phần tử từ luồng **đứng đầu heap** nhưng **không đặt lại** luồng đó vào heap theo khóa kế tiếp. Nó trả hết luồng đầu (`1, 3`) rồi mới sang luồng khác (`2, 4`). Luồng "đã trộn" vì vậy **không còn đúng thứ tự**. Bước sort-merge với changelog, vốn giả định thứ tự, **bỏ lỡ** `DELETE key=2`, và bản ghi đã xóa **xuất hiện lại**. Update cũng có thể bị áp sai (suy luận).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, gồm `table/scanner/SortMergeReader` (lớp con `ConcatRecordIterator`) và `table/scanner/batch/LakeSnapshotAndLogSplitScanner`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `ConcatRecordIterator`: constructor đưa phần tử đầu của mỗi luồng vào `PriorityQueue` theo comparator khóa chính.
    ```java
    public boolean hasNext() {
        while (!priorityQueue.isEmpty()) {
            CloseableIterator<LogRecord> iterator = priorityQueue.peek();
            if (iterator.hasNext()) return true;
            priorityQueue.poll().close();
        }
        return false;
    }
    public LogRecord next() {
        if (!hasNext()) throw new NoSuchElementException();
        return priorityQueue.peek().next();          // không poll + re-offer theo khóa mới
    }
    ```
    `peek().next()` tiêu thụ luồng đầu mà không sắp xếp lại heap. Kết quả là nối tiếp, không phải trộn.
  - Hai nơi tạo `SortMergeReader`:
    - `LakeSnapshotAndLogSplitScanner.pollBatch` truyền `lakeRecordIterators`, một danh sách gồm **một iterator cho mỗi lake split** (`initializeLakeRecordIterators`: `for (LakeSplit lakeSplit : lakeSplits) recordReaders.add(lakeSource.createRecordReader(...))`).
    - `KvSnapshotAndLogBatchScanner.pollBatch` truyền **một** `snapshotRecords` (KV snapshot). Constructor dùng một luồng, nên không đi qua `ConcatRecordIterator` với nhiều luồng.
  - `initializeLakeRecordIterators` yêu cầu reader là `SortedRecordReader`, nếu không thì ném `UnsupportedOperationException("lake records must instance of sorted view.")`. Iceberg không có reader loại này (#3048).
  - `LakeSnapshotAndLogSplitScanner` được tạo từ `fluss-flink` `lake/LakeSplitReaderGenerator`.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Union read bảng PK** có lake **Paimon** (hoặc Hudi). Iceberg chưa hỗ trợ đường này.
2. Một bucket có **nhiều lake split** mà khóa **xen kẽ** nhau giữa các split.
3. Changelog Fluss (phần chưa tier) có **DELETE hoặc UPDATE** cho các khóa nằm xen giữa.

Issue nói về **batch read**. Mình chưa kiểm tra union read streaming có đi qua cùng scanner hay không.

**Phiên bản:** báo trên `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job hoặc truy vấn đọc trạng thái đầy đủ của bảng PK có lake Paimon qua Fluss (Flink batch, đối soát, export).

**Hậu quả:**
1. **Kết quả đọc sai:** bản ghi đã xóa hiện lại. Với update, có thể ra cả giá trị cũ lẫn giá trị mới, hoặc giá trị cũ (suy luận).
2. Kết quả **không đúng thứ tự khóa**. Downstream giả định thứ tự có thể sai thêm.
3. **Dữ liệu lưu trữ không hỏng.** Lookup của AI agent (đọc KV qua RPC) **không** bị.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4979 trong `relate_issue.md` ghi: *"Lakehouse tiering (chung) | batch read merge snapshot sai thứ tự, bản ghi đã DELETE xuất hiện lại | 🟠 | ❌ PR #5036 chưa merge"*. Đối chiếu:
- **Đúng.**
- **Bổ sung phạm vi:** chỉ union read bảng PK với lake **Paimon hoặc Hudi**, khi bucket có nhiều lake split. Batch read **không có lake** (từ KV snapshot) **không** bị.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu CDP tier **bảng profile (PK)** sang **Paimon** và có job batch đọc trạng thái đầy đủ qua Fluss (union read).
- Không ảnh hưởng luồng lookup và ghi chính.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Với export hoặc đối soát cần chính xác: đọc **trực tiếp từ bảng Paimon** (Paimon tự merge đúng), hoặc đọc qua Fluss **không dùng lake** (batch read từ KV snapshot), thay vì union read.
- **Kiểm tra mẫu:** so số bản ghi và một số khóa đã DELETE gần đây giữa kết quả union read và kết quả lookup.
- Giảm số lake split trên mỗi bucket (compaction phía Paimon, file lớn hơn). Đây là suy luận: ít split thì ít xen kẽ.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện với Paimon thật.
- Mình chưa kiểm tra Paimon tạo bao nhiêu lake split cho mỗi bucket trong thực tế.
- Đường union read **streaming** đã kiểm tra trong `LakeSplitReaderGenerator.getBatchScanner` (1.0.0): khi `lakeSplit.isStreaming()`, mã dùng `SeekableLakeSnapshotSplitScanner`, **không** dùng `SortMergeReader`. Vậy lỗi này chỉ ảnh hưởng **batch**, khớp với mô tả của issue.
- Mình chưa đọc diff của PR #5036.
