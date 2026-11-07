# Issue #3048 — Iceberg không hỗ trợ union read (batch) cho bảng Primary Key

Link: https://github.com/apache/fluss/issues/3048 — Phiên bản được báo: `0.9.0` (người báo: Yohahaha, 09/04/2026). Người báo chỉ ra:
- `IcebergLakeSource#createRecordReader` trả `IcebergRecordReader`, **không phải** `SortedRecordReader`, nên `LakeSnapshotAndLogSplitScanner` lỗi khi `pollBatch`;
- test `testReadIcebergLakeTable` chỉ đọc `$lake` chứ không thật sự union read.

Có 4 bình luận:
- maintainer: *"known feature gap"*, vì union read bảng PK sẽ cần merge-sort nặng, và hướng đi là **deletion vectors**;
- một người đề xuất sắp xếp khi ghi hoặc khi đọc;
- người khác chỉ ra rằng sắp xếp khi đọc có lỗi đúng sai (`IcebergRecordAsFlussRecordIterator` dùng lại đối tượng row), và PR #2561 đang treo;
- maintainer trỏ tới FIP-47 (DV cho Paimon) và WIP FIP-31 (DV cho Iceberg).

Không có PR đang hoạt động. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bảng **PK** tier sang **Iceberg**.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và làm rõ **phạm vi chính xác**:
> - **Union read streaming** bảng PK trên Iceberg **đi đường khác**, không cần reader sắp xếp, nên theo mã **không bị** lỗi này;
> - **union read batch** thì **bị**, vì đi qua `LakeSnapshotAndLogSplitScanner`, và nó ném `UnsupportedOperationException("lake records must instance of sorted view.")`.
>
> Tài liệu Iceberg của 1.0.0 lại ghi bảng PK *"Supported in both batch and streaming mode"*, **mâu thuẫn** với mã cho chế độ batch. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Union read** | Đọc kết hợp phần lịch sử trong lake với phần mới trong log Fluss (xem #3691). |
| **Union read bảng PK, batch** | Phải trộn theo khóa: snapshot lake (đã sắp xếp theo khóa) với changelog Fluss chưa tier (gom theo khóa), để ra trạng thái cuối của mỗi khóa. Cần reader lake **trả bản ghi đã sắp xếp theo khóa**. |
| **Union read bảng PK, streaming** | Phát dữ liệu lake trước, rồi phát changelog Fluss tiếp theo dưới dạng **luồng thay đổi**. Flink tự áp `-U/+U/-D`, nên **không cần** trộn sắp xếp. |
| **`SortedRecordReader`** | Giao diện reader lake trả bản ghi theo thứ tự khóa, kèm comparator. Paimon có (`PaimonSortedRecordReader`), Hudi có. **Iceberg không có** trong 1.0.0. |
| **Equality delete** | Cách Iceberg biểu diễn update và delete theo giá trị khóa. Tài liệu 1.0.0 nói update của bảng PK được tier sang Iceberg dưới dạng equality delete. |
| **Deletion vectors (DV)** | Bitmap đánh dấu dòng đã xóa, giúp đọc mà không cần trộn nặng. Đây là hướng thiết kế dài hạn (FIP-47, FIP-31). |

## Tóm tắt

`LakeSnapshotAndLogSplitScanner`, scanner cho **union read batch** (một bucket = snapshot lake + đoạn log tới stopping offset), yêu cầu mọi reader lake là `SortedRecordReader` để làm sort-merge với changelog. `IcebergRecordReader` **không** cài giao diện này, nên union read **batch** bảng PK trên Iceberg **lỗi**.

Union read **streaming** dùng `SeekableLakeSnapshotSplitScanner`: đọc snapshot lake theo thứ tự lake split, rồi để log Fluss chạy tiếp như luồng changelog. Không cần sắp xếp, nên Iceberg dùng được (theo mã).

Hướng giải quyết chính thức là deletion vectors cho Iceberg (FIP-31, đang ở dạng WIP). Các cách tạm như sắp xếp khi ghi hoặc khi đọc đều có nhược điểm và chưa được chấp nhận.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-lake/fluss-lake-iceberg`: `source/IcebergLakeSource`, `source/IcebergRecordReader`;
  - `fluss-client`: `table/scanner/batch/LakeSnapshotAndLogSplitScanner`;
  - `fluss-flink`: `lake/LakeSplitReaderGenerator`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `IcebergLakeSource.createRecordReader(...)`: `return new IcebergRecordReader(context.lakeSplit().fileScanTask(), table, project);`, với `public class IcebergRecordReader implements RecordReader` (không phải `SortedRecordReader`).
  - `LakeSnapshotAndLogSplitScanner.initializeLakeRecordIterators()`:
    ```java
    if (reader instanceof SortedRecordReader) { rowComparator = ((SortedRecordReader) reader).order(); }
    else { throw new UnsupportedOperationException("lake records must instance of sorted view."); }
    ```
  - `LakeSplitReaderGenerator.getBatchScanner(LakeSnapshotAndFlussLogSplit lakeSplit)`:
    - nếu `lakeSplit.isStreaming()` thì `new SeekableLakeSnapshotSplitScanner(lakeSource, lakeSplits, currentLakeSplitIndex)`;
    - ngược lại (batch, có stopping offset) thì `new LakeSnapshotAndLogSplitScanner(...)`.
- **Tài liệu 1.0.0** (`website/docs/streaming-lakehouse/datalake-formats/iceberg.md`, mục *Union Read*): *"Primary key tables: Supported in both batch and streaming mode. Updates tiered to Iceberg are written as equality deletes."* Mâu thuẫn với mã ở chế độ **batch**. Có thể tài liệu nói về trường hợp log sau snapshot **rỗng**, nhưng mình chưa kiểm tra liệu khi đó `initializeLakeRecordIterators` có còn được gọi hay không.

## 2. Khi nào lỗi xảy ra

- Bảng **PK** có lake **Iceberg**.
- Truy vấn Flink **batch** (`execution.runtime-mode = batch`) đọc bảng **không có hậu tố** (union read). Mình chưa kiểm tra trường hợp log rỗng.
- **Không** xảy ra với: streaming union read (theo mã), `$lake` (đọc thẳng Iceberg), bảng log trên Iceberg, hay bảng PK trên Paimon.

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

- Job batch (đối soát, export, backfill) đọc bảng PK trên Iceberg qua Fluss **lỗi** với `UnsupportedOperationException`. Lỗi rõ ràng, không sai dữ liệu ngầm.
- Người làm theo tài liệu sẽ bất ngờ, vì tài liệu ghi là hỗ trợ.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` ghi *"#3048 (chưa hỗ trợ union read bảng PK)"* trong nhóm Iceberg. Đối chiếu:
- **Chính xác hơn:** chỉ **union read batch** bị. Union read streaming đi đường khác (theo mã).
- **Bổ sung:** tài liệu 1.0.0 ghi là hỗ trợ cả batch, mâu thuẫn với mã.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu tier **bảng profile (PK)** sang **Iceberg** và cần **batch đọc trạng thái đầy đủ** qua Fluss.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Với bảng PK cần union read batch: dùng **Paimon** (có `PaimonSortedRecordReader`), nhưng lưu ý #4979.
- Nếu dùng Iceberg:
  - đọc lịch sử bằng `$lake` (Iceberg tự áp equality delete), và dữ liệu mới bằng Fluss, rồi tự ghép ở tầng ứng dụng;
  - hoặc dùng union read **streaming** có giới hạn.
- **Test** union read batch với đúng định dạng lake trước khi dựa vào tài liệu.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện ở cả batch lẫn streaming.
- Mình chưa kiểm tra union read streaming bảng PK trên Iceberg có cho kết quả đúng hay không (equality delete cộng changelog).
- Mình chưa kiểm tra trường hợp batch mà log sau snapshot rỗng.
- Mình chưa kiểm tra plugin Iceberg 1.12 trên `main` có thêm `SortedRecordReader` hay không.
