# Issue #2088 — Bật tiering sang Paimon (có deletion vectors) thì query bảng lỗi `Failed to generate hybrid lake fluss splits`

Link: https://github.com/apache/fluss/issues/2088 — Phiên bản được báo: `0.8.0` (người báo: luxl6, 03/12/2025). Issue có:
- DDL bảng PK với `'table.datalake.enabled' = 'true'`, `'paimon.deletion-vectors.enabled' = 'true'`, `'paimon.file.format' = 'parquet'`;
- lệnh chạy tiering service (Paimon, Hive metastore, warehouse trên HDFS);
- stack trace: `FlinkRuntimeException: Failed to generate hybrid lake fluss splits`, nguyên nhân `java.util.NoSuchElementException`.

Có hai bình luận:
- một người hỏi tiến độ;
- maintainer cho biết theo người dùng trên DingTalk, **bỏ `paimon.deletion-vectors.enabled` thì hết lỗi**, nên *"might be a bug of union read for deletion vector table"*.

Không có PR liên kết. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bật lakehouse **Paimon** với **deletion vectors**.

> **Mức chắc chắn:** thấp về nguyên nhân (stack trace không đầy đủ, không ai phân tích). Theo lịch sử git, union read bảng Paimon có deletion vectors đã được **làm lại đáng kể** sau 0.8: [#2326](https://github.com/apache/fluss/pull/2326) *"Fix union read paimon dv table issue"* (11/02/2026, 52 file), sửa issue [#2121](https://github.com/apache/fluss/issues/2121) *"UnionRead dv enabled paimon table will loss data"*, cùng các sửa tiếp `#2640`, `#3519` (15/09/2026). Tất cả đều có trong 1.0.0. **Có khả năng** #2088 đã hết, nhưng **chưa ai xác nhận** và mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Tiering service ("layered service")** | Job Flink đẩy dữ liệu Fluss sang Paimon (xem #2648). |
| **Deletion vectors (DV)** | Kỹ thuật của Paimon: đánh dấu dòng bị xóa hoặc cập nhật bằng một **bitmap** gắn với file, thay vì viết lại file. Đọc nhanh hơn, nhưng dữ liệu **mới nhất ở level 0** chỉ "đọc được" sau khi Paimon **compact**. |
| **Readable snapshot** | Với bảng DV, snapshot **mới commit** chưa chắc đã đọc được đầy đủ (dữ liệu ở L0 chưa compact). "Readable snapshot" là snapshot đã compact đủ để đọc đúng. |
| **Union read / hybrid split** | Flink đọc phần cũ từ Paimon và phần mới từ log Fluss. Mỗi bucket thành một **hybrid split** (lake split cộng offset log bắt đầu). Enumerator sinh các split này lúc khởi động (`generateHybridLakeFlussSplits`). |
| **`NoSuchElementException`** | Gọi `next()` hoặc lấy phần tử từ một tập **rỗng**. |

## Tóm tắt

Ở 0.8: bảng PK bật Paimon với DV. Mọi query Flink (union read) lỗi **ngay ở enumerator** khi sinh hybrid split, với `NoSuchElementException`. Tắt DV thì hết.

Sau 0.8, cơ chế union read cho bảng DV được làm lại: Fluss lưu và dùng **readable snapshot** thay vì snapshot mới commit (#2326, sửa lỗi **mất dữ liệu** khi union read bảng DV ở #2121), và chọn offset theo readable snapshot (#3519). Những thay đổi này chạm đúng vùng sinh hybrid split của bảng DV. Vì vậy **có thể** lỗi của #2088 đã không còn, nhưng mình không chứng minh được.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink` (`source/enumerator/FlinkSourceEnumerator.generateHybridLakeFlussSplits`, `lake/LakeSplitGenerator`), `fluss-lake/fluss-lake-paimon` (lake source, `DvTableReadableSnapshotRetriever`), và phía server (lưu thông tin snapshot lake).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkSourceEnumerator`: `lakeSplitGenerator.generateHybridLakeFlussSplits()` trong `try`, mọi lỗi được bọc thành `FlinkRuntimeException("Failed to generate hybrid lake fluss splits", e)`. Đây đúng là thông báo trong issue.
  - `LakeSplitGenerator`: lấy phần tử đầu bằng `lakeSplits.isEmpty() ? null : lakeSplits.values().iterator().next()`, có kiểm tra rỗng. Mình **không tìm thấy** điểm gây `NoSuchElementException` trong lớp này ở 1.0.0. Điều này hợp với giả thuyết đường code đã khác so với 0.8.
  - `PaimonLakeCommitter`: với bảng DV, sau khi commit, nó dùng `DvTableReadableSnapshotRetriever` để xác định **readable snapshot** và offset tương ứng (`LakeCommitResult.withReadableSnapshot(...)`, hoặc `unknownReadableSnapshot(...)` nếu chưa xác định được).
- **Lịch sử git** (các commit trong `v1.0.0` chạm vào Paimon DV):
  - `058cf13a` (#1725, 09/2025): hỗ trợ tier bảng Paimon có DV;
  - `5a4374aa` (#2326, 11/02/2026): sửa union read bảng DV (#2121), 52 file, thêm ghi chú nâng cấp 0.9;
  - `5fd8d9cb` (#2640, 12/02/2026): bỏ tính lại readable snapshot khi đã có;
  - `cde4dce5` (#3519, 15/09/2026): sửa việc chọn readable snapshot và offset;
  - trên `main`: `b450e4c1` (#4583): *"Resolve DV offsets from committed COMPACT"*.
- **Ràng buộc mới từ 0.9** (ghi chú nâng cấp trong #2326): bảng **partition** có cột partition **không phải String** thì **không được** bật Paimon cùng DV (*"Only support String type as partitioned key when 'deletion-vectors.enabled' is set to true for paimon"*). Mã 1.0.0 có thông báo lỗi này trong `PaimonConversions`.

## 2. Khi nào lỗi xảy ra

- **Theo issue (0.8):** union read bảng PK có Paimon DV.
- **Trên 1.0.0:** chưa xác định. Có thể còn xảy ra nếu bảng DV **chưa có readable snapshot** (chưa compact lần nào) và đường xử lý trường hợp "unknown readable snapshot" có lỗi. Đây là suy luận, chưa kiểm chứng.

## 3. Tác động đến ai, làm sai lệch gì

- **Nếu còn lỗi:** không query được bảng qua Flink union read. Job fail ngay khi khởi động. Lỗi lộ ra ngay.
- **Liên quan (#2121, đã sửa trong 1.0.0):** trước #2326, union read bảng DV có thể **mất dữ liệu ngầm** (không đọc phần L0). Đây là rủi ro nặng hơn, và đã được sửa.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng nhóm Paimon trong `relate_issue.md` ghi #2088 là ❌. Đối chiếu:
- **Nên là ❔:** vùng mã liên quan đã thay đổi lớn trong 0.9 và 1.0 (#2326, #3519), nên không thể khẳng định lỗi còn hay hết khi chưa có xác nhận.

## 5. Mức ảnh hưởng với CDP

- Chỉ liên quan nếu tier sang **Paimon với deletion vectors**.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Nếu dùng Paimon DV:
  - **test union read** trên 1.0.0 với đúng cấu hình, kể cả khi bảng **chưa compact** lần nào;
  - đọc ghi chú nâng cấp 0.9 và 1.0 về DV (ràng buộc cột partition kiểu String).
- Nếu không cần DV: **không bật** `paimon.deletion-vectors.enabled`, để tránh vùng mã phức tạp này.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện trên 1.0.0.
- Mình không biết chính xác chỗ ném `NoSuchElementException` ở 0.8 (stack trace trong issue không đầy đủ).
- Mình chưa đọc diff của #2326 và #3519, chỉ đọc tiêu đề, phạm vi và ghi chú nâng cấp.
