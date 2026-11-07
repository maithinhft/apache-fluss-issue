# Issue #2045 — Query `$lake` của bảng không trả về dữ liệu dù file Paimon đã có trên HDFS

Link: https://github.com/apache/fluss/issues/2045 — Phiên bản được báo: `0.8.0` (người báo: WuWei-art, 28/11/2025). Môi trường:
- Flink 1.20.1;
- `lib/` có `paimon-flink-1.20-1.2.0.jar`, một file `.jarbak` của bản 1.0.1, `fluss-lake-paimon-0.8.0`, `fluss-fs-hdfs-0.8.0`, `fluss-flink-1.20-0.8.0`;
- tiering service chạy với Paimon filesystem metastore trên HDFS.

Người báo tạo bảng PK `fluss_order_with_lake` với `'table.datalake.enabled' = 'true'`, `'table.datalake.freshness' = '30s'`, `'paimon.file.format' = 'orc'`, **`'paimon.deletion-vectors.enabled' = 'true'`**, rồi `INSERT` **1 dòng**.
- Trên HDFS đã có `bucket-2/changelog-*.orc` và `bucket-2/data-*.orc`.
- `SELECT * FROM fluss_order_with_lake` trả 1 dòng.
- `SELECT * FROM fluss_order_with_lake$lake` trả **rỗng**.

Không có bình luận, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bật lakehouse Paimon.

> **Mức chắc chắn:** trung bình. Không ai phân tích issue này. Mình đã đọc mã `v1.0.0` để biết `$lake` đọc từ đâu, và đối chiếu với issue đã đóng [#2121](https://github.com/apache/fluss/issues/2121) cùng thời kỳ. Giải thích khả dĩ nhất: **bảng bật deletion vectors nên Paimon không đọc file ở level 0**. Một dòng vừa ghi (chưa compact) nằm ở L0, nên `$lake` rỗng. Nếu đúng, đây là **hành vi của Paimon**, không phải lỗi Fluss. Mình **chưa tái hiện**.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`$lake`** | Hậu tố tên bảng trong Fluss catalog của Flink: `SELECT * FROM t$lake` đọc **riêng phần dữ liệu trong lake**, tức đọc thẳng bảng Paimon tương ứng. |
| **Deletion vectors (DV)** | Kỹ thuật của Paimon: đánh dấu dòng bị xóa hoặc cập nhật bằng bitmap. Khi bật DV, việc đọc chỉ dựa vào file **đã compact** (level ≥ 1). **File level 0 (mới ghi) không được đọc** cho tới khi compact. |
| **Level 0 (L0)** | Tầng file mới nhất của cây LSM trong Paimon, chưa được compact. |
| **Compaction** | Gộp và sắp xếp file. Với Paimon DV, compaction đưa dữ liệu từ L0 lên level cao hơn và sinh deletion vector. |
| **`table.datalake.auto-compaction`** | Option của Fluss: tiering service tự kích hoạt compaction khi ghi vào lake. **Mặc định `false`.** |
| **Snapshot lake** | Phiên bản đã commit của bảng Paimon. |

## Tóm tắt

`$lake` **đọc trực tiếp** bảng Paimon bằng Paimon Flink catalog. Bảng của người báo bật **deletion vectors**, và dữ liệu là **một dòng vừa ghi**. Tiering ghi file vào **level 0** (thấy được qua tên file `data-*`, `changelog-*` trong `bucket-2`). Với DV bật, Paimon **không đọc L0** cho tới khi có compaction, mà `table.datalake.auto-compaction` mặc định **tắt** và một dòng thì không kích hoạt compaction tự nhiên. Vậy `$lake` **rỗng là đúng hành vi**. Trong khi đó query thường (`SELECT * FROM t`) đọc từ Fluss nên vẫn thấy dòng.

Issue #2121 (cùng thời kỳ, đã đóng, sửa bằng #2326 trong 0.9) mô tả đúng hiện tượng này cho **union read**: *"in dv mode, when read the latest commit snapshot, the data in l0 level won't be read which case data loss"*. #2326 sửa **union read** của Fluss (dùng *readable snapshot*). Còn `$lake` thì đọc thẳng Paimon, nên vẫn theo hành vi của Paimon.

Một nguyên nhân khả dĩ khác (chưa loại trừ): **lệch phiên bản Paimon**. `lib/` của Flink có `paimon-flink-1.20-1.2.0`, trong khi `fluss-lake-paimon` gói kèm `paimon-bundle` (phiên bản theo Fluss), và còn một file `.jarbak` của 1.0.1.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink` (`catalog/FlinkTableFactory`, `lake/LakeFlinkCatalog`) cùng **Paimon** (đọc bảng DV).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkCatalog.LAKE_TABLE_SPLITTER = "$lake"`.
  - `FlinkTableFactory.createDynamicTableSource`: nếu tên bảng chứa `$lake` thì `resolveToLakeIdentifier(...)` và `lakeTableFactory.createDynamicTableSource(context, lakeIdentifier)`, tức **giao cho lake** đọc.
  - `LakeFlinkCatalog.PaimonCatalogFactory` dùng `org.apache.paimon.flink.FlinkCatalogFactory.createCatalog(...)`, tức **Paimon Flink catalog** trên classpath của Flink.
  - `fluss-lake/fluss-lake-paimon/pom.xml` phụ thuộc `paimon-bundle` (và `paimon-flink-${flink.major.version}` khi build).
  - `ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION`: `defaultValue(false)`, mô tả *"If true, compaction will be triggered automatically when tiering service writes to the datalake"*.
  - `PaimonLakeCommitter`: với bảng DV, sau commit, nó tìm **readable snapshot** (`DvTableReadableSnapshotRetriever`). Đây là bằng chứng Fluss biết snapshot mới commit của bảng DV chưa chắc đã đọc được.
- **Hành vi DV của Paimon** (L0 không được đọc khi bật DV) là kiến thức về Paimon, khớp với mô tả ở #2121. Mình **không** đọc mã Paimon để xác nhận.

## 2. Khi nào lỗi xảy ra

- Bảng có lake Paimon với **`paimon.deletion-vectors.enabled = true`**.
- Dữ liệu vừa được tier và **chưa compact** (lượng ít, hoặc `table.datalake.auto-compaction = false`).
- Đọc bằng `$lake`, hoặc bằng bất kỳ engine nào đọc thẳng Paimon.

## 3. Tác động đến ai, làm sai lệch gì

- `$lake` hoặc các engine đọc Paimon (Spark, Trino) **không thấy dữ liệu mới** cho tới khi compact. Với bảng ít ghi, dữ liệu có thể "vô hình" rất lâu.
- **Không mất dữ liệu:** file có trên HDFS, và query qua Fluss vẫn thấy.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #2045 trong nhóm Paimon (❌). Đối chiếu:
- **Có khả năng không phải lỗi Fluss** mà là hành vi của Paimon DV khi chưa compact. Nên ghi **❔**, kèm giải thích này.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu tier sang Paimon **có bật DV** và có engine khác đọc trực tiếp Paimon (BI, analytics).

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Nếu bật DV: đặt **`'table.datalake.auto-compaction' = 'true'`**, hoặc chạy compaction Paimon định kỳ, để dữ liệu trong lake đọc được.
- Nếu không cần DV: **không bật** `paimon.deletion-vectors.enabled`.
- Giữ **một** phiên bản `paimon-flink` trên classpath Flink, khớp với phiên bản Paimon mà Fluss 1.0.0 dùng. Xóa các file thừa như `.jarbak`.
- Khi kiểm tra "lake có dữ liệu chưa", xem **snapshot và level file** của Paimon chứ không chỉ xem file có trên HDFS hay không.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện, và chưa đọc mã Paimon về việc bỏ qua L0 khi bật DV.
- Mình chưa xác định phiên bản Paimon mà `fluss-lake-paimon` 0.8 và 1.0 gói kèm, để loại trừ khả năng lệch phiên bản.
- Mình chưa kiểm tra `auto-compaction = true` có làm `$lake` thấy dữ liệu ngay sau một vòng tiering hay không.
