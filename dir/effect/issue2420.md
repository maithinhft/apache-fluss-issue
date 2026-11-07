# Issue #2420 — `IcebergLakeCommitter` có thể bị chặn rất lâu khi trong cùng một lần commit có cả dữ liệu mới lẫn kết quả compaction

Link: https://github.com/apache/fluss/issues/2420 — Phiên bản được báo: `0.8.0` (người báo: rionmonster, 20/01/2026). Issue có:
- chuỗi sự kiện 3 bước;
- đoạn mã trích từ `IcebergLakeCommitter`;
- timeline log chẩn đoán qua nhiều epoch.

Có một bình luận của chính người báo: lỗi **khá hiếm**, cần đủ 3 điều kiện (bật compaction; có dữ liệu mới trong cùng chu kỳ commit với kết quả compaction; cả hai được commit trong cùng một lần `commit()`). Người báo tái hiện được nhiều lần trong test khi tìm hiểu #2262, nhưng chưa chứng minh đây là nguyên nhân của #2262. Trạng thái: còn mở. PR [#2421](https://github.com/apache/fluss/pull/2421) **đang mở**.
**Nhóm B:** chỉ khi tier sang **Iceberg** và **bật compaction do Fluss thực hiện** (`table.datalake.auto-compaction`, mặc định **tắt**).

> **Mức chắc chắn:** cao về cơ chế. Mình đã đọc `IcebergLakeCommitter` trong `v1.0.0`: thứ tự commit và cách validate khớp với issue. Mức "chặn rất lâu" phụ thuộc cấu hình retry commit của Iceberg. Đó là kiến thức về Iceberg, mình không đọc mã Iceberg. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Commit Iceberg / snapshot N** | Mỗi lần commit tạo một snapshot mới của bảng Iceberg. |
| **`AppendFiles` / `RowDelta`** | Kiểu commit để thêm dữ liệu: chỉ thêm file (bảng log), hoặc thêm dữ liệu kèm delete file (bảng PK). |
| **Rewrite (compaction)** | Gộp file nhỏ thành file lớn: commit kiểu `RewriteFiles` thay một nhóm file cũ bằng file mới. |
| **`validateFromSnapshot(S)`** | Rewrite yêu cầu Iceberg kiểm tra rằng **từ snapshot S** (lúc lập kế hoạch rewrite) tới hiện tại, **không có thay đổi xung đột** với các file bị thay, ví dụ không có delete mới áp lên chúng. |
| **Commit retry** | Commit bị xung đột thì Iceberg **thử lại** theo `commit.retry.*` của bảng (số lần, thời gian chờ tăng dần, tổng thời gian tối đa). |
| **`table.datalake.auto-compaction`** | Option của Fluss: tiering service tự compact khi ghi vào lake. **Mặc định `false`** (`ConfigOptions.TABLE_DATALAKE_AUTO_COMPACTION`). |

## Tóm tắt

Trong một lần `commit()` của tiering:
1. **Commit dữ liệu mới trước** (`AppendFiles` hoặc `RowDelta`), tạo snapshot **N+1**.
2. **Rồi commit kết quả rewrite** với `validateFromSnapshot(N)`, trong đó N là snapshot lúc lập kế hoạch rewrite.
3. Snapshot N+1 vừa tạo (nhất là với bảng PK có delete file mới) bị coi là **thay đổi xung đột**, nên validate thất bại.
4. Iceberg **retry**, nhưng lần nào cũng validate từ N, nên **lần nào cũng thất bại**.
5. Commit rewrite bị **chặn** cho tới khi hết retry, rồi lỗi. Fluss bắt lỗi, **xóa các file rewrite vừa thêm**, và trả `null`.
6. Dữ liệu mới đã commit ở bước 1 vẫn còn.

Kết quả: **tiering của vòng đó bị chậm** (thời gian chờ retry), và **kết quả compaction bị bỏ**. Nếu lặp lại, compaction không bao giờ thành công khi có ghi liên tục.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-lake/fluss-lake-iceberg`, `tiering/IcebergLakeCommitter` (`commit`, `commitRewrite`).
- **Đã xác nhận trong mã `v1.0.0`:**
  ```java
  public LakeCommitResult commit(IcebergCommittable committable, Map<String, String> snapshotProperties) {
      icebergTable.refresh();
      SnapshotUpdate<?> snapshotUpdate = committable.getDeleteFiles().isEmpty()
              ? icebergTable.newAppend()  /* + data files */
              : icebergTable.newRowDelta() /* + data files + delete files */;
      long snapshotId = commit(snapshotUpdate, snapshotProperties);          // bước 1: snapshot N+1
      List<RewriteDataFileResult> rewriteDataFileResults = committable.rewriteDataFileResults();
      if (!rewriteDataFileResults.isEmpty()) {
          Long rewriteCommitSnapshotId = commitRewrite(rewriteDataFileResults, snapshotProperties);   // bước 2
          ...
      }
  }
  private Long commitRewrite(...) {
      icebergTable.refresh();
      RewriteFiles rewriteFiles = icebergTable.newRewrite();
      try {
          rewriteFiles.validateFromSnapshot(rewriteDataFileResults.get(0).snapshotId());   // validate từ N
          ... addFile / deleteFile ...
          return commit(rewriteFiles, snapshotProperties);
      } catch (Exception e) {
          LOG.error("Failed to commit rewrite files to iceberg, delete rewrite added files {}.", ...);
          CatalogUtil.deleteFiles(icebergTable.io(), rewriteAddedDataFiles, "data file", true);
          return null;
      }
  }
  ```
  Cấu trúc y hệt đoạn trích trong issue.
- **Trên `main`:** #5064 (05/11/2026) *"Keep rewrite files when the Iceberg commit outcome is unknown"*. Khi commit rewrite trả `CommitStateUnknownException`, nó **không xóa** file đã thêm, để tránh bảng trỏ tới file đã bị xóa. Đây là một lỗi **khác** trong cùng hàm (chính `catch` đó). Mình đọc mô tả commit, không đọc diff. Thay đổi này không sửa lỗi của #2420.

## 2. Khi nào lỗi xảy ra

Cần **cả ba** (theo người báo):
1. **Bật compaction** của tiering (`table.datalake.auto-compaction = true`).
2. **Có dữ liệu mới** trong cùng chu kỳ commit với kết quả compaction (gần như luôn đúng nếu bảng được ghi liên tục).
3. Cả hai được commit **trong cùng một lời gọi `commit()`**.

Với bảng **PK** (có delete file mới ở bước 1), xung đột với rewrite dễ xảy ra hơn (suy luận từ cơ chế validate).

**Phiên bản:** báo trên `0.8.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** bảng tier sang Iceberg có bật auto-compaction.

**Hậu quả:**
1. **Tiering chậm lại** trong thời gian Iceberg retry. Theo issue là *"extremely long"*; thời gian thực tế phụ thuộc `commit.retry.*` của bảng Iceberg.
2. **Compaction không có tác dụng:** file rewrite bị xóa, bảng vẫn nhiều file nhỏ, nên đọc chậm.
3. **Log ERROR** `"Failed to commit rewrite files to iceberg"` lặp lại.
4. **Không mất dữ liệu:** dữ liệu mới đã commit ở bước 1.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #2420 trong nhóm Iceberg (❌). Đối chiếu:
- **Đúng.**
- **Bổ sung:** chỉ khi bật `table.datalake.auto-compaction`, mà giá trị này **mặc định tắt**.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu tier sang Iceberg **và** bật auto-compaction.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Giữ `table.datalake.auto-compaction = false`** với bảng Iceberg ghi liên tục. Chạy compaction Iceberg **riêng** (Spark hoặc Flink rewrite action) vào giờ thấp điểm. Lưu ý #4940: compaction song song với tiering bảng PK cũng có vấn đề.
- Nếu bật: theo dõi log `"Failed to commit rewrite files to iceberg"` và thời gian mỗi vòng tiering.
- Có thể giảm `commit.retry.num-retries` và tổng thời gian retry của bảng Iceberg để rút ngắn thời gian bị chặn. Cái giá là các xung đột hợp lệ khác cũng ít được retry hơn.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện, và chưa đo thời gian bị chặn với cấu hình retry mặc định của Iceberg 1.10.
- Mình chưa đọc diff của PR #2421 và #5064.
- Mình chưa kiểm tra bảng **log** (chỉ `AppendFiles`, không có delete) có gây xung đột với rewrite hay không. Theo cơ chế, việc chỉ thêm file thường không xung đột với rewrite, nhưng mình chưa xác nhận.
