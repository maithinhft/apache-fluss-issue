# Issue #898 — Drop một bảng partition chưa có partition nào thì đường dẫn bảng còn sót trong Coordinator

Link: https://github.com/apache/fluss/issues/898 — Phiên bản được báo: `main` thời khoảng 0.7 (người báo: swuferhong, 03/06/2025, repo cũ `alibaba/fluss`). Issue ngắn, có giải thích nguyên nhân. Có một bình luận xin nhận làm. Trạng thái: còn mở. PR [#987](https://github.com/apache/fluss/pull/987) **đang mở**.

> **Mức chắc chắn:** cao, và kết luận là **đã sửa trong 1.0.0**. Mình đọc mã `v1.0.0` và thấy `CoordinatorEventProcessor.processDropTable` có đoạn xử lý **đúng trường hợp này**, kèm comment giải thích. Đoạn đó đến từ commit [#3482](https://github.com/apache/fluss/pull/3482) (14/06/2026, có trong tag `v1.0.0`). Issue vẫn mở trên GitHub, có lẽ vì #3482 không liên kết tới nó. Mình chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`coordinatorContext`** | Bộ nhớ trong Coordinator: danh sách bảng, partition, bucket, replica, và **hàng đợi bảng đang chờ xóa** (`tablesToBeDeleted`). |
| **`resumeDeletions` / `completeDeleteTable`** | `resumeDeletions` duyệt các bảng đang chờ xóa. Bảng nào có **mọi** replica đã `ReplicaDeletionSuccessful` thì `completeDeleteTable`: xóa khỏi `coordinatorContext`, dọn ZooKeeper, giảm `tableCount`. |
| **Xóa theo bucket** | Với bảng thường, `resumeDeletions` được gọi khi có **phản hồi xóa replica** (`DeleteReplicaResponseReceivedEvent`). |
| **Bảng partition chưa có partition** | Bảng khai báo `PARTITIONED BY` mà chưa có partition nào. Replica của bảng partition nằm **trong từng partition**, nên ở cấp bảng **không có bucket hay replica nào**. |
| **`fluss_coordinator_tableCount`** | Metric số bảng Coordinator đang quản lý. |

## Tóm tắt

Với bảng thường, việc xóa kết thúc khi phản hồi xóa của bucket cuối cùng về, và phản hồi đó gọi `resumeDeletions`. Với bảng partition **chưa có partition nào**, không có bucket để xóa, nên **không có phản hồi nào** gọi `resumeDeletions`. Bảng **nằm mãi** trong hàng đợi xóa của Coordinator, kéo theo `tableCount` bị lệch, cho tới khi có một lần xóa bảng khác "tình cờ" gọi `resumeDeletions`, hoặc tới khi Coordinator khởi động lại (`TableManager.startup()` có gọi `resumeDeletions()`).

Trong `1.0.0`, `processDropTable` **chủ động gọi `resumeDeletions()`** cho bảng partition. Với bảng không có replica cấp bảng, `areAllReplicasInState(..., ReplicaDeletionSuccessful)` đúng một cách hiển nhiên (tập rỗng), nên bảng được hoàn tất xóa ngay.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/CoordinatorEventProcessor.processDropTable`, `coordinator/TableManager`, `coordinator/CoordinatorContext`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - Cuối `processDropTable`:
    ```java
    // For partitioned tables, the dropped table has no table-level replicas
    // (all buckets live under partitionAssignments), so getAllReplicasForTable
    // returns empty and areAllReplicasInState(.., ReplicaDeletionSuccessful)
    // is vacuously true. Without this explicit trigger, the table would linger
    // in tablesToBeDeleted (and the tableCount metric) until some unrelated
    // replica-deletion response happens to invoke resumeDeletions(), which
    // can be never if no other table is being dropped.
    if (dropTableInfo.isPartitioned()) {
        tableManager.resumeDeletions();
    }
    ```
    Comment này mô tả đúng nội dung của #898.
  - `CoordinatorContext.areAllReplicasInState(tableId, state)` = `getAllReplicasForTable(tableId).stream().allMatch(...)`, nên đúng khi tập rỗng.
  - `TableManager.resumeTableDeletions()`: nếu `areAllReplicasInState(tableId, ReplicaDeletionSuccessful)` thì `completeDeleteTable(tableId)`.
  - Lịch sử git: commit `3b0afc0e` *"[server] Fix tableCount metric after dropping partitioned tables (#3482)"* (14/06/2026) sửa `CoordinatorEventProcessor.java` (+11 dòng), `CoordinatorEventManager.java`, và thêm test. Commit này nằm trong `v1.0.0` (`git tag --contains`).
- **Mình chưa đối chiếu:** bảng partition **có** partition thì `resumeDeletions` gọi ngay lúc drop sẽ "hoàn tất" xóa bảng **trước** khi các partition xóa xong hay không. Comment nói bảng partition không có replica cấp bảng, nên bảng có thể được đánh dấu xóa xong trong khi partition vẫn đang xóa. Mình chưa kiểm tra thứ tự này có gây vấn đề gì.

## 2. Khi nào lỗi xảy ra

- **Bản cũ (0.7–0.9):** tạo bảng partition rồi drop **trước khi** có partition nào (thường gặp trong script tự động hoặc test, hoặc khi auto-partition tắt).
- **Bản `1.0.0`:** theo mã, không còn xảy ra.

## 3. Tác động đến ai, làm sai lệch gì

- **Bản cũ:** rác trong bộ nhớ Coordinator, `tableCount` lệch. Có thể ảnh hưởng tạo lại bảng cùng tên (chưa xác minh). Tự hết khi restart Coordinator.
- **Bản `1.0.0`:** không còn tác động theo mã.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` gom #898 vào nhóm "Coordinator state machine log ERROR gây nhiễu", với ghi chú *"Không thấy fix nào cho các issue này trong 1.0.0"*. Đối chiếu:
- **Sai về trạng thái:** 1.0.0 **đã có fix** (#3482). Nên là **✅** (đã xác minh trong mã), dù issue chưa đóng.
- **Sai về phân loại:** #898 không phải lỗi "log ERROR", mà là "bảng không được dọn khỏi Coordinator".

## 5. Mức ảnh hưởng với CDP

- **Không còn ảnh hưởng** trên 1.0.0.
- Có thể đề nghị maintainer đóng issue và liên kết với #3482.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test drop bảng partition rỗng trên 1.0.0.
- Mình chưa kiểm tra thứ tự "hoàn tất xóa bảng" so với "xóa xong các partition" khi bảng partition **có** partition (mục 1).
- Mình chưa đọc PR #987 để biết nó khác gì so với #3482.
