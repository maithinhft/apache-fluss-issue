# Issue #1629 — Drop bảng hoặc partition khi một TabletServer đang crash thì để lại replica rác

Link: https://github.com/apache/fluss/issues/1629 — Phiên bản được báo: `main` thời khoảng 0.8 (người báo: zcoo, 01/09/2025). Issue mô tả hiện tượng, nghi ngờ nguyên nhân, và đề xuất "một manager định kỳ đối chiếu `coordinatorContext` với ZooKeeper". Có một bình luận phản biện: chỉ cần retry gửi `StopReplica`, và sau N lần thì coi là thành công như `CoordinatorContext` đang làm, vì quét định kỳ sẽ tạo tải lên ZooKeeper. Không có PR liên kết. Trạng thái: còn mở.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và xác nhận nguyên nhân người báo nghi ngờ: khi TabletServer chết hoặc quay lại, Coordinator **bỏ qua** các replica đang chờ xóa. Trên `main` có commit [#4505](https://github.com/apache/fluss/pull/4505) *"Resume pending replica deletion after server restart"* (15/10/2026), nhắm **đúng kịch bản này**. Mình đã đọc một phần diff của nó. #4505 **không có trong 1.0.0**. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Replica rác (residual replica)** | Thư mục dữ liệu (log, KV) của bucket thuộc bảng hoặc partition **đã xóa** nhưng vẫn còn trên đĩa TabletServer. |
| **`StopReplicaRequest(delete = true)`** | Lệnh xóa replica Coordinator gửi TabletServer (xem #3357). |
| **`DeleteReplicaResponseReceivedEvent`** | Sự kiện Coordinator nhận khi có phản hồi xóa. Không có sự kiện này thì replica không chuyển trạng thái. |
| **`NewTabletServerEvent` / `DeadTabletServerEvent`** | Sự kiện khi một TabletServer **đăng ký** (khởi động hoặc quay lại) hoặc **mất đăng ký** (chết) trong ZooKeeper. |
| **`isToBeDeleted(bucket)`** | Bucket thuộc bảng hoặc partition đang trong hàng đợi xóa. |
| **`ReplicaDeletionIneligible`** | Trạng thái "chưa thể xóa" (ví dụ vì server đang chết). Trạng thái này có trong máy trạng thái của 1.0.0 nhưng chưa được dùng cho kịch bản server chết. #4505 dùng nó. |

## Tóm tắt

Khi drop bảng hoặc partition, nếu một TabletServer giữ replica **crash rồi khởi động lại** giữa chừng, nó không nhận được `StopReplicaRequest`, hoặc Coordinator gửi không tới được (#3357), nên không có phản hồi. Trong 1.0.0:
- khi server **chết**, Coordinator **bỏ qua** các replica đang chờ xóa;
- khi server **quay lại**, Coordinator cũng **bỏ qua** chúng.

Không có cơ chế retry hay quét định kỳ, nên replica trên server đó **không bao giờ bị xóa**. Theo người báo, **restart Coordinator** thì phần dư được dọn.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/CoordinatorEventProcessor` (`processDeadTabletServer`, `processNewTabletServer`), `coordinator/CoordinatorRequestBatch`, `coordinator/TableManager`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `processDeadTabletServer`: chỉ xử lý replica **không** thuộc bảng đang xóa:
    ```java
    coordinatorContext.replicasOnTabletServer(tabletServerId).stream()
        .filter(tableBucketReplica -> !coordinatorContext.isToBeDeleted(tableBucketReplica.getTableBucket()))
    ```
  - `processNewTabletServer`: cùng bộ lọc đó, rồi `replicaStateMachine.handleStateChanges(replicas, OnlineReplica)`. Replica đang chờ xóa trên server vừa quay lại **không được gửi lại lệnh xóa**.
  - `CoordinatorRequestBatch.sendStopRequest`: gửi lỗi thì chỉ log rồi `return` (xem #3357).
  - Hệ quả: replica kẹt ở `ReplicaDeletionStarted`, và `TableManager.isEligibleForDeletion` (yêu cầu không replica nào ở trạng thái đó) không bao giờ đúng.
- **Trên `main` (#4505):**
  - Mô tả commit: *"Mark pending replica deletions as ineligible only while their tablet server is offline. When the server re-registers, move those replicas through OfflineReplica and ReplicaDeletionStarted to rebuild StopReplica requests. Keep offline replicas on live servers eligible for immediate deletion so transient replica failures do not stall table or partition drops."*
  - Diff ở `CoordinatorEventProcessor` thêm `tableManager.failReplicaDeletion(tabletServerId)` (khi server chết) và `tableManager.resumeReplicaDeletion(tabletServerId)` (khi server quay lại).
  - Diff còn bỏ qua phản hồi tới muộn của replica không còn ở `ReplicaDeletionStarted`, và không đánh dấu offline cho bucket đang xóa. Phần này cũng giải quyết #764.
  - Commit sửa 8 file, thêm `ReplicaDeletionInterleavingTest` và `ReplicaDeletionRecoveryITCase`.

## 2. Khi nào lỗi xảy ra

Drop bảng hoặc partition (kể cả **auto-partition hết hạn**) trong khi một TabletServer giữ replica của nó đang crash, restart hoặc chưa đăng ký lại.

**Phiên bản:** báo trên `main` thời 0.8. Còn ở `1.0.0`. Đã có fix trên `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster có xóa bảng hoặc partition trùng lúc TabletServer gặp sự cố.

**Hậu quả:**
1. **Rác trên đĩa** của TabletServer đó (log, KV, có thể cả file remote nếu xóa remote do replica đó phụ trách).
2. **Coordinator kẹt** việc xóa bảng hoặc partition. Metric `tableCount` và `partitionCount` lệch.
3. Khi TabletServer **restart lần sau**, nó có thể recover cả thư mục rác (#3223), nên khởi động chậm hơn, hoặc lỗi nếu thư mục hỏng (#3224).
4. **Không ảnh hưởng bảng khác.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #1629 trong `relate_issue.md` ghi: *"❔ | DDL hiếm"*. Đối chiếu:
- **Nên là ⏭️**: lỗi đã xác nhận còn trong mã 1.0.0, và `main` đã có fix (#4505) cho đúng kịch bản.
- **"DDL hiếm" chưa đúng** với bảng có auto-partition hết hạn (xem #3357).

## 5. Mức ảnh hưởng với CDP

- Tương tự #3357: mỗi lần TabletServer gặp sự cố hoặc rolling restart trùng lúc partition hết hạn bị xóa thì có thể để lại rác và trạng thái kẹt.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Restart Coordinator** sau các sự cố hoặc bảo trì TabletServer nếu thấy `tableCount` hoặc `partitionCount` lệch, hoặc có partition đã hết hạn nhưng vẫn còn trong Coordinator. Theo người báo, cách này dọn được.
- **Không drop bảng thủ công** trong lúc rolling restart TabletServer.
- **Định kỳ đối chiếu** thư mục bucket trên đĩa với các bảng và partition còn tồn tại.
- **Cân nhắc cherry-pick #4505** nếu tự build. Thay đổi khá lớn (khoảng 100 dòng logic ở `TableManager` và state machine), nên cần test kỹ.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa đọc toàn bộ diff của #4505 (`TableManager.failReplicaDeletion` và `resumeReplicaDeletion`).
- Mình chưa kiểm tra chắc chắn rằng restart Coordinator dọn được rác (chỉ dựa vào người báo).
