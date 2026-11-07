# Issue #764 — Máy trạng thái replica ghi log ERROR khi replica đang bị xóa bị đẩy sang trạng thái "offline"

Link: https://github.com/apache/fluss/issues/764 — Phiên bản được báo: `0.6.0` (người báo: wuchong, 19/04/2025, repo cũ `alibaba/fluss`). Issue lấy log ERROR từ test `TableManagerITCase#testPartitionedTableManagement`. Có 6 bình luận phân tích. Kết luận chung giữa maintainer và người đóng góp:
- chuyển `ReplicaDeletionStarted` → `OfflineReplica` **đúng là không hợp lệ**;
- không nên log ERROR, vì ERROR ngụ ý hệ thống có lỗi;
- có thể sửa ở phía TabletServer để không sinh ra `StorageException`.

PR [#795](https://github.com/apache/fluss/pull/795) (thêm trạng thái hợp lệ) đã **bị đóng, không merge**, vì maintainer cho rằng đó không phải cách sửa đúng. Trạng thái: còn mở.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0`: đường gây log ERROR còn nguyên. Trên `main`, commit [#4505](https://github.com/apache/fluss/pull/4505) (15/10/2026, không có trong 1.0.0) có thêm điều kiện **bỏ qua phản hồi lỗi tới muộn của replica đang xóa**. Điều kiện này nhắm đúng nguyên nhân của issue. Mình đã đọc đoạn diff đó nhưng chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`ReplicaStateMachine`** | Bộ theo dõi trạng thái từng replica trong Coordinator. Mỗi trạng thái đích có tập **trạng thái trước hợp lệ**. Chuyển từ trạng thái không nằm trong tập đó thì bị từ chối và **log ERROR**. |
| **`ReplicaDeletionStarted`** | Replica đang được xóa (do drop bảng hoặc partition). |
| **`OfflineReplica`** | Replica bị coi là hỏng. |
| **`NotifyLeaderAndIsr` / phản hồi** | Lệnh Coordinator gửi TabletServer khi tạo replica hoặc đổi leader. TabletServer trả kết quả từng bucket, gồm thành công hoặc lỗi. |
| **`StorageException` "replica offline"** | Lỗi TabletServer trả khi không tạo được replica, ví dụ không đọc được schema bảng trong ZooKeeper vì bảng **đã bị xóa** (xem #4522). |
| **Phản hồi tới muộn** | Phản hồi của một lệnh gửi trước khi bảng bị xóa, nhưng về sau khi Coordinator đã bắt đầu xóa. |

## Tóm tắt

Trình tự (theo phân tích trong bình luận, đã đối chiếu mã 1.0.0):
1. Coordinator tạo bảng hoặc partition, rồi gửi `NotifyLeaderAndIsr` cho TabletServer.
2. Bảng bị drop ngay sau đó: Coordinator xóa mọi node ZooKeeper của bảng, và chuyển replica sang `ReplicaDeletionStarted`.
3. TabletServer xử lý `NotifyLeaderAndIsr` **muộn**, không tìm thấy schema trong ZooKeeper, và trả lỗi `StorageException`.
4. Coordinator nhận phản hồi lỗi. `processNotifyLeaderAndIsrResponseReceivedEvent` coi **mọi** bucket lỗi là offline, rồi gọi `onReplicaBecomeOffline`. Hàm này cố chuyển replica sang `OfflineReplica`.
5. Replica đang ở `ReplicaDeletionStarted`, không nằm trong tập trạng thái trước hợp lệ của `OfflineReplica`, nên bị **từ chối, kèm 2 dòng log ERROR**.

Không có hậu quả chức năng: việc chuyển trạng thái bị từ chối là **đúng**, và việc xóa vẫn tiếp tục. Vấn đề là **log ERROR gây nhiễu**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/statemachine/ReplicaState`, `ReplicaStateMachine`, `coordinator/CoordinatorEventProcessor`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `ReplicaState.OfflineReplica.getValidPreviousStates()` = `EnumSet.of(NewReplica, OnlineReplica, OfflineReplica)`, không có `ReplicaDeletionStarted`.
  - `ReplicaStateMachine.logInvalidTransition(...)`: `LOG.error("Replica state for {} should be in the {} before moving to state {}, but the current state is {}.")`, rồi `logFailedSateChange(...)`: `LOG.error("Fail to change state for table bucket replica {} from {} to {}, reason: {}.")`.
  - `CoordinatorEventProcessor.processNotifyLeaderAndIsrResponseReceivedEvent`: *"if the error code is not none, we will consider it as offline"*, rồi `onReplicaBecomeOffline(offlineReplicas)`. Không kiểm tra replica có đang bị xóa hay không.
- **Trên `main` (#4505), diff ở cùng hàm:**
  ```java
  TableBucket tableBucket = notifyLeaderAndIsrResultForBucket.getTableBucket();
  if (coordinatorContext.isToBeDeleted(tableBucket)
          || !coordinatorContext.getAssignment(tableBucket).contains(serverId)) {
      // A late activation failure must not reset a deletion state to OfflineReplica
      // or recreate a replica that has already been deleted or migrated away.
      continue;
  }
  ```
  Đây đúng là trường hợp của issue.

## 2. Khi nào lỗi xảy ra

- Drop bảng hoặc partition **ngay sau khi tạo**, khi TabletServer chưa xử lý xong `NotifyLeaderAndIsr`.
- Hoặc partition **hết hạn** (auto-partition) đúng lúc một lệnh `NotifyLeaderAndIsr` cho nó (ví dụ do đổi leader) đang trên đường.
- Mỗi replica bị ảnh hưởng sinh ra 2 dòng ERROR.

**Phiên bản:** báo trên `0.6.0`. Còn ở `1.0.0`. `main` đã có thay đổi liên quan.

## 3. Tác động đến ai, làm sai lệch gì

- **Log ERROR trên Coordinator** khi drop bảng hoặc partition. Có thể **kích hoạt alert theo log ERROR**, hoặc làm người vận hành nghĩ có sự cố.
- **Không ảnh hưởng chức năng hay dữ liệu**, vì việc chuyển trạng thái sai bị chặn đúng.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng ghi chú cuối mục 4.4 của `relate_issue.md` xếp #764 vào nhóm *"Coordinator state machine log ERROR gây nhiễu"*, kèm *"Không thấy fix nào cho các issue này trong 1.0.0"*. Đối chiếu:
- **Đúng** cho 1.0.0.
- **Bổ sung:** `main` đã có #4505 bỏ qua đúng loại phản hồi tới muộn này, nên với 1.0.0 trạng thái là **⏭️** (fix nằm trong một thay đổi lớn hơn về xóa replica, xem #1629).

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Chủ yếu ảnh hưởng **chất lượng alert**, vì CDP có partition hết hạn tự động hằng ngày.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Trong rule alert theo log ERROR của Coordinator, **loại trừ** các dòng `Replica state for ... before moving to state OfflineReplica, but the current state is ReplicaDeletionStarted` (và dòng `Fail to change state ... from ReplicaDeletionStarted to OfflineReplica` đi kèm).
- Không nên loại trừ mọi ERROR của `ReplicaStateMachine`. Chỉ loại đúng cặp trạng thái này.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test `TableManagerITCase` trên 1.0.0 để đếm số dòng log.
- Mình chưa kiểm tra #4505 đã loại hết log ERROR này trên `main` hay chưa (chỉ đọc diff).
