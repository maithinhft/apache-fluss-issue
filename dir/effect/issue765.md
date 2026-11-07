# Issue #765 — Máy trạng thái bucket ghi log ERROR khi bucket đã bị xóa bị đẩy sang trạng thái "offline" hoặc "online"

Link: https://github.com/apache/fluss/issues/765 — Phiên bản được báo: khoảng `0.6.0` (người báo: wuchong, 19/04/2025, repo cũ `alibaba/fluss`). Issue chỉ dán log ERROR từ test `TableManagerITCase#testPartitionedTableManagement` và ghi *"This is similar to #764"*. Không có bình luận, không có PR. Trạng thái: còn mở.

> **Mức chắc chắn:** trung bình–cao. Issue không phân tích, nhưng mình đã đọc mã `v1.0.0` và thấy đường gây lỗi **cùng gốc với #764**: phản hồi `NotifyLeaderAndIsr` lỗi tới muộn sau khi bảng đã bị xóa. Trên `main`, #4505 bỏ qua chính loại phản hồi đó (xem #764), nên có lẽ cũng giải quyết luôn #765. Mình chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`TableBucketStateMachine`** | Bộ theo dõi trạng thái từng **bucket** trong Coordinator: `NonExistentBucket`, `NewBucket`, `OnlineBucket`, `OfflineBucket`. |
| **`NonExistentBucket`** | Bucket đã bị xóa (hoặc chưa từng tồn tại). |
| **Tập trạng thái trước hợp lệ** | Trong 1.0.0: `OnlineBucket` và `OfflineBucket` chỉ chấp nhận đi từ `{NewBucket, OnlineBucket, OfflineBucket}`. |
| **Leader offline** | Khi replica đang làm leader của bucket bị coi là offline, Coordinator chuyển bucket sang `OfflineBucket` rồi thử `OnlineBucket` (bầu lại). |

## Tóm tắt

Log trong issue:
```
Table bucket TableBucket{... partition=2025} should be one of [OnlineBucket, NewBucket, OfflineBucket] states before moving to OfflineBucket state. Instead it is in NonExistentBucket.
Fail to change state for table bucket ... from NonExistentBucket to OfflineBucket.
... before moving to OnlineBucket state. Instead it is in NonExistentBucket.
Fail to change state for table bucket ... from NonExistentBucket to OnlineBucket.
```

Đây là cặp **OfflineBucket rồi OnlineBucket** mà `onReplicaBecomeOffline` thực hiện cho các bucket có leader là replica vừa bị coi là offline. Bucket đó **đã bị xóa** (`NonExistentBucket`), nên cả hai lần chuyển đều bị từ chối, kèm log ERROR. Giải thích khả dĩ nhất là cùng kịch bản với #764: TabletServer trả lỗi `NotifyLeaderAndIsr` **muộn** cho bucket của partition vừa bị xóa (suy luận từ mã, issue không nói).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/statemachine/BucketState`, `TableBucketStateMachine`, `coordinator/CoordinatorEventProcessor.onReplicaBecomeOffline`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `BucketState`:
    - `OnlineBucket.getValidPreviousStates()` = `EnumSet.of(NewBucket, OnlineBucket, OfflineBucket)`;
    - `OfflineBucket` có cùng tập;
    - `NonExistentBucket` chỉ đi từ `OfflineBucket`.
  - `CoordinatorEventProcessor.onReplicaBecomeOffline(...)`: với các replica offline đang là leader của bucket (`leaderAndIsr.leader() == offlineReplica.getReplica()`), gọi:
    ```java
    tableBucketStateMachine.handleStateChange(bucketWithOfflineLeader, OfflineBucket);
    tableBucketStateMachine.handleStateChange(bucketWithOfflineLeader, OnlineBucket);
    ```
    Không lọc bucket đã hoặc đang bị xóa.
  - `onReplicaBecomeOffline` được gọi từ `processNotifyLeaderAndIsrResponseReceivedEvent` khi có bucket lỗi (xem #764, #3011).
- **Trên `main` (#4505):** `processNotifyLeaderAndIsrResponseReceivedEvent` bỏ qua bucket `isToBeDeleted(...)` **hoặc** không còn phân bổ cho server đó (`!getAssignment(tableBucket).contains(serverId)`). Bucket đã xóa xong (không còn phân bổ) sẽ không còn đi vào `onReplicaBecomeOffline`. Mình suy luận điều này cũng loại được log của #765, nhưng chưa kiểm chứng.

## 2. Khi nào lỗi xảy ra

- Drop bảng hoặc partition (kể cả hết hạn tự động) trong khi có phản hồi `NotifyLeaderAndIsr` lỗi tới muộn cho bucket của nó, **sau khi** bucket đã chuyển sang `NonExistentBucket`.
- Test của issue dùng bảng auto-partition theo năm (`tb1_YEAR`, partition `2025`).

**Phiên bản:** báo trên `0.6.0`. Còn ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

- **Log ERROR gây nhiễu** trên Coordinator.
- **Không ảnh hưởng chức năng:** chuyển trạng thái sai bị chặn đúng. Bucket đã xóa không bị "hồi sinh".

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` gom #765 vào nhóm "log ERROR gây nhiễu, không thấy fix trong 1.0.0". Đối chiếu:
- **Đúng** cho 1.0.0.
- **Bổ sung:** có thể đã được giải quyết trên `main` bởi #4505 (cùng thay đổi với #764 và #1629), nhưng chưa có xác nhận.

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Giống #764, chủ yếu là nhiễu alert khi partition hết hạn.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Loại trừ khỏi alert các dòng ERROR của `TableBucketStateMachine` có `Instead it is in NonExistentBucket` (khi đích là `OfflineBucket` hoặc `OnlineBucket`).

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test để xác nhận kịch bản "phản hồi tới muộn" là nguồn duy nhất của log này.
- Mình chưa kiểm tra các đường khác gọi `handleStateChange(..., OnlineBucket)` (ví dụ `triggerOnlineBucketStateChange`) có thể chạm bucket đã xóa hay không.
