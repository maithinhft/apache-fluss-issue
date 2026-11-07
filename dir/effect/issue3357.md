# Issue #3357 — Xóa bảng bị kẹt vĩnh viễn khi lệnh `StopReplica` không gửi tới được TabletServer

Link: https://github.com/apache/fluss/issues/3357 — Phiên bản được báo: `0.9.0` (người báo: gyang94, 22/05/2026). Issue phân tích chi tiết, kèm máy trạng thái, đoạn mã gây lỗi và lý do vòng kẹt không thể tự thoát. Không có bình luận. Trạng thái: còn mở. PR [#3369](https://github.com/apache/fluss/pull/3369) đã **bị đóng, không merge**.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và `main`, đoạn bỏ qua lỗi vẫn còn nguyên. Có một điểm đáng chú ý: 1.0.0 đã thêm **khung gửi lại theo từng TabletServer** ([#3391](https://github.com/apache/fluss/pull/3391), 11/09/2026), và commit đó khởi đầu với tên "fix: stop-replica-failed". Nhưng theo mô tả commit và theo mã, phần StopReplica đã được **trả về hành vi cũ**: hàm `enqueueRequest` của khung mới **không có nơi nào gọi**. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`StopReplicaRequest`** | Lệnh Coordinator gửi TabletServer để dừng và xóa replica khi drop bảng hoặc partition. |
| **Gửi thất bại (transport failure)** | Request **không tới được** TabletServer: lỗi mạng, server đang tắt, chết, hoặc đã bị gỡ khỏi danh sách kết nối. Khác với trường hợp "tới nơi nhưng server trả lỗi" (#4516). |
| **Máy trạng thái replica** | `OnlineReplica` → `OfflineReplica` → **`ReplicaDeletionStarted`** → `ReplicaDeletionSuccessful` → `NonExistentReplica`. |
| **`isEligibleForDeletion`** | Điều kiện để Coordinator **gửi lại** lệnh xóa bảng hoặc partition: bảng nằm trong hàng đợi xóa **và không có replica nào** đang ở `ReplicaDeletionStarted`. |
| **`resumeDeletions`** | Bước kiểm tra lại các bảng hoặc partition đang chờ xóa: hoàn tất nếu mọi replica đã xóa xong, gửi lại nếu đủ điều kiện. |
| **`DeleteReplicaResponseReceivedEvent`** | Sự kiện Coordinator nhận khi có phản hồi xóa. **Không có sự kiện này thì trạng thái replica không đổi.** |
| **`fluss_coordinator_tableCount`** | Metric số bảng Coordinator đang quản lý. |

## Tóm tắt

Khi drop bảng, Coordinator chuyển replica sang `ReplicaDeletionStarted` và gửi `StopReplicaRequest(delete = true)`. Nếu request **không gửi tới được**, callback chỉ log WARN rồi `return`: không sinh sự kiện, không đổi trạng thái. Replica **kẹt** ở `ReplicaDeletionStarted`. Vì điều kiện gửi lại yêu cầu **không** replica nào ở trạng thái đó, nên **không có đường nào** đưa replica ra khỏi trạng thái này, và việc xóa bảng **kẹt vĩnh viễn**. Hệ quả nhìn thấy được: `fluss_coordinator_tableCount` cao hơn số bảng thật trong ZooKeeper. Theo #1629, cách thoát là **restart Coordinator**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/CoordinatorRequestBatch.sendStopRequest`, `coordinator/CoordinatorChannelManager`, `coordinator/TableManager`, `coordinator/CoordinatorEventProcessor`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - Callback trong `CoordinatorRequestBatch.sendStopRequest`:
    ```java
    if (throwable != null) {
        // todo: in FLUSS-55886145, we will introduce a sender thread to send the request.
        // in here, we just ignore the error.
        LOG.warn("Failed to send stop replica request to tablet server {}.", serverId, throwable);
        return;
    }
    ```
  - `CoordinatorChannelManager.sendStopBucketReplicaRequest(...)` gọi `sendRequest(...)`, tức gửi **trực tiếp** qua gateway (`requestFunction.apply(gateway, request).whenComplete(responseConsumer)`), **không** qua hàng đợi có retry. Thêm một trường hợp tệ hơn: nếu **không có gateway** cho server đó (server đã bị coi là offline), `sendRequest` chỉ log `"Can't not send {} to the tablet server {} as the server is offline."` và **không gọi callback**.
  - `TableManager.isEligibleForDeletion(tableId)`: `isTableQueuedForDeletion(tableId) && !isAnyReplicaInState(tableId, ReplicaDeletionStarted)`. Tương tự cho partition.
  - `TableManager.resumeTableDeletions()` hoàn tất xóa khi `areAllReplicasInState(tableId, ReplicaDeletionSuccessful)`, ngược lại chỉ gửi lại nếu `isEligibleForDeletion`. Replica kẹt ở `ReplicaDeletionStarted` thì cả hai nhánh đều không chạy.
  - `CoordinatorEventProcessor.processDeadTabletServer` **bỏ qua** các replica thuộc bảng đang xóa (`filter(... !coordinatorContext.isToBeDeleted(...))`). Server chết cũng không gỡ được trạng thái kẹt.
- **Về khung retry #3391 (đã đọc mã và mô tả commit):**
  - 1.0.0 có `CoordinatorChannelManager.enqueueRequest(...)` và `ControlRequestSendThread`, với mô tả *"retries transport failures and timeouts until the request succeeds or the channel is removed"*.
  - Mô tả commit #3391: *"Keep the generic per-tablet-server queue, sender, retry ... while restoring concrete StopReplica handling to the main branch behavior."*
  - Mình tìm `enqueueRequest(` trong `fluss-server/src/main` của `v1.0.0` và chỉ thấy định nghĩa, **không có lời gọi**. Trên `main`, `sendStopBucketReplicaRequest` vẫn gọi `sendRequest`.
  - Nói cách khác, khung retry đã có sẵn nhưng **chưa được nối vào** StopReplica.

## 2. Khi nào lỗi xảy ra

Drop bảng hoặc partition (kể cả **auto-partition hết hạn**) đúng lúc một TabletServer giữ replica của nó:
- đang restart hoặc rolling upgrade;
- mất mạng tạm thời;
- vừa chết (gateway đã bị gỡ).

**Phiên bản:** báo trên `0.9.0`. Còn ở `1.0.0` và `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster có xóa bảng hoặc partition trùng lúc TabletServer gián đoạn.

**Hậu quả:**
1. **Bảng hoặc partition kẹt** ở trạng thái đang xóa trong Coordinator. `tableCount` và `partitionCount` cao hơn thực tế.
2. **Dữ liệu replica trên TabletServer đó không bị xóa**, nên tồn rác trên đĩa (xem #1629, #3223).
3. **Có thể ảnh hưởng việc tạo lại bảng cùng tên** khi bảng cũ còn kẹt (suy luận, liên quan #4741 và #4755).
4. **Không ảnh hưởng bảng khác.**
5. **Tự hết khi restart Coordinator.** Theo #1629, Coordinator dựng lại trạng thái từ ZooKeeper và thử xóa lại.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3357 trong `relate_issue.md` ghi: *"❌ PR #3369 chưa merge | DDL hiếm"*. Đối chiếu:
- **❌ đúng.** PR #3369 đã **bị đóng** chứ không phải "chưa merge".
- **"DDL hiếm" chưa chính xác với CDP.** Auto-partition hết hạn là một lần drop partition **mỗi chu kỳ** (mỗi ngày hoặc mỗi giờ), nên trùng với restart TabletServer không phải chuyện hiếm.
- **Bổ sung:** khung retry #3391 đã có trong 1.0.0 nhưng không được dùng cho StopReplica.

## 5. Mức ảnh hưởng với CDP

- Bảng sự kiện hoặc context có partition hết hạn tự động: mỗi lần rolling restart có thể làm vài partition kẹt xóa.
- Ảnh hưởng chính là **sai metric, rác trên đĩa, trạng thái Coordinator bẩn**. Không ảnh hưởng ghi hay lookup dữ liệu hiện hành.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Theo dõi log Coordinator** `"Failed to send stop replica request to tablet server"` và `"Can't not send StopReplicaRequest to the tablet server ... as the server is offline"`.
- **So sánh định kỳ** `tableCount` và `partitionCount` của Coordinator với số bảng và partition thật (qua Admin API). Lưu ý #5038 về metric Coordinator đứng yên. Nếu lệch, **restart Coordinator** vào giờ thấp điểm.
- **Tránh rolling restart TabletServer vào giờ auto-partition hết hạn**. Lịch dọn partition mặc định theo đơn vị partition, và giờ cụ thể phụ thuộc cấu hình múi giờ của bảng.
- Kiểm tra dung lượng đĩa và thư mục mồ côi sau mỗi đợt bảo trì.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa kiểm tra chắc chắn rằng restart Coordinator thì xóa lại đúng (chỉ dựa vào #1629).
- Mình chưa đọc diff của #3391 để biết tại sao phần StopReplica bị gỡ ra.
- Mình chưa kiểm tra các loại request điều khiển khác (`NotifyLeaderAndIsr`, `UpdateMetadata`) có cùng kiểu "gửi lỗi thì bỏ qua" hay không.
