# Issue #4516 — Coordinator coi việc xóa replica là "xong" dù TabletServer chưa hề xác nhận

Link: https://github.com/apache/fluss/issues/4516 — Phiên bản được báo: `0.9.0` (người báo: swuferhong, 29/09/2026). Issue liệt kê hai lỗi, hành vi mong đợi và phạm vi. Issue ghi rõ **chỉ** nói về việc xử lý `StopReplicaResponse` theo từng bucket; phần retry khi gửi lỗi (#3357) và khôi phục sau khi TabletServer restart được xử lý riêng. Không có bình luận. Trạng thái: còn mở. PR [#4517](https://github.com/apache/fluss/pull/4517) **đang mở**. Issue có tham chiếu tới #4503.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` ở cả Coordinator (xử lý phản hồi, đếm số lần thử) và TabletServer (các mã lỗi trả về khi xóa). Hai lỗi issue nêu đều còn nguyên. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Drop table / partition** | Xóa bảng hoặc partition. Coordinator phải ra lệnh cho **từng TabletServer** xóa replica của mọi bucket liên quan, cả local lẫn remote. |
| **`StopReplicaRequest(delete = true)`** | Lệnh "dừng và xóa replica này". Một request có thể chứa nhiều bucket. **Phản hồi trả kết quả cho từng bucket.** |
| **Máy trạng thái replica** | Coordinator theo dõi từng replica: `OnlineReplica` → `OfflineReplica` → `ReplicaDeletionStarted` → `ReplicaDeletionSuccessful` → `NonExistentReplica`. Bảng hoặc partition chỉ được dọn khỏi Coordinator khi **mọi** replica tới `ReplicaDeletionSuccessful`. |
| **`DELETE_TRY_TIMES`** | Số lần xóa thất bại tối đa trước khi Coordinator **bỏ cuộc và coi như xóa thành công**. Trong 1.0.0 là **5**. |
| **`OfflineReplica` (phía TabletServer)** | Replica mà TabletServer đánh dấu hỏng, thường do lỗi đĩa. |
| **Fenced leader epoch** | Request mang leader epoch **cũ hơn** epoch TabletServer đang biết, nên bị từ chối. |

## Tóm tắt

Khi Coordinator nhận `StopReplicaResponse`, có hai chỗ sai:
1. **Chỉ duyệt các bucket có trong phản hồi.** Bucket nào được hỏi nhưng **không có** trong phản hồi thì không ghi nhận gì: không thành công, không thất bại. Theo mã, replica đó **ở mãi** trạng thái `ReplicaDeletionStarted` (giống hậu quả của #3357).
2. **Xóa thất bại được thử lại tối đa 5 lần, rồi bị đánh dấu là thành công**, dù TabletServer chưa bao giờ xác nhận. Bảng hoặc partition vì thế có thể "xóa xong" trong khi trên một TabletServer **replica vẫn còn**, có thể vẫn đang online.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/CoordinatorRequestBatch` (callback của `sendStopRequest`), `coordinator/CoordinatorEventProcessor.processDeleteReplicaResponseReceived`, `coordinator/CoordinatorContext.retryDeleteAndSuccessDeleteReplicas`, và phía TabletServer `replica/ReplicaManager.stopReplicas`.
- **Đã xác nhận trong mã `v1.0.0` (chỗ sai 1):** callback xử lý phản hồi trong `CoordinatorRequestBatch`:
  ```java
  for (PbStopReplicaRespForBucket stopReplicaRespForBucket : stopReplicasResps) {   // chỉ bucket có trong response
      ...
      if (deletedReplicaBuckets.contains(tableBucket)) {
          if (stopReplicaRespForBucket.hasErrorCode()) { ... lỗi ... } else { ... thành công ... }
      }
  }
  ```
  Không có bước đối chiếu `deletedReplicaBuckets` (các bucket **đã hỏi**) với các bucket **có trong phản hồi**. Bucket bị thiếu không sinh ra kết quả nào.
- **Đã xác nhận trong mã `v1.0.0` (chỗ sai 2):**
  - `CoordinatorContext`: `public static final int DELETE_TRY_TIMES = 5;`.
  - `retryDeleteAndSuccessDeleteReplicas`:
    ```java
    if (failDeleteNumbers.getOrDefault(tableBucketReplica, 0) >= DELETE_TRY_TIMES) {
        LOG.warn("Delete replica {} failed, retry times is equal to the max retry times {},"
                + " just mark it as a successful replica deletion directly.", ...);
        successDeleteReplicas.add(tableBucketReplica);
    } else { failDeleteNumbers.merge(...); retryDeleteReplicas.add(...); }
    ```
  - `processDeleteReplicaResponseReceived` chuyển nhóm "thử lại" về `ReplicaDeletionStarted` (gửi lại lệnh xóa), và chuyển nhóm "coi như thành công" sang `ReplicaDeletionSuccessful`, rồi `tableManager.resumeDeletions()`.
  - `main` vẫn giữ nguyên logic này.
- **Phía TabletServer, những lỗi có thể khiến xóa "thất bại" (đã đọc `ReplicaManager.stopReplicas` trong `v1.0.0`):**
  - Replica local đang **offline** (`OfflineReplica`): trả `Errors.LOG_STORAGE_EXCEPTION, "local replica is offline"`, và **không xóa gì**. Sau 5 lần, Coordinator coi là đã xóa, nhưng **dữ liệu vẫn nằm trên đĩa** của server đó.
  - **Epoch cũ:** trả `FENCED_LEADER_EPOCH_EXCEPTION` (*"Ignore the stop replica request ... invalid leader epoch"*). Replica vẫn **online** trên TabletServer. Sau 5 lần, Coordinator coi là đã xóa, nên một replica của bảng đã xóa có thể **vẫn đang được TabletServer phục vụ** trong bộ nhớ. Mình chưa xác minh điều gì xảy ra với nó sau đó.
  - Lỗi I/O khi xóa thư mục (`sweepOrphanTabletDirs`, hoặc `stopReplica` ném exception): trả lỗi tương ứng.

## 2. Khi nào lỗi xảy ra

- **Chỗ sai 1:** phản hồi thiếu bucket. Mình chưa thấy đường nào trong `stopReplicas` của 1.0.0 bỏ qua bucket mà không thêm kết quả. Mọi nhánh đều `result.add(...)`, nên trường hợp này có thể chỉ xảy ra khi lệch phiên bản hoặc có lỗi khác.
- **Chỗ sai 2:** xóa thất bại liên tiếp 5 lần cho cùng một replica. Ví dụ:
  - đĩa của TabletServer đang hỏng, nên replica offline;
  - TabletServer có thông tin epoch mới hơn Coordinator (sau thay đổi leader hoặc Coordinator failover);
  - lỗi quyền hoặc I/O khi xóa thư mục.
- Áp dụng cho mọi lần **drop table, drop partition**, kể cả **auto-partition hết hạn** (partition cũ bị xóa tự động mỗi ngày).

**Phiên bản:** báo trên `0.9.0`. Còn ở `1.0.0` và `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster có xóa bảng hoặc partition (gồm partition hết hạn tự động) trùng lúc có sự cố trên một TabletServer.

**Hậu quả:**
1. **Dữ liệu rác trên đĩa TabletServer:** thư mục log và KV của bảng hoặc partition đã xóa vẫn còn. Đĩa đầy dần (xem #3224, #3223). Khi TabletServer restart, nó có thể **recover cả thư mục rác** (#3223).
2. **Replica "ma" còn online** (trường hợp epoch cũ): replica của bảng đã xóa vẫn tồn tại trong bộ nhớ TabletServer. Hậu quả cụ thể chưa rõ: tốn tài nguyên, và có thể xung đột nếu tạo lại bảng cùng tên (xem #4741, #4755). Đây là suy luận.
3. **Trạng thái Coordinator và TabletServer lệch nhau**, nên việc chẩn đoán khó hơn.
4. **Chỗ sai 1:** bảng hoặc partition kẹt ở trạng thái đang xóa (giống #3357).
5. **Không ảnh hưởng dữ liệu của bảng khác** đang hoạt động.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4516 trong `relate_issue.md` ghi: *"❌ PR #4517 chưa merge | Chỉ khi drop table/partition. Có thể để rác trên đĩa"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:**
  - "Drop partition" gồm cả **auto-partition hết hạn**, nên không hiếm với bảng sự kiện theo ngày.
  - Ngoài rác trên đĩa, trường hợp **epoch cũ** có thể để lại replica **online**.

## 5. Mức ảnh hưởng với CDP

- Bảng sự kiện hoặc context có TTL partition sẽ bị xóa partition **hằng ngày**. Mỗi lần trùng sự cố TabletServer (đĩa hỏng, server restart) có thể để lại rác.
- Ảnh hưởng chủ yếu là **dung lượng đĩa** và **độ sạch của cluster**, không phải dữ liệu đang phục vụ.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Alert theo log Coordinator** `"Delete replica {} failed, retry times is equal to the max retry times"`. Mỗi dòng là một replica có thể còn rác. Ghi lại bucket và server để dọn tay.
- **Định kỳ đối chiếu** thư mục bucket trên đĩa từng TabletServer với danh sách bảng và partition còn tồn tại (qua Admin API), rồi dọn thư mục mồ côi **khi TabletServer đã dừng**.
- Từ 1.0.0, TabletServer dọn thư mục KV cũ khi khởi động (#4407). Restart TabletServer có thể dọn một phần rác KV. Mình chưa xác minh với thư mục log.
- Tránh để đĩa hỏng kéo dài. Thay đĩa hoặc server có replica offline sớm.

## 6. Những điều chưa kiểm chứng

- Mình chưa tìm được đường nào trong 1.0.0 khiến phản hồi thiếu bucket.
- Mình chưa kiểm tra số phận của replica "ma" (trường hợp epoch cũ) sau khi Coordinator coi là đã xóa: nó có tự dọn khi TabletServer nhận metadata mới không.
- Mình chưa đọc #4503 và diff của PR #4517.
- Mình chưa kiểm tra #4407 có dọn thư mục **log** của bảng đã xóa hay không.
