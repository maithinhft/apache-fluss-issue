# Issue #2405 — Rebalance với tag `PERMANENT_OFFLINE` báo "COMPLETED" nhưng leader có thể chưa rời khỏi server cần gỡ

Link: https://github.com/apache/fluss/issues/2405 — Phiên bản được báo: `0.8.0` (người báo: swuferhong, 29/01/2026). Issue gốc rất ngắn, tham chiếu #2401. Có 2 bình luận:
- **Một contributor tái hiện được.** Đây là **race condition** khi tải cao (50 bảng, 400 bucket). Nguyên nhân: `CoordinatorEventProcessor` gửi `StopReplica` theo kiểu **"bắn rồi quên"** (fire-and-forget) và đánh dấu task **`COMPLETED` ngay sau khi gửi RPC**. Nếu TabletServer đang bận, nó **chưa thực sự** dừng replica và **chưa nhường leader**, dù phía người dùng đã thấy task hoàn tất. Hướng sửa:
  - `RebalanceManager` theo dõi các lần xóa đang chờ;
  - chờ `DeleteReplicaResponseReceivedEvent` rồi mới `COMPLETED`;
  - xử lý `DeadTabletServerEvent` để task không treo.
- Người báo xác nhận đã giao việc.

Trạng thái: còn mở. PR [#2524](https://github.com/apache/fluss/pull/2524) **đang mở**.
**Nhóm B:** chỉ khi dùng tính năng **rebalance** (`admin.rebalance(...)`, có từ 0.8), ví dụ để **gỡ một TabletServer** khỏi cluster bằng tag `PERMANENT_OFFLINE`.

> **Mức chắc chắn:** cao về việc mã `v1.0.0` vẫn đánh dấu `COMPLETED` ngay sau khi **gửi** yêu cầu dừng replica cũ (đã đọc `CoordinatorEventProcessor`). Mức độ "leader không rời đi" (đúng hay chỉ chậm) dựa vào bình luận tái hiện, mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Rebalance** | Coordinator tính kế hoạch di chuyển replica và leader giữa các TabletServer theo mục tiêu (`GoalType`), rồi thực hiện từng bucket. Tiến độ xem bằng `admin.listRebalanceProgress(id)`. |
| **Server tag** | Nhãn gắn cho TabletServer bằng `admin.addServerTag(...)`:<br>• **`PERMANENT_OFFLINE`**: *"All buckets on these servers will be migrated away"* (tài liệu `rebalance.md`), dùng khi **gỡ hẳn** server;<br>• `TEMPORARY_OFFLINE`: tạm thời. |
| **Reassignment** | Đổi danh sách replica của bucket: thêm replica mới (AR), đợi bắt kịp, chuyển leader, rồi **gỡ replica cũ** (RR). |
| **`StopReplica`** | RPC Coordinator gửi TabletServer để dừng (và xóa) replica cũ. **Bất đồng bộ**: Coordinator gửi đi rồi tiếp tục. |
| **`RebalanceStatus.COMPLETED`** | Trạng thái hoàn tất của task từng bucket. Người vận hành dựa vào nó để biết **đã an toàn tắt server**. |

## Tóm tắt

Quy trình gỡ server theo tài liệu là: gắn tag `PERMANENT_OFFLINE`, chạy rebalance, đợi trạng thái `COMPLETED`, rồi **tắt server**. Trong `v1.0.0`, mỗi bucket được đánh dấu `COMPLETED` **ngay sau khi Coordinator gửi đi** các thay đổi trạng thái (LeaderAndIsr, StopReplica), **không chờ phản hồi** TabletServer. Khi cluster tải cao, server cũ có thể **chưa xử lý** việc dừng replica hay nhường leader. Người vận hành thấy "COMPLETED" và tắt server, trong khi một số bucket **vẫn có leader trên server đó**. Hệ quả là bucket mất leader tạm thời, và phải bầu lại leader (unclean nếu ISR không đủ).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/CoordinatorEventProcessor` (`tryToCompleteRebalanceTask`, `tryToCompleteReassignmentTask`, `stopRemovedReplicasOfReassignedBucket`) và `coordinator/rebalance/RebalanceManager`.
- **Đã xác nhận trong mã `v1.0.0`:** trong nhánh hoàn tất reassignment:
  ```java
  // B2. ... maybeReassignedBucketLeaderIfRequired(tableBucket, targetReplicas);
  // B3. coordinatorContext.updateBucketReplicaAssignment(tableBucket, targetReplicas);
  // B4. updateBucketEpochAndSendRequest(tableBucket, targetReplicas);
  // B5/B6. replicas in RR -> Offline -> ReplicaMigrationStarted
  stopRemovedReplicasOfReassignedBucket(tableBucket, removingReplicas);
  // B7. updateReplicaAssignmentForBucket(tableBucket, targetReplicas);   (ghi ZooKeeper)
  // B8. updateTabletServerMetadataCache(...)
  // B8. Mark the ongoing rebalance task to finish.
  rebalanceManager.finishRebalanceTask(tableBucket, RebalanceStatus.COMPLETED);
  ```
  `stopRemovedReplicasOfReassignedBucket` chỉ gọi `replicaStateMachine.handleStateChanges(...)` qua `OfflineReplica`, `ReplicaMigrationStarted` rồi `NonExistentReplica`. Các bước này **xếp lệnh RPC gửi đi**, không chờ kết quả. Ngay dòng sau là `COMPLETED`.
- Nhánh **chỉ đổi leader** (`isLeaderChanged && !isBeingReassigned`) thì chặt chẽ hơn: chờ phản hồi `NotifyLeaderAndIsr` **thành công từ leader mới**, rồi đọc ZooKeeper xác nhận `currentLeader == newLeader` trước khi `COMPLETED`.
- `RebalanceManager`: server có tag `PERMANENT_OFFLINE` hoặc `TEMPORARY_OFFLINE` bị coi là offline khi lập kế hoạch. `CoordinatorMetadataCache` không trả server `PERMANENT_OFFLINE` cho việc gán bucket mới.

## 2. Khi nào lỗi xảy ra

- Chạy **rebalance có di chuyển replica** (reassignment), điển hình là gỡ server bằng `PERMANENT_OFFLINE`.
- Cluster **tải cao** hoặc TabletServer cũ **xử lý chậm** (nhiều bucket, hàng đợi request dài). Bình luận tái hiện dùng 50 bảng và 400 bucket.
- Người vận hành **tắt server ngay** khi thấy `COMPLETED`.

**Phiên bản:** báo trên `0.8.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

- **Tín hiệu "xong" sai lệch:** `listRebalanceProgress` báo hoàn tất khi thực tế chưa xong.
- Nếu tắt server lúc này: các bucket còn leader trên server đó **mất leader** cho tới khi Coordinator phát hiện server chết và bầu lại. Ghi và đọc vào bucket đó **lỗi hoặc chậm** trong khoảng thời gian đó.
- Nếu ISR mới chưa đủ: có rủi ro bầu leader không sạch, xem các issue nhóm A về bầu leader (suy luận).
- Nếu không tắt server: lệnh `StopReplica` cuối cùng vẫn được xử lý, nên chỉ là **chậm**, không hỏng (suy luận).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Rebalance / Typed API / JMX | #2405, ... | 🟢 | ❌"*. Đối chiếu:
- **Đúng** về trạng thái ❌.
- **Bổ sung:** nhánh "chỉ đổi leader" có kiểm tra chặt, chỉ nhánh **di chuyển replica** đánh dấu xong quá sớm.

## 5. Mức ảnh hưởng với CDP

- Liên quan khi **thu hẹp hoặc thay** TabletServer (bảo trì, đổi node, scale down). Tần suất thấp, nhưng mỗi lần làm đều có rủi ro.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Đừng tắt server ngay khi thấy COMPLETED.** Kiểm tra thêm, trước khi tắt:
  - server đó **không còn là leader** của bucket nào. Xem metadata bucket qua Admin API (`getTableInfo` / metadata), hoặc metric leader count của server (nếu có);
  - số replica trên server về 0 (metric hoặc log `StopReplica`).
- **Chờ thêm** một khoảng (vài phút), và chạy rebalance vào **giờ thấp điểm** để TabletServer xử lý kịp.
- Tắt server theo cách **graceful** (SIGTERM, không `kill -9`) để server nhường leader có trật tự. Mình chưa kiểm tra quy trình controlled shutdown của 1.0.0.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa đọc diff của PR #2524 và chưa đọc #2401.
- Mình chưa kiểm tra metric nào trong 1.0.0 cho biết chính xác **số leader trên mỗi TabletServer** để làm bước kiểm tra trước khi tắt.
