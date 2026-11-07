# Issue #3011 — Replica bị đánh dấu "offline" mãi mãi, khiến bucket không bao giờ bầu được leader

Link: https://github.com/apache/fluss/issues/3011 — Phiên bản được báo: `0.9.0` (người báo: platinumhamburg, 07/04/2026). Issue ngắn, tự mô tả nguyên nhân và hướng sửa, không có bình luận. Trạng thái: còn mở. PR gắn kèm [#3012](https://github.com/apache/fluss/pull/3012) (cùng tác giả) đã **bị đóng, không merge** (02/09/2026).

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` ở cả TabletServer lẫn Coordinator. Có **hai thay đổi khác** (không phải PR #3012) đã vào `1.0.0` và xử lý gần như đúng kịch bản của issue: [#3671](https://github.com/apache/fluss/pull/3671) (bỏ qua fetcher khi chưa có leader) và [#3539](https://github.com/apache/fluss/pull/3539) (Coordinator tự thử lại replica offline sau 1 phút). Nhưng phần "mọi lỗi đều bị coi là offline" mà PR #3012 muốn sửa thì **vẫn còn**. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Coordinator** | "Bộ não" của cluster: quyết định replica nằm ở đâu, ai làm leader, và gửi lệnh cho TabletServer. |
| **`NotifyLeaderAndIsr`** | Lệnh Coordinator gửi TabletServer: "bucket này, leader là server X, ISR là [..], leader epoch là N". TabletServer trả kết quả **cho từng bucket**, gồm thành công hoặc mã lỗi. |
| **`leaderId` / `NO_LEADER`** | ID server làm leader. `LeaderAndIsr.NO_LEADER = -1` nghĩa là "hiện không có leader". |
| **Bầu leader (leader election)** | Coordinator chọn một replica **đang online** trong ISR (hoặc trong danh sách replica) để làm leader. |
| **`replicasOnOffline`** | Bản đồ trong `CoordinatorContext`: server → các bucket mà replica trên server đó bị coi là **hỏng**. `isReplicaOnline()` loại các replica này, nên chúng **không được bầu làm leader** dù server vẫn sống. |
| **`STORAGE_EXCEPTION`** | Mã lỗi "lỗi lưu trữ" TabletServer trả về. |
| **Fetcher** | Luồng follower kéo dữ liệu từ leader (xem #594). |
| **`OfflineBucket` / `OnlineBucket`** | Trạng thái bucket trong Coordinator. `OfflineBucket` nghĩa là bucket không có leader dùng được. |

## Tóm tắt

Khi một bucket **chưa có leader**, Coordinator vẫn gửi `NotifyLeaderAndIsr` cho các follower. Ở `0.9.0`, TabletServer coi "không có leader" là lỗi và trả `STORAGE_EXCEPTION`. Coordinator nhận lỗi và **đánh dấu replica đó offline** (thêm vào `replicasOnOffline`). Vì replica offline không được bầu, các lần bầu sau **không còn ứng viên**. Không có cơ chế nào gỡ dấu offline khi server vẫn sống, nên bucket **mất leader vĩnh viễn** cho tới khi restart.

Ở `1.0.0`, hai thay đổi khác đã vào:
- TabletServer **bỏ qua im lặng** khi `leaderId == NO_LEADER` (#3671). Đây đúng là hướng sửa issue đề xuất.
- Coordinator **tự gỡ dấu offline và bầu lại sau 1 phút** (#3539).

Vì vậy kịch bản "vĩnh viễn" không còn. Nhưng **mọi lỗi** trong phản hồi `NotifyLeaderAndIsr`, kể cả lỗi tạm thời, vẫn đánh dấu replica offline, và bucket có thể mất leader **khoảng 60 giây**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm:
  - `ReplicaManager.addFetcherForReplicas` (TabletServer);
  - `CoordinatorEventProcessor.processNotifyLeaderAndIsrResponseReceivedEvent` và `onReplicaBecomeOffline` (Coordinator);
  - `CoordinatorContext.isReplicaOnline`.
- **Đã xác nhận trong mã `v1.0.0` (TabletServer):** `addFetcherForReplicas` có ba nhánh:
  ```java
  if (leaderId == null) {            // trả StorageException("Could not find leader for follower replica ...")
  } else if (leaderId == LeaderAndIsr.NO_LEADER) {
      LOG.info("Skip adding fetcher for follower replica {} without leader.", tb);
  } else { /* thêm fetcher */ }
  ```
  - Nhánh `NO_LEADER` được thêm bởi #3671 (16/07/2026, "skip adding fetchers for followers without a leader"). Mục đích ban đầu của #3671 là tránh log `ServerNode -1 is not available in metadata cache`, nhưng nó cũng loại bỏ kịch bản của #3011.
  - `NotifyLeaderAndIsrData.getLeader()` trả kiểu `int`. Khi Coordinator không có leader, nó gửi `-1`, và giá trị này được ghi vào `Replica.leaderReplicaIdOpt`. Vì vậy trong 1.0.0, "không có leader" rơi vào nhánh `NO_LEADER`, **không** rơi vào nhánh `null`. Nhánh `null` chỉ còn khi replica chưa từng nhận thông tin leader. Mình suy ra từ mã và chưa tìm được đường nào dẫn tới trường hợp đó.
- **Đã xác nhận trong mã `v1.0.0` (Coordinator):**
  - `processNotifyLeaderAndIsrResponseReceivedEvent`: *"if the error code is not none, we will consider it as offline"*. **Mọi** bucket trả lỗi, bất kể mã lỗi gì, đều bị đưa vào `onReplicaBecomeOffline`. Code này `addOfflineBucketInServer(...)`, bầu lại leader cho bucket mà leader chính là replica đó, rồi gọi `scheduleOfflineLeaderRetryIfNeeded()`.
  - `scheduleOfflineLeaderRetryIfNeeded`: nếu còn replica offline trên TabletServer **đang sống**, Coordinator lên lịch `offline-leader-retry` sau `coordinator.offline-leader.retry-delay` (mặc định **1 phút**). Mô tả cấu hình này: *"lets a leader that was rejected because of temporary tablet-server conditions, such as disk write protection, become electable"*. Cơ chế này thêm từ #3539 (07/07/2026, "Reject KV leader promotion under disk write lock").
  - `processRetryOfflineLeader`: gỡ dấu offline (`removeOfflineBucketInServer`), chuyển replica sang `OnlineReplica`, rồi `triggerOnlineBucketStateChange()` để bầu lại. Nếu server vẫn từ chối, phản hồi lỗi sẽ đặt dấu offline lại và lên lịch lần thử tiếp theo.
  - Chỉ thử lại replica còn ở trạng thái `OfflineReplica` và không thuộc bảng đang bị xóa (`retryableOfflineReplicasOnLiveTabletServers`).
- **Điều PR #3012 muốn làm mà 1.0.0 chưa có** (theo mô tả commit trong PR, mình không đọc diff): chỉ coi là offline với lỗi **nghiêm trọng** (`STORAGE_EXCEPTION`, `LOG_STORAGE_EXCEPTION`, `KV_STORAGE_EXCEPTION`, `UNKNOWN_SERVER_ERROR`), còn các lỗi khác là tạm thời và không ảnh hưởng tư cách được bầu.

## 2. Khi nào lỗi xảy ra

**Ở `0.9.0` (bản gốc của issue):** bucket rơi vào trạng thái không có leader. Ví dụ:
- Mọi replica trong ISR cùng chết.
- Leader chết mà các replica khác chưa đủ điều kiện.
- Ngay sau khi tạo bảng mà chưa replica nào tạo được (xem #4522).

Khi đó Coordinator gửi `NotifyLeaderAndIsr(leader = -1)` cho các follower.

**Ở `1.0.0` (phần còn lại):** bất kỳ lỗi nào trong phản hồi `NotifyLeaderAndIsr` của một replica đều đánh dấu nó offline 1 phút. Các lỗi đã thấy trong mã và issue khác:
- `FencedLeaderEpochException` (leader epoch cũ hơn) trong `Replica.makeFollower`.
- `SchemaNotExistException` khi ZooKeeper follower chậm (#4522).
- Bị từ chối vì disk write protection (#3539).

Nếu replica bị đánh dấu là ứng viên duy nhất, bucket không có leader cho tới lần thử lại.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** mọi bảng, khi cluster có sự cố làm bucket mất leader hoặc khi TabletServer trả lỗi tạm thời.

**Hậu quả:**
1. **Trên 0.9.0:** bucket mất leader **vĩnh viễn**. Ghi và lookup theo các key thuộc bucket đó lỗi cho tới khi restart Coordinator (`replicasOnOffline` nằm trong bộ nhớ, và `CoordinatorContext` được xóa khi Coordinator khởi động lại).
2. **Trên 1.0.0 (suy luận từ mã):** bucket có thể **mất leader khoảng 60 giây** (thời gian `retry-delay`) mỗi khi gặp lỗi tạm thời, rồi tự phục hồi nếu server khỏe lại. Trong lúc đó:
   - Flink sink ghi vào bucket này retry rồi có thể failover.
   - Lookup của AI agent theo key thuộc bucket này lỗi hoặc treo tới timeout. Xem #4043: lookup vào bucket lỗi có thể chiếm hết permit của client.
3. **Không mất dữ liệu đã commit.** Dữ liệu vẫn nằm trên replica, chỉ là không ai phục vụ.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3011 trong `relate_issue.md` ghi: *"🟡 Mã `v1.0.0` đã có cơ chế retry định kỳ các replica offline trên TabletServer còn sống. PR gốc #3012 chưa merge. Giảm từ Rất cao xuống Trung bình."* Đối chiếu:
- **Đúng** về cơ chế retry (#3539).
- **Bổ sung:** nguyên nhân gốc của issue (TabletServer trả lỗi khi `leader = -1`) đã được sửa bởi #3671. Dòng cũ có thể hiểu nhầm là TabletServer vẫn còn lỗi đó.
- **Chính xác hơn về PR:** #3012 đã **bị đóng**, không phải "chưa merge". Không có PR nào đang mở cho phần "chỉ coi lỗi nghiêm trọng là offline".
- 🟡 vẫn hợp lý: kịch bản chính đã hết, nhưng lỗi tạm thời vẫn gây mất leader khoảng 1 phút.

## 5. Mức ảnh hưởng với CDP

- Ảnh hưởng tới **luồng ghi profile và lookup của AI agent** trong những phút có sự cố hoặc ngay sau khi tạo bảng. Không phải lỗi thường trực.
- Kết hợp với #4522 (tạo bảng mới khi ZooKeeper follower chậm): bucket mới có thể không có leader **60 giây** sau `CREATE TABLE`.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Alert theo `offlineBucketCount`** của Coordinator. Lưu ý #5038: metric Coordinator có thể đứng yên sau khi bầu lại Coordinator.
- **Đặt timeout, retry và circuit breaker** cho lookup ở tầng AI agent, để vài chục giây bucket không có leader không làm treo agent.
- **Cân nhắc giảm `coordinator.offline-leader.retry-delay`** (ví dụ 10–20 giây) để rút ngắn thời gian mất leader. Cái giá là Coordinator thử lại thường xuyên hơn khi server thật sự hỏng. Đây là ý của mình, cần thử trước.
- Theo dõi log Coordinator `"The replica {} become offline."` và `"Retrying {} offline replicas on live tablet servers ..."` để biết khi nào cơ chế này kích hoạt.
- Không chạy cluster với replication factor 1 cho bảng quan trọng. Với 1 replica, mất leader là mất phục vụ hoàn toàn.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện cả kịch bản cũ (0.9.0) lẫn kịch bản "lỗi tạm thời, mất 60 giây" trên 1.0.0.
- Mình chưa tìm được đường nào trong 1.0.0 khiến `leaderId == null` khi vào `addFetcherForReplicas`.
- Mình chưa đọc diff của #3012 để biết chính xác danh sách lỗi mà nó coi là nghiêm trọng (chỉ đọc mô tả commit).
- Mình chưa kiểm tra metric `offlineBucketCount` có tính cả bucket đang chờ retry hay không.
