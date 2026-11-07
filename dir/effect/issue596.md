# Issue #596 — Log của bucket bị cắt (truncate) về offset 0 khi một TabletServer gặp sự cố

Link: https://github.com/apache/fluss/issues/596 — Phiên bản được báo: `0.5.0` (người báo: swuferhong, 13/03/2025, repo cũ `alibaba/fluss`). Issue **rất ngắn**: một câu, hai ảnh chụp (một ảnh là lỗi phía Flink sink, một ảnh là log server cho thấy log bị cắt về 0). Cả hai ảnh không đọc được dạng chữ. Có đúng một bình luận của chính người báo: *"khi tắt idempotence của job ghi thì hiện tượng không xảy ra"*. Không có phân tích, không có PR. Trạng thái: còn mở. Cùng người, cùng ngày, người này báo cả #594 (bão `FetchLogRequest`).

> **Mức chắc chắn:** thấp về nguyên nhân thật của issue (không có log dạng chữ). Mình đã đọc mã `v1.0.0` và tìm được **ba đường cắt log** trên follower. Có **hai tình huống cụ thể** mà các đường đó có thể cắt **về 0**. Cả hai đều là suy luận từ mã, **chưa tái hiện**. Trong giai đoạn 0.6–0.9 đã có nhiều fix đúng vùng này, và không có báo cáo lặp lại.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Truncate log (cắt log)** | Xóa phần cuối log của một replica, đưa log end offset (LEO) lùi về offset cũ hơn. Follower làm việc này khi phát hiện log của mình có phần leader không có. |
| **Cắt về 0** | Xóa **toàn bộ** log local của bucket trên replica đó. Replica phải sao chép lại từ đầu. Nếu nó đang là bản duy nhất giữ dữ liệu, hoặc được bầu làm leader trước khi sao chép lại, thì **mất dữ liệu**. |
| **LEO (log end offset)** | Offset kế tiếp sẽ được ghi, tức "độ dài" log. |
| **High watermark (HW)** | Offset tới đó dữ liệu được coi là đã cam kết (mọi replica trong ISR đều có). |
| **`high-watermark-checkpoint`** | File trên mỗi thư mục dữ liệu của TabletServer, lưu HW của từng bucket. Được ghi định kỳ mỗi `log.replica.high-watermark.checkpoint-interval` (mặc định **5 giây**) và khi tắt server. |
| **ISR (in-sync replicas)** | Tập replica đang bắt kịp leader. Fluss chỉ bầu leader **trong ISR** (clean election). |
| **Unclean shutdown** | Server chết đột ngột (kill -9, mất điện, OOM) và không kịp ghi checkpoint hay flush. |
| **Idempotence / writer state** | Cơ chế chống ghi trùng bằng writer ID và batch sequence (xem #594). Lỗi sequence trên follower cũng dẫn tới một đường cắt log. |

## Tóm tắt

Trong `v1.0.0`, follower cắt log ở **ba chỗ**:
1. **Khi trở thành follower:** `truncateToHighWatermark`, tức cắt về HW **của chính nó**.
2. **Khi gặp lỗi offset out-of-range:** `fetchOffsetAndTruncate`, tức cắt về **LEO của leader**.
3. **Khi gặp lỗi sequence:** `truncateToLeaderEndOffsetSnapshot`, có điều kiện chặn không cho cắt về 0.

Hai tình huống có thể cắt **về 0**:
- **(A)** File `high-watermark-checkpoint` **không có** mục cho bucket, ví dụ vì server chết trước lần checkpoint đầu tiên sau khi bucket được tạo, hoặc vì file mất hay hỏng. Khi đó HW khởi tạo bằng **0**, và lúc replica trở thành follower, nó **cắt toàn bộ log**. Nếu leader còn sống, follower chỉ phải sao chép lại (tốn thời gian, không mất dữ liệu).
- **(B)** Một replica có **log rỗng** (ví dụ sau khi mất đĩa, xem #4566) nhưng **vẫn nằm trong ISR** trên ZooKeeper được bầu làm leader. Các follower khác thấy LEO của leader là 0, nên **cắt về 0 theo leader**. Đây là **mất dữ liệu** thật.

Chưa đủ thông tin để biết tình huống nào, nếu có, là nguyên nhân của #596. Chi tiết "tắt idempotence thì hết" cho thấy thời đó còn liên quan tới đường lỗi sequence. Đường này đã được #731 vá (thêm điều kiện `!= 0`).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm:
  - `replica/ReplicaManager` (`truncateToHighWatermark`, `checkpointHighWatermarks`);
  - `replica/Replica.createLog`;
  - `replica/fetcher/ReplicaFetcherThread` (`fetchOffsetAndTruncate`, `truncateToLeaderEndOffsetSnapshot`);
  - `coordinator/statemachine/ReplicaLeaderElection`.
- **Đường 1: cắt về HW khi trở thành follower (đã đọc mã `v1.0.0`):**
  - Khi xử lý `NotifyLeaderAndIsr` để trở thành follower, `ReplicaManager` gọi `truncateToHighWatermark(replicasBecomeFollower)`, tức `replica.truncateTo(logTablet.getHighWatermark())`. Kèm comment: *"TODO this logic need to be removed after we introduce leader epoch cache ... Trace by #673"*.
  - HW lúc khởi động lấy từ `Replica.createLog`:
    ```java
    Optional<Long> watermarkOpt = lazyHighWatermarkCheckpoint.fetch(tableBucket);
    long watermark = watermarkOpt.orElseGet(() -> {
        LOG.info("No local checkpoint high watermark found for table bucket {}", tableBucket);
        return 0L;
    });
    log.updateHighWatermark(watermark);
    ```
    Không có mục checkpoint thì **HW = 0**, kéo theo cắt về 0 khi làm follower. Đây là **tình huống (A)**.
  - `checkpointHighWatermarks` chạy định kỳ (mặc định 5 giây). Mỗi lần nó ghi lại **cả file** chỉ với các replica **đang online** (`getOnlineReplicaList()`), nên bucket mới tạo mà server chết trong vòng 5 giây sẽ không có mục. Mình suy ra thêm, chưa kiểm chứng: replica đang offline lúc ghi cũng có thể bị bỏ khỏi file.
- **Đường 2: cắt về LEO của leader khi out-of-range (đã đọc mã `v1.0.0`):**
  - `ReplicaFetcherThread.fetchOffsetAndTruncate` lấy `leaderEndOffset = leader.fetchLocalLogEndOffset(...)`. Nếu nhỏ hơn LEO của follower thì `truncate(tableBucket, leaderEndOffset)`.
  - Comment ngay đó thừa nhận: *"There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now."*
  - Không có điều kiện chặn `leaderEndOffset == 0`. Nếu leader có log rỗng, follower bị cắt về 0. Đây là **tình huống (B)**.
  - Fluss chỉ bầu leader trong ISR (`DefaultLeaderElection` lọc `isr.contains(replica)`), nên (B) chỉ xảy ra khi một replica **trong ISR** có log rỗng. Ví dụ: TabletServer mất toàn bộ đĩa local rồi khởi động lại, log được khởi tạo ở offset 0 (`LogLoader` có `TODO: use logStartOffset if issue #744 ready`, xem #4566), trong khi ISR trên ZooKeeper vẫn còn tên nó cho tới khi bị loại sau `log.replica.max-lag-time` (30 giây).
- **Đường 3: cắt khi gặp lỗi sequence (đã đọc mã và diff):** `truncateToLeaderEndOffsetSnapshot` được thêm bởi #731 (12/04/2025, sau ngày issue được báo) với điều kiện `leaderEndOffsetSnapshot != 0L && leaderEndOffsetSnapshot < localLogEndOffset`. Nghĩa là đường này **cố ý không cắt về 0**. Mình không có mã 0.5 trong tay để biết trước đó follower xử lý lỗi sequence thế nào.

## 2. Khi nào lỗi xảy ra

- **Tình huống (A)**, khi đồng thời:
  1. TabletServer chết **đột ngột**;
  2. bucket chưa từng được checkpoint HW (tạo trong vòng 5 giây trước khi chết), hoặc file checkpoint mất hay hỏng;
  3. sau khi lên lại, replica đó được Coordinator giao làm **follower**.
- **Tình huống (B)**, khi đồng thời:
  1. một TabletServer **mất dữ liệu local** (mất PV, bị xóa thư mục) rồi khởi động lại trong khi tên nó **vẫn còn trong ISR**;
  2. leader hiện tại chết hoặc bị chuyển, và replica có log rỗng được bầu.
  - Với replication factor 3, cần thêm điều kiện các replica khác cũng không được chọn (đã chết hoặc ngoài ISR), nên rất hiếm. Với replication factor 2 thì khả năng cao hơn.
- **Phiên bản:** báo trên `0.5.0`. Cả ba đường cắt log và điểm "HW mặc định 0" vẫn còn ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster có replication, khi có server chết đột ngột hoặc mất đĩa.

**Hậu quả:**
1. **Tình huống (A):** follower xóa toàn bộ log local rồi sao chép lại từ leader.
   - Tốn mạng và thời gian. Bucket under-replicated trong lúc đó.
   - Với bảng PK, KV của follower cũng phải dựng lại.
   - **Không mất dữ liệu** nếu leader còn sống và giữ đủ log. Nếu log cũ ở leader đã được đẩy lên remote storage, follower phải fetch từ remote.
2. **Tình huống (B):** các replica còn lại **bị cắt khớp với leader rỗng**, nên **mất dữ liệu** trên toàn bucket.
   - Với bảng PK, KV được dựng lại từ snapshot cộng log, và nếu log đã mất thì KV cũng lùi lại.
   - Với CDP, đây là mất hoặc lùi dữ liệu profile.
3. **Phía Flink sink (theo ảnh trong issue):** sink nhận lỗi và failover. Mình không đọc được nội dung lỗi.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #596 trong `relate_issue.md` ghi: *"❔ Nhiều fix truncate/recovery sau 0.5 (#676, #731, #4013). Không có báo cáo lặp lại. Theo dõi. Replication factor ≥ 3, `acks=-1`."* Đối chiếu:
- **❔ là hợp lý**, vì không xác định được nguyên nhân gốc.
- **Bổ sung:** mã 1.0.0 vẫn còn hai đường có thể cắt về 0, là (A) và (B). Không có điều kiện chặn ở `truncateToHighWatermark` hay `fetchOffsetAndTruncate`. Giải pháp gốc là **leader epoch cache** (#673), và nó **chưa có** trong 1.0.0 (xem #4269).
- **#676** (thay đổi thứ tự cập nhật HW giữa leader và follower, sửa #674 "mất dữ liệu khi recover sau unclean shutdown") **có trong 1.0.0**, và đúng là liên quan tới HW. #731 chỉ vá đường lỗi sequence.

## 5. Mức ảnh hưởng với CDP

- Rủi ro (B) là **mất dữ liệu profile**, nên dù hiếm vẫn đáng chuẩn bị. Rủi ro (A) chỉ là tốn tài nguyên khi phục hồi.
- Kết hợp với #4566 (mất đĩa thì KV không recover được): kịch bản **mất PV trên Kubernetes** là nơi các lỗi này gặp nhau.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Replication factor 3** cho mọi bảng quan trọng, **`client.writer.acks=-1`** cho writer, và đặt **`log.replica.min-in-sync-replicas-number` = 2**. Mặc định của cấu hình này là **1**, tức với `acks=-1` mà ISR chỉ còn một replica thì ghi vẫn thành công, và dữ liệu khi đó chỉ nằm trên một máy. Mình chưa kiểm chứng cấu hình này chặn được tình huống (B), vì (B) là chuyện bầu leader chứ không phải chuyện ghi.
- **Dùng PV bền** cho TabletServer, không dùng `emptyDir` hay local ephemeral. Khi thay đĩa hoặc node, **đợi replica bị loại khỏi ISR** (hoặc chủ động gỡ bằng rebalance) trước khi cho TabletServer với đĩa trống tham gia lại.
- **Tắt TabletServer có trật tự** (graceful), để file `high-watermark-checkpoint` được ghi khi shutdown (`ReplicaManager.shutdown` có gọi `checkpointHighWatermarks()`).
- **Alert:**
  - log `"No local checkpoint high watermark found for table bucket"` khi khởi động (dấu hiệu tình huống A);
  - log `"Reset fetch offset for bucket {} from {} to current leader's latest offset {}"` (đường 2), đặc biệt khi offset mới là 0;
  - log start offset hoặc LEO của bucket lùi đột ngột.
- **Diễn tập:** kill -9 TabletServer ngay sau khi tạo bảng mới. Thử cả việc xóa thư mục dữ liệu của một TabletServer rồi khởi động lại, trong khi một server khác cũng bị tắt. Sau đó so số bản ghi.

## 6. Những điều chưa kiểm chứng

- Nguyên nhân thật của #596: ảnh không đọc được dạng chữ, và mình không có mã 0.5.
- Mình chưa tái hiện tình huống (A) hay (B) trên 1.0.0.
- Mình chưa kiểm tra replica đang offline có bị bỏ khỏi file checkpoint HW khi ghi lại hay không.
- Mình chưa kiểm tra `log.replica.min-in-sync-replicas-number` có ảnh hưởng tới việc bầu leader hay không, tức có chặn được (B) hay không.
- Mình chưa kiểm tra đường cắt khi log đã được đẩy lên remote storage (local rỗng nhưng remote còn).
