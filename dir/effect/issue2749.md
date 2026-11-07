# Issue #2749 — Khi một TabletServer restart, các TabletServer khác liên tục ghi cảnh báo "fetch log request timeout"

Link: https://github.com/apache/fluss/issues/2749 — Phiên bản được báo: `0.8.0` (người báo: zuston, 28/02/2026). Issue **rất ngắn**: chỉ có một đoạn log và dòng "could you help check this?". Không có phân tích nguyên nhân, không có bình luận trả lời, không có PR. Trạng thái: còn mở.

> **Mức chắc chắn:** issue chỉ mô tả **hiện tượng trong log**. Người báo **không nêu hậu quả nào khác** (không nói mất dữ liệu, không nói độ trễ). Câu "Độ trễ replication tăng khi restart node" trong `relate_issue.md` là **suy luận** chứ không phải điều issue nói. Mình đã đọc mã `v1.0.0` để giải thích **vì sao** timeout là 30 giây và **vì sao** đổi hai tham số mà người báo thử không có tác dụng. Mình **không biết** điều gì khiến request không có phản hồi trong lúc restart, và chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Replica / leader / follower** | Mỗi bucket có nhiều bản sao (replica) trên các TabletServer khác nhau. Một bản là **leader** (nhận ghi và phục vụ đọc), các bản còn lại là **follower** (sao chép từ leader để dự phòng). |
| **Replication (sao chép)** | Follower liên tục xin dữ liệu mới từ leader bằng yêu cầu **fetch log**, rồi ghi vào đĩa của mình. |
| **`ReplicaFetcherThread`** | Luồng trên TabletServer đóng vai follower, gửi `FetchLogRequest` tới **một** TabletServer leader cụ thể cho mọi bucket mà nó theo dõi từ leader đó. |
| **`FetchLogRequest`** | Yêu cầu "cho tôi các bản ghi mới từ offset X cho các bucket này". |
| **`TimeoutException`** | Chờ phản hồi quá thời hạn mà không có phản hồi nào. |
| **Restart TabletServer** | Tắt rồi bật lại một TabletServer (nâng cấp, sự cố, pod Kubernetes bị thay). Trong lúc tắt, nó không trả lời request. |
| **ISR (in-sync replicas)** | Tập các replica đang bắt kịp leader. Follower nào không fetch hoặc tụt lại quá `log.replica.max-lag-time` (mặc định 30 giây) sẽ bị leader loại khỏi ISR. |
| **Backoff (lùi lại)** | Thời gian chờ trước khi thử lại sau khi lỗi. Ở đây `log.replica.fetch.backoff-interval`, mặc định 1 giây. |
| **`log.replica.fetcher-number`** | Số luồng fetch **trên mỗi TabletServer nguồn** (mặc định 1). Tăng lên thì chia bucket ra nhiều luồng hơn. |
| **`log.replica.fetch.wait-max-time`** | Thời gian tối đa mà **leader** được giữ lại một yêu cầu fetch khi chưa đủ dữ liệu (mặc định 500 ms). Đây là chờ phía **leader**, không phải thời hạn chờ phía follower. |

## Tóm tắt

Trong `v1.0.0`, `ReplicaFetcherThread` gửi `FetchLogRequest` tới leader rồi chờ bằng `future.get(timeoutSeconds, SECONDS)` với `timeoutSeconds` **cố định bằng 30** (hàm dựng công khai truyền thẳng số 30, không đọc từ cấu hình). Khi leader đang restart và không trả lời, follower chờ đủ 30 giây rồi ghi cảnh báo `TimeoutException`, đánh dấu **mọi bucket mà luồng đó theo dõi** là lỗi, nghỉ 1 giây (`log.replica.fetch.backoff-interval`) rồi thử lại, nên log lặp lại khoảng mỗi 31 giây cho tới khi leader trả lời hoặc leadership được chuyển. Hai tham số mà người báo thử (`log.replica.fetcher-number`, `log.replica.fetch.wait-max-time`) **không liên quan tới thời hạn 30 giây này**, nên log vẫn lặp như cũ.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, phần sao chép replica: `ReplicaFetcherThread`, `RemoteLeaderEndpoint`, `ReplicaFetcherManager`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `ReplicaFetcherThread(name, replicaManager, leader, fetchBackOffMs)` gọi hàm dựng kia với `timeoutSeconds = 30`. Hàm dựng có tham số timeout là package-private (dùng cho test). Ở `v0.8.0-incubating` cũng là `private final int timeoutSeconds = 30;`. Nên **không có cấu hình nào đổi được 30 giây này**.
  - `processFetchLogRequest`: `responseData = fetchFuture.get(timeoutSeconds, TimeUnit.SECONDS);`. Khi bắt được ngoại lệ (kể cả `TimeoutException`) và luồng còn đang chạy thì `LOG.warn("Error in response from leader server {} for fetch log request from table ids: {}", ...)`, rồi thêm **tất cả bucket của luồng** (`fairBucketStatusMap.bucketSet()`) vào `bucketsWithError`.
  - `handleBucketWithError` gọi `delayBuckets(buckets, fetchBackOffMs)`, tức hoãn các bucket đó theo `log.replica.fetch.backoff-interval` (mặc định 1 giây) trước lần fetch kế.
  - Trong mã có sẵn `TODO this need not blocking to wait fetch log complete, change to async` ngay chỗ này: luồng bị **chặn** tối đa 30 giây mỗi lần.
  - Mỗi luồng fetcher gắn với **một** TabletServer leader (`LeaderEndpoint` duy nhất). `ReplicaFetcherManager` chọn luồng theo `tableBucket.hashCode() % log.replica.fetcher-number` cho mỗi server nguồn. Nên một luồng tới leader đang restart không ảnh hưởng luồng khác tới leader khỏe (mình suy ra từ cấu trúc, chưa tái hiện).
  - Địa chỉ leader lấy từ metadata cache qua `serverNodeMetadataCache.apply(leaderId)` ở **mỗi lần gửi**; nếu không có thì ném `RuntimeException("ServerNode ... is not available in metadata cache.")` với chú thích "fetch thread expects to retry".
- **Vì sao hai tham số người báo thử không giúp (đã đọc mô tả cấu hình và mã):** `log.replica.fetcher-number` chỉ chia bucket ra nhiều luồng hơn (tăng 10 thì có thể ra 10 luồng cùng báo lỗi, tức nhiều dòng log hơn); `log.replica.fetch.wait-max-time` quy định leader giữ yêu cầu bao lâu khi chưa đủ dữ liệu. Không tham số nào chạm tới `timeoutSeconds = 30`.
- **Vì sao request không trả lời mà không có lỗi ngắt kết nối:** mình **không biết**. Khi kết nối bị đứt rõ ràng (leader tắt hẳn và gửi RST), `ServerConnection` hoàn thành các request đang bay bằng `NetworkException` ngay, nên lỗi sẽ khác (không phải timeout). Timeout xảy ra khi kết nối không được báo là đứt (ví dụ máy/pod biến mất không đóng kết nối, hoặc server đang tắt dở mà không phản hồi). Khớp với việc RPC client `v1.0.0` **không có timeout cho request đang bay** (`TODO: maybe we need to add timeout for the inflight requests`, đã gặp ở #692), nên fetcher phải tự đặt thời hạn 30 giây.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Một TabletServer là leader của một số bucket đang restart hoặc không phản hồi**, trong khi các TabletServer khác là follower của các bucket đó và đang fetch từ nó. (Issue không nói server nào in log; theo nội dung log đó là phía follower đang fetch.)
2. **Yêu cầu fetch không nhận phản hồi cũng không nhận lỗi ngắt kết nối** trong 30 giây.
3. **Tình trạng đó kéo dài** (cho tới khi leader phản hồi lại, hoặc Coordinator chuyển leadership và follower được gán fetcher mới tới leader mới).

Log lặp mỗi khoảng 31 giây (30 giây chờ + 1 giây lùi) cho **mỗi luồng fetcher** đang trỏ tới server đó.

**Phiên bản:** báo trên `0.8.0`; hằng số 30 giây và cách xử lý lỗi y hệt ở `v1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành cluster nhiều TabletServer có replication, khi restart/nâng cấp từng node.

**Hậu quả mà issue nêu:** chỉ có **log cảnh báo lặp lại** với stack trace. Không có hậu quả nào khác được nêu.

**Hậu quả có thể (suy luận từ mã, chưa quan sát):**
1. **Nhiễu log:** cảnh báo WARN kèm stack trace lặp lại, dễ che mất cảnh báo thật.
2. **Sao chép tạm dừng cho các bucket có leader đang restart:** về bản chất vì leader không phản hồi nên không có gì để sao chép; không phải lỗi của follower. Khi leader trở lại, fetch tiếp tục (tối đa trễ thêm vài giây vì chu kỳ 30 giây + 1 giây).
3. **Thời gian phục hồi sau restart có thể dài hơn cần thiết:** vì mỗi lần thử bị chặn tới 30 giây, nếu leader vừa sống lại giữa chu kỳ chờ thì follower chỉ phát hiện ở lần fetch kế tiếp (suy luận; mình chưa đo).
4. **Không có bằng chứng mất dữ liệu.** Mình chưa kiểm tra tương tác với ISR/ghi `acks` trong lúc này.

**Với `relate_issue.md`:** câu *"Độ trễ replication tăng khi restart node"* hợp lý về mặt cơ chế nhưng **không được issue xác nhận**. Nên đọc là "log cảnh báo lặp, có thể kèm trễ phục hồi sao chép sau restart".

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

- Trạng thái ❌ (không có PR) **đúng**: mình không thấy PR nào liên kết trong trang issue.
- Mô tả "Fetch log request timeout lặp lại khi một TabletServer restart" **đúng** với nội dung issue.
- "Độ trễ replication tăng" là suy luận, nên được ghi rõ là chưa xác nhận.
- Mình chưa xác định đây là **lỗi** hay **hành vi chấp nhận được** (WARN khi leader không phản hồi có thể là chủ ý). Điều đáng cải thiện là: thời hạn 30 giây không cấu hình được, request chặn luồng, cảnh báo kèm stack trace lặp lại.

## 5. Mức ảnh hưởng với CDP

- **Chỉ liên quan nếu Fluss chạy nhiều TabletServer có replication** (ví dụ 3 node trên Kubernetes) và bạn restart/nâng cấp node. Một node duy nhất không có follower thì không có luồng fetch.
- Ảnh hưởng chủ yếu là **log và thời gian phục hồi sau restart**, ít khả năng làm sai dữ liệu lookup hay ghi context cho AI agent. Tác động tới độ trễ ghi (nếu ghi phải chờ ISR) chưa được kiểm chứng.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Nâng cấp/restart từng node một và chờ node trước vào lại ISR đầy đủ** rồi mới sang node kế (cũng giúp #4256, #2892).
- **Đừng kỳ vọng đổi `log.replica.fetcher-number` hoặc `log.replica.fetch.wait-max-time` sẽ làm hết log.** Đổi các tham số này không đổi thời hạn 30 giây.
- **Hạ mức log hoặc lọc cảnh báo** `ReplicaFetcherThread` trong lúc bảo trì có kế hoạch, hoặc xem nó là bình thường nếu kèm `TimeoutException` đúng thời điểm restart. Cần phân biệt với lỗi kéo dài sau khi node đã lên lại.
- **Cảnh báo theo ISR/under-replicated và độ trễ replication** (nếu có metric) thay vì dựa vào chuỗi log này.
- **Diễn tập restart một node** ở môi trường thử, quan sát độ trễ ghi và thời gian ISR đầy đủ trở lại.

## 6. Những điều chưa kiểm chứng

- Điều gì khiến request fetch không có phản hồi (hay lỗi ngắt kết nối) khi node restart: chưa rõ, mình chưa tái hiện.
- Log trong issue do server nào in và thời điểm so với vòng đời restart: issue không nói.
- Ảnh hưởng thực tế lên độ trễ replication, ISR và ghi `acks`: chưa đo.
- Mình thấy ở `v1.0.0` dòng `leader.fetchLeaderEndOffsetSnapshot(tableBucket).get();` **không có timeout** trong cùng `ReplicaFetcherThread` (các lời gọi khác có `get(timeoutSeconds, ...)`). Mình chưa xem nó chạy khi nào và có thể treo luồng không; chỉ ghi lại là một điểm cần kiểm tra thêm, không thuộc issue này.
- Mình chưa kiểm tra cách Coordinator chuyển leadership trong lúc node restart (thời gian chuyển, điều kiện) nên chưa biết log kéo dài bao lâu trong thực tế.
