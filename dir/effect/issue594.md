# Issue #594 — Một TabletServer sập rồi lên lại thì các server nhận "cơn bão" `FetchLogRequest`

Link: https://github.com/apache/fluss/issues/594 — Phiên bản được báo: `main` thời 0.6 (người báo: swuferhong, 13/03/2025, repo cũ `alibaba/fluss`). Issue **rất ngắn**: một câu mô tả và một ảnh chụp biểu đồ số `FetchLogRequest` tăng vọt (ảnh, không đọc được dạng chữ). Có đúng một bình luận của chính người báo: *"khi tắt idempotence của job ghi (`'client.writer.enable-idempotence'='false'`) thì hiện tượng không xảy ra"*. Không có log, không có phân tích, không có PR. Trạng thái: còn mở. Cùng người, cùng ngày, người này báo cả #596 (log bị cắt về 0).

> **Mức chắc chắn:** issue không nêu nguyên nhân. Mình đã đọc mã `v1.0.0` và tìm được **một cơ chế cụ thể** khớp với cả hai dữ kiện ("bão fetch" và "chỉ xảy ra khi bật idempotence"). Một issue khác đã đóng, [#1624](https://github.com/apache/fluss/issues/1624), mô tả đúng hiện tượng này với một nguyên nhân cụ thể đã được sửa ([#1639](https://github.com/apache/fluss/pull/1639), có trong 1.0.0). Cơ chế khuếch đại (retry không có thời gian chờ) **vẫn còn** trong `1.0.0`. Mình **chưa tái hiện** và **không chứng minh được** đây đúng là nguyên nhân của #594.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Replica / leader / follower** | Mỗi bucket có nhiều bản sao (replica) trên các TabletServer khác nhau. Một bản là **leader** (nhận ghi và phục vụ đọc), các bản còn lại là **follower** (sao chép từ leader để dự phòng). |
| **`ReplicaFetcherThread`** | Luồng trên TabletServer đóng vai follower. Nó gửi `FetchLogRequest` tới **một** TabletServer leader cụ thể cho mọi bucket mà nó theo dõi từ leader đó. |
| **`FetchLogRequest`** | Yêu cầu "cho tôi các bản ghi mới từ offset X cho các bucket này". Follower dùng nó để sao chép, client dùng nó để đọc log. |
| **Idempotence (ghi lũy đẳng)** | Cơ chế chống ghi trùng khi client gửi lại. Mỗi writer có một **writer ID**, và mỗi batch gửi cho một bucket mang **batch sequence** tăng dần (0, 1, 2…). Server nhớ sequence cuối của từng writer: batch trùng thì bỏ qua, batch "nhảy cóc" thì báo lỗi. **Bật mặc định** (`client.writer.enable-idempotence = true`). |
| **Writer state** | Thông tin idempotence mà server lưu: với mỗi writer ID, batch sequence cuối và offset của nó. **Mỗi replica, kể cả follower,** tự dựng writer state riêng từ các batch nó đã ghi. |
| **`OutOfOrderSequenceException` / `DuplicateSequenceException`** | Lỗi khi batch đến không phải "sequence cuối + 1", hoặc khi batch có sequence đã thấy rồi. |
| **Writer ID hết hạn** | Writer không ghi gì trong `server.writer-id.expiration-time` (mặc định 12 giờ) thì writer state của nó bị xóa. Việc dọn chạy mỗi `server.writer-id.expiration-check-interval` (10 phút), **độc lập trên từng server**. |
| **`leaderEndOffsetSnapshot`** | Log end offset của leader **tại thời điểm nó trở thành leader**. Follower dùng nó làm điểm cắt an toàn khi phát hiện log lệch. |
| **Truncate (cắt log)** | Follower xóa phần cuối log của mình về một offset cũ hơn để khớp với leader. |
| **Backoff** | Thời gian chờ trước khi thử lại. Với fetcher là `log.replica.fetch.backoff-interval` (mặc định 1 giây). |
| **ISR (in-sync replicas)** | Tập các replica đang bắt kịp leader. Follower tụt lại quá `log.replica.max-lag-time` (30 giây) sẽ bị loại khỏi ISR. |

## Tóm tắt

Follower **có kiểm tra batch sequence** khi ghi dữ liệu sao chép từ leader. Nếu writer state trên follower **lệch** so với dữ liệu leader gửi về, follower ném `OutOfOrderSequenceException` hoặc `DuplicateSequenceException`. Lệch có thể do follower vừa phục hồi sau sự cố và dựng lại writer state sai, hoặc do writer ID hết hạn ở hai server vào hai thời điểm khác nhau.

Trong mã `v1.0.0`, nhánh xử lý hai lỗi này **không hoãn bucket lại**. Follower chỉ thử cắt log về `leaderEndOffsetSnapshot`, và **nếu điều kiện cắt không thỏa thì không làm gì cả**. Vòng fetch kế tiếp gửi lại **ngay lập tức** đúng offset cũ, nhận đúng dữ liệu cũ, gặp đúng lỗi cũ. Đây là một vòng lặp **không có thời gian chờ**, tạo ra lượng `FetchLogRequest` và băng thông vào rất lớn.

Kiểm tra sequence chỉ áp dụng cho batch **có writer ID**, tức khi bật idempotence. Điều này khớp với bình luận *"tắt idempotence thì hết"*. Issue #1624 (09/2025, đã đóng) mô tả đúng hiện tượng: *"The network inbound will keep high ... because the fetcher thread keeps retrying to fetch data"*.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `replica/fetcher/ReplicaFetcherThread`, `log/LogTablet` (`appendAsFollower`, `analyzeAndValidateWriterState`) và `log/WriterAppendInfo` (`maybeValidateDataBatch`, `inSequence`).
- **Đã xác nhận trong mã `v1.0.0`: follower có kiểm tra sequence, nhưng chỉ với batch có writer ID.**
  - `appendAsFollower` gọi `append(records, false)`. Hàm này gọi `analyzeAndValidateWriterState(validRecords, appendAsLeader)` cho cả leader lẫn follower.
  - Trong `analyzeAndValidateWriterState`, việc kiểm tra chỉ chạy khi `batch.hasWriterId()`. Batch ghi khi tắt idempotence **không có writer ID**, nên không bao giờ bị kiểm tra.
  - Kiểm tra dùng `SequenceValidation.ENFORCE`. `maybeValidateDataBatch` ném `OutOfOrderSequenceException("Out of order batch sequence for writer ...")` khi `inSequence(...)` sai.
  - `inSequence` trong 1.0.0:
    ```java
    return (lastBatchSeq == NO_BATCH_SEQUENCE && (isWriterInBatchExpired || !isAppendAsLeader))
            || nextBatchSeq == lastBatchSeq + 1L
            || (nextBatchSeq == 0 && lastBatchSeq == Integer.MAX_VALUE);
    ```
    Với follower (`!isAppendAsLeader`): nếu follower **không có** writer state của writer đó thì **mọi** sequence đều được chấp nhận. Đây là phần sửa của #1639 cho trường hợp writer hết hạn trên follower trước. Nhưng nếu follower **có** writer state mà sequence cuối không khớp với batch leader gửi, lỗi vẫn bị ném.
- **Đã xác nhận trong mã `v1.0.0`: nhánh xử lý lỗi sequence không hoãn bucket.**
  - Khi ghi dữ liệu fetch về mà gặp `DuplicateSequenceException`, `OutOfOrderSequenceException` hoặc `InvalidOffsetException`, `ReplicaFetcherThread` log ERROR `"Founding recoverable error while processing data for bucket {} at offset {}, try to truncate to LeaderEndOffsetSnapshot"` rồi gọi `truncateToLeaderEndOffsetSnapshot(...)`. Ngay trên đó là comment `TODO this part of logic need to be removed after we introduce leader epoch cache. Trace by #673`.
  - `truncateToLeaderEndOffsetSnapshot` chỉ cắt khi `leaderEndOffsetSnapshot != 0L && leaderEndOffsetSnapshot < localLogEndOffset`. Nếu điều kiện **không** thỏa (follower không có dữ liệu "thừa" so với leader, chỉ có writer state lệch), hàm **không làm gì**. Offset fetch giữ nguyên, bucket **không** được thêm vào `bucketsWithError`.
  - Chỉ các bucket trong `bucketsWithError` mới bị `handleBucketWithError` → `delayBuckets(..., fetchBackOffMs)` hoãn 1 giây. Bucket gặp lỗi sequence **không bị hoãn**, nên vòng `doWork()` kế tiếp gửi lại ngay.
  - Phía leader, yêu cầu fetch mà đã có dữ liệu sẵn sẽ được trả lời **ngay**, không chờ `log.replica.fetch.wait-max-time`. Vì vậy mỗi vòng chỉ tốn một lượt mạng cộng một lần đọc đĩa, và vòng lặp chạy hết tốc độ. Mỗi vòng còn log một dòng ERROR kèm stack trace.
- **Một nguồn "bão fetch" khác đã được sửa:** [#2075](https://github.com/apache/fluss/pull/2075) (02/12/2025, có trong 0.9.0 và 1.0.0), tên *"Avoid ReplicaFetcher busy loop retry storm during leader election or bucket migration"*.
  - Trước #2075, nếu leader cũ trả `NOT_LEADER_OR_FOLLOWER` (thường gặp khi một TabletServer sập và leadership đang được chuyển), fetcher chỉ log debug và **không hoãn bucket**, nên cũng lặp liên tục.
  - #2075 thêm `replicasWithError.add(tableBucket)` cho nhánh này. Mình đã đọc diff, và trong `v1.0.0` nhánh `NOT_LEADER_OR_FOLLOWER` đã có dòng này.
  - Vì vậy, nếu #594 thực ra do đổi leader chứ không do sequence, thì đường đó **đã được sửa**. Tuy nhiên, giả thuyết này không giải thích được chi tiết "tắt idempotence thì hết" trong bình luận của người báo.
- **Thêm một điểm (đã đọc mã):** `truncateToLeaderEndOffsetSnapshot` gọi `leader.fetchLeaderEndOffsetSnapshot(tableBucket).get()` **không có timeout**, trong khi các lời gọi khác trong lớp đều dùng `get(timeoutSeconds, ...)`. Nếu leader không trả lời, luồng fetcher có thể bị chặn tại đây. Mình chưa thử.
- **Vì sao hay xảy ra "khi một TabletServer sập rồi lên lại" (suy luận, ghép từ mã và các issue cùng thời kỳ):** khi phục hồi, follower **dựng lại writer state** từ snapshot writer state và các segment trên đĩa. Bước này đã từng sai nhiều lần:
  - [#705](https://github.com/apache/fluss/issues/705): snapshot writer state không được flush khi tắt.
  - [#704](https://github.com/apache/fluss/issues/704): sau unclean shutdown, batch bị coi nhầm là trùng.
  - [#1386](https://github.com/apache/fluss/pull/1386): recovery lỗi do writer ID hết hạn.
  - [#2188](https://github.com/apache/fluss/pull/2188): kiểm tra sequence ngay lúc dựng lại writer state.

  Writer state sai thì follower gặp lỗi sequence ngay khi bắt đầu fetch lại, và rơi vào vòng lặp ở trên.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Job ghi bật idempotence** (mặc định).
2. **Cluster có replication** (replication factor ≥ 2), để có follower đang fetch.
3. **Writer state trên follower lệch với log của leader** cho ít nhất một writer. Ví dụ:
   - TabletServer **sập hoặc restart** và dựng lại writer state sai (#704, #705, #2188). Đây đúng là bối cảnh của #594.
   - Writer ID hết hạn ở hai server **lệch thời điểm** (#1624). Trường hợp follower hết hạn trước đã được #1639 sửa. Trường hợp leader hết hạn trước thì mình chưa phân tích.
   - Log của follower và leader **phân kỳ** sau khi đổi leader (#4269, chưa sửa trong 1.0.0).
4. **Điều kiện cắt log không thỏa**, nên vòng lặp không tự thoát được.

**Khi nào vòng lặp dừng:**
- Khi writer state được "sửa", ví dụ writer hết hạn trên **cả hai** phía. #1624 viết: *"the fetcher ... will recover after 12 hours"*.
- Khi leader đổi.
- Khi restart follower.

**Phiên bản:** báo trên `main` thời 0.6. Cơ chế "không backoff" vẫn còn ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster nhiều TabletServer có ghi idempotent (tức hầu hết mọi người), sau sự cố hoặc restart node.

**Hậu quả:**
1. **Lưu lượng mạng và CPU tăng cao** ở cả leader (phục vụ fetch liên tục) và follower (gửi và xử lý liên tục). Đây là điều #594 và #1624 cùng quan sát. Dữ liệu còn bị leader đọc lại từ đĩa mỗi vòng (suy luận).
2. **Bucket chỉ còn một replica khỏe.** Follower không tiến được nên bị loại khỏi ISR. #1624 viết rõ: *"The problematic bucket will only exist in one online replica. Which is not high available."* Nếu leader đó chết tiếp, bucket không phục vụ được.
3. **Log ERROR tràn ngập:** mỗi vòng một dòng kèm stack trace. Có thể làm đầy đĩa log hoặc che lỗi khác.
4. **Bucket khác trên cùng luồng fetcher:** một luồng phục vụ nhiều bucket tới cùng leader (chọn theo `hashCode % log.replica.fetcher-number`). Vòng lặp nhanh khiến request của chúng cũng bị gửi liên tục, nên tốn tài nguyên, dù chúng vẫn tiến được (suy luận).
5. **Không có bằng chứng sai dữ liệu đã commit.** Client vẫn ghi vào leader bình thường, và `acks=-1` không chờ follower đã rời ISR.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #594 trong `relate_issue.md` ghi: *"Bão `FetchLogRequest` khi một TabletServer down rồi phục hồi | main (0.6) | ❔ | Như #2749"*. Đối chiếu:
- **"Như #2749" chưa đúng.** #2749 là chuyện fetch **chờ hết 30 giây rồi timeout** (request ít, chậm). #594 theo phân tích trên là chuyện fetch **lặp lại không chờ** (request rất nhiều). Hai cơ chế khác nhau.
- **❔ vẫn hợp lý**, nhưng nên ghi rõ:
  - Các nguyên nhân cụ thể đã biết đều đã được sửa trong 1.0.0: [#731](https://github.com/apache/fluss/pull/731) (sửa #704), [#732](https://github.com/apache/fluss/pull/732) (sửa #705), #1386, #1639 (sửa #1624), #2188, #4013, và #2075 (backoff khi leader trả `NOT_LEADER_OR_FOLLOWER`).
  - Cơ chế khuếch đại (không backoff) cùng việc chưa có leader epoch cache (#673) **vẫn còn**.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu Fluss chạy **≥ 3 TabletServer có replication** và có restart hoặc sự cố node, tức gần như chắc chắn trong production.
- Ảnh hưởng chủ yếu là **tải hệ thống và mất dự phòng (HA)** cho bucket bị dính. Ít khả năng làm sai dữ liệu lookup của AI agent, nhưng bucket chỉ còn một replica là rủi ro chờ sự cố tiếp theo.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Không tắt idempotence** để né lỗi. Tắt thì khi writer gửi lại có thể ghi trùng, đặc biệt với bảng log.
- **Alert theo log ERROR** `Founding recoverable error while processing data for bucket ... try to truncate to LeaderEndOffsetSnapshot` trên TabletServer. Dòng này lặp liên tục cho cùng một bucket là dấu hiệu của vòng lặp.
- **Alert khi bucket under-replicated** (ISR nhỏ hơn số replica) kéo dài sau khi node đã lên lại.
- **Theo dõi băng thông vào và số request fetch** của TabletServer sau mỗi lần restart node.
- **Cách thoát vòng lặp:** restart TabletServer follower bị dính để nó dựng lại writer state. Nếu vẫn lặp, có thể phải xóa replica local để nó sao chép lại từ đầu. Đây là suy luận, cần thử ở môi trường test.
- **Tắt node có trật tự** (graceful) thay vì kill, để snapshot writer state được ghi đầy đủ (liên quan #705).
- **Diễn tập:** kill -9 một TabletServer khi đang ghi idempotent, rồi theo dõi số request fetch, log ERROR và ISR sau khi nó lên lại.

## 6. Những điều chưa kiểm chứng

- Nguyên nhân thật của #594: issue không có log, ảnh không đọc được dạng chữ.
- Mình chưa tái hiện vòng lặp trên 1.0.0 và chưa đo tốc độ request trong vòng lặp.
- Mình chưa phân tích trường hợp leader hết hạn writer ID **trước** follower.
- Mình chưa kiểm tra đường fetch từ **remote storage** (`processFetchResultFromRemoteStorage`) có cùng hành vi khi gặp lỗi sequence hay không.
- Mình chưa kiểm tra lời gọi `fetchLeaderEndOffsetSnapshot(...).get()` không có timeout có chặn được fetcher trong thực tế hay không.
