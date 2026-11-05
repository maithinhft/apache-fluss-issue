# Issue #4043 — Lookup: một bucket gặp sự cố có thể chiếm hết "permit" của cả client, chặn lookup tới các bucket khỏe

Link: https://github.com/apache/fluss/issues/4043 — Phiên bản được báo: `0.9.0`. Tiêu đề trên GitHub: *"[Client] Add client.lookup.max-inflight-requests-per-bucket to restrict lookup requests per server"*, tức là issue **đề xuất thêm một cấu hình mới** (giới hạn theo bucket). PR đề xuất: [#4044](https://github.com/apache/fluss/pull/4044), **chưa merge**, không có bình luận trong issue. Người báo nhận làm PR.

> **Mức chắc chắn:** đây là một **đề xuất cải tiến dựa trên phân tích thiết kế**, không kèm log hay bản tái hiện sự cố thật. Mình đã đọc mã `LookupSender` và `LookupQueue` tại `v1.0.0` và xác nhận cơ chế giới hạn chung đúng như mô tả. Mình **chưa chạy tái hiện**, và chưa đo xem trong thực tế permit bị chiếm đến mức nào.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Lookup** | Tra cứu một dòng của bảng khóa chính (PK) theo khóa, bằng RPC tới TabletServer đang giữ bucket chứa khóa đó. |
| **Bucket** | Mỗi bảng chia thành nhiều bucket; mỗi bucket có một **leader** (một TabletServer) phục vụ đọc/ghi. Một khóa thuộc về đúng một bucket. |
| **Leader failover** | Khi TabletServer giữ leader của bucket gặp sự cố (hoặc đang được nâng cấp), một replica khác được bầu làm leader mới. Trong khoảng đó, leader cũ có thể không phản hồi. |
| **Batch lookup** | Client gom nhiều lookup đi cùng một server thành một RPC (tối đa `client.lookup.max-batch-size`, mặc định 128). |
| **RPC in-flight (đang bay)** | Request đã gửi đi nhưng chưa nhận phản hồi. |
| **Semaphore / permit** | Bộ đếm giới hạn đồng thời: muốn gửi một RPC phải lấy một permit, nhận phản hồi (hoặc lỗi) xong mới trả. Hết permit thì người muốn gửi phải **chờ**. |
| **`client.lookup.max-inflight-requests`** | Số RPC lookup chưa được trả lời tối đa **trên toàn client**. Mặc định `128` (đã đọc trong `ConfigOptions` của `v1.0.0`). |
| **`client.lookup.max-retries`** | Số lần thử lại khi lỗi tạm thời. Mặc định `Integer.MAX_VALUE`, tức là gần như thử lại không giới hạn. |
| **Circuit breaker (cầu dao)** | Cơ chế tự ngắt gửi tới một đích đang lỗi trong một lúc để không làm nghẽn phần còn lại. Issue nói rõ đề xuất của họ **không phải** circuit breaker theo node. |
| **Head-of-line blocking** | Hàng đợi bị chặn bởi phần tử đầu hàng khiến phần tử phía sau (dù khỏe) không đi được. |

## Tóm tắt

Client Fluss giới hạn số RPC lookup đang bay bằng **một semaphore chung cho cả client** (mặc định 128 permit). Khi một bucket đang failover và server cũ không phản hồi, các RPC tới bucket đó **giữ permit** cho đến khi nhận lỗi hoặc hết thời gian. Nếu có nhiều lookup tới đúng bucket đó, chúng có thể chiếm gần hết hoặc hết permit. Lúc ấy `LookupSender` **bị chặn khi muốn gửi thêm**, và các lookup tới bucket khỏe (cả trên server khỏe) cũng không gửi đi được. Một sự cố cục bộ ở một bucket trở thành tăng độ trễ hoặc timeout cho toàn bộ lookup của client đó.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, phần lookup: `LookupSender`, `LookupQueue`, `LookupClient`.
- **Cơ chế (đã đọc tại `v1.0.0`):**
  - `LookupSender` có `Semaphore maxInFlightReuqestsSemaphore = new Semaphore(maxFlightRequests)`, với `maxFlightRequests` lấy từ `client.lookup.max-inflight-requests`.
  - `sendLookupRequestAndHandleResponse` gọi `maxInFlightReuqestsSemaphore.acquire()` **trước khi gửi**, rồi chỉ `release()` trong `finally` khi có phản hồi hoặc lỗi. Hàm `acquire()` là chặn (blocking). Cũng áp dụng cho prefix lookup.
  - Comment trong mã: *"Normal and historical lookups share the in-flight permits for simplicity"* (issue #3766), cho thấy các loại lookup dùng chung permit.
  - `LookupSender.run()` chạy **một luồng** duy nhất, nên khi `acquire()` chặn thì cả luồng gửi đứng chờ, không chỉ yêu cầu đang gửi.
- **Điều kiện permit "kẹt":** permit chỉ trả khi RPC có phản hồi hoặc lỗi. Nếu server cũ không đáp và kết nối chưa báo hỏng, permit bị giữ đến khi lỗi/timeout xảy ra. Mình **chưa xác nhận** cơ chế timeout chính xác của một RPC lookup phía client (có tham số `client.request.timeout` được truyền vào request ở `LookupClient`, nhưng mình chưa lần theo nó để biết thời gian tối đa giữ permit).
- **Gom nhóm theo server nhưng giới hạn theo client:** `LookupSender.groupByLeaderAndType` nhóm lookup theo leader, nhưng semaphore không phân biệt leader hay bucket. `LookupQueue` hiện chỉ gom theo `max-batch-size` và `batch-timeout`, không có hạn mức theo bucket.
- **Hướng sửa theo issue và PR #4044:**
  - Thêm cấu hình `client.lookup.max-inflight-requests-per-bucket`. Khi một bucket đạt hạn mức, lookup tiếp theo của bucket đó **ở lại trong hàng đợi** còn bucket khác vẫn được gửi.
  - PR đổi `LookupQueue`, `LookupSender`, thêm `LookupQueueKey` và `ConfigOptions`, cộng thêm "drain request từ bucket ưu tiên cùng nhau".
  - Issue nói rõ giới hạn của hướng này: nhiều bucket cùng nằm trên một server hỏng vẫn có thể cùng chiếm permit chung, nên đây **không phải** circuit breaker theo node.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Có sự kiện làm một số bucket tạm thời không phản hồi:** leader failover, rolling upgrade, TabletServer chết hoặc quá tải, lỗi mạng một chiều.
2. **Nhiều lookup đang hướng tới đúng các bucket đó** cùng lúc, đủ để chiếm gần hết 128 permit (mặc định). Khi tải lookup thấp, chưa chắc chạm ngưỡng.
3. **Các RPC tới bucket lỗi không trả về nhanh** (không có lỗi mạng tức thời), nên permit bị giữ lâu.
4. Có lookup tới bucket khác **cùng client** cần gửi trong thời gian đó.

**Triệu chứng dự kiến (suy luận từ mã, chưa quan sát):** độ trễ lookup tăng cho mọi khóa, dù chỉ một vài bucket gặp sự cố; timeout hàng loạt phía ứng dụng; `LookupSender` đứng im trên `Semaphore.acquire`.

**Phiên bản:** báo trên `0.9.0`. Mình đọc `LookupSender` `v1.0.0` và thấy cùng cấu trúc (một semaphore chung, không có giới hạn theo bucket/server), nên `1.0.0` còn hành vi này.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:**
- Mọi ứng dụng dùng Fluss client để **lookup** bảng PK: Flink lookup join, AI agent đọc context, dịch vụ tự viết. Một `FlussConnection` dùng chung một `LookupClient` nên chia sẻ cùng semaphore.
- Nặng nhất khi tải lookup đồng thời cao và có sự kiện failover hoặc rolling upgrade.

**Hậu quả:**
1. **Lỗi lan rộng:** chỉ vài bucket có vấn đề nhưng độ trễ/timeout xuất hiện ở mọi lookup của client (nhiễu loạn chéo).
2. **Không có dữ liệu sai:** đây là vấn đề sẵn sàng và độ trễ, không phải sai lệch dữ liệu. Lookup hoặc thành công muộn hoặc timeout.
3. **Cả luồng gửi bị chặn:** vì chỉ có một luồng `LookupSender`, khi nó chờ permit thì mọi hàng đợi phía sau cũng đợi.
4. **Hồi phục phụ thuộc thời gian chờ lỗi:** với `max-retries` mặc định gần như vô hạn, lookup lỗi sẽ được thử lại và có thể quay lại cạnh tranh permit trong lúc failover. Đây là suy luận của mình từ giá trị mặc định, chưa quan sát thực tế.

**Mức ảnh hưởng với CDP:** liên quan trực tiếp tới luồng **lookup cho AI agent** và Flink lookup join. Rủi ro chính xảy ra lúc rolling upgrade hoặc failover khi tải lookup cao: agent bị chậm hoặc timeout dù đa số bucket khỏe.

**Biện pháp giảm thiểu (suy ra từ phân tích, không phải từ issue):**
- **Đặt timeout ở tầng ứng dụng/agent** cho mỗi lookup, kèm retry có giới hạn và circuit breaker riêng (đúng như `relate_issue.md` khuyến nghị). Điều này ngăn việc agent chờ vô hạn.
- **Tăng `client.lookup.max-inflight-requests`** (mặc định 128) để cần nhiều bucket lỗi mới chiếm hết permit. Đổi lại sẽ nhiều request dồn lên server hơn khi hệ thống đang yếu; cần thử tải.
- **Giảm `client.lookup.max-retries`** từ giá trị gần vô hạn xuống con số hữu hạn, để lookup lỗi không tiếp tục quay lại chiếm permit. Cần thử vì có thể làm lookup thất bại sớm hơn trong failover ngắn.
- **Tách client theo nhóm bảng** (ví dụ mỗi nhóm một `FlussConnection`) nếu các bảng có mức quan trọng khác nhau, vì semaphore là theo client. Đây là ý tưởng của mình, chưa kiểm chứng.
- **Rolling upgrade ngoài giờ cao điểm**, từng node, chờ ISR đầy đủ.
- **Giám sát** độ trễ lookup phía client, số lookup đang chờ trong hàng đợi và theo dõi PR #4044.
