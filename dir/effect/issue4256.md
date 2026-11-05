# Issue #4256 — Rolling upgrade: client giữ kết nối tới endpoint cũ, endpoint đó đã thuộc về TabletServer khác

Link: https://github.com/apache/fluss/issues/4256 — Phiên bản được báo: `0.8.0`. PR sửa: [#4263](https://github.com/apache/fluss/pull/4263), **đã merge vào `main` ngày 2026-10-29**, nên **không có trong tag `v1.0.0`** (tách nhánh 2026-09-15, phát hành 2026-09-18).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Rolling upgrade** | Nâng cấp cluster từng node một: tắt một TabletServer, chạy lại bản mới, rồi sang node kế tiếp để cluster luôn phục vụ được. |
| **Server UID / server ID** | Tên logic cố định của một TabletServer (ví dụ `ts-3`). Giữ nguyên qua các lần restart. |
| **Endpoint** | Địa chỉ mạng thực tế `host:port` của server. Trên Kubernetes, khi pod restart thì thường đổi IP, nên endpoint của cùng một server UID có thể thay đổi. |
| **Tái sử dụng endpoint** | Địa chỉ `host:port` cũ của server A, sau khi A chuyển đi, được cấp cho server B. Trên Kubernetes/EKS, IP của pod cũ có thể được cấp cho pod mới. |
| **Metadata (phía client)** | "Bản đồ" cluster trong bộ nhớ của client: server nào (UID) ở endpoint nào, bucket nào do server nào làm leader. |
| **Kết nối được cache** | Client mở kết nối mạng (Netty) tới server rồi giữ lại để dùng lại cho các request sau. |
| **`NotLeaderOrFollowerException`** | Lỗi server trả về khi nó nhận một request cho bucket mà nó **không phải** leader hoặc follower của bucket đó, tức request đã đến nhầm server. |
| **`listOffsets`** | Một thao tác Admin hỏi offset đầu/cuối của các bucket; trong issue nó là ví dụ của request bị gửi nhầm. |
| **`NetworkException`** | Lỗi mạng khi không kết nối được. Thường làm client bỏ kết nối hỏng và làm mới metadata. |
| **ISR** | Tập replica được coi là theo kịp leader (xem [issue4269.md](issue4269.md)). |

## Tóm tắt

Client Fluss cache kết nối tới TabletServer **chỉ theo server UID**. Trong rolling upgrade, nếu server A đổi sang endpoint Y và endpoint cũ X lại được cấp cho server B, thì kết nối mà client đã mở tới X vẫn được dùng cho "server A", dù đầu bên kia thực ra là server B. Mọi request cho bucket do A làm leader đều đến nhầm B và nhận `NotLeaderOrFollowerException`. Vì X vẫn kết nối được, không có lỗi mạng nào để client bỏ kết nối; và kể cả metadata đã cập nhật `A -> Y`, việc tra kết nối vẫn trả lại kết nối cũ theo UID. Lỗi vì thế kéo dài.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-rpc` (tầng kết nối mạng của client) và cách `fluss-client` sử dụng nó khi gặp lỗi.
- **Điểm gây lỗi** (đã đọc tại tag `v1.0.0`): `NettyClient` giữ các kết nối trong `Map<String, ServerConnection> connections` mà **khóa là server UID** (chú thích trong mã: *"The key is the server uid (e.g., "cs-2", "ts-3")"*). `getOrCreateConnection(node)` dùng `connections.computeIfAbsent(serverId, ...)`, nên nếu UID đã có kết nối thì dùng lại, **không so xem host và port của `ServerNode` mới có khác không**.
- **Thành phần thứ hai** theo issue: khi nhận `NotLeaderOrFollowerException`, hàm gọi chỉ hoàn tất future bằng lỗi, **không ngắt kết nối đã cache và không buộc làm mới metadata**. Các lần thử lại ở phía người gọi lại dùng đúng kết nối đó.
- **Vì sao không có lỗi mạng làm "tự lành":** endpoint cũ vẫn kết nối được (vì đã thuộc server khác), nên không có `NetworkException` để làm client hủy kết nối. Đây là điểm khác với trường hợp đổi leader thoáng qua trong issue #3389.
- **Cách sửa đã merge (trên `main`, PR #4263):** khóa kết nối đổi từ `serverUid` thành **`serverUid + host + port`**. Các kết nối tới nhiều endpoint của cùng một UID có thể cùng tồn tại; mỗi request dùng endpoint trong `ServerNode` mà nó mang theo. `disconnect(serverUid)` đóng tất cả endpoint của UID đó; thêm `disconnect(ServerNode)` và `isReady(ServerNode)` theo endpoint; metric tách theo endpoint. Kết nối thừa được dọn bằng cơ chế idle của Netty sau `netty.connection.max-idle-time` (mặc định 10 phút). PR sửa 6 file, gồm `NettyClient`, `RpcClient`, `ServerConnection` và `MetadataUpdater`.
- **Hướng khác issue đề xuất nhưng PR không chọn:** issue đề xuất xử lý `NotLeaderOrFollowerException` bằng cách làm mất hiệu lực metadata, ngắt kết nối, làm mới và thử lại có giới hạn. Người sửa chọn sửa ở tầng truyền tải (khóa kết nối theo endpoint) và cố ý không thêm chính sách retry mới, vì nguyên nhân dai dẳng nằm ở việc cache che mất endpoint mới.

## 2. Khi nào lỗi xảy ra

Chuỗi sự kiện theo issue (minh họa, không cần tái hiện đúng IP):

| Bước | Việc xảy ra |
|---|---|
| 1 | Client cache metadata `server A -> endpoint X`. |
| 2 | Rolling upgrade: server A restart tại endpoint **Y**, endpoint **X** được tái sử dụng bởi server **B**. |
| 3 | Trước khi metadata của client được làm mới, client mở kết nối cho "server A" tới endpoint X. Kết nối được cache dưới UID của A nhưng đầu bên kia thực ra là B. |
| 4 | Metadata có thể được làm mới thành `A -> Y`, nhưng tra kết nối vẫn trả kết nối cũ vì khóa chỉ là UID. |
| 5 | Request cho bucket do A làm leader đến B, nhận `NotLeaderOrFollowerException`. |
| 6 | Lỗi không làm hủy kết nối, nên các lần thử lại vẫn lỗi. |

**Điều kiện cần:**
1. Cluster trải qua rolling upgrade hoặc restart làm **endpoint của server đổi** (điển hình trên Kubernetes khi IP pod đổi).
2. Một endpoint cũ được **tái sử dụng** cho server khác, hoặc cũ vẫn kết nối được. Một người dùng báo trong issue rằng trên EKS, IP của pod cũ được cấp cho pod khác sau khoảng 30 phút.
3. Client mở kết nối tới endpoint cũ **trước khi** metadata được làm mới.

**Phiên bản:** bản gốc báo trên `0.8.0`. Mình đã đọc `NettyClient` tại `v1.0.0` và thấy khóa vẫn chỉ là server UID, nên `1.0.0` còn lỗi này. PR #4263 chỉ có trên `main`.

**Một dữ liệu bổ sung từ người dùng trên 1.0.0 (chưa được trả lời dứt khoát trong issue):** sau khi một TabletServer restart với IP pod mới, các TabletServer khác (qua `ReplicaFetcherThread`) vẫn **mở kết nối mới** tới IP cũ trong khoảng 10 phút, với lỗi `Connection refused` và `connection timed out after 15000 ms`, làm các bucket của server vừa restart nằm ngoài ISR trong thời gian đó. Họ hỏi PR #4263 có bao phủ phía server hay chỉ phía client; maintainer chỉ trả lời rằng PR đang chờ merge. Mình chưa kiểm tra xem PR có sửa luồng này không (PR có sửa `NettyClient`, và mình chưa xác minh `ReplicaFetcherThread` có dùng cùng lớp đó hay không), nên không khẳng định.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:**
- Ứng dụng dùng Fluss client (Flink sink/source, lookup, ứng dụng tự viết, Admin) chạy **liên tục qua một đợt rolling upgrade** trên môi trường mà IP/endpoint server có thể đổi và tái sử dụng, như Kubernetes.
- Theo dữ liệu từ người dùng, cả giao tiếp **giữa các server** (replication fetcher) cũng có thể bị chậm hồi phục sau restart, làm ISR nhỏ lại.

**Hậu quả:**
1. **Request đến nhầm server và thất bại lặp lại** với `NotLeaderOrFollowerException`, trong khi cluster thực ra khỏe. Issue minh họa bằng `listOffsets`; mọi request đi qua kết nối được cache theo UID đó đều có nguy cơ tương tự (đây là suy luận của mình từ cách cache hoạt động, issue chỉ nêu ví dụ `listOffsets`).
2. **Không tự hồi phục** nếu không có lỗi mạng: client không bỏ kết nối sai, nên lỗi kéo dài cho đến khi client được khởi động lại hoặc kết nối hết hạn do idle.
3. **Nguy cơ về an toàn kết nối:** người dùng đề xuất kiểm tra server ID và cluster ID trong `ApiVersionsResponse` khi bắt tay, vì hiện chỉ kiểm loại server; mình chưa kiểm tra đề xuất này đã được xử lý chưa.
4. **Không có dữ liệu sai âm thầm được ghi nhận:** request nhầm server bị từ chối rõ ràng bằng exception, không phải ghi nhầm. Rủi ro là tính sẵn sàng (ghi, lookup, Admin bị lỗi kéo dài).

**Biện pháp giảm thiểu:**
- Dùng **hostname ổn định** (StatefulSet + headless service) thay vì IP pod thô làm endpoint quảng bá, để endpoint không đổi và không bị tái sử dụng. Đây là khuyến nghị đã có trong `relate_issue.md`, và issue #2076 nói chart Helm hiện quảng bá IP pod.
- Rolling upgrade từng node và theo dõi lỗi `NotLeaderOrFollowerException` ở client; nếu lỗi kéo dài thì **restart client**.
- Chờ bản có #4263 (1.0.1 hoặc sau) hoặc tự build 1.0.0 kèm patch #4263. Trước khi cherry-pick, nên kiểm tra PR có phụ thuộc thay đổi khác trên `main` không (mình chưa kiểm tra).
