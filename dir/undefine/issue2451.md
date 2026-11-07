# Issue #2451 — "Invalid timeout for ReplicaFetcherThread": TabletServer restart xong vẫn kẹt ở trạng thái khởi tạo

Link: https://github.com/apache/fluss/issues/2451 — Phiên bản được báo: `0.8.0`. Issue **chưa đóng, không có PR, không có kết luận về nguyên nhân gốc**. Có 2 người trao đổi: người báo (`zuston`) và maintainer (`wuchong`). Người báo không trả lời lại yêu cầu thread dump bổ sung.

> **Cách đọc file này:** hai bên đưa ra hai nhận định trái nhau (fetcher kẹt, hay `loadLogs` chậm). Mình đã đối chiếu từng nhận định với mã nguồn `v0.8.0-incubating` (bản được báo) và `v1.0.0`. Kết luận ở mục 4. Mình **không tái hiện** sự cố và **không có** dữ liệu runtime nào ngoài một ảnh jstack trong issue.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **TabletServer** | Tiến trình lưu dữ liệu của Fluss. Mỗi bucket có một replica nằm trên vài TabletServer; một replica là leader, các replica khác là follower. |
| **Leader / follower** | Leader nhận ghi từ client; follower liên tục kéo (fetch) dữ liệu mới từ leader để theo kịp. |
| **`ReplicaFetcherThread`** | Luồng trên TabletServer **đóng vai follower**, gửi request fetch tới leader rồi ghi dữ liệu nhận được vào log local. Tên luồng `ReplicaFetcherThread-0-1` nghĩa là fetcher số 0, kéo từ leader là server 1 (suy ra từ cách đặt tên, mình không kiểm tra lớp tạo tên). |
| **`loadLogs` / log recovery** | Giai đoạn khởi động: TabletServer nạp lại log của mọi bucket từ đĩa. Nếu lần tắt trước **không sạch** (xem dưới), nó phải quét lại các segment để kiểm tra, rất tốn thời gian. |
| **Tắt sạch / không sạch** | Khi tắt gọn, `LogManager` tạo file `.fluss_cleanshutdown` trong thư mục dữ liệu. Lần khởi động sau thấy file này thì **bỏ qua** việc quét lại; không thấy (do kill -9, OOM, mất điện, pod bị giết giữa chừng…) thì phải quét. Không phải lỗi phần cứng. |
| **`TIMED_WAITING` trong jstack** | Trạng thái luồng đang chờ có thời hạn (ví dụ `Future.get(30s)`). Đây là trạng thái **bình thường** của luồng đang đợi phản hồi, không tự nó chứng minh luồng bị treo. |
| **`elapsed` trong jstack** | Thời gian từ lúc **luồng được tạo**, không phải thời gian luồng đứng ở dòng hiện tại. |
| **`becomeLeaderOrFollower`** | RPC mà Coordinator gửi cho TabletServer để giao cho nó vai trò leader hoặc follower của các bucket. |

## Tóm tắt

Người báo restart một TabletServer, thấy nó "vẫn ở trạng thái khởi tạo" và trong jstack có `ReplicaFetcherThread-0-1` đứng ở `CompletableFuture.get` (có timeout) tại `ReplicaFetcherThread.processFetchLogRequest`. Họ cho rằng timeout 30 giây "không có tác dụng" và fetcher đang treo. Maintainer trả lời rằng fetcher chạy luồng riêng nên khó chặn quá trình khởi tạo, và theo kinh nghiệm thì server thường kẹt lâu ở giai đoạn `loadLogs` sau tắt không sạch.

Sau khi đối chiếu mã: **maintainer đúng về cấu trúc** (fetcher không thể chặn khởi tạo), nhưng **giả thuyết `loadLogs` không khớp với chính jstack trong issue**; **người báo chưa chứng minh được** timeout vô tác dụng. Chưa ai tìm ra nguyên nhân thật.

## 1. Lỗi nằm ở thành phần nào

Chưa xác định. Các thành phần liên quan đã đọc:

- `ReplicaFetcherThread.processFetchLogRequest` (`fluss-server`). Ở `v0.8.0-incubating` dòng 227: `responseData = leader.fetchLog(fetchLogContext).get(timeoutSeconds, TimeUnit.SECONDS)` với `timeoutSeconds = 30`. Ở `v1.0.0`: vẫn `fetchFuture.get(timeoutSeconds, TimeUnit.SECONDS)`, mặc định 30, và bổ sung xử lý để giải phóng buffer khi phản hồi đến muộn. **Timeout có tồn tại ở cả hai bản.**
- `TabletServer.startServices` (`fluss-server`). Thứ tự khởi động **giống nhau ở `0.8.0` và `1.0.0`**:
  1. `logManager.startup()` (nạp log, chạy đồng bộ),
  2. `kvManager.startup()`,
  3. `replicaManager.startup()`,
  4. `rpcServer.start()`,
  5. `registerTabletServer()` (đăng ký lên ZooKeeper).
- `ReplicaManager.becomeLeaderOrFollower` → `makeFollowers` → `replicaFetcherManager.addFetcherForBuckets(...)`. Đây là nơi duy nhất mình thấy tạo fetcher cho bucket (đọc `v1.0.0`).

## 2. Khi nào hiện tượng xảy ra

Theo issue: sau khi restart một TabletServer. Người báo viết rằng trước khi restart, máy này là leader của bảng. Họ thấy trong log và thread dump không có "relevant connection attempts or stack traces". Mình không biết "trạng thái khởi tạo" mà họ nhìn thấy cụ thể là gì (log, UI hay trạng thái bucket); issue không nói.

## 3. Kiểm chứng từng nhận định

### 3.1. Nhận định của maintainer: "fetcher chạy luồng riêng nên không chặn khởi tạo"

**Đúng về mặt mã nguồn.** Lý do (đã đọc `0.8.0` và `1.0.0`):
- Fetcher chỉ được tạo từ `makeFollowers`, mà `makeFollowers` chỉ chạy khi nhận `becomeLeaderOrFollower` từ Coordinator.
- RPC server chỉ bắt đầu nhận request ở bước 4 và đăng ký lên ZooKeeper ở bước 5, đều **sau** `logManager.startup()` (nạp log chạy đồng bộ).
- Vì vậy khi còn trong `loadLogs`, **chưa thể tồn tại `ReplicaFetcherThread` nào**; và `ReplicaFetcherThread` không nằm trên đường đi của `startServices`. Fetcher không thể làm chậm bước khởi động.

### 3.2. Nhận định của maintainer: "thường kẹt ở `loadLogs` khi tắt không sạch"

- **Cơ chế có thật (đã đọc):** thiếu `.fluss_cleanshutdown` thì `LogLoader.recoverLog` quét lại segment (code kiểm tra `isCleanShutdown`), và `loadLogs` chạy đồng bộ trước khi server nhận RPC. Đây là nguyên nhân hợp lý cho việc khởi động chậm nói chung (sau crash).
- **Nhưng không khớp với jstack trong issue này.** Trong jstack, `ReplicaFetcherThread-0-1` có `elapsed=3682.24s`, nghĩa là luồng đã được tạo khoảng **61 phút** trước lúc dump. Vì fetcher chỉ được tạo sau `becomeLeaderOrFollower`, tức là sau khi `loadLogs` xong, RPC server đã chạy và Coordinator đã liên lạc, nên **ít nhất trong ~1 giờ đó, `loadLogs` không phải thứ đang chặn** (cho dù có thể từng chậm trước đó). Đây là suy luận từ mã và một con số trong jstack; mình không có thêm log.
- Maintainer cũng không khẳng định đây là trường hợp của người báo, mà chỉ nói kinh nghiệm chung và xin thêm thread dump để xác định.

### 3.3. Nhận định của người báo: "timeout 30 giây không có tác dụng"

**Không có bằng chứng trong issue.**
- Mã có timeout 30s ở cả hai bản (mục 1).
- Jstack chụp ra `TIMED_WAITING (parking)` tại `CompletableFuture.timedGet`. Đó đúng là trạng thái của một lần `get(30, SECONDS)` đang chờ. **Một ảnh chụp duy nhất không cho biết luồng đã đứng đó bao lâu**, vì `elapsed` là tuổi của luồng chứ không phải thời gian ở dòng đó. Luồng có thể vừa hết timeout, thử lại, rồi đứng ở cùng dòng, vẫn là hành vi đúng.
- Điểm chưa giải thích được: nếu mỗi lần chờ đều timeout thì `processFetchLogRequest` sẽ ghi cảnh báo `Error in response from leader server ...` mỗi 30 giây. Người báo nói không thấy log/ stack trace liên quan. Hoặc là họ không tìm thấy vì lọc sai, hoặc phản hồi đến đều đặn (không timeout), hoặc nguyên nhân khác. **Chưa kết luận được.**
- Tên luồng cho biết fetcher này kéo từ **server 1**. Người báo nói họ xem log và thread dump "trên server-1". Nếu server 1 chính là máy vừa restart thì có thể họ đang nhìn nhầm máy; mình không xác định được từ issue.

## 4. Kết luận cuối cùng

| Câu hỏi | Kết luận | Mức chắc chắn |
|---|---|---|
| `ReplicaFetcherThread` có thể chặn khởi tạo TabletServer không? | **Không**, theo thứ tự khởi động và nơi tạo fetcher. | Cao (đã đọc mã `0.8.0` và `1.0.0`) |
| Timeout 30s của fetcher có "vô tác dụng" không? | **Chưa có bằng chứng.** Mã có timeout; jstack khớp với việc đang chờ có thời hạn. | Trung bình (một jstack, không có log) |
| Server kẹt vì `loadLogs` sau tắt không sạch? | **Cơ chế có thật** và là nguyên nhân thường gặp của khởi động chậm. **Nhưng không giải thích được issue này**, vì fetcher đã sống ~61 phút nghĩa là `loadLogs` đã xong. | Cơ chế: cao. Áp dụng cho issue: thấp |
| Nguyên nhân thật của sự cố là gì? | **Chưa biết.** Cả hai bên chưa đúng hoàn toàn. | — |

Điều mình **chưa kiểm tra** và có thể đáng thử khi điều tra: sau khi `becomeLeaderOrFollower`, các bucket mà node này trở lại làm leader/follower còn phải khôi phục KV, đồng bộ với leader mới rồi mới vào ISR; mình chưa đọc đoạn này nên đây chỉ là hướng nghi ngờ, không phải kết luận. Một thread dump đầy đủ (không chỉ fetcher) kèm log `becomeLeaderOrFollower`/`makeFollowers` của node vừa restart sẽ giúp xác định.

**Lưu ý về `relate_issue.md`:** dòng #2451 ghi "`ReplicaFetcherThread` treo sau restart, timeout không có tác dụng". Theo kiểm chứng ở trên, đây là cách diễn đạt **của người báo, chưa được chứng minh**; nên đổi thành "TabletServer ở trạng thái khởi tạo lâu sau restart, nguyên nhân chưa xác định; giả thuyết `loadLogs` sau tắt không sạch".

## 5. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành khi restart hoặc rolling upgrade TabletServer, đặc biệt sau tắt không sạch, hoặc khi node là leader nhiều bucket trước đó.

**Hậu quả có thể thấy:**
1. **Node mất khá lâu mới phục vụ lại được.** Nếu nguyên nhân là `loadLogs` thì thời gian tỉ lệ với số bucket, số segment và dung lượng cần quét.
2. **Mất khả năng chịu lỗi tạm thời:** trong lúc đó node không tham gia ISR; nếu có thêm node khác sự cố, bucket có thể mất dự phòng.
3. **Không có dữ liệu sai âm thầm** được ghi nhận trong issue; đây là chậm hoặc kẹt, chưa phải sai lệch dữ liệu.

**Mức ảnh hưởng với CDP:** ảnh hưởng chủ yếu tới tính sẵn sàng khi rolling upgrade/restart trên Kubernetes. Do nguyên nhân chưa xác định, nên coi là rủi ro vận hành cần diễn tập.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Cấp đủ `terminationGracePeriodSeconds` cho pod để TabletServer kịp tắt sạch (có file `.fluss_cleanshutdown`), tránh bị giết giữa chừng.
- Rolling restart **từng node**, chờ node trước vào lại ISR và hết replica lag rồi mới sang node kế tiếp.
- Đặt readiness/liveness probe đủ rộng để Kubernetes không giết pod đang recovery (việc đó tạo thêm một vòng tắt không sạch).
- Replication factor ≥ 3.
- Trong buổi diễn tập kill cứng node, **đo thời gian từ lúc khởi động đến lúc node sẵn sàng**, và lưu thread dump đầy đủ nếu thấy kẹt.
- Theo dõi issue: nếu người báo bổ sung thread dump đầy đủ thì sẽ rõ nguyên nhân.
