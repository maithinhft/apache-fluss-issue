# Issue #692 — Tác vụ đẩy log lên remote storage có thể bị treo vĩnh viễn khi Coordinator failover

Link: https://github.com/apache/fluss/issues/692 — Phiên bản được báo: `0.6.0`. Tiêu đề: *"LogTieringTask#commitRemoteLogManifest() may block forever when coordinator failover"*. Người báo: luoyuxia (một committer). Chỉ có phần mô tả, **không kèm log, không có bình luận, không có PR**. Chính người báo ghi: *"TBH, I'm still wondering why it'll block forever"*, tức là **họ chưa hiểu tại sao nó treo**, mới thấy hiện tượng. Họ hy vọng issue #279 (timeout toàn cục cho request của client) sẽ giải quyết.

> **Mức chắc chắn:** đây là một **quan sát chưa có nguyên nhân gốc**. Mình đọc mã `v1.0.0` và xác nhận lời gọi chặn vẫn còn, và RPC client vẫn **chưa có timeout cho request đang bay** (xem mục 1). Mình **không tái hiện** được việc "treo vĩnh viễn" và **không biết** vì sao nó xảy ra.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Remote log (tiered log / remote storage)** | Fluss không giữ mãi toàn bộ log trên đĩa của TabletServer. Các **segment** (đoạn log) cũ được sao chép lên kho từ xa (S3, OSS, HDFS...) rồi xóa bản trên đĩa local. Phần log đã đẩy lên gọi là remote log. |
| **Log tiering (phân tầng log)** | Quá trình đẩy segment cũ lên remote storage. |
| **`LogTieringTask`** | Tác vụ định kỳ chạy trong TabletServer, mỗi bucket có một tác vụ riêng: sao chép segment lên remote, rồi báo cho Coordinator. |
| **Remote log manifest (bản kê)** | File mô tả "những segment nào đã nằm ở remote, từ offset nào tới offset nào". Sau khi ghi file này lên remote, TabletServer phải **báo (commit) cho Coordinator**. |
| **`commitRemoteLogManifest`** | RPC gửi lên Coordinator để chốt bản kê mới. Coordinator ghi nhận vào ZooKeeper. Chỉ khi commit thành công, TabletServer mới coi các segment đó "đã được lưu an toàn ở remote". |
| **Coordinator failover** | Coordinator hiện tại chết hoặc được thay (nâng cấp, pod bị khởi động lại) và một Coordinator khác lên thay. Trong lúc đó, kết nối cũ bị đứt hoặc treo. |
| **`.get()` không có timeout** | `CompletableFuture.get()` chờ vô thời hạn cho tới khi có kết quả hoặc lỗi. Nếu kết quả không bao giờ tới, luồng gọi **đứng im mãi**. |
| **In-flight request** | Yêu cầu đã gửi đi mà chưa nhận phản hồi. |
| **Idle connection (kết nối nhàn rỗi)** | Kết nối không có dữ liệu đọc/ghi nào trong một khoảng thời gian. Netty có thể tự đóng nó sau thời gian đó. |
| **Scheduled thread pool** | Nhóm luồng chạy các tác vụ theo lịch. Số luồng có hạn: nếu tác vụ chiếm luồng mà không trả về, các tác vụ khác phải chờ. |
| **Segment, `highestCopiedEndOffset`** | Segment là một file log. `highestCopiedEndOffset` là offset cao nhất đã được sao chép lên remote **và đã commit**. TabletServer chỉ được xóa segment local tới offset này. |

## Tóm tắt

Khi TabletServer đã chép segment lên remote, nó gửi `commitRemoteLogManifest` cho Coordinator và chờ phản hồi bằng `.get()` **không có thời hạn**. Nếu Coordinator failover đúng lúc đó và phản hồi không bao giờ về (cũng không có lỗi ngắt kết nối), luồng của tác vụ đứng chờ mãi. Mỗi tác vụ bị treo chiếm một luồng trong nhóm luồng tiering (mặc định 4 luồng). Nếu đủ nhiều tác vụ bị treo, **việc đẩy log lên remote dừng hẳn** trên TabletServer đó. Vì đĩa local chỉ được dọn tới phần đã commit lên remote, đĩa local sẽ dần đầy. Người báo chưa hiểu cơ chế treo; lý thuyết thì kết nối đứt phải khiến `get()` trả về lỗi.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server` (`LogTieringTask`, `RemoteLogManager`, `LogTablet`) và `fluss-rpc` (client Netty).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `LogTieringTask.commitRemoteLogManifest` vẫn là: `return coordinatorGateway.commitRemoteLogManifest(request).get().isCommitSuccess();` — `.get()` **không có timeout**.
  - Bao quanh là vòng thử lại tối đa 10 lần (`retrySendCommitTimes <= 10`), nhưng vòng này chỉ kích hoạt khi `get()` **ném ngoại lệ** (ví dụ lỗi mạng). Nếu `get()` không bao giờ trả về thì vòng thử lại không có cơ hội chạy.
  - `ServerConnection` (client RPC) còn nguyên comment `// TODO: maybe we need to add timeout for the inflight requests` ngay chỗ ghi nhận yêu cầu đang bay. Tức là RPC client **vẫn chưa có timeout theo từng request** ở `1.0.0`; #279 mà issue hy vọng sẽ giải quyết **chưa được thực hiện** trong phần này (theo mã mình đọc).
  - `RemoteLogManager` dùng `Executors.newScheduledThreadPool(REMOTE_LOG_MANAGER_THREAD_POOL_SIZE)` (`remote.log-manager.thread-pool-size`, mặc định **4**), mỗi bucket có một `LogTieringTask` lên lịch bằng `scheduleWithFixedDelay` (khoảng nghỉ `remote.log.task-interval-duration`, mặc định 1 phút). Tác vụ treo trong `get()` giữ luôn luồng đó, và lần chạy kế của chính bucket đó cũng không bắt đầu được.
- **Có một lưới an toàn có thể làm kết thúc việc chờ (suy luận, chưa tái hiện):** `NettyClientHandler` có xử lý `IdleStateEvent`: khi kết nối **không có đọc/ghi nào** trong `netty.connection.max-idle-time` (mặc định **10 phút**), nó đóng kết nối và log `Connection ... is idle, closing...`. Việc đóng kết nối sẽ làm `ServerConnection.close` báo lỗi cho mọi yêu cầu đang bay (`completeExceptionally`), nên `get()` sẽ ném lỗi và vòng thử lại 10 lần mới chạy. Vậy **tác vụ không treo "mãi mãi" theo nghĩa đen** nếu kết nối đó thực sự yên lặng, mà bị chặn tối đa khoảng 10 phút mỗi lần (mặc định). Nhưng nếu cùng kết nối đó còn có lưu lượng khác tới Coordinator (các RPC khác từ cùng TabletServer) thì nó **không nhàn rỗi**, nên lưới an toàn này không kích hoạt. Đây là suy luận từ mã, mình chưa thử.
- **Vì sao treo mà không có lỗi ngắt kết nối:** vẫn chưa biết. Mình không thể giải thích thêm so với chính người báo.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Bật remote log** (cluster có `remote.data.dir` và bảng dùng log tiering). Nếu bảng không dùng tầng remote thì không có tác vụ này.
2. **Coordinator failover (hoặc không phản hồi) đúng lúc TabletServer đang chờ phản hồi `commitRemoteLogManifest`.** Cửa sổ thời gian này ngắn (chỉ trong lúc chờ một RPC) nhưng lặp lại cho mỗi bucket mỗi chu kỳ (mặc định 1 phút), nên xác suất trúng không nhỏ khi Coordinator xoay vòng lâu (ví dụ nâng cấp từng node trên Kubernetes).
3. **Phản hồi không về và không có lỗi mạng.** Điều kiện này người báo cũng chưa lý giải. Ví dụ có thể là kết nối cũ không bị báo lỗi (nửa mở) trong khi Coordinator đã thay.
4. **Đủ nhiều tác vụ cùng kẹt thì mới tắc hẳn:** với mặc định 4 luồng, nếu ≥ 4 `LogTieringTask` cùng treo thì cả nhóm luồng bị chiếm. (Số luồng nhỏ hơn số bucket nên các bucket phải luân phiên dùng chung, điều này làm hệ thống dễ bị tắc hơn bạn nghĩ.)

**Phiên bản:** báo trên `0.6.0`; `.get()` không timeout và comment TODO của RPC vẫn còn ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người dùng TabletServer có **bật remote log** và Coordinator có thể failover.

**Hậu quả:**
1. **Việc đẩy log lên remote dừng lại** trên tác vụ (hoặc cả TabletServer, nếu cả nhóm luồng bị chiếm).
2. **Đĩa local đầy dần.** `LogTablet.deleteExpiredSegments` giới hạn việc dọn tới `highestCopiedEndOffset` khi remote log được bật, và `deleteSegmentsAlreadyExistsInRemote` cũng chỉ xóa tới phần đã commit. Mà `highestCopiedEndOffset` chỉ được cập nhật **sau khi commit thành công**. Vậy khi commit kẹt, các segment đã quá hạn cũng không được xóa khỏi local. (Đã đọc mã; mình chưa đo tốc độ đầy đĩa.)
3. **Không mất dữ liệu đã commit.** Dữ liệu chưa commit vẫn còn nguyên trên đĩa local, chỉ không được dọn. Mình không thấy cơ chế nào làm sai dữ liệu từ lỗi này.
4. **Khó phát hiện:** tác vụ chỉ log `LOG.warn`/`error` khi có ngoại lệ; một tác vụ chờ vô hạn thì **không log gì**.

**Liên hệ với #3224:** dòng #692 trong `relate_issue.md` nói lỗi này "dẫn tới #3224" (đĩa đầy). Cơ chế "commit kẹt → không xóa segment → đĩa đầy" mình đọc được trong mã là hợp lý, nhưng mình **chưa xác nhận** là nguyên nhân thực tế của #3224 (chưa đọc kỹ issue đó).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

- Dòng trong `relate_issue.md` ghi "❔ Không có PR/fix" — **đúng**: không có PR riêng, và mình xác nhận trong mã `1.0.0` lời gọi vẫn không có timeout.
- Câu "Remote log tiering (đẩy log lên S3) bị kẹt thì đĩa local đầy" — **đúng về cơ chế** (mục 3.2).
- Cần lưu ý: lỗi này **chưa có bằng chứng tái hiện** và người báo không biết nguyên nhân. Mức "🟠/🔴" nên đọc là "rủi ro tiềm tàng, chưa xác định", không phải "lỗi đã được chứng minh". Và chính cơ chế idle-timeout (mục 1) có thể chặn thời gian treo ở ~10 phút trong nhiều trường hợp.

## 5. Mức ảnh hưởng với CDP

- Chỉ liên quan nếu **cluster bật remote log tiering** (đẩy log lên S3/OSS/HDFS). Nếu CDP chỉ dùng bảng khóa chính (KV) để lookup và bảng log chỉ để ghi context cho AI agent mà không bật remote log, thì không dính. Bạn nên kiểm tra cấu hình `remote.data.dir` của cluster.
- Nếu bật: rủi ro tập trung quanh **lúc Coordinator xoay vòng** (nâng cấp Fluss, restart pod Coordinator trên Kubernetes). Lúc đó theo dõi đĩa local của TabletServer.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Giám sát dung lượng đĩa local và độ trễ của remote log** (khoảng cách giữa log end và `remoteLogEndOffset`), cảnh báo sớm khi đĩa tăng bất thường. Fluss `1.0.0` có `server.data-disk.write-limit-ratio` (mặc định 0.85) từ chối ghi khi đĩa đầy, chặn bớt hậu quả xấu nhất (đã xem ở phần #3224, chưa kiểm chứng hết).
- **Nâng cấp Coordinator ngoài giờ cao điểm**, từng bước, và theo dõi log của TabletServer. Nếu thấy tác vụ tiering ngừng log sau khi Coordinator đổi, **restart TabletServer đó** (đây là suy luận từ cơ chế: restart tạo lại tác vụ và đóng kết nối cũ).
- **Cân nhắc giảm `netty.connection.max-idle-time`** để rút ngắn thời gian chờ tối đa trong trường hợp kết nối nhàn rỗi, nhưng nó ảnh hưởng mọi kết nối nên cần thử trước. Giá trị mặc định là 10 phút. Đây là ý của mình, chưa kiểm chứng.
- **Tăng `remote.log-manager.thread-pool-size`** (mặc định 4) để một vài tác vụ kẹt không chiếm hết nhóm luồng. Đổi lại tốn thêm luồng và băng thông upload.
- Theo dõi #279 (timeout cho request của client) và #692.

## 6. Những điều chưa kiểm chứng

- Nguyên nhân thật khiến `get()` không trả về khi Coordinator failover: chưa ai biết, kể cả người báo.
- Mình chưa tái hiện việc treo, chưa đo thời gian chặn thực tế với `max-idle-time` mặc định, và chưa xác nhận kết nối tới Coordinator có lưu lượng khác khiến idle-timeout không kích hoạt hay không.
- Mình chưa đọc trạng thái hiện tại của #279 (chỉ thấy comment TODO còn nguyên ở `1.0.0`).
- Mình chưa xác nhận mối liên hệ với #3224 trong thực tế.
- Mình chưa kiểm tra cách Coordinator xử lý trùng/phản hồi cho `commitRemoteLogManifest` khi failover (phía server).
