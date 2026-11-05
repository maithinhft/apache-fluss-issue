**Ký hiệu trạng thái trong 1.0.0:**

| Ký hiệu | Ý nghĩa |
|---|---|
| ❌ | **Chưa fix** trong 1.0.0: PR fix chưa merge hoặc chưa có PR, hoặc đã xác nhận lỗi còn trong mã `v1.0.0` |
| ⏭️ | **Chưa có trong 1.0.0, đã fix trên `main`** (sau khi tách nhánh): chờ 1.0.1/1.1, hoặc tự cherry-pick |
| 🟡 | **Đã giảm thiểu một phần** trong 1.0.0 (có fix liên quan, nhưng chưa đóng hẳn) |
| ✅ | **Đã khắc phục trong 1.0.0** dù issue chưa đóng (đã xác minh trong mã) |
| ❔ | Issue cũ, không có PR, không thấy tái hiện gần đây: **chưa xác định**, cần test |

**Mức ảnh hưởng tới CDP:**

| Mức | Ý nghĩa với dự án |
|---|---|
| 🔴 Rất cao | Có thể mất, sai hoặc lệch dữ liệu profile, hay làm sập/kẹt luồng ghi hoặc lookup chính. Phải có biện pháp trước khi go-live |
| 🟠 Cao | Gây gián đoạn (job Flink failover, client treo, server không lên) trong kịch bản vận hành thường gặp như upgrade, restart hay sự cố mạng/đĩa |
| 🟡 Trung bình | Chỉ xảy ra ở kịch bản cụ thể (DDL, schema evolution, partition hết hạn…) hoặc ảnh hưởng giám sát. Có workaround |
| 🟢 Thấp | Edge case hiếm, thông báo lỗi khó hiểu, ảnh hưởng nhỏ |


## Tóm tắt

| Issue | Vấn đề ngắn gọn | Phiên bản báo lỗi | Trạng thái trong 1.0.0 |
|---|---|---|---|
| [#4290](https://github.com/apache/fluss/issues/4290) | Ghi KV treo khi WAL của một batch lớn hơn memory pool | 0.9.0 | ❌ Còn lỗi (cả `main`) |
| [#2967](https://github.com/apache/fluss/issues/2967) | Batch có quá nhiều bản ghi làm tràn vector change type | 0.9.0 | ❌ Còn lỗi, **đã có người gặp trên 1.0.0** |
| [#2209](https://github.com/apache/fluss/issues/2209) | Client deadlock khi lookup đồng thời trên bảng partition | 0.8.0 | ❌ Còn lỗi, PR fix đã bị đóng |
| [#4269](https://github.com/apache/fluss/issues/4269) | Replica trong ISR lệch dữ liệu sau khi chuyển leader | 0.9.0 | ❌ Còn lỗi, PR fix đã bị đóng |
| [#4566](https://github.com/apache/fluss/issues/4566) | Bảng KV không khôi phục được khi mất toàn bộ đĩa local | 1.0.0 | ❌ Còn lỗi |
| [#4362](https://github.com/apache/fluss/issues/4362) | Coordinator không lấy lại leadership sau khi ZK session hết hạn | 0.9.0 | ❌ Còn lỗi. Fix nằm ở `fluss-shaded`, chưa phát hành |
| [#4489](https://github.com/apache/fluss/issues/4489) | KV snapshot dừng vĩnh viễn sau một lỗi tạm thời của object store | main, tái hiện trên 1.0.0 | ❌ Còn lỗi (cả `main`) |
| [#3224](https://github.com/apache/fluss/issues/3224) | TabletServer không recover được sau khi đầy đĩa | 0.9.0 | 🟡 Đã có cơ chế chặn ghi khi đĩa gần đầy, lỗi recovery vẫn còn |
| [#4928](https://github.com/apache/fluss/issues/4928) | Metadata client thiếu mapping bảng làm writer kẹt | Client dựa trên 0.9 | ❌ Còn lỗi theo phân tích mã `v1.0.0` |
| [#4256](https://github.com/apache/fluss/issues/4256) | Client dùng kết nối tới endpoint cũ khi rolling upgrade | 0.8.0 | ⏭️ Còn lỗi trong 1.0.0, đã fix trên `main` |
| [#742](https://github.com/apache/fluss/issues/742) | Flink sink treo khi mọi IP trong metadata đều đổi | main (thời 0.6/0.7) | 🟡 Kịch bản chính đã xử lý từ 0.9.0, phần còn lại thuộc #4256 |
| [#3228](https://github.com/apache/fluss/issues/3228) | Job Flink lỗi `OutOfRange` vĩnh viễn khi resume sau thời gian dài | 0.9.0 | ❌ Còn lỗi (cả `main`) |
| [#3084](https://github.com/apache/fluss/issues/3084) | Job đọc log lỗi out-of-range khi restore từ checkpoint | 0.9.0 | 🟡 Nguyên nhân gốc đã giảm thiểu, `main` có thêm fix |
| [#4043](https://github.com/apache/fluss/issues/4043) | Một bucket lỗi chiếm hết permit, chặn lookup tới bucket khác | 0.9.0 | ❌ Còn lỗi. `main` chỉ giảm nhẹ |

---

## Chi tiết

### 🔴 Rất cao

#### [#4290](https://github.com/apache/fluss/issues/4290): Ghi KV treo vô thời hạn khi WAL của một batch vượt memory pool

**Vấn đề:**
- Khi ghi vào bảng Primary Key, TabletServer dựng WAL (changelog) cho cả batch bằng các page lấy dần từ memory pool dùng chung (`server.buffer.memory-size`, mặc định 256 MB, page 128 KB).
- Có trường hợp WAL của **một** batch lớn hơn toàn bộ pool. Ví dụ: bảng rộng, partial update nhỏ nhưng changelog ghi cả dòng.
- Khi đó batch đã giữ hết page nhưng vẫn xin thêm. Các page chỉ được trả khi batch xong, nên batch tự chờ chính nó (deadlock).
- Trong lúc chờ, request giữ **KV write lock** của bucket và chiếm một RPC worker, làm các request khác trên TabletServer cũng bị nghẽn.
- Kiểm tra hiện có chỉ chặn trường hợp **một lần xin** vượt pool. Ở đây mỗi lần chỉ xin một page nhỏ nên kiểm tra không bắt được.

**Phiên bản gặp lỗi:** 0.9.0. Memory pool cho WAL có trong 1.0.0, nên 1.0.0 cũng bị.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`:
  - `LazyMemorySegmentPool.allocatePages` chỉ fail ngay khi `requiredPages > maxPages`. Ngược lại nó gọi `waitForSegment`, hàm này chờ trong `maxTimeToBlockMs`.
  - Server tạo pool bằng `createServerBufferPool`, với `server.buffer.wait-timeout` mặc định là `Long.MAX_VALUE` nano giây, tức **chờ vô hạn**.
- File `LazyMemorySegmentPool.java` không thay đổi gì trên `main` sau 1.0.0.
- PR fix [#4294](https://github.com/apache/fluss/pull/4294) **đang mở**.
- 1.0.0 có thêm metric cho KV WAL memory pool (commit `0b72a7a7`), dùng để giám sát được.
- Giảm thiểu bằng cấu hình (suy ra từ mã, chưa test):
  - Đặt `server.buffer.wait-timeout` hữu hạn (ví dụ 30–60s). Khi đó batch sẽ **fail** và được retry thay vì treo mãi.
  - Tăng `server.buffer.memory-size`.
  - Giảm `client.writer.batch-size`.

#### [#2967](https://github.com/apache/fluss/issues/2967): Batch có quá nhiều bản ghi làm tràn vector change type

**Vấn đề:**
- Khi server dựng changelog dạng Arrow cho một batch ghi KV, mỗi dòng changelog tốn 1 byte để lưu change type (`+I`, `-U`, `+U`, `-D`).
- Vùng nhớ cho các byte này **cố định** bằng page đầu tiên, tức `server.buffer.page-size` (mặc định 128 KB = 131.072 byte).
- Nếu một batch sinh ra hơn khoảng 131 nghìn dòng changelog thì ghi tràn và ném `IndexOutOfBoundsException`. Client nhận `UnknownServerException`, Flink sink fail.
- Thao tác update sinh 2 dòng (`-U` và `+U`), nên chỉ khoảng **65 nghìn dòng update** trong một batch là đủ gây lỗi.
- Kiểm tra `recordsCount > capacity` còn sai lệch một đơn vị (off-by-one), nên lỗi nổ ra ở `MemorySegment.put` thay vì báo rõ ràng.

**Phiên bản gặp lỗi:** 0.9.0. **Đã có người gặp lại trên 1.0.0** (bình luận ngày 2026-10-27: Flink sink partial update/delete, khoảng 66 nghìn dòng thì lỗi, `capacity: 131072`).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`: `ChangeTypeVectorWriter.writeChangeType` vẫn dùng capacity cố định, kèm `TODO: support AbstractPagedOutputView to have extendable capacity`.
- File không thay đổi trên `main`.
- PR [#3004](https://github.com/apache/fluss/pull/3004) **đang mở**. Maintainer phản hồi ngày 2026-10-29 rằng sẽ đẩy fix.
- Giảm thiểu: giảm `client.writer.batch-size` để mỗi batch không tới 65 nghìn dòng update, hoặc tăng `server.buffer.page-size` (ví dụ 1 MB) để nới capacity.

#### [#2209](https://github.com/apache/fluss/issues/2209): Client deadlock khi lookup đồng thời cao trên bảng partition

**Vấn đề:**
- Khi lookup bảng **partition**, client cần biết partition ID. Nếu chưa có trong cache, client gửi request metadata và **chờ đồng bộ**.
- Nếu lookup được gọi từ Netty IO thread (ví dụ gọi lookup trong callback của một lookup hoặc request khác), IO thread bị chặn trong lúc chờ metadata.
- Khi tải đồng thời cao, mọi IO thread có thể cùng bị chặn, không còn thread nào nhận response metadata. Kết quả là **toàn bộ client treo** vô thời hạn.

**Phiên bản gặp lỗi:** 0.8.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`:
  - `AbstractLookuper` vẫn gọi đồng bộ `metadataUpdater.checkAndUpdatePartitionMetadata(...)`.
  - `MetadataUpdater.updateMetadata` vẫn là `synchronized`.
- Trên `main`, `AbstractLookuper.java` không đổi.
- PR fix [#2212](https://github.com/apache/fluss/pull/2212) đã bị **đóng mà không merge** (2026-09-02). Hiện không có PR thay thế.
- Lookup bảng **không partition** không đi qua nhánh này.

#### [#4269](https://github.com/apache/fluss/issues/4269): Replica trong ISR lệch dữ liệu sau khi chuyển leader "sạch"

**Vấn đề:**
- Kịch bản tái hiện: leader A ghi một batch với `acks=1`, và batch này chỉ kịp sang C, chưa sang B.
- Sau đó leader được chuyển sạch (clean reassignment) sang B. B ghi dữ liệu **khác** vào **cùng các offset** đó.
- Follower chỉ so sánh offset, không so sánh lịch sử leader epoch, nên C giữ bản ghi cũ còn A và B giữ bản ghi mới, trong khi **cả 3 vẫn nằm trong ISR**.
- Write mới vẫn được xác nhận thành công với `acks=-1`. Nếu sau đó C lên làm leader, người đọc thấy dữ liệu khác.
- Clean reassignment xảy ra khi rebalance, controlled shutdown hoặc rolling restart.

**Phiên bản gặp lỗi:** 0.9.0 (tái hiện được với log table thông thường).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`:
  - Chưa có leader epoch cache.
  - `ReplicaFetcherThread` chỉ cắt log (truncate) theo end offset của leader, và còn ghi chú `TODO ... after we introduce leader epoch cache` (theo dõi ở #673).
- `main` vẫn còn nguyên các TODO này.
- PR fix [#4273](https://github.com/apache/fluss/pull/4273) ("Reconcile divergent replica logs using leader epochs") đã bị **đóng mà không merge** (2026-09-14).
- Các commit ISR trong 1.0.0 (#3702, #3920) và trên `main` (#3781, #4869) sửa những vấn đề khác, không giải quyết lỗi lệch dữ liệu này.

#### [#4566](https://github.com/apache/fluss/issues/4566): Bảng KV không khôi phục được khi TabletServer mất toàn bộ dữ liệu local

**Vấn đề:**
- Khi mất toàn bộ đĩa local nhưng ZooKeeper, KV snapshot và remote WAL vẫn còn, Fluss lẽ ra phải phục hồi bảng từ snapshot cộng với WAL.
- Thực tế, log local rỗng được khởi tạo từ offset 0, trong khi snapshot cần replay từ một offset cao hơn. Recovery đọc offset không tồn tại ở local và thất bại.
- Lần recovery thất bại để lại KV tablet "nửa vời" đã đăng ký. Các lần thử sau báo `Duplicate kv tablet directories`, che mất lỗi gốc.
- Ghi chú trong issue: kể cả khi được fix, các write chỉ nằm trên các replica local đã mất vẫn có thể mất.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`: `LogLoader` khởi tạo segment rỗng ở offset `0L` với `TODO: use logStartOffset if issue #744 ready`.
- 1.0.0 có commit #4407 (dọn thư mục KV cũ khi khởi động). Commit này **không** khôi phục mốc offset của log từ remote, nên không sửa được lỗi này.
- PR fix [#4567](https://github.com/apache/fluss/pull/4567) **đang mở**.

#### [#4362](https://github.com/apache/fluss/issues/4362): Coordinator không lấy lại leadership sau khi ZooKeeper session hết hạn

**Vấn đề:**
- Coordinator bị GC dài nên mất ZK session. Sau đó nó kết nối lại ZK thành công và dọn dẹp xong các leader service.
- Nhưng nó **không bao giờ tham gia bầu leader lại**. Process vẫn sống và vẫn xuất metric, nhưng cluster không có Coordinator leader.
- Không có leader thì auto-partition (tạo hoặc xóa partition) dừng, DDL bị từ chối, và failover bucket không được xử lý.
- Chỉ restart Coordinator mới hồi phục. Nghi ngờ nguyên nhân là node cha của election trên ZK bị xóa (`NONODE`) và Curator `LeaderLatch` không tạo lại được.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Maintainer xác nhận (bình luận ngày 2026-10-10): lỗi **đã được fix trong `fluss-shaded`** và đang chờ bản phát hành mới của `fluss-shaded`.
- Kiểm tra repo `apache/fluss-shaded`:
  - Fix là commit `ab8dacb` (2026-09-18), nâng Curator từ **5.4.0 lên 5.8.0**.
  - Bản phát hành duy nhất của `fluss-shaded` vẫn là `v1.0-incubating` (2025-08), dùng Curator 5.4.0.
- Fluss 1.0.0 và cả `main` vẫn dùng `fluss.shaded.version = 1.0-incubating`, tức vẫn là Curator 5.4.0.
- PR phía Fluss [#4382](https://github.com/apache/fluss/pull/4382) **đang mở**.
- Đây là thay đổi dependency, nên muốn có fix thì cần chờ `fluss-shaded` phát hành rồi Fluss nâng phiên bản. Việc tự build sẽ phức tạp hơn so với cherry-pick một commit.

#### [#4489](https://github.com/apache/fluss/issues/4489): KV snapshot dừng vĩnh viễn sau một lỗi tạm thời của object store

**Vấn đề:**
- Snapshot KV được lập lịch theo kiểu chuỗi: mỗi lần chạy xong mới hẹn lần sau.
- Nếu bước **khởi tạo** snapshot ném exception (ví dụ S3/HDFS mất kết nối tạm thời), code chỉ log một dòng ERROR rồi `return`, mà không hẹn lần tiếp theo.
- Chuỗi bị đứt, nên bucket đó **không bao giờ snapshot nữa** cho tới khi restart TabletServer.
- Không có retry và không có metric cảnh báo. Hệ quả:
  - WAL tích tụ.
  - Recovery sau sự cố phải replay rất dài.
  - Rủi ro cộng hưởng với #4566.

**Phiên bản gặp lỗi:** báo trên `main`. Người báo **tái hiện trên Fluss 1.0.0** (dừng object store khoảng 6 phút, sau đó không có snapshot mới trong hơn 14 phút).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận: `PeriodicSnapshotManager.triggerSnapshot()` trong `v1.0.0` và `main` giống hệt nhau, nhánh `catch` chỉ log rồi `return`.
- Fix trước đó #2978 (có trong 1.0.0) xử lý một nguyên nhân khác.
- PR fix [#4492](https://github.com/apache/fluss/pull/4492) **đang mở**.
- Dấu hiệu nhận biết trong log: `Fail to init snapshot during triggering snapshot.`, sau đó không còn log snapshot nào của bucket đó.

#### [#3224](https://github.com/apache/fluss/issues/3224): TabletServer không recover được sau khi tắt vì đầy đĩa

**Vấn đề:**
- TabletServer tắt vì `No space left on device`. Khi khởi động lại, quá trình recovery thất bại.
- Kể cả khi đã drop partition liên quan bằng admin, server vẫn cố recover partition đó (liên quan #3223).
- Issue cũng đề xuất chặn ghi khi dung lượng đĩa vượt ngưỡng, để không đi tới tình trạng đầy hẳn.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: 🟡 Đã giảm thiểu một phần.**
- Báo cáo [relate_issue.md](relate_issue.md) trước đó ghi là không có fix. Thông tin đó **sai**: 1.0.0 đã có **cơ chế bảo vệ đĩa**:
  - #3340: thêm disk-usage write protection.
  - #3612: thêm ngưỡng phục hồi (recover ratio).
  - #3539: từ chối promote KV leader khi đang khóa ghi.
- Cấu hình liên quan:
  - `server.data-disk.write-limit-ratio`, mặc định 0.85: từ chối ghi khi đĩa đạt 85%.
  - `server.data-disk.write-recover-ratio`, mặc định 0.80.
  - `server.data-disk.check-interval`, mặc định 30s.
- Phần **recovery sau khi đã đầy đĩa** và việc **recover cả partition đã drop** thì chưa có fix hay PR.
- Lưu ý thêm: trong 1.0.0, khi bị từ chối ghi (`DISK_WRITE_LOCKED`), writer phía client retry liên tục không có backoff. `main` đã thêm backoff ở #4293 (2026-10-20), nhưng thay đổi này không có trong 1.0.0.

#### [#4928](https://github.com/apache/fluss/issues/4928): Metadata client thiếu mapping bảng làm writer kẹt

**Vấn đề:**
- Khi khởi tạo table, `MetadataUpdater.updateMetadata()` **nuốt** lỗi timeout hoặc lỗi retriable.
- Nếu request `getTableInfo()` riêng thành công, `getTable()` vẫn trả về bình thường dù cache **thiếu mapping tableId → tablePath**.
- Về sau, khi ghi vào bảng partition, client chỉ xin metadata của partition, không xin metadata của bảng cha. Vì vậy client không bao giờ tự bổ sung được mapping bị thiếu.
- Mọi lần ghi đều lỗi `table path not found for tableId ...` và writer **kẹt** cho tới khi restart.

**Phiên bản gặp lỗi:** sự cố xảy ra với Java client dựa trên 0.9. Người báo đã đọc mã `v1.0.0` và `main` và xác nhận các đường code này vẫn còn, nhưng chưa tái hiện trên upstream.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (theo phân tích mã của người báo, chưa có PR).

### 🟠 Cao

#### [#4256](https://github.com/apache/fluss/issues/4256): Client dùng kết nối tới endpoint cũ khi rolling upgrade

**Vấn đề:**
- Khi rolling upgrade trên K8s, TabletServer X khởi động lại với địa chỉ mới, còn địa chỉ cũ của X được một TabletServer Y khác dùng lại.
- Client cache kết nối **theo server ID**, nên vẫn dùng kết nối cũ. Request gửi cho X thực chất tới Y, và nhận `NotLeaderOrFollowerException`.
- Client không ngắt kết nối và không refresh metadata, nên các lần retry vẫn đi vào kết nối sai. Lỗi kéo dài cho tới khi restart client.
- Một bình luận trên 1.0.0 (2026-10-27) cho thấy phía **server** cũng bị ảnh hưởng: replica fetcher của các TabletServer khác tiếp tục gọi IP cũ khoảng 10 phút. Trong thời gian đó, server vừa restart nằm ngoài ISR.

**Phiên bản gặp lỗi:** 0.8.0. Vẫn gặp trên 1.0.0.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`.**
- Đã xác nhận trong mã `v1.0.0`: `NettyClient` cache `connections` với key là `node.uid()`, không tính host và port.
- Fix [#4263](https://github.com/apache/fluss/pull/4263) đã merge vào `main` ngày 2026-10-29 (commit `9c15c19e`). Fix này làm kết nối nhận biết endpoint (key gồm uid, host và port) và sửa `MetadataUpdater`.
- Fix này **chưa được backport** sang `release-1.0`.
- Chưa rõ #4263 có xử lý được trường hợp replica fetcher phía server hay không. Câu hỏi trong bình luận chưa được trả lời.

#### [#742](https://github.com/apache/fluss/issues/742): Flink sink ghi bảng PK có thể treo khi upgrade cluster

**Vấn đề:**
- Khi các pod TabletServer lần lượt restart và đổi IP, có thể tới lúc **mọi** địa chỉ trong metadata của client đều sai.
- Lúc đó client không còn server nào để hỏi metadata mới, nên Flink sink treo.
- Đề xuất trong issue: quay lại dùng `bootstrap.servers` để lấy metadata.

**Phiên bản gặp lỗi:** `main` tại thời điểm báo (2025-04, khoảng 0.6/0.7).

**Trạng thái trong 1.0.0: 🟡 Kịch bản chính đã được xử lý, issue chưa đóng.**
- #2098 (có từ 0.9.0) thêm danh sách TabletServer không khả dụng. Khi mọi server đã biết đều không khả dụng, client **khởi tạo lại từ bootstrap server**.
- #3390 (có trong 1.0.0) bổ sung vòng lặp refresh cho tới khi gặp server sống hoặc re-bootstrap.
- #4095 (có trong 1.0.0) giảm connect timeout xuống 15s để hồi phục nhanh hơn.
- Đã kiểm tra trong `MetadataUpdater` của `v1.0.0`: có log `No available tablet server ... re-initializing cluster using bootstrap server`.
- Trường hợp **chưa** được xử lý là địa chỉ cũ **vẫn kết nối được** vì đã bị server khác dùng lại. Trường hợp này thuộc #4256, và chỉ được fix trên `main`.

#### [#3228](https://github.com/apache/fluss/issues/3228): Job Flink lỗi `OutOfRangeException` vĩnh viễn khi resume sau gián đoạn dài

**Vấn đề:**
- Job Flink đọc log của Fluss bị dừng lâu. Trong thời gian đó, log local đã bị xóa theo TTL hoặc đẩy lên remote/lake.
- Khi job restore từ checkpoint, offset đã lưu không còn trong khoảng đọc được, nên server trả `LOG_OFFSET_OUT_OF_RANGE`.
- Client ném `FetchException`, job fail. Restart lại cũng fail theo cùng cách, nên job **không bao giờ phục hồi** được nếu vẫn giữ checkpoint.
- Với bảng bật lake tiering, dữ liệu vẫn còn trong lake, nhưng client không có đường đọc dự phòng sang lake.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`: `AbstractLogFetchCollector.handleInitializeErrors` gặp `LOG_OFFSET_OUT_OF_RANGE_EXCEPTION` thì ném `FetchException` luôn, không reset offset và không fallback sang lake.
- `main` vẫn như vậy.
- PR [#3308](https://github.com/apache/fluss/pull/3308) (fallback đọc từ lake) **đang mở**.
- Lưu ý khi lên 1.1: `log.retention.roll-active-segment.enabled` sẽ mặc định là `true`, nên log hết hạn bị dọn sớm hơn và rủi ro gặp lỗi này tăng nếu `table.log.ttl` ngắn.

#### [#3084](https://github.com/apache/fluss/issues/3084): Job đọc log table lỗi out-of-range khi restore từ checkpoint

**Vấn đề:**
- Job backfill chạy ổn khoảng một tuần. Sau khi restart từ checkpoint thì một subtask lỗi `LogOffsetOutOfRangeException`: offset 1.546.148 trong khi server chỉ còn từ 2.199.904.
- Giả thuyết được maintainer đồng ý: trước đó, việc tải remote log bị **treo âm thầm** (lỗi tải không được báo lên). Offset của split vì thế không tiến, trong khi log trên server vẫn hết hạn dần. Tới khi restart, offset trong checkpoint đã không còn tồn tại.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: 🟡 Nguyên nhân gốc đã giảm thiểu, `main` có thêm fix.**
- Có trong 1.0.0:
  - #3104: bắt mọi `Throwable` để lỗi tải remote file không bị nuốt. Đây là fix cho issue #3085 (fetch remote log bị kẹt), issue này đã **đóng**. Fix cũng được backport vào 0.9.1 qua #3136.
  - #2786: hủy các lượt tải đang chờ khi đóng downloader.
  - #3816: tránh lỗi EOF khi tải remote log đồng thời.
- Chỉ có trên `main` (merge 2026-09-22, sau khi tách nhánh): [#1752](https://github.com/apache/fluss/pull/1752) ném exception sau vài lần retry tải remote log thất bại, để job fail sớm thay vì treo.
- Chưa sửa: khi offset trong checkpoint **thực sự** đã hết hạn thì job vẫn fail vĩnh viễn, giống #3228.

#### [#4043](https://github.com/apache/fluss/issues/4043): Một bucket lỗi chiếm hết permit, chặn lookup tới các bucket khác

**Vấn đề:**
- Client lookup chỉ có một giới hạn chung cho số RPC đang chờ phản hồi, là `client.lookup.max-inflight-requests`, áp dụng cho toàn bộ client.
- Khi failover (ví dụ rolling upgrade), các RPC gửi tới leader cũ hoặc leader đang không khả dụng nằm chờ tới khi timeout hoặc đứt kết nối.
- Nếu nhiều lookup cùng nhắm vào bucket bị ảnh hưởng, chúng chiếm hết permit chung. Sender bị chặn, nên lookup tới các bucket và server **khỏe** cũng không gửi đi được.
- Đề xuất trong issue: thêm giới hạn theo từng bucket, `client.lookup.max-inflight-requests-per-bucket`.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Đã xác nhận trong mã `v1.0.0`: `LookupSender` chỉ có một `Semaphore` chung (`maxInFlightReuqestsSemaphore`) và không có option giới hạn theo bucket.
- PR [#4044](https://github.com/apache/fluss/pull/4044) **đang mở**.
- Trên `main` (sẽ vào 1.1, không có trong 1.0.0) có hai thay đổi giúp giảm nhẹ:
  - #4970: lookup bị lỗi retriable sẽ retry với exponential backoff, cấu hình qua `client.lookup.retry-backoff` (100ms) và `client.lookup.retry-backoff-max` (1s).
  - #4991: với bucket **không có leader**, lookup được hoãn theo từng bucket, và client không còn block chờ metadata cho từng lookup.
- Hai thay đổi này **không** thêm giới hạn in-flight theo bucket. Trường hợp leader cũ vẫn nhận kết nối nhưng không trả lời vẫn chưa được xử lý.

---