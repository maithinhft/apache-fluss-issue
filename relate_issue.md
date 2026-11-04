# Apache Fluss 1.0.0: các issue ảnh hưởng tới dự án CDP

* **Ngày phân tích:** 2026-11-04
* **Đầu vào:** 145 open bug trong [issue.md](issue.md). Đã bỏ 36 issue thuộc nhóm ⚪ (test flaky, CI, website, docs), còn **109 issue** được đánh giá.
* **Phiên bản mục tiêu:** Apache Fluss **1.0.0** (tag `v1.0.0`, ngày 2026-09-18, tách từ `main` ngày 2026-09-15).

---

## 1. Bối cảnh dự án dùng để lọc

| Vai trò của Fluss trong CDP | Thành phần Fluss liên quan |
|---|---|
| **Sink đầu ra của Flink** (profile, feature, segment của khách hàng, cập nhật near realtime) | Bảng Primary Key (KV + WAL/changelog), Flink sink, Java writer (UpsertWriter), auto-partition |
| **Nguồn lookup** cho Flink (lookup join, làm giàu sự kiện) | Flink lookup source, `PrimaryKeyLookuper`/`LookupSender`, metadata client |
| **Nguồn context cho AI agent đọc trực tiếp** | Java client lookup / prefix lookup, có thể dùng REST Gateway; cần độ trễ thấp, đọc nhất quán, không treo |
| **Vận hành production** | Coordinator HA + ZooKeeper, TabletServer (replication, recovery, KV snapshot, remote storage), rolling upgrade trên K8s, metrics |

Các thành phần **không nằm trong luồng chính** gồm Lakehouse tiering (Paimon/Iceberg/Lance), Kafka gateway, Spark, Rust/C++/Elixir client và YARN. Issue thuộc các thành phần này được đưa vào **nhóm B (ảnh hưởng có điều kiện)**: chỉ cần xem lại nếu bạn bật tính năng tương ứng.

## 2. Cách kiểm tra issue đã được fix trong 1.0.0 hay chưa

Cả 145 issue đều đang **Open**. Tuy vậy, issue Open không có nghĩa chắc chắn là lỗi còn nguyên, nên mình kiểm tra theo 3 bước:

1. **PR liên kết:** đọc trang của từng issue để lấy các PR có tham chiếu tới nó.
2. **Đối chiếu git:** clone `apache/fluss` rồi kiểm tra PR có nằm trong lịch sử của tag `v1.0.0` không, hay chỉ có trên `main` (tức là merge sau 2026-09-15, chưa vào 1.0.0). Mình cũng tìm theo từ khóa trong commit log để bắt các fix đi qua PR khác.
3. **Đọc mã nguồn `v1.0.0`:** áp dụng cho các issue quan trọng báo trên phiên bản cũ hơn 1.0.0 (#2209, #2943, #2967, #3011, #1044, #311, #4362).

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

## 3. Tóm tắt kết quả

| Nhóm | Số issue | Ghi chú |
|---|--:|---|
| **A: Ảnh hưởng trực tiếp** tới CDP | **71** | 9 Rất cao · 19 Cao · 19 Trung bình · 24 Thấp |
| **B: Ảnh hưởng có điều kiện** (chỉ khi bật lakehouse, Kafka gateway, SASL…) | **31** | |
| **C: Không ảnh hưởng** | **7** | Rust/JPMS/YARN/Lance/script local… |
| **Tổng** | **109** | Không tính 36 issue ⚪ |

**Những điểm quan trọng nhất:**

* **Tất cả 9 issue Rất cao vẫn tồn tại trong 1.0.0.** Không cái nào có fix merge vào 1.0.0, và đa số còn chưa merge vào `main`.
* Trong các issue báo trên bản trước 1.0.0, chỉ có **4** issue đã được khắc phục hoặc giảm thiểu rõ ràng trong 1.0.0: #311 ✅, #1044 ✅, #3011 🟡, #3466 🟡. Thêm vài issue về `OutOfOrderSequence` (#709, #1163) đã có nhiều fix liên quan (🟡).
* **3** issue đã fix trên `main` nhưng **không có trong 1.0.0**: #4256 (endpoint cũ khi rolling upgrade), #3084 (out-of-range khi restore), #5038 (metrics Coordinator đứng yên). Nên cân nhắc chờ bản 1.0.1 hoặc tự build bản vá.
* Hai rủi ro lớn nhất cho CDP:
  1. **Luồng ghi bảng PK:** #4290 (ghi KV treo khi WAL vượt memory pool, đặc biệt với bảng profile rộng có partial update), #2967 (sink lỗi khi batch có quá nhiều bản ghi) và #4269 (replica phân kỳ).
  2. **Luồng lookup cho AI agent:** #2209 (client deadlock khi lookup đồng thời cao trên bảng partition), #4043 (một bucket lỗi chặn lookup tới mọi bucket khác) và #4706 (lookup bảng đã drop không bao giờ trả về).

---

## 4. Nhóm A: Ảnh hưởng trực tiếp tới CDP

### 4.1. 🔴 Rất cao: cần xử lý trước khi go-live

| Issue | Thành phần | Vấn đề | Phiên bản báo | Trạng thái trong 1.0.0 | Vì sao ảnh hưởng CDP | Biện pháp |
|---|---|---|---|---|---|---|
| [#4290](https://github.com/apache/fluss/issues/4290) | Server / KV write | Một batch KV tạo WAL lớn hơn shared memory pool thì ghi **treo vô thời hạn**, giữ KV lock và RPC worker, kéo theo các request khác | 0.9.0 | ❌ PR [#4294](https://github.com/apache/fluss/pull/4294) chưa merge. Memory pool WAL có trong 1.0.0 | Bảng profile khách hàng thường **rộng (nhiều cột)** và được cập nhật từng phần (partial update). Đúng kịch bản "update nhỏ sinh full-row changelog image lớn" mà issue mô tả. Treo ghi đồng nghĩa với treo cả lookup của AI agent trên cùng TabletServer | Tăng kích thước WAL memory pool. Giới hạn batch size của writer (`client.writer.batch-size`). Theo dõi metric WAL memory pool (mới có trong 1.0.0). Load test với bảng có số cột thật |
| [#2967](https://github.com/apache/fluss/issues/2967) | Writer / Arrow | `ChangeTypeVectorWriter` có capacity cố định. Batch có quá nhiều bản ghi thì `OutOfBounds` và sink lỗi | 0.9.0 | ❌ **Đã xác nhận trong mã `v1.0.0`**: `ChangeTypeVectorWriter.writeChangeType` vẫn dùng capacity cố định, chỉ có TODO. PR [#3004](https://github.com/apache/fluss/pull/3004) chưa merge | Flink sink ghi số lượng lớn bản ghi nhỏ (sự kiện, cập nhật thuộc tính) vào một batch có thể chạm ngưỡng, dẫn tới job failover lặp lại | Giảm `client.writer.batch-size` và kiểm tra với dữ liệu thật (nhiều bản ghi kích thước nhỏ) |
| [#2209](https://github.com/apache/fluss/issues/2209) | Client lookup | Cập nhật metadata **đồng bộ** trên Netty IO thread khi lookup đồng thời cao trên **bảng partition**, gây deadlock và treo toàn bộ client | 0.8.0 | ❌ **Đã xác nhận trong mã `v1.0.0`**: `AbstractLookuper` vẫn gọi đồng bộ `checkAndUpdatePartitionMetadata`, `MetadataUpdater.updateMetadata` vẫn `synchronized`. PR [#2212](https://github.com/apache/fluss/pull/2212) chưa merge | Đúng luồng **AI agent / Flink async lookup** với tải đồng thời cao. Treo client nghĩa là agent mất context | Ưu tiên lookup trên **bảng PK không partition** cho context agent. Gọi lookup từ thread pool riêng, không chain lookup trong callback của lookup khác. Đặt timeout ở tầng ứng dụng |
| [#4269](https://github.com/apache/fluss/issues/4269) | Server / Replication | Clean leader reassignment có thể để **WAL phân kỳ** giữa các replica trong ISR, kể cả khi `acks=-1` | 0.9.0 | ❌ PR [#4273](https://github.com/apache/fluss/pull/4273) chưa merge | Rolling restart, rebalance hay controlled shutdown đều gây clean leader reassignment. Replica lệch thì sau failover dữ liệu profile có thể khác nhau | Không rebalance hoặc chuyển leader thủ công khi đang ghi nặng. Rolling restart từng node, chờ ISR đầy đủ rồi mới chuyển sang node tiếp theo |
| [#4566](https://github.com/apache/fluss/issues/4566) | Server / KV recovery | Mất **toàn bộ** dữ liệu local của TabletServer thì KV recovery thất bại dù snapshot và remote WAL vẫn còn (`Duplicate kv tablet directories`) | 1.0.0 | ❌ PR [#4567](https://github.com/apache/fluss/pull/4567) chưa merge | Trên K8s, mất PV hoặc thay node là kịch bản thật. Bảng PK (nguồn lookup) không khôi phục được thì mất nguồn context | Dùng PV bền (không dùng emptyDir/local ephemeral). Replication factor ≥ 3. **Diễn tập** kịch bản mất đĩa trước go-live |
| [#4362](https://github.com/apache/fluss/issues/4362) | Coordinator / ZK | Coordinator **không giành lại leadership** sau khi ZK session hết hạn (thường do GC dài) | 0.9.0 | ❌ Đã xác nhận trong mã `v1.0.0` (xem [issue4362.md](issue4362.md)). PR [#4382](https://github.com/apache/fluss/pull/4382) chưa merge | Không có Coordinator leader thì không tạo được partition mới (auto-partition), không DDL, không xử lý failover bucket. Luồng ghi vào partition mới sẽ lỗi | Chạy **≥ 2 Coordinator**. Tối ưu GC và heap. Alert khi không có active Coordinator. Runbook restart tự động (liveness probe dựa trên leadership) |
| [#4489](https://github.com/apache/fluss/issues/4489) | Server / KV snapshot | Một lỗi tạm thời khi khởi tạo snapshot (object store chập chờn) làm **KV snapshot dừng vĩnh viễn** cho bucket, không cảnh báo | main (code path có trong 1.0.0) | ❌ PR [#4492](https://github.com/apache/fluss/pull/4492) chưa merge. #2978 trong 1.0.0 chỉ fix một trường hợp khác | Snapshot dừng thì recovery phải replay changelog rất dài, WAL/log tích tụ, rủi ro khi xảy ra sự cố như #4566 | Giám sát "thời điểm snapshot cuối" của từng bucket. Sau sự cố S3/HDFS, restart TabletServer nếu snapshot không tiến triển |
| [#3224](https://github.com/apache/fluss/issues/3224) | Server recovery | TabletServer tắt do **đầy đĩa** thì recovery thất bại. Không có ngưỡng chặn ghi khi đĩa gần đầy | 0.9.0 | ❌ Không có PR, không có fix trong 1.0.0 | Luồng sự kiện CDP tăng đột biến có thể làm đầy đĩa | Alert dung lượng đĩa sớm (ví dụ 70%/80%). Bật remote log tiering, cấu hình `table.log.ttl` và local segment TTL hợp lý |
| [#4928](https://github.com/apache/fluss/issues/4928) | Java client / Metadata | Khởi tạo metadata "thành công" nhưng thiếu mapping bảng, khiến ghi vào partition lỗi lặp lại `table path not found`, writer **kẹt** | Client dựa trên 0.9 | ❌ Không có PR. Chưa xác minh trên 1.0.0, nên giả định còn | Flink sink ghi vào bảng partition (sự kiện theo ngày) có thể kẹt âm thầm | Alert khi throughput sink về 0 hoặc lỗi lặp lại. Restart job/writer |

### 4.2. 🟠 Cao

| Issue | Thành phần | Vấn đề | Phiên bản báo | Trạng thái trong 1.0.0 | Ảnh hưởng CDP / Biện pháp |
|---|---|---|---|---|---|
| [#4256](https://github.com/apache/fluss/issues/4256) | Client | Rolling upgrade: endpoint cũ bị TabletServer khác dùng lại, gây `NotLeaderOrFollowerException` kéo dài, client không tự hồi phục | 0.8.0 | ⏭️ Đã fix trên `main` bằng [#4263](https://github.com/apache/fluss/pull/4263) (2026-10-29), **không có trong 1.0.0** | Flink sink, lookup và AI agent đều bị khi upgrade trên K8s. Dùng StatefulSet + hostname ổn định. Upgrade xong thì restart client nếu lỗi kéo dài. Cân nhắc cherry-pick #4263 |
| [#742](https://github.com/apache/fluss/issues/742) | Flink sink | Sink ghi bảng PK có thể **block vĩnh viễn** khi upgrade cluster (IP pod đổi) | main (0.6) | 🟡 1.0.0 đã có cơ chế re-bootstrap khi toàn bộ địa chỉ server đổi (`MetadataUpdater`), nhưng trường hợp endpoint bị tái sử dụng chỉ fix ở #4263 (`main`) | Như #4256 |
| [#2892](https://github.com/apache/fluss/issues/2892) | Writer client | Sender I/O thread crash "Accounted size went negative" (Arrow double-release) khi upgrade server | 0.9.0 | ❌ Không có PR/fix | Writer chết thì Flink job failover. Upgrade server ngoài giờ cao điểm, theo dõi job |
| [#2943](https://github.com/apache/fluss/issues/2943) | Server recovery | `NoSuchElementException` khi recover log segment rỗng, TabletServer **không khởi động được** | 0.9.0 | ❌ **Đã xác nhận trong mã `v1.0.0`**: `LogSegment.readNextOffset` vẫn gọi `Iterables.getLast(batches)` không kiểm tra rỗng | Sau crash hoặc mất điện, node có thể không lên được và cần xóa segment rỗng thủ công. Chuẩn bị runbook |
| [#3228](https://github.com/apache/fluss/issues/3228) | Flink source | Resume sau gián đoạn dài thì `OutOfRangeException` do log đã bị xóa hoặc offload | 0.9.0 | ❌ PR [#3308](https://github.com/apache/fluss/pull/3308) chưa merge | Job Flink đọc changelog từ Fluss (ví dụ tính segment từ bảng profile) không phục hồi được sau sự cố dài. Đặt `table.log.ttl` dài hơn thời gian gián đoạn tối đa chấp nhận được |
| [#3084](https://github.com/apache/fluss/issues/3084) | Flink source | Job đọc log table restore từ checkpoint lỗi out-of-range offset | 0.9.0 | ⏭️ Fix một phần [#1752](https://github.com/apache/fluss/pull/1752) merge vào `main` ngày 2026-09-22, **không có trong 1.0.0** | Như #3228 |
| [#4043](https://github.com/apache/fluss/issues/4043) | Client lookup | Không giới hạn in-flight theo bucket: khi failover, lookup vào bucket lỗi chiếm hết permit và **chặn cả lookup tới bucket khỏe** | 0.9.0 | ❌ PR [#4044](https://github.com/apache/fluss/pull/4044) chưa merge | AI agent bị tăng độ trễ hoặc timeout hàng loạt khi chỉ một bucket gặp sự cố. Đặt timeout và circuit breaker ở tầng agent |
| [#1825](https://github.com/apache/fluss/issues/1825) | Server / Replication | Follower có thể ngừng sync từ leader do lỗi mạng chưa rõ, phải restart pod | main (≈0.8) | ❔ Không có PR/fix | ISR co lại, mất khả năng chịu lỗi. Alert replica lag và kích thước ISR |
| [#2451](https://github.com/apache/fluss/issues/2451) | Server / Replication | `ReplicaFetcherThread` treo sau restart, timeout không có tác dụng | 0.8.0 | ❌ Không có fix sau 0.8.0 | Như #1825 |
| [#1619](https://github.com/apache/fluss/issues/1619) | Coordinator / Auto-partition | Sau khi restart Coordinator có thể **không tự tạo partition mới**, ghi dữ liệu thất bại | main (≈0.8) | ❔ Không có PR. Nhiều thay đổi auto-partition trong 1.0.0 nhưng không fix trực tiếp | Bảng sự kiện partition theo ngày sẽ lỗi ghi khi sang ngày mới. Đặt `table.auto-partition.num-precreate` ≥ 2–3. Alert khi partition ngày mai chưa có |
| [#4726](https://github.com/apache/fluss/issues/4726) | Writer client | Batch ghi vào auto-partition **vừa hết hạn** bị retry vô hạn (chiếm buffer ghi, spam log) | 1.0.0 | ❌ PR [#4727](https://github.com/apache/fluss/pull/4727) chưa merge | Sự kiện CDP **đến trễ** rơi vào partition đã hết hạn có thể làm nghẽn buffer của writer. Lọc dữ liệu trễ trong Flink trước khi sink. Retention partition dài hơn độ trễ tối đa |
| [#4310](https://github.com/apache/fluss/issues/4310) | Flink sink | Bucket shuffle dùng `numBuckets` cố định cấp bảng. Sau khi **rescale bucket của partition**, cùng một key bị phân tán qua nhiều subtask, rủi ro sai thứ tự cập nhật | main (tính năng per-partition bucket count [#3908] có trong 1.0.0) | ❌ PR [#4311](https://github.com/apache/fluss/pull/4311) chưa merge | Chỉ xảy ra nếu dùng tính năng rescale bucket mới của 1.0.0 trên bảng PK partition. **Không rescale bucket** khi có Flink sink đang ghi |
| [#3466](https://github.com/apache/fluss/issues/3466) | Server / Client / ZK | Partition hết hạn gây bão `GET_METADATA` vào ZooKeeper (tới 1 phút) | 0.9.0 | 🟡 1.0.0 có negative cache cho partition không tồn tại ([#3511](https://github.com/apache/fluss/pull/3511)), giảm tải đáng kể nhưng issue chưa đóng | Bảng sự kiện có TTL partition. Giám sát request rate của ZK vào thời điểm partition hết hạn |
| [#2086](https://github.com/apache/fluss/issues/2086) | Client | `FlussConnection` không lấy được table khi server được chọn chưa sẵn sàng (không thử server khác) | 0.8.0 | ⏭️ Fix liên quan [#4873](https://github.com/apache/fluss/pull/4873) (retry trên địa chỉ Coordinator cũ) chỉ có trên `main` | Dịch vụ AI agent khởi động hoặc kết nối lại thất bại. Thêm retry khi khởi tạo connection ở tầng ứng dụng |
| [#3553](https://github.com/apache/fluss/issues/3553) | Build / Client jar | `fluss-client`, `fluss-flink-*` chứa jackson-core không shade, gây `NoSuchMethodError` | main | ❌ PR [#3884](https://github.com/apache/fluss/pull/3884) chưa merge vào `main` | Service AI agent (Spring/Jackson mới) và job Flink có thể lỗi classpath. Kiểm tra `mvn dependency:tree`, đặt jar Fluss lên trước hoặc relocate |
| [#1163](https://github.com/apache/fluss/issues/1163) | Writer | `OutOfOrderException` khi nâng cấp cluster, checkpoint thất bại | 0.6.0 | 🟡 Nhiều fix liên quan đã vào 1.0.0 (#1386, #1639, #2188, #2827, #4013) | Rolling upgrade. Cần test lại trên 1.0.0 |
| [#692](https://github.com/apache/fluss/issues/692) | Remote log | `commitRemoteLogManifest` có thể block mãi khi Coordinator failover | 0.6.0 | ❔ Không có PR/fix | Remote log tiering (đẩy log lên S3) bị kẹt thì đĩa local đầy (dẫn tới #3224). Giám sát lag của remote log |
| [#665](https://github.com/apache/fluss/issues/665) | Server / KV | `IndexOutOfBoundsException` khi put KV với lượng dữ liệu lớn | 0.6.0 | ❔ Có các fix Arrow liên quan (#2040, #2165) nhưng chưa chắc khắc phục đúng lỗi này | Load test ghi KV khối lượng lớn trước go-live |
| [#4000](https://github.com/apache/fluss/issues/4000) | Flink pushdown | Predicate `BETWEEN` lỗi runtime sau pushdown | 0.9.0 | ❌ PR [#3947](https://github.com/apache/fluss/pull/3947) chưa merge | Query Flink SQL trên bảng Fluss dùng `BETWEEN` (khoảng thời gian, điểm số) sẽ lỗi. Viết thành `>= AND <=` |

### 4.3. 🟡 Trung bình

| Issue | Vấn đề (tóm tắt) | Phiên bản báo | Trạng thái 1.0.0 | Ảnh hưởng CDP / Biện pháp |
|---|---|---|---|---|
| [#4706](https://github.com/apache/fluss/issues/4706) | Lookup trên bảng đã drop **không bao giờ trả về** | 1.0.0 | ❌ PR [#4757](https://github.com/apache/fluss/pull/4757) chưa merge | AI agent treo nếu bảng bị drop hoặc tạo lại. Bắt buộc timeout ở tầng agent |
| [#4741](https://github.com/apache/fluss/issues/4741) | Drop rồi tạo lại table/partition cùng tên: metadata bảng mới có thể bị xóa nhầm | 1.0.0 | ❌ | Tránh drop/recreate cùng tên. Dùng tên có version (`profile_v2`) |
| [#4787](https://github.com/apache/fluss/issues/4787) | Auto-partition race với drop table/database | 1.0.0 | ❌ PR [#4788](https://github.com/apache/fluss/pull/4788) chưa merge | Như #4741 |
| [#4755](https://github.com/apache/fluss/issues/4755) | Metadata cũ hoặc sai cho bảng bị tạo lại; bảng không tồn tại trả `UNKNOWN_SERVER_ERROR` | main (code có trong 1.0.0) | ❌ PR [#4758](https://github.com/apache/fluss/pull/4758) chưa merge | Như #4741 |
| [#3423](https://github.com/apache/fluss/issues/3423) | Projection pushdown chọn **cột mới thêm** thất bại trên batch dữ liệu cũ | 0.9.0 | ❌ PR [#4927](https://github.com/apache/fluss/pull/4927) chưa merge | CDP thường xuyên `ADD COLUMN` thuộc tính mới. Job đọc log/changelog có thể lỗi. Test schema evolution trước |
| [#4302](https://github.com/apache/fluss/issues/4302) | Java writer không chặn NOT NULL: null bị ghi thành `""`/`0` | main (code có trong 1.0.0) | ❌ PR [#4305](https://github.com/apache/fluss/pull/4305) chưa merge | Nếu ghi trực tiếp bằng Java client. Flink sink có `table.exec.sink.not-null-enforcer` chặn trước. Validate ở ứng dụng |
| [#3362](https://github.com/apache/fluss/issues/3362) | Cache metadata partition của writer không được dọn, CPU tăng dần | 0.9.0 | ❌ | Job sink chạy lâu trên bảng nhiều partition. Restart định kỳ hoặc giới hạn số partition |
| [#2749](https://github.com/apache/fluss/issues/2749) | Fetch log request timeout lặp lại khi một TabletServer restart | 0.8.0 | ❌ | Độ trễ replication tăng khi restart node |
| [#594](https://github.com/apache/fluss/issues/594) | Bão `FetchLogRequest` khi một TabletServer down rồi phục hồi | main (0.6) | ❔ | Như #2749 |
| [#3011](https://github.com/apache/fluss/issues/3011) | Replica bị đánh dấu offline mãi, bucket mất leader vĩnh viễn | 0.9.0 | 🟡 **Mã `v1.0.0` đã có cơ chế retry định kỳ** các replica offline trên TabletServer còn sống (`retryableOfflineReplicasOnLiveTabletServers`). PR gốc [#3012](https://github.com/apache/fluss/pull/3012) chưa merge | Giảm từ Rất cao xuống Trung bình. Vẫn nên alert khi có bucket không có leader |
| [#596](https://github.com/apache/fluss/issues/596) | Log bị truncate về 0 khi một TabletServer lỗi | 0.5.0 | ❔ Nhiều fix truncate/recovery sau 0.5 (#676, #731, #4013). Không có báo cáo lặp lại | Theo dõi. Replication factor ≥ 3, `acks=-1` |
| [#709](https://github.com/apache/fluss/issues/709) | Shutdown sạch nhiều TabletServer thì client nhận hàng loạt `OutOfOrderSequenceException` | main (0.6) | 🟡 Như #1163 | Rolling restart từng node |
| [#288](https://github.com/apache/fluss/issues/288) | Partition mới tạo khi bật partition discovery gây job Flink failover ("Leader not found") | 0.5.0 | 🟡 #1633 (discovery fixed-delay) đã vào 1.0.0, chưa chắc khắc phục hoàn toàn | Job đọc bảng partition theo ngày. Test lúc chuyển ngày |
| [#5038](https://github.com/apache/fluss/issues/5038) | Metrics Coordinator đóng băng sau khi re-election | 1.0.0 | ⏭️ Fix [#5039](https://github.com/apache/fluss/pull/5039) trên `main` (2026-11-04) | Cảnh báo sai. Không chỉ dựa vào metric Coordinator |
| [#4139](https://github.com/apache/fluss/issues/4139) | Histogram không loại mẫu cũ, percentile P99 lỗi thời | main | ⏭️ Fix liên quan [#4829](https://github.com/apache/fluss/pull/4829) trên `main` (2026-10-29) | Alert độ trễ lookup/ghi P99 có thể sai. Dùng thêm metric phía client |
| [#2076](https://github.com/apache/fluss/issues/2076) | Helm chart: `advertised.listeners` thiếu INTERNAL | 0.8.0 | ⏭️ Liên quan [#4108](https://github.com/apache/fluss/pull/4108) (listener cho client ngoài cluster) mới merge vào `main` | Nếu triển khai bằng Helm và AI agent chạy **ngoài** K8s cluster. Tự cấu hình listeners |
| [#4522](https://github.com/apache/fluss/issues/4522) | ZK follower chậm: bucket offline ~60s ngay sau khi tạo bảng | main (code có trong 1.0.0) | ❌ PR [#4524](https://github.com/apache/fluss/pull/4524) chưa merge | Chỉ ngay sau `CREATE TABLE`. Chờ bảng sẵn sàng rồi mới start job |
| [#4511](https://github.com/apache/fluss/issues/4511) | Codec Zstd rò rỉ off-heap buffer khi nén/giải nén ném exception | 0.9.0 | ❌ PR [#4513](https://github.com/apache/fluss/pull/4513) chưa merge | Zstd là compression mặc định của Arrow log. Chỉ rò khi có lỗi. Theo dõi direct memory |
| [#513](https://github.com/apache/fluss/issues/513) | `flushKv` và `updateHighWatermark` không atomic, có thể không thấy giá trị vừa ghi | main (0.6) | 🟡 #179 (KV hiển thị trước CDC log) đã có, issue vẫn mở | Ảnh hưởng read-your-write khi AI agent đọc ngay sau khi Flink ghi (cửa sổ ngắn) |

### 4.4. 🟢 Thấp

| Issue | Vấn đề | Phiên bản báo | Trạng thái 1.0.0 | Ghi chú |
|---|---|---|---|---|
| [#311](https://github.com/apache/fluss/issues/311) | Timeout cập nhật metadata 3s quá ngắn với job lớn | main (0.5) | ✅ **Mã `v1.0.0` dùng 30s** (`MetadataUtils`) | Đã khắc phục trên thực tế. Không cấu hình được |
| [#1044](https://github.com/apache/fluss/issues/1044) | Buffer có thể bị release 2 lần trong ServerHandler | 0.6.0 | ✅ `FlussRequest.releaseBuffer` dùng `ReferenceCountUtil.safeRelease` (#1043), không còn crash | Có thể có log warning |
| [#4516](https://github.com/apache/fluss/issues/4516) | Xóa replica bị coi là xong dù TabletServer chưa xác nhận | 0.9.0 | ❌ PR [#4517](https://github.com/apache/fluss/pull/4517) chưa merge | Chỉ khi drop table/partition. Có thể để rác trên đĩa |
| [#3357](https://github.com/apache/fluss/issues/3357) | Xóa bảng kẹt khi StopReplica thất bại | 0.9.0 | ❌ PR [#3369](https://github.com/apache/fluss/pull/3369) chưa merge | DDL hiếm |
| [#1629](https://github.com/apache/fluss/issues/1629) | Drop table khi TabletServer crash để lại dữ liệu rác | main | ❔ | DDL hiếm |
| [#4599](https://github.com/apache/fluss/issues/4599) | Race giữa KV snapshot và xóa replica | main | ❌ | Chỉ log lỗi |
| [#1256](https://github.com/apache/fluss/issues/1256) | Exception khó hiểu khi truy cập partition đang bị xóa | 0.7.0 | ❌ PR [#1257](https://github.com/apache/fluss/pull/1257) chưa merge | |
| [#1819](https://github.com/apache/fluss/issues/1819) | Lookup và scan trả giá trị kiểu `TIME` khác nhau (mất phần ms) | main | ❌ PR [#1822](https://github.com/apache/fluss/pull/1822) chưa merge | Tránh dùng kiểu `TIME`, dùng `TIMESTAMP` |
| [#4295](https://github.com/apache/fluss/issues/4295) | Timestamp trước 1970 có phần dưới ms không decode được | main | ❌ PR [#4296](https://github.com/apache/fluss/pull/4296) chưa merge | Ngày sinh khách hàng trước 1970. Dùng `DATE` hoặc `TIMESTAMP(3)` |
| [#4548](https://github.com/apache/fluss/issues/4548) | Auto-partition đơn vị QUARTER luôn bị từ chối | 1.0.0 | ❌ | Không dùng QUARTER |
| [#4538](https://github.com/apache/fluss/issues/4538) | `Decimal#hashCode` không nhất quán với `equals` | 1.0.0 | ❌ | Tránh dùng `Decimal` làm key HashMap ở ứng dụng |
| [#5021](https://github.com/apache/fluss/issues/5021) | Metric `lastPollSecondsAgo` báo rất lớn trước lần poll đầu (cảnh báo giả) | main | ❌ | Bỏ qua alert lúc khởi động |
| [#777](https://github.com/apache/fluss/issues/777) | Bảng replica = 1: fetch chỉ hoàn tất khi hết thời gian chờ | 0.6.0 | ❔ | Production dùng replica ≥ 3 nên không gặp |
| [#2653](https://github.com/apache/fluss/issues/2653) | `table.log.ttl = -1` bị từ chối | main | ❌ PR [#2621](https://github.com/apache/fluss/pull/2621) chưa merge | Đặt TTL rất lớn thay thế |
| [#1854](https://github.com/apache/fluss/issues/1854) | Container chạy bằng root | 0.7.0 | ❔ | Đặt `securityContext.runAsUser` |

> Thêm nhóm metrics phụ (đều 🟢, không ảnh hưởng dữ liệu): [#3304](https://github.com/apache/fluss/issues/3304), [#1037](https://github.com/apache/fluss/issues/1037) (độ trễ event-time của Flink source sai khi đọc nhiều partition). Coordinator state machine log ERROR gây nhiễu: [#764](https://github.com/apache/fluss/issues/764), [#765](https://github.com/apache/fluss/issues/765), [#898](https://github.com/apache/fluss/issues/898). Rò rỉ khi đóng sender: [#321](https://github.com/apache/fluss/issues/321). Recover cả partition đã xóa: [#3223](https://github.com/apache/fluss/issues/3223). Uber-jar `fluss-fs-*` lộ jackson/guava: [#4116](https://github.com/apache/fluss/issues/4116), [#4072](https://github.com/apache/fluss/issues/4072). Không thấy fix nào cho các issue này trong 1.0.0.

---

## 5. Nhóm B: Ảnh hưởng có điều kiện (chỉ khi bật tính năng tương ứng)

| Điều kiện | Issue | Mức nếu dùng | Trạng thái 1.0.0 |
|---|---|---|---|
| **Kafka gateway** (module `fluss-kafka` có trong 1.0.0) | [#4963](https://github.com/apache/fluss/issues/4963): mất dữ liệu âm thầm khi 2 tên topic (`t` và `fluss.t`) trỏ cùng bucket | 🔴 | ❌ PR #4964 chưa merge. **Chỉ dùng một kiểu tên topic** |
| **Lakehouse tiering (chung)** | [#2648](https://github.com/apache/fluss/issues/2648): commit trùng khi heartbeat timeout | 🔴 | ❌ |
| | [#4676](https://github.com/apache/fluss/issues/4676): scanner kẹt khi `EARLIEST` trỏ offset chỉ còn trong lake | 🟠 | ❌ |
| | [#3921](https://github.com/apache/fluss/issues/3921): tiering kẹt sau khi Coordinator failover | 🟠 | ❌ PR #3923 chưa merge |
| | [#3691](https://github.com/apache/fluss/issues/3691): scan đọc file lake lịch sử bất chấp `table.log.ttl` | 🟠 | ❌ |
| | [#4979](https://github.com/apache/fluss/issues/4979): batch read merge snapshot sai thứ tự, bản ghi đã DELETE xuất hiện lại | 🟠 | ❌ PR #5036 chưa merge |
| | [#4702](https://github.com/apache/fluss/issues/4702), [#2422](https://github.com/apache/fluss/issues/2422): mất báo cáo tiering, thiếu metric lake | 🟡 | ❌ |
| **Paimon** | [#1314](https://github.com/apache/fluss/issues/1314) (NoClassDefFoundError HDFS), [#2088](https://github.com/apache/fluss/issues/2088), [#2267](https://github.com/apache/fluss/issues/2267), [#2045](https://github.com/apache/fluss/issues/2045), [#2790](https://github.com/apache/fluss/issues/2790), [#4320](https://github.com/apache/fluss/issues/4320) | 🟠 → 🟢 | ❌ (#1314 có PR #4203 fail-fast, chưa merge) |
| **Iceberg** | [#4940](https://github.com/apache/fluss/issues/4940), [#3048](https://github.com/apache/fluss/issues/3048) (chưa hỗ trợ union read bảng PK), [#2420](https://github.com/apache/fluss/issues/2420), [#3812](https://github.com/apache/fluss/issues/3812) | 🟠 → 🟢 | ❌ |
| **REST Gateway** (nếu AI agent đọc qua REST) | [#4214](https://github.com/apache/fluss/issues/4214): DECIMAL không nhận ký hiệu số mũ | 🟢 | ❌ |
| **SASL / security token** | [#2698](https://github.com/apache/fluss/issues/2698) (mật khẩu có ký tự đặc biệt), [#5012](https://github.com/apache/fluss/issues/5012) (gia hạn token lặp chặt) | 🟡 | ❌ (#5013 chưa merge) |
| **Flink Delta Join** | [#2771](https://github.com/apache/fluss/issues/2771): NPE `releaseBucket` | 🟡 | ❌ |
| **Flink SQL batch / ad-hoc** | [#2353](https://github.com/apache/fluss/issues/2353) (limit scan bảng partition), [#3219](https://github.com/apache/fluss/issues/3219) (limit scan kiểu phức hợp), [#2397](https://github.com/apache/fluss/issues/2397) (`UPDATE` bảng PK) | 🟡 → 🟢 | #2353: 🟡 #1934 đã vào 1.0.0. Còn lại ❌ |
| **Flink SQL Gateway / Flink 2.2 catalog** | [#2198](https://github.com/apache/fluss/issues/2198) (rò classloader), [#3975](https://github.com/apache/fluss/issues/3975) | 🟢 | ❌ |
| **Rebalance / Typed API / JMX** | [#2405](https://github.com/apache/fluss/issues/2405), [#4303](https://github.com/apache/fluss/issues/4303), [#2712](https://github.com/apache/fluss/issues/2712), [#2750](https://github.com/apache/fluss/issues/2750) | 🟢 | ❌ |

## 6. Nhóm C: Không ảnh hưởng tới CDP

[#4270](https://github.com/apache/fluss/issues/4270) (Rust client), [#4045](https://github.com/apache/fluss/issues/4045) (JPMS), [#2235](https://github.com/apache/fluss/issues/2235) (YARN), [#1260](https://github.com/apache/fluss/issues/1260) (Lance), [#3192](https://github.com/apache/fluss/issues/3192) (`local-cluster.sh`), [#1627](https://github.com/apache/fluss/issues/1627) (Maven Central), [#3628](https://github.com/apache/fluss/issues/3628) (`Schema.PrimaryKey` equals, nội bộ), cùng các issue Iceberg/Paimon/Spark nếu bạn **không** dùng lakehouse. Ngoài ra là 36 issue ⚪ (test, CI, docs) đã bỏ từ đầu.

---

## 7. Khuyến nghị cho việc triển khai 1.0.0

**Trước go-live (bắt buộc):**

1. **Load test luồng ghi bảng PK** với schema profile thật (số cột thật, partial update, batch lớn) để bắt #4290 và #2967. Theo dõi metric WAL memory pool. Tinh chỉnh `client.writer.batch-size` và kích thước memory pool.
2. **Load test lookup đồng thời** với đúng mẫu truy cập của AI agent, đặc biệt trên bảng partition (#2209, #4043). Ưu tiên bảng context cho agent là **PK không partition**. Bắt buộc timeout, retry và circuit breaker ở tầng agent (#4706).
3. **Diễn tập sự cố:** mất PV của một TabletServer (#4566), đầy đĩa (#3224), restart cứng node (#2943), mất ZK session của Coordinator (#4362), sự cố S3/HDFS (#4489).
4. **HA:** ≥ 2 Coordinator, ZooKeeper 3–5 node tách riêng, replication factor 3, `acks=-1`.

**Vận hành:**

5. **Rolling upgrade/restart:** từng node, chờ ISR đầy đủ, dùng StatefulSet + hostname ổn định (#4256, #742, #4269, #2892, #1163). Không rebalance khi tải cao.
6. **DDL:** không drop rồi tạo lại bảng cùng tên (#4741, #4755, #4787, #4706). Không rescale bucket trên bảng có sink đang ghi (#4310). Chờ bảng sẵn sàng sau `CREATE TABLE` (#4522).
7. **Auto-partition:** `num-precreate` ≥ 2–3, alert khi thiếu partition tương lai (#1619, #4362). Lọc dữ liệu trễ trước khi ghi vào partition sắp hết hạn (#4726).
8. **Schema evolution:** test `ADD COLUMN` với job đọc log/changelog đang chạy (#3423).

**Giám sát (alert bắt buộc):** có active Coordinator hay không; số bucket không có leader; ISR shrink và replica lag; thời điểm KV snapshot cuối của từng bucket; dung lượng đĩa; lag của remote log; throughput sink bằng 0; độ trễ lookup phía client. Không tin tuyệt đối vào metric Coordinator và histogram (#5038, #4139).

**Về phiên bản:** các fix #4263 (endpoint cũ), #1752 (out-of-range) và #5039 (metrics) đã có trên `main`. Nên theo dõi bản **1.0.1**, hoặc tự build 1.0.0 cộng các patch cherry-pick nếu cần vào production sớm. Các fix quan trọng nhất (#4294, #4273, #4567, #4382, #2212, #4492) **vẫn đang ở trạng thái PR mở**, nên theo dõi tiến độ merge.

> **Giới hạn của phân tích:** trạng thái fix được xác định bằng PR liên kết và lịch sử git tại ngày 2026-11-04, cộng với đọc mã `v1.0.0` cho 7 issue (#2209, #2943, #2967, #3011, #1044, #311, #4362). Các issue đánh dấu ❔ chưa được tái hiện thực nghiệm. Mức ảnh hưởng là đánh giá theo kiến trúc CDP mô tả ở mục 1, nên cần điều chỉnh lại nếu bạn có dùng lakehouse, Kafka gateway hay REST Gateway.
