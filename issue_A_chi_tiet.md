# Nhóm A: các issue ảnh hưởng trực tiếp tới CDP (chi tiết)

* **Cập nhật:** 2026-11-08. Gồm **66 issue nhóm A** (ảnh hưởng trực tiếp tới CDP). Không gồm 5 issue chưa đủ thông tin để đánh giá: #1825, #2451, #2892, #2943, #3553.
* **Nguồn:** nội dung và bình luận của từng issue trên GitHub, PR liên quan; và mã nguồn tag `v1.0.0`.
* **Trạng thái** trong file này là trạng thái đã đối chiếu lại. Nhiều chỗ **khác** với bản tổng hợp ban đầu. Ví dụ: #2076 ❌ (không phải ⏭️), #3223 và #898 ✅, #1619 ✅, #4755 và #4295 ⏭️, #2086 🟡.

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

Mỗi mục chi tiết gồm: **Vấn đề**, **Điều kiện kích hoạt** (khi có nhiều điều kiện thì ghi rõ "cần đồng thời"), **⚠️ Note** (nếu có: phần chưa kiểm chứng; ghi "điều kiện mơ hồ" khi issue không có log/tái hiện hoặc nguyên nhân chỉ là giả thuyết, **cần test thực tế** trước khi kết luận), **Phiên bản gặp lỗi**, **Trạng thái trong 1.0.0**, **Giảm thiểu**.

---

## Tóm tắt

| Issue | Vấn đề ngắn gọn | Phiên bản báo lỗi | Trạng thái trong 1.0.0 |
|---|---|---|---|
| **🔴 Rất cao** | | | |
| [#4290](https://github.com/apache/fluss/issues/4290) | Ghi KV treo vô thời hạn khi WAL của một batch vượt memory pool | 0.9.0 | ❌ Còn lỗi |
| [#2967](https://github.com/apache/fluss/issues/2967) | Batch có quá nhiều dòng changelog làm tràn vector change type | 0.9.0, gặp lại trên 1.0.0 | ❌ Còn lỗi |
| [#2209](https://github.com/apache/fluss/issues/2209) | Client bị chặn (có thể deadlock) khi lookup đồng thời cao trên bảng partition | 0.8.0 | ❌ Còn lỗi |
| [#4269](https://github.com/apache/fluss/issues/4269) | Replica trong ISR lệch dữ liệu sau khi chuyển leader "sạch" | 0.9.0 | ❌ Còn lỗi |
| [#4566](https://github.com/apache/fluss/issues/4566) | Bảng KV không khôi phục được khi TabletServer mất toàn bộ dữ liệu local | 1.0.0 | ❌ Còn lỗi |
| [#4362](https://github.com/apache/fluss/issues/4362) | Coordinator không lấy lại leadership sau khi ZooKeeper session hết hạn | 0.9.0 | ❌ Còn lỗi |
| [#4489](https://github.com/apache/fluss/issues/4489) | KV snapshot dừng vĩnh viễn sau một lỗi tạm thời của object store | báo trên `main`, gặp lại trên 1.0.0 | ❌ Còn lỗi |
| [#3224](https://github.com/apache/fluss/issues/3224) | TabletServer không recover được sau khi tắt vì đầy đĩa | 0.9.0 | 🟡 Đã giảm thiểu một phần |
| [#4928](https://github.com/apache/fluss/issues/4928) | Metadata client thiếu mapping bảng làm writer kẹt | Java client dựa trên 0.9 | ❌ Còn lỗi |
| **🟠 Cao** | | | |
| [#4256](https://github.com/apache/fluss/issues/4256) | Client dùng kết nối tới endpoint cũ khi rolling upgrade | 0.8.0 | ⏭️ Còn lỗi, đã fix trên `main` |
| [#742](https://github.com/apache/fluss/issues/742) | Flink sink ghi bảng PK có thể treo khi upgrade cluster | `main` tại thời điểm báo | 🟡 Kịch bản chính đã được xử lý, issue chưa đóng |
| [#4043](https://github.com/apache/fluss/issues/4043) | Một bucket lỗi chiếm hết permit, chặn lookup tới các bucket khác | 0.9.0 | ❌ Còn lỗi |
| [#1619](https://github.com/apache/fluss/issues/1619) | Coordinator restart có thể không tự tạo được partition mới | `main` thời khoảng 0.8 | ✅ cho race trong issue (đã xác minh trong mã) |
| [#4726](https://github.com/apache/fluss/issues/4726) | Batch ghi vào auto-partition vừa hết hạn bị thử lại vô hạn | 1.0.0 | ❌ Còn lỗi |
| [#4310](https://github.com/apache/fluss/issues/4310) | Flink sink bảng PK không phục hồi được từ checkpoint sau khi đổi số bucket của partition | `main` (mã có trong 1.0.0) | ❌ Còn lỗi |
| [#2086](https://github.com/apache/fluss/issues/2086) | `FlussConnection.getTable` thất bại khi TabletServer được chọn đang không sẵn sàng | 0.8.0 | 🟡 Đã giảm nhẹ |
| [#1163](https://github.com/apache/fluss/issues/1163) | Writer ném `OutOfOrderSequenceException` khi nâng cấp cluster | 0.6.0 | 🟡 Đã có nhiều fix liên quan |
| [#692](https://github.com/apache/fluss/issues/692) | `commitRemoteLogManifest` có thể treo vĩnh viễn khi Coordinator failover | 0.6.0 | ❔ Chưa xác định |
| [#665](https://github.com/apache/fluss/issues/665) | `IndexOutOfBoundsException` khi put KV với lượng dữ liệu lớn | 0.6.0 | ❌ Còn lỗi |
| [#4000](https://github.com/apache/fluss/issues/4000) | `PredicateConverter` xử lý sai `BETWEEN`, gây lỗi lúc chạy | 0.9.0 | ❌ Còn lỗi trong mã |
| **🟡 Trung bình** | | | |
| [#4706](https://github.com/apache/fluss/issues/4706) | Lookup trên bảng đã drop không bao giờ trả về | 1.0.0 | ❌ Còn lỗi |
| [#4741](https://github.com/apache/fluss/issues/4741) | Drop rồi tạo lại table/partition cùng tên: metadata của bảng mới có thể bị xóa nhầm | 1.0.0 | ❌ Còn lỗi |
| [#4787](https://github.com/apache/fluss/issues/4787) | Auto-partition race với drop table/database | 1.0.0 | ❌ Còn lỗi |
| [#4755](https://github.com/apache/fluss/issues/4755) | Server trả sai mã lỗi cho bảng không tồn tại và dùng metadata cũ khi bảng bị tạo lại | `main`, mã gây lỗi có trong 1.0.0 | ⏭️ Còn lỗi, đã fix trên `main` |
| [#3423](https://github.com/apache/fluss/issues/3423) | Sau `ADD COLUMN`, đọc log có chiếu cột lỗi trên các batch dữ liệu cũ | 0.9.0, gặp lại trên 1.0.0 | ❌ Còn lỗi |
| [#4302](https://github.com/apache/fluss/issues/4302) | Java writer không chặn vi phạm NOT NULL, null bị ghi thành `""`/`0` | `main` | ❌ Còn lỗi |
| [#3362](https://github.com/apache/fluss/issues/3362) | Writer không dọn thông tin partition cũ, CPU tăng dần theo thời gian chạy | 0.9.0 | ❌ Còn lỗi |
| [#594](https://github.com/apache/fluss/issues/594) | Bão `FetchLogRequest` khi một TabletServer sập rồi lên lại | `main` thời 0.6 | ❔ Chưa xác định |
| [#596](https://github.com/apache/fluss/issues/596) | Log của bucket bị cắt về offset 0 khi một TabletServer gặp sự cố | 0.5.0 | ❔ Chưa xác định |
| [#709](https://github.com/apache/fluss/issues/709) | Tắt nhiều TabletServer thì client nhận hàng loạt `OutOfOrderSequenceException` | `main` thời 0.6 | 🟡 Đã giảm thiểu phía client |
| [#288](https://github.com/apache/fluss/issues/288) | Partition mới được tạo làm job Flink đang đọc bị lỗi "Leader not found" | 0.5.0 | 🟡 Đã giảm thiểu |
| [#5038](https://github.com/apache/fluss/issues/5038) | Metric Coordinator bị "đóng băng" sau khi mất rồi lấy lại leadership | 1.0.0 | ⏭️ Còn lỗi, đã fix trên `main` |
| [#4139](https://github.com/apache/fluss/issues/4139) | Histogram không loại mẫu cũ, percentile P99 "đứng" ở giá trị lỗi thời | `main`, có từ ít nhất 0.9.1 | ⏭️ Còn lỗi, đã fix trên `main` |
| [#2076](https://github.com/apache/fluss/issues/2076) | Helm chart chỉ quảng bá địa chỉ CLIENT, địa chỉ INTERNAL dùng Pod IP thô | 0.8.0 | ❌ Còn lỗi, cả `main` |
| [#4522](https://github.com/apache/fluss/issues/4522) | Bucket offline khoảng 60 giây ngay sau khi tạo bảng vì đọc phải ZooKeeper follower chưa cập nhật | `main` | ❌ Còn lỗi |
| [#4511](https://github.com/apache/fluss/issues/4511) | Codec Zstd rò rỉ off-heap buffer khi nén/giải nén ném exception | 0.9.0 | ❌ Còn lỗi |
| [#513](https://github.com/apache/fluss/issues/513) | `flushKv` và `updateHighWatermark` không nguyên tử, lo có khe hở "thấy log trước khi thấy KV" | `main` thời 0.6 | 🟡 |
| **🟢 Thấp** | | | |
| [#311](https://github.com/apache/fluss/issues/311) | Timeout cập nhật metadata quá ngắn khiến job ghi báo nhầm "partition không tồn tại" | `main` thời 0.5 | ✅ trên thực tế cho đường ghi, nhưng nên coi là 🟡 |
| [#1044](https://github.com/apache/fluss/issues/1044) | Buffer của request có thể bị release nhiều lần trên server RPC | 0.6.0 | ✅ không còn crash, nhưng nên coi là 🟡 |
| [#4516](https://github.com/apache/fluss/issues/4516) | Coordinator coi việc xóa replica là xong dù TabletServer chưa xác nhận | 0.9.0 | ❌ Còn lỗi |
| [#3357](https://github.com/apache/fluss/issues/3357) | Xóa bảng kẹt vĩnh viễn khi `StopReplica` không gửi tới được TabletServer | 0.9.0 | ❌ Còn lỗi |
| [#1629](https://github.com/apache/fluss/issues/1629) | Drop bảng hoặc partition khi một TabletServer đang crash thì để lại replica rác | `main` thời 0.8 | ⏭️ Còn lỗi, đã fix trên `main` |
| [#4599](https://github.com/apache/fluss/issues/4599) | KV snapshot định kỳ "đua" với việc xóa replica, gây lỗi `registry is already closed` | `main` | ❌ Còn lỗi |
| [#1256](https://github.com/apache/fluss/issues/1256) | Client nhận exception khó hiểu khi đọc partition đang bị xóa | 0.7.0 | ❌ Còn lỗi |
| [#1819](https://github.com/apache/fluss/issues/1819) | Cùng một giá trị `TIME`, lookup và scan trả kết quả khác nhau | `main` thời 0.8 | ❌ Còn lỗi |
| [#4295](https://github.com/apache/fluss/issues/4295) | Khóa chính TIMESTAMP trước 1970 không giải mã được ở bộ giải mã khóa Iceberg | `main` | ⏭️ Còn lỗi, đã fix trên `main` |
| [#4548](https://github.com/apache/fluss/issues/4548) | Auto-partition từ chối mọi partition QUARTER nhưng chấp nhận ngày không tồn tại | 1.0.0 | ❌ Còn lỗi |
| [#4538](https://github.com/apache/fluss/issues/4538) | `Decimal#hashCode` không nhất quán với `Decimal#equals` | 1.0.0 | ❌ Còn lỗi |
| [#5021](https://github.com/apache/fluss/issues/5021) | Ba lỗi nhỏ ở Java client: mô tả SASL bị đảo, `lastPollSecondsAgo` sai trước lần poll đầu, mã lỗi lạ bị mất | `main` | ❌ Còn lỗi |
| [#777](https://github.com/apache/fluss/issues/777) | Bảng 1 replica: fetch đang chờ dữ liệu chỉ hoàn tất khi hết thời gian chờ | 0.6.0 | ❌ Còn lỗi |
| [#2653](https://github.com/apache/fluss/issues/2653) | Đặt `table.log.ttl = -1` (giữ log vô hạn) bị từ chối | `main` thời 0.9 | 🟡 |
| [#1854](https://github.com/apache/fluss/issues/1854) | Container Docker của Fluss chạy bằng root | 0.7.0 | ❌ Còn lỗi |
| [#3304](https://github.com/apache/fluss/issues/3304) | `currentFetchEventTimeLag` báo thấp hơn thực tế khi các bucket có độ trễ chênh nhau | 0.9.0 | ❌ Còn lỗi |
| [#1037](https://github.com/apache/fluss/issues/1037) | `currentEmitEventTimeLag` của Flink source luôn cao khi đọc bảng nhiều partition | 0.6.0 | ❔ Chưa xác định |
| [#764](https://github.com/apache/fluss/issues/764) | Máy trạng thái replica log ERROR khi replica đang xóa bị đẩy sang "offline" | 0.6.0 | ⏭️ Còn lỗi, đã có fix trên `main` |
| [#765](https://github.com/apache/fluss/issues/765) | Máy trạng thái bucket log ERROR khi bucket đã xóa bị đẩy sang "offline"/"online" | khoảng 0.6.0 | ❌ Còn lỗi |
| [#898](https://github.com/apache/fluss/issues/898) | Drop bảng partition chưa có partition nào thì bảng còn sót trong Coordinator | `main` thời khoảng 0.7 | ✅ Đã khắc phục |
| [#321](https://github.com/apache/fluss/issues/321) | Rò rỉ bộ nhớ Arrow khi writer bị đóng trước khi gửi hết batch | 0.5 | ❌ Còn lỗi |
| [#3223](https://github.com/apache/fluss/issues/3223) | TabletServer vẫn recover log của bucket thuộc partition đã bị xóa | 0.9.0 | ✅ Đã khắc phục |
| [#4116](https://github.com/apache/fluss/issues/4116) | Các uber-jar `fluss-fs-*` lộ jackson và guava không relocate | `main` | ❌ Còn lỗi |
| [#4072](https://github.com/apache/fluss/issues/4072) | Uber-jar `fluss-fs-s3` chứa khoảng 1.114 lớp jackson không relocate | `main` | ❌ Còn lỗi |
| **Đã đánh giá lại: không ảnh hưởng tới CDP hoặc đã khắc phục** | | | |
| [#3228](https://github.com/apache/fluss/issues/3228) | Job Flink lỗi `OutOfRange` vĩnh viễn khi resume sau gián đoạn dài | 0.9.0 | ❌ Còn lỗi |
| [#3084](https://github.com/apache/fluss/issues/3084) | Một bucket kẹt âm thầm khi tải remote log, restart thì `OutOfRange` | 0.9.0 | 🟡 Nguyên nhân gốc đã giảm thiểu, `main` có thêm fix |
| [#2749](https://github.com/apache/fluss/issues/2749) | Fetch log request timeout lặp lại khi một TabletServer restart | 0.8.0 | ❌ (hành vi giữ nguyên), nhưng không ảnh hưởng |
| [#3011](https://github.com/apache/fluss/issues/3011) | Replica bị đánh dấu offline mãi, bucket mất leader vĩnh viễn | 0.9.0 | 🟡 (kịch bản chính ✅) |
| [#3466](https://github.com/apache/fluss/issues/3466) | Partition hết hạn gây bão `GET_METADATA` vào ZooKeeper | 0.9.0 | 🟡 Đã giảm một phần |

**Các issue có điều kiện kích hoạt mơ hồ (14), nên test thực tế trước khi kết luận:** #4290, #3224, #4928, #4726, #1163, #692, #4000, #4741, #594, #596, #4511, #513, #1037, #3084.

---

## Chi tiết

### 🔴 Rất cao

#### [#4290](https://github.com/apache/fluss/issues/4290): Ghi KV treo vô thời hạn khi WAL của một batch vượt memory pool

**Vấn đề:**
- Khi ghi vào bảng PK, TabletServer dựng WAL (changelog) cho cả batch bằng các page lấy dần từ memory pool dùng chung (`server.buffer.memory-size`, mặc định 256 MB, page 128 KB).
- WAL của **một** batch có thể lớn hơn toàn bộ pool. Batch đã giữ hết page nhưng vẫn xin thêm, mà page chỉ được trả khi batch xong, nên batch **tự chờ chính nó** (deadlock).
- Trong lúc chờ, request giữ **KV write lock** của bucket và chiếm một RPC worker, làm các request khác trên TabletServer cũng nghẽn.
- Kiểm tra hiện có chỉ chặn trường hợp **một lần xin** vượt pool. Ở đây mỗi lần chỉ xin một page nên không bắt được.

**Điều kiện kích hoạt (cần đồng thời):**
1. Ghi vào **bảng PK**.
2. Tổng dung lượng WAL do **một** batch sinh ra lớn hơn phần pool còn trống (tối đa `server.buffer.memory-size`). WAL có thể lớn hơn nhiều so với batch client gửi, vì changelog image `FULL` ghi **cả dòng trước và sau** (`-U`/`+U`). Kịch bản điển hình: **bảng rộng** (nhiều cột, cột lớn) với **partial update** chỉ đổi vài cột, nên batch gửi đi nhỏ nhưng changelog ghi cả dòng.
3. `server.buffer.wait-timeout` để mặc định `Long.MAX_VALUE` (chờ vô hạn).

> ⚠️ **Note: điều kiện mơ hồ.** Chưa phân tích sâu issue này. Chưa có tỉ lệ cụ thể giữa kích thước batch client và WAL sinh ra, cũng chưa biết các batch đồng thời trên cùng TabletServer cùng chiếm pool có làm ngưỡng thấp hơn hay không (nhiều khả năng là có, vì pool dùng chung). Chưa tái hiện.

**Phiên bản gặp lỗi:** 0.9.0. Memory pool cho WAL có trong 1.0.0, nên 1.0.0 cũng bị.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`).
- `LazyMemorySegmentPool.allocatePages` chỉ fail ngay khi `requiredPages > maxPages`. Ngược lại nó gọi `waitForSegment`, chờ trong `maxTimeToBlockMs`.
- Server tạo pool bằng `createServerBufferPool`, với `server.buffer.wait-timeout` mặc định `Long.MAX_VALUE` nano giây.
- File `LazyMemorySegmentPool.java` không đổi trên `main`. PR [#4294](https://github.com/apache/fluss/pull/4294) **đang mở**.
- 1.0.0 có metric cho KV WAL memory pool (commit `0b72a7a7`).

**Giảm thiểu (suy ra từ mã, chưa test):**
- Đặt `server.buffer.wait-timeout` hữu hạn (ví dụ 30–60s) để batch **fail** và được retry thay vì treo.
- Tăng `server.buffer.memory-size`. Giảm `client.writer.batch-size`.
- Load test với schema profile thật (số cột thật, partial update). Theo dõi metric WAL memory pool.

#### [#2967](https://github.com/apache/fluss/issues/2967): Batch có quá nhiều dòng changelog làm tràn vector change type

**Vấn đề:**
- Khi server dựng changelog dạng Arrow cho một batch ghi KV, mỗi dòng changelog tốn 1 byte để lưu change type (`+I`, `-U`, `+U`, `-D`).
- Vùng nhớ cho các byte này **cố định** bằng một page, tức `server.buffer.page-size` (mặc định 128 KB = 131.072 byte).
- Batch sinh hơn khoảng 131 nghìn dòng changelog thì ghi tràn, ném `IndexOutOfBoundsException: capacity: 131072, index: 131072, put length: 1`. Client nhận `UnknownServerException`, Flink sink fail.
- Kiểm tra `recordsCount > capacity` sai lệch một đơn vị (off-by-one), nên lỗi nổ ở `MemorySegment.put` thay vì báo rõ ràng.

**Điều kiện kích hoạt:**
1. **Số dòng changelog** (không phải số bản ghi client gửi) của **một** batch ghi vượt khoảng `server.buffer.page-size − header` (mặc định khoảng 131.000 dòng). Số dòng changelog sinh ra theo thao tác (changelog image `FULL`):

   | Thao tác | Dòng changelog |
   |---|---|
   | Insert khóa mới | 1 (`+I`) |
   | Upsert / partial update lên dòng đã có | 2 (`-U`, `+U`) |
   | Partial delete (giữ cột khác) | 2 (`-U`, `+U`) |

   Vì vậy khoảng **65 nghìn update/partial delete** trên dòng đã tồn tại là đủ.
2. Client gom đủ nhiều bản ghi vào một batch. `client.writer.batch-size` mặc định `2mb`. Bản ghi **delete chỉ mang khóa** hoặc bản ghi rất nhỏ thì một batch 2 MB chứa được rất nhiều dòng. Upsert mang cả dòng thì hiếm khi chạm ngưỡng.

Ví dụ trong issue: Flink `FlussSink` partial update, khoảng 1 triệu dòng, parallelism 1, lỗi sau khoảng 66.000 dòng.

> ⚠️ **Note:** ngưỡng chính xác là `131072 − headerSize`, chưa tính `headerSize`. Chưa kiểm tra chế độ changelog image `WAL` và log format `INDEXED` (`IndexWalBuilder`) có bị không. Lỗi trong issue nằm ở nhánh Arrow.

**Phiên bản gặp lỗi:** 0.9.0. **Đã có người gặp lại trên 1.0.0** (bình luận 2026-10-27, `capacity: 131072`).

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`).
- `ChangeTypeVectorWriter.writeChangeType` vẫn dùng capacity cố định, kèm `TODO: support AbstractPagedOutputView to have extendable capacity`.
- PR [#3004](https://github.com/apache/fluss/pull/3004) **đang mở**. Maintainer phản hồi ngày 2026-10-29 rằng sẽ đẩy fix.

**Giảm thiểu:** giảm `client.writer.batch-size` để mỗi batch không tới 65 nghìn dòng update, hoặc tăng `server.buffer.page-size` (ví dụ 1 MB) để nới capacity. Load test với mẫu xóa/cập nhật hàng loạt.

#### [#2209](https://github.com/apache/fluss/issues/2209): Client bị chặn (có thể deadlock) khi lookup đồng thời cao trên bảng partition

**Vấn đề:**
- Khi lookup bảng **partition**, client cần biết partition ID. Nếu chưa có trong cache, client gửi request metadata và **chờ đồng bộ**.
- Nếu lookup được gọi từ Netty IO thread, IO thread bị chặn trong lúc chờ metadata.
- Khi tải đồng thời cao, mọi IO thread có thể cùng bị chặn, không còn thread nào nhận response metadata. Kết quả là **toàn bộ client treo**.

**Điều kiện kích hoạt (cần đồng thời):**
1. **Bảng có partition.** Bảng không partition không gọi `resolvePartitionRouting`.
2. **Client chưa có metadata của partition cần tra:** vừa khởi động, vừa sang ngày mới (partition mới), partition vừa được tạo hoặc bị xóa.
3. **Nhiều lookup đồng thời** cùng gặp thiếu metadata.
4. **Dạng nặng nhất (deadlock IO thread):** `lookup()` được gọi **từ bên trong Netty IO thread**, ví dụ trong `.thenCompose(...)` nối tiếp kết quả của lookup hoặc lần ghi trước.

**Với Flink async lookup chuẩn:** `FlinkAsyncLookupFunction` gọi `lookup()` từ thread của Flink, callback không gọi lookup lần nữa, nên **không** gặp dạng deadlock IO thread. Chỉ gặp dạng nhẹ: thread Flink bị chặn tối đa khoảng 30 giây mỗi lần thiếu metadata, và các lần cập nhật khác xếp hàng sau khóa `synchronized`.

> ⚠️ **Note:** chưa kiểm tra response RPC của client được hoàn tất trên IO thread hay trên executor riêng, nên chưa chắc dạng deadlock IO thread tái hiện được trên 1.0.0. Hành vi khi hết timeout 30 giây (lỗi hay tiếp tục) cũng suy ra từ mã, chưa chạy thử.

**Phiên bản gặp lỗi:** 0.8.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- `AbstractLookuper` vẫn gọi đồng bộ `metadataUpdater.checkAndUpdatePartitionMetadata(...)`. `MetadataUpdater.updateMetadata` vẫn `synchronized`. `main` không đổi.
- PR [#2212](https://github.com/apache/fluss/pull/2212) đã bị **đóng mà không merge** (2026-09-02). Không có PR thay thế.

**Giảm thiểu:** ưu tiên bảng context cho AI agent là **PK không partition**. Không gọi lookup trong callback của lookup hoặc ghi khác, gọi từ thread pool riêng. Timeout ở tầng ứng dụng. Pre-create partition để giảm trường hợp thiếu metadata.

#### [#4269](https://github.com/apache/fluss/issues/4269): Replica trong ISR lệch dữ liệu sau khi chuyển leader "sạch"

**Vấn đề:**
- Khi đổi leader, replica chỉ dựa vào **offset** để biết mình khớp với leader, không so lịch sử leader epoch.
- Follower vừa nhận thêm dữ liệu từ leader cũ mà leader mới chưa nhận thì hai bên có nội dung khác nhau tại cùng offset. Leader mới ghi đè các offset đó, trả OK với `acks=-1`, trong khi một replica trong ISR vẫn giữ dữ liệu cũ.

**Kịch bản tái hiện trong issue** (log table, 3 replica A/B/C, A là leader, ISR = [A, B, C]):

| Bước | Việc xảy ra |
|---|---|
| 1 | Ghi `[0,10)`, commit trên cả ba |
| 2 | Ghi thêm `[10,15)` vào A với `acks=1`. Chỉ C nhận kịp, B vẫn ở 10 và **vẫn trong ISR** |
| 3 | Đổi leader sạch sang B (`ReassignmentLeaderElection`) |
| 4 | Client ghi bản ghi **khác** vào B, chiếm `[10,15)`, `acks=-1` trả OK |
| 5 | Cuối cùng HW cả ba là 15, nhưng C giữ bản ghi cũ của A tại offset 10–14 |

**Điều kiện kích hoạt (cần đồng thời):**
1. Leader có dữ liệu **chưa được cả ISR nhận**: ghi `acks=1`, hoặc ghi `acks=-1` đang chờ.
2. Một follower **còn trong ISR nhưng tụt lại** sau leader.
3. Đổi leader **sạch** (không cần crash) sang đúng follower tụt lại đó: rolling restart, rebalance, controlled shutdown.
4. Có ghi mới vào leader mới trước khi các follower hòa giải xong.

> ⚠️ **Note:** chưa dựng lại chi tiết bước 5 (vì sao A theo B còn C thì không); chỉ trích kết quả issue báo. Chưa kiểm tra riêng với bảng PK (KV dựng từ log). Với `acks=-1` ở bước 2 thì xác suất thấp hơn nhưng chưa loại trừ.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`).
- Chưa có leader epoch cache. `ReplicaFetcherThread` chỉ truncate theo end offset của leader, có `TODO ... after we introduce leader epoch cache` (#673).
- PR [#4273](https://github.com/apache/fluss/pull/4273) đã bị **đóng mà không merge** (2026-09-14).
- Các commit ISR #3702, #3920, #3781, #4869 sửa vấn đề khác.

**Giảm thiểu:** dùng `acks=-1`. Không rebalance hay chuyển leader thủ công khi đang ghi nặng. Rolling restart từng node, chờ ISR đầy đủ và replica lag bằng 0 rồi mới sang node kế.

#### [#4566](https://github.com/apache/fluss/issues/4566): Bảng KV không khôi phục được khi TabletServer mất toàn bộ dữ liệu local

**Vấn đề:**
- Mất toàn bộ đĩa local nhưng ZooKeeper, KV snapshot và remote WAL còn, Fluss lẽ ra phục hồi bảng từ snapshot cộng WAL.
- Thực tế log local rỗng được tạo từ offset **0**, trong khi snapshot cần replay từ offset cao hơn (ví dụ 1000). Recovery đọc offset không tồn tại ở local và thất bại. Ranh giới log không được khôi phục từ remote.
- Lần recovery thất bại để lại KV tablet nửa vời đã đăng ký. Các lần thử sau báo `Duplicate kv tablet directories`, che lỗi gốc.
- Kể cả khi fix, các write chỉ nằm trên replica local đã mất vẫn mất.

**Điều kiện kích hoạt (cần đồng thời):**
1. **Bảng PK**.
2. Đã có ít nhất **một KV snapshot hoàn tất** và WAL cần thiết đã lên remote storage.
3. **Mọi replica** của bucket mất toàn bộ dữ liệu local (KV, WAL, checkpoint, clean-shutdown marker). Chỉ một replica mất đĩa thì không gây lỗi vì còn replica khác.
4. Khởi động lại và truy cập bảng; khi replica thành leader thì chạy `createKv()`.

Kịch bản thực tế: K8s dùng `emptyDir` hoặc ổ tạm (mặc định của Helm chart, xem #1854), mất cả node, cài lại TabletServer không giữ đĩa.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- `LogLoader` khởi tạo segment rỗng ở offset `0L` với `TODO: use logStartOffset if issue #744 ready`.
- #4407 (dọn thư mục KV cũ khi khởi động) có trong 1.0.0 nhưng không khôi phục mốc offset từ remote.
- PR [#4567](https://github.com/apache/fluss/pull/4567) **đang mở**.

**Giảm thiểu:** PV bền (bật `tablet.storage.enabled` trong Helm), replication factor ≥ 3, rải replica qua nhiều node/zone. Diễn tập mất đĩa trước go-live.

#### [#4362](https://github.com/apache/fluss/issues/4362): Coordinator không lấy lại leadership sau khi ZooKeeper session hết hạn

**Vấn đề:**
- Coordinator bị GC dài nên mất ZK session. Sau đó kết nối lại ZK thành công (`RECONNECTED`) và dọn xong leader service (`Coordinator leader services cleaned up successfully`).
- Nhưng nó **không bao giờ tham gia bầu lại**: không có dòng "has become the leader". Process vẫn sống, vẫn xuất metric, nhưng bộ đếm sự kiện đứng yên.
- Không có leader thì auto-partition dừng, DDL bị từ chối, failover bucket không được xử lý. Chỉ restart mới hồi phục.
- Nghi ngờ: node cha của election trên ZK bị xóa (`NONODE`), Curator `LeaderLatch` không tạo lại được.

**Điều kiện kích hoạt (cần đồng thời):**
1. Coordinator **mất ZooKeeper session**: GC dừng ứng dụng quá session timeout (log issue: không nghe từ server 162.997 ms), nghẽn mạng, ZooKeeper quá tải.
2. Kết nối ZK phục hồi nhưng bước bầu lại thất bại âm thầm.
3. **Cluster chỉ có một Coordinator** thì cluster mất leader hoàn toàn. Có Coordinator thứ hai thì nhiều khả năng nó được bầu thay, nhưng process bị kẹt không quay lại làm standby khỏe.

> ⚠️ **Note:** nguyên nhân gốc (Curator 5.4.0) mới là nghi ngờ được maintainer xác nhận gián tiếp qua fix ở `fluss-shaded`. Chưa rõ mọi lần mất session đều dẫn tới kẹt, hay chỉ khi rơi vào trạng thái `NONODE` cụ thể. Điều kiện 3 (có Coordinator thứ hai thì được cứu) là suy luận.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- Maintainer xác nhận (2026-10-10) fix nằm ở `fluss-shaded`: commit `ab8dacb` (2026-09-18) nâng Curator **5.4.0 → 5.8.0**. Bản phát hành duy nhất của `fluss-shaded` vẫn là `v1.0-incubating` (Curator 5.4.0).
- Fluss 1.0.0 và `main` vẫn dùng `fluss.shaded.version = 1.0-incubating`. PR [#4382](https://github.com/apache/fluss/pull/4382) đang mở.

**Giảm thiểu:** chạy **≥ 2 Coordinator**. Tối ưu GC/heap, tăng ZK session timeout có cân nhắc. Alert "không có Coordinator leader" bằng ZooKeeper hoặc Admin API (đừng dựa `activeCoordinatorCount`, xem #5038). Liveness probe dựa trên leadership để tự restart.

#### [#4489](https://github.com/apache/fluss/issues/4489): KV snapshot dừng vĩnh viễn sau một lỗi tạm thời của object store

**Vấn đề:**
- Snapshot KV được lập lịch kiểu chuỗi: mỗi lần chạy xong mới hẹn lần sau (`schedule(triggerSnapshot, delay)` + `scheduleNextSnapshot()`).
- Bước **khởi tạo** snapshot (`target.initSnapshot()`) ném exception thì `PeriodicSnapshotManager.triggerSnapshot()` chỉ log `Fail to init snapshot during triggering snapshot.` rồi `return`, **không hẹn lần tiếp theo**.
- Chuỗi bị đứt, bucket đó **không bao giờ snapshot nữa** tới khi restart TabletServer. Nhánh lỗi ở pha bất đồng bộ (`whenComplete`) thì có đếm và retry, chỉ nhánh init làm đứt chuỗi.
- Không retry, không metric cảnh báo. Hệ quả: WAL tích tụ, recovery replay rất dài, cộng hưởng với #4566.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng PK có KV snapshot định kỳ (`kv.snapshot.interval`).
2. **Remote storage (S3/HDFS/OSS) lỗi tạm thời đúng lúc khởi tạo snapshot**: object store dừng, mất kết nối, timeout. Người báo tái hiện bằng cách dừng object store khoảng 6 phút với `kv.snapshot.interval: 60s`.
3. Lỗi xảy ra ở bước **init**, không phải pha upload bất đồng bộ.

Ghi vẫn chạy bình thường trong và sau sự cố (đường ghi local không bị ảnh hưởng), nên rất khó phát hiện.

**Phiên bản gặp lỗi:** báo trên `main`. Người báo **tái hiện trên Fluss 1.0.0**: sau khi object store hồi phục, không có snapshot mới trong hơn 14 phút; restart một tablet thì có snapshot mới sau khoảng 50 giây.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`, file giống hệt).
- Fix trước đó #2978 (có trong 1.0.0) xử lý một nguyên nhân khác.
- PR fix [#4492](https://github.com/apache/fluss/pull/4492) **đang mở**.
- Dấu hiệu trong log: `Fail to init snapshot during triggering snapshot.`, sau đó không còn log snapshot nào của bucket đó. Lưu ý #4599 sinh log tương tự khi xóa/đổi leader.

**Giảm thiểu:** giám sát thời điểm snapshot cuối của từng bucket (ví dụ qua ZooKeeper hoặc thư mục snapshot trên remote). Sau sự cố S3/HDFS, restart TabletServer nếu snapshot không tiến.

#### [#3224](https://github.com/apache/fluss/issues/3224): TabletServer không recover được sau khi tắt vì đầy đĩa

**Vấn đề:**
- TabletServer tắt vì `No space left on device`. Khi khởi động lại, recovery thất bại (log trong issue là ảnh, không đọc được dạng chữ).
- Ở 0.9.0, kể cả khi đã drop partition liên quan bằng admin, server vẫn cố recover partition đó (#3223).
- Issue đề xuất chặn ghi khi dung lượng đĩa vượt ngưỡng.

**Điều kiện kích hoạt (cần đồng thời):**
1. Đĩa dữ liệu của TabletServer **đầy hẳn** khi đang ghi, server tắt vì `No space left on device`.
2. Khởi động lại khi trên đĩa còn segment/tablet ghi dở do đầy đĩa.

> ⚠️ **Note: điều kiện mơ hồ.** Log recovery chỉ là ảnh, không biết lỗi cụ thể (segment nào, exception gì). Có thể trùng cơ chế với #2943 (segment không có batch sau khi ghi dở), nhưng chưa xác nhận. Trong 1.0.0, khả năng đầy hẳn đã giảm nhờ cơ chế chặn ghi ở 85%, nhưng các thứ không đi qua đường ghi (ví dụ tải KV snapshot, compaction RocksDB, log khác trên cùng đĩa) vẫn có thể làm đầy đĩa (suy luận, chưa kiểm tra).

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: 🟡 Đã giảm thiểu một phần.**
- 1.0.0 có cơ chế bảo vệ đĩa: #3340 (disk-usage write protection), #3612 (recover ratio), #3539 (từ chối promote KV leader khi đang khóa ghi).
  - `server.data-disk.write-limit-ratio` mặc định 0.85: từ chối ghi khi đĩa đạt 85%.
  - `server.data-disk.write-recover-ratio` mặc định 0.80.
  - `server.data-disk.check-interval` mặc định 30s.
- Phần "recover cả partition đã drop" đã được xử lý bởi #3388 (xem #3223).
- Phần **recovery sau khi đã đầy đĩa** với partition còn tồn tại thì chưa có fix hay PR.
- Khi bị từ chối ghi (`DISK_WRITE_LOCKED`), writer phía client 1.0.0 retry liên tục **không backoff**. `main` thêm backoff ở #4293 (2026-10-20).

**Giảm thiểu:** alert dung lượng đĩa sớm (70%/80%). Bật remote log, đặt `table.log.ttl` và local segment TTL hợp lý. Đĩa riêng cho dữ liệu Fluss.

#### [#4928](https://github.com/apache/fluss/issues/4928): Metadata client thiếu mapping bảng làm writer kẹt

**Vấn đề:**
1. **Khởi tạo nuốt lỗi.** `getTable()` nạp metadata bị timeout hoặc lỗi retriable thì lỗi bị bỏ qua (`MetadataUpdater.updateMetadata`, cùng gốc với #311). Hàm vẫn trả `Table` bình thường dù cache **thiếu mapping `tableId` → table path**.
2. **Làm mới không tự chữa.** Ghi vào bảng partition chỉ xin metadata của partition, phản hồi chỉ có `tableId`. Client ném `IllegalArgumentException: table path not found for tableId <id> in cluster`, và mọi lần làm mới sau đều lặp lại vì không bao giờ xin metadata bảng cha.

Writer **kẹt**, ghi lỗi liên tục, không báo lỗi lúc khởi động. Lỗi xảy ra trước khi bản ghi vào accumulator, nên không có batch nào được gửi.

**Điều kiện kích hoạt (cần đồng thời):**
1. **Bảng có partition.**
2. **Lần nạp metadata ban đầu thất bại âm thầm**: timeout hoặc lỗi retriable khi server, ZooKeeper hoặc mạng chậm lúc ứng dụng khởi động.
3. `admin.getTableInfo()` **vẫn thành công**, nên `getTable()` không ném lỗi.
4. Sau đó ghi vào partition **client chưa có trong cache** (partition ngày mới hoặc lần ghi đầu).

> ⚠️ **Note: điều kiện mơ hồ.** Người báo **chưa tái hiện** trên upstream. Sự cố xảy ra ở client hạ nguồn dựa trên 0.9, và việc khởi tạo bị timeout (điều kiện 2) chỉ là **nghi vấn**, log không xác nhận. Chưa kiểm tra có đường nào khác làm thiếu mapping, và Flink sink xử lý lỗi lặp lại này ra sao (retry, fail job hay chỉ log).

**Phiên bản gặp lỗi:** Java client dựa trên 0.9. Các đường code còn trong `v1.0.0` và `main`.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (theo phân tích mã, chưa có PR). Liên quan #483, #1458 (refactor metadata).

**Giảm thiểu:** alert khi throughput sink về 0 hoặc lỗi `table path not found` lặp lại. Restart job/writer. Khởi động ứng dụng khi cluster đã ổn định.

### 🟠 Cao

#### [#4256](https://github.com/apache/fluss/issues/4256): Client dùng kết nối tới endpoint cũ khi rolling upgrade

**Vấn đề:**
- Client cache kết nối tới TabletServer **chỉ theo server UID** (`NettyClient`, key `node.uid()`).
- Rolling upgrade: server A chuyển sang endpoint Y, endpoint cũ X được cấp cho server B. Kết nối client đã mở tới X vẫn được dùng cho "A", nên request cho bucket của A tới nhầm B và nhận `NotLeaderOrFollowerException`.
- X vẫn kết nối được, nên không có lỗi mạng để bỏ kết nối. Kể cả metadata đã cập nhật `A -> Y`, tra kết nối vẫn trả kết nối cũ. Lỗi kéo dài tới khi restart client.

**Điều kiện kích hoạt (cần đồng thời):**
1. Rolling upgrade hoặc restart làm **endpoint của server đổi** (điển hình trên Kubernetes khi IP pod đổi).
2. Endpoint cũ được **tái sử dụng** cho server khác, hoặc vẫn kết nối được. Một người dùng báo trên EKS IP pod cũ được cấp cho pod khác sau khoảng 30 phút.
3. Client mở kết nối tới endpoint cũ **trước khi** metadata được làm mới.

> ⚠️ **Note:** phía server, một người dùng 1.0.0 thấy replica fetcher mở kết nối **mới** tới IP cũ khoảng 10 phút (`Connection refused`, `connection timed out after 15000 ms`), server vừa restart nằm ngoài ISR. Đây là triệu chứng khác (IP cũ không còn ai dùng) và chưa rõ nguyên nhân hay #4263 có bao phủ không. Maintainer chưa trả lời.

**Phiên bản gặp lỗi:** 0.8.0. Vẫn gặp trên 1.0.0.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`.** Fix [#4263](https://github.com/apache/fluss/pull/4263) merge 2026-10-29 (commit `9c15c19e`): key kết nối gồm uid, host và port, kèm sửa `MetadataUpdater`. Chưa backport sang `release-1.0`.

**Giảm thiểu:** StatefulSet + hostname ổn định. Upgrade xong mà lỗi `NotLeaderOrFollowerException` kéo dài thì restart client. Cân nhắc cherry-pick #4263.

#### [#742](https://github.com/apache/fluss/issues/742): Flink sink ghi bảng PK có thể treo khi upgrade cluster

**Vấn đề:**
- Ví dụ trong issue: bảng PK 128 bucket, sink parallelism 128. Do bucket shuffle, một task có thể chỉ ghi vào bucket của một vài TabletServer.
- Khi các pod TabletServer lần lượt restart và đổi IP, tới lúc **mọi** địa chỉ trong metadata của client (task đó) đều sai. Client không còn server nào để hỏi metadata mới, sink **treo**.
- Đề xuất trong issue: dùng `bootstrap.servers` để lấy metadata (gộp vào #483).

**Điều kiện kích hoạt (cần đồng thời):**
1. Pod TabletServer **đổi IP** khi restart (Kubernetes).
2. Rolling restart **liên tục nhiều node** đến mức mọi địa chỉ client đang biết đều không còn đúng.
3. (1.0.0) Trường hợp còn lại: địa chỉ cũ **vẫn kết nối được** vì đã bị server khác dùng lại, nên client không coi server là "không khả dụng" (#4256).

**Phiên bản gặp lỗi:** `main` tại thời điểm báo (2025-04, khoảng 0.6/0.7).

**Trạng thái trong 1.0.0: 🟡 Kịch bản chính đã được xử lý, issue chưa đóng.**
- #2098 (từ 0.9.0) thêm danh sách TabletServer không khả dụng. Khi mọi server đã biết đều không khả dụng, client **khởi tạo lại từ bootstrap server** (log `No available tablet server ... re-initializing cluster using bootstrap server` trong `MetadataUpdater`).
- #3390 (1.0.0) thêm vòng lặp refresh tới khi gặp server sống hoặc re-bootstrap.
- #4095 (1.0.0) giảm connect timeout xuống 15s.
- Trường hợp endpoint bị tái sử dụng thuộc #4256, chỉ fix trên `main`.

**Giảm thiểu:** `bootstrap.servers` trỏ vào tên DNS ổn định (headless service) thay vì IP. Rolling upgrade từng node. Như #4256.

#### [#4043](https://github.com/apache/fluss/issues/4043): Một bucket lỗi chiếm hết permit, chặn lookup tới các bucket khác

**Vấn đề:**
- Client lookup chỉ có một giới hạn chung cho số RPC đang chờ phản hồi, là `client.lookup.max-inflight-requests` (mặc định 128), áp dụng cho toàn bộ client.
- Khi failover, các RPC gửi tới leader cũ hoặc leader không khả dụng **giữ permit** tới khi lỗi hoặc timeout.
- Nhiều lookup cùng nhắm bucket bị ảnh hưởng thì chiếm hết permit. `LookupSender` bị chặn ở `Semaphore.acquire`, nên lookup tới bucket và server **khỏe** cũng không gửi được.

**Điều kiện kích hoạt (cần đồng thời):**
1. Có sự kiện làm một số bucket tạm không phản hồi: leader failover, rolling upgrade, TabletServer chết hoặc quá tải, lỗi mạng một chiều.
2. **Nhiều lookup hướng tới đúng các bucket đó** cùng lúc, đủ chiếm gần hết 128 permit. Tải lookup thấp thì chưa chắc chạm ngưỡng.
3. RPC tới bucket lỗi **không trả về nhanh** (không có lỗi mạng tức thời), nên permit bị giữ lâu.
4. Có lookup tới bucket khác **trên cùng client** cần gửi trong lúc đó.

> ⚠️ **Note:** issue là **đề xuất cải tiến dựa trên phân tích thiết kế**, không có log hay tái hiện sự cố thật. Chưa đo được permit bị chiếm tới mức nào trong thực tế. Triệu chứng (độ trễ tăng cho mọi khóa, timeout hàng loạt) là suy luận từ mã.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.**
- `LookupSender` chỉ có một `Semaphore` chung (`maxInFlightReuqestsSemaphore`), không có option theo bucket.
- PR [#4044](https://github.com/apache/fluss/pull/4044) **đang mở**.
- `main` có hai thay đổi giảm nhẹ (không có trong 1.0.0): #4970 (retry lookup với exponential backoff, `client.lookup.retry-backoff` 100ms, `client.lookup.retry-backoff-max` 1s) và #4991 (hoãn lookup theo bucket khi bucket không có leader). Cả hai **không** thêm giới hạn in-flight theo bucket.

**Giảm thiểu:** timeout và circuit breaker ở tầng AI agent. Tách client lookup cho các luồng quan trọng. Tăng `client.lookup.max-inflight-requests` có cân nhắc.

#### [#1619](https://github.com/apache/fluss/issues/1619): Coordinator restart có thể không tự tạo được partition mới

**Vấn đề:**
- Theo issue (thời 0.8): khi Coordinator khởi động, `AutoPartitionManager` được kích hoạt ngay trong `initCoordinatorContext()`, nhưng `serverMetadataCache.updateMetadata()` (danh sách TabletServer sống) lại chạy **sau** đó.
- Việc tạo partition cần danh sách server sống để phân bổ bucket, nên nhận danh sách rỗng và lỗi `InvalidReplicationFactorException: Replication factor: 3 larger than available tablet servers: 0`.
- Đề xuất: cập nhật metadata server trước `initCoordinatorContext()`.

**Điều kiện kích hoạt (bản cũ, cần đồng thời):**
1. Bảng có **auto-partition** bật.
2. **Coordinator restart** (hoặc failover sang Coordinator khác).
3. Đúng lúc đó cần tạo partition mới (ví dụ partition của ngày hiện tại hoặc pre-create còn thiếu).

**Đã kiểm tra trong mã `v1.0.0`:** trong `CoordinatorEventProcessor.initCoordinatorContext()`, `serverMetadataCache.updateMetadata(...)` và `updateTabletServerMetadataCacheWhenStartup(...)` **chạy trước** `autoPartitionManager.initAutoPartitionTables(...)`, kèm comment *"Auto-partition initialization schedules creation checks immediately. Start it only after the observed KV leader replica count and live tablet server resources are restored."* Thứ tự gây lỗi trong issue **đã được sửa**.

> ⚠️ **Note:** vẫn có thể thiếu partition nếu lúc Coordinator khởi động **số TabletServer đã đăng ký ít hơn replication factor** (ví dụ restart toàn cluster, TabletServer lên sau Coordinator). Khi đó `createPartition` bắt `Exception`, log ERROR, và chỉ thử lại ở lần kiểm tra kế tiếp (`auto-partition.check.interval`, mặc định **10 phút**). Ngoài ra 1.0.0 có thêm lý do bỏ qua tạo partition (`TooManyPartitions`, `TooManyBuckets`, `InsufficientKvLeaderReplicaCapacity`) chỉ log WARN. Chưa tìm được commit đã đổi thứ tự.

**Phiên bản gặp lỗi:** `main` thời khoảng 0.8.

**Trạng thái trong 1.0.0: ✅ cho race trong issue (đã xác minh trong mã)**, dù issue chưa đóng. Rủi ro còn lại (thiếu server lúc khởi động) chỉ làm chậm tối đa một chu kỳ kiểm tra.

**Giảm thiểu:** `table.auto-partition.num-precreate` ≥ 2–3 để có dư partition tương lai. Alert khi partition ngày mai chưa có. Khi restart toàn cluster, khởi động TabletServer trước Coordinator.

#### [#4726](https://github.com/apache/fluss/issues/4726): Batch ghi vào auto-partition vừa hết hạn bị thử lại vô hạn

**Vấn đề:**
- Từ #2940, ghi vào partition **đã hết hạn** thì `WriterClient#send` ném `InvalidPartitionException` ngay (lỗi rõ ràng).
- Nhưng nếu bản ghi đã vào bộ đệm lúc partition **còn hợp lệ**, rồi partition hết hạn **trước khi batch được gửi**, `Sender` coi đó là "unknown leader" và làm mới metadata, nhận `PartitionNotExistException`.
- Với bảng **không có historical partition**, `Sender` bỏ qua ngoại lệ và **giữ batch trong hàng đợi**. Partition không bao giờ được tạo lại, nên vòng làm mới metadata lặp mãi và write future **không bao giờ hoàn tất** (chiếm buffer ghi).

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng **auto-partition**, **không** bật historical partition. Bảng có bật thì nhận `PartitionNotExistException` rõ ràng, không kẹt.
2. Bản ghi vào bộ đệm khi partition **còn trong cửa sổ giữ lại**, nhưng batch **chưa gửi được ngay** vì client chưa biết leader (partition đang chờ tạo, metadata chưa tới).
3. Partition **hết hạn trước khi batch được gửi**: sự kiện đến trễ của một ngày sắp rơi khỏi cửa sổ, và việc gửi bị trì hoãn (batch chưa đầy, `batch-timeout` chưa tới, server chậm).

> ⚠️ **Note: điều kiện mơ hồ.** Issue viết từ phân tích mã, không có log sự cố thật. Cửa sổ thời gian (giữa lúc vào bộ đệm và lúc gửi) thường rất ngắn với tải bình thường, nên xác suất thực tế chưa rõ. Điều kiện 2 (partition còn hợp lệ nhưng chưa có leader) mâu thuẫn một phần với việc partition sắp hết hạn (partition cũ thường đã có leader từ lâu); có lẽ chỉ xảy ra khi metadata của client chưa có partition đó. Log `PartitionNotExistException` chỉ ở mức `DEBUG` trong 1.0.0, nên với log mặc định có thể **không thấy gì**. Chưa tái hiện.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4727](https://github.com/apache/fluss/pull/4727) đang mở.

**Giảm thiểu:** lọc dữ liệu trễ gần ranh giới retention trong Flink trước khi sink. Đặt retention partition dài hơn độ trễ tối đa của sự kiện. Alert khi request metadata tăng bất thường hoặc throughput sink giảm.

#### [#4310](https://github.com/apache/fluss/issues/4310): Flink sink bảng PK không phục hồi được từ checkpoint sau khi đổi số bucket của partition

**Vấn đề:**
- Bucket shuffle của sink dùng `numBucket` **cố định cấp bảng** (chốt lúc submit job), không theo số bucket thật của từng partition.
- Partition có số bucket `C ≠ N` thì `hash % N` (chọn subtask) lệch với `hash % C` (bucket thật), nên **một bucket có thể được nhiều subtask ghi đồng thời**.
- Với bảng dùng undo recovery, mỗi `UndoRecoveryOperator` ghi offset khác nhau cho cùng bucket. Khi restore, `RecoveryOffsetManager.putMergedOffset` ném `IllegalStateException("Conflicting checkpoint offsets ...")`. Job **không phục hồi được** từ checkpoint/savepoint.
- Cùng một key **không** bị phân tán qua nhiều subtask (cùng key cùng kênh), nên không phải lỗi "sai thứ tự cập nhật" như bản tổng hợp ban đầu ghi.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng PK **có partition**.
2. Đã `ALTER TABLE ... SET ('bucket.num' = ...)`, nên các partition có số bucket khác nhau (tính năng per-partition bucket count #3908 có trong 1.0.0).
3. Flink sink đang chạy dùng bucket shuffle với `numBucket` chốt lúc submit.
4. Bảng dùng merge engine **`AGGREGATION`** (undo recovery chỉ bật cho loại này).
5. Job **failover** và restore từ checkpoint/savepoint.

> ⚠️ **Note:** với bảng PK thường (không AGGREGATION), chưa thấy cơ chế gây hậu quả nào ngoài việc nhiều subtask cùng ghi một bucket (ảnh hưởng hiệu năng/thứ tự giữa các key khác nhau, chưa đánh giá).

**Phiên bản gặp lỗi:** `main`, nhưng cả `FlinkRowDataChannelComputer` lẫn tính năng rescale theo partition đều có trong 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4311](https://github.com/apache/fluss/pull/4311) đang mở.

**Giảm thiểu:** không rescale bucket trên bảng có Flink sink đang ghi, đặc biệt bảng AGGREGATION. Nếu buộc phải rescale, chạy lại job sink không dùng state cũ.

#### [#2086](https://github.com/apache/fluss/issues/2086): `FlussConnection.getTable` thất bại khi TabletServer được chọn đang không sẵn sàng

**Vấn đề:**
- Khi mở bảng, client hỏi một TabletServer **ngẫu nhiên** trong danh sách. Nếu server đó vừa tắt, `getTableInfo` nhận `DisconnectException` và `getTable` thất bại ngay, không thử server khác.
- Với Flink, task mở source reader hoặc lookup function bị lỗi, job failover.
- Ở `v1.0.0`, `getTableInfo` đi qua `RetryableGatewayClientProxy`: gặp lỗi đáng thử lại thì làm mới metadata và **thử lại một lần**.

**Điều kiện kích hoạt (cần đồng thời):**
1. Một TabletServer tạm không sẵn sàng (nâng cấp, restart, pod bị thay) đúng lúc client gọi `getTable`.
2. Client **chọn trúng** server đó (ngẫu nhiên, xác suất tỉ lệ với số server hỏng).
3. Client khởi tạo lại thường xuyên: Flink mở source/lookup lúc khởi động hoặc failover.
4. **Ở 1.0.0 cần thêm:** lần thử lại sau khi làm mới metadata **cũng thất bại** (ví dụ nhiều server cùng lỗi trong rolling upgrade).

> ⚠️ **Note:** chưa tái hiện trên 1.0.0, nên chưa biết retry một lần có đủ hay không. Thời gian chờ có thể dài vì `refreshClusterUntilAvailable` trả một connection timeout cho mỗi server hỏng.

**Phiên bản gặp lỗi:** 0.8.0.

**Trạng thái trong 1.0.0: 🟡 Đã giảm nhẹ** bằng retry một lần. bản tổng hợp ban đầu ghi ⏭️ với "fix liên quan #4873" là sai: #4873 là issue khác (`Admin#getServerNodes()` giữ địa chỉ Coordinator cũ).

**Giảm thiểu:** retry khởi tạo connection/table ở tầng ứng dụng. Rolling upgrade từng node.

#### [#1163](https://github.com/apache/fluss/issues/1163): Writer ném `OutOfOrderSequenceException` khi nâng cấp cluster

**Vấn đề:**
- Trong lúc nâng cấp cluster, Flink sink (append, parallelism 1024) thất bại checkpoint với `OutOfOrderSequenceException: Out of order batch sequence for writer 62979 at offset 2900 in table-bucket ...: 14 (incoming batch seq.), -1 (current batch seq.)`.
- `-1 (current batch seq.)` nghĩa là **leader mới không có writer state** cho writer đó, trong khi client gửi batch sequence 14. Leader không có state thì đòi sequence 0, nên từ chối.
- Client 0.6 coi đây là lỗi không phục hồi, job failover.

**Điều kiện kích hoạt (cần đồng thời):**
1. Ghi **bật idempotence** (mặc định).
2. **Nâng cấp/restart cluster** làm leader của bucket chuyển sang replica khác.
3. Replica lên làm leader **không có writer state** của writer đang ghi. Các khả năng: writer state chưa được sao chép hoặc dựng lại khi recovery (#704, #705, #2188), writer ID đã hết hạn trên replica đó (`server.writer-id.expiration-time`, mặc định 7 ngày, #1624), hoặc log phân kỳ (#4269).

> ⚠️ **Note: điều kiện mơ hồ.** Issue chỉ có stack trace, không mô tả trình tự upgrade, không biết server nào thiếu writer state và vì sao. Chưa tái hiện trên 1.0.0, nên không biết client mới có xử lý được trường hợp `current seq = -1` hay không.

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: 🟡 Đã có nhiều fix liên quan** (#1386, #1639, #2188, #2827, #4013 phía server; #2827, #2953, #4125 phía client xử lý `OutOfOrderSequence` "giả"). Client 1.0.0 gặp `OutOfOrderSequence`/`UnknownWriterId` không tiến triển thì `resetWriterId()` và xin writer ID mới, nhưng batch đó vẫn thất bại nên job Flink vẫn có thể failover. Gốc rễ (chưa có leader epoch, #673; writer epoch #5091) chưa sửa.

**Giảm thiểu:** rolling upgrade **từng node**, chờ ISR đầy đủ. Dùng controlled shutdown. Test lại rolling upgrade trên 1.0.0 với tải thật.

#### [#692](https://github.com/apache/fluss/issues/692): `commitRemoteLogManifest` có thể treo vĩnh viễn khi Coordinator failover

**Vấn đề:**
- Sau khi chép segment lên remote, TabletServer gửi `commitRemoteLogManifest` cho Coordinator và chờ bằng `.get()` **không có thời hạn**. RPC client chưa có timeout cho request đang bay (TODO, chờ #279).
- Coordinator failover đúng lúc đó mà phản hồi không về (và không có lỗi ngắt kết nối) thì luồng tác vụ đứng chờ mãi.
- Mỗi tác vụ treo chiếm một luồng của nhóm luồng tiering (mặc định 4). Đủ nhiều tác vụ treo thì việc đẩy log lên remote **dừng hẳn** trên TabletServer đó, đĩa local đầy dần (vì chỉ dọn tới phần đã commit lên remote).

**Điều kiện kích hoạt (cần đồng thời):**
1. **Bật remote log** (`remote.data.dir` có cấu hình, mặc định task chạy mỗi `remote.log.task-interval-duration` = 1 phút).
2. **Coordinator failover hoặc không phản hồi** đúng lúc TabletServer chờ phản hồi. Cửa sổ ngắn nhưng lặp lại cho mỗi bucket mỗi phút, nên dễ trúng khi Coordinator xoay vòng lâu (nâng cấp từng node trên K8s).
3. **Phản hồi không về và không có lỗi mạng** (ví dụ kết nối nửa mở).
4. **≥ 4 tác vụ** cùng treo (bằng số luồng mặc định) thì mới tắc hẳn.

> ⚠️ **Note: điều kiện mơ hồ.** Chính người báo (một committer) viết *"I'm still wondering why it'll block forever"*, tức chưa hiểu cơ chế treo. Lý thuyết thì kết nối đứt phải làm `get()` trả lỗi. Điều kiện 3 chưa ai lý giải. Cơ chế idle-timeout của kết nối có thể chặn thời gian treo ở khoảng 10 phút trong nhiều trường hợp. Chưa tái hiện, không có log.

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: ❔ Chưa xác định.** `.get()` không timeout vẫn còn. Không có PR.

**Giảm thiểu:** giám sát lag của remote log (offset đã đẩy lên remote so với LEO) và dung lượng đĩa. Sau khi Coordinator failover mà remote log không tiến, restart TabletServer bị ảnh hưởng.

#### [#665](https://github.com/apache/fluss/issues/665): `IndexOutOfBoundsException` khi put KV với lượng dữ liệu lớn

**Vấn đề:**
- Ghi khối lượng lớn (ví dụ ghi hơn 10 triệu bản ghi, rồi consume bảng đó ghi sang bảng khác) thì server log `Error put records to local kv` với `IndexOutOfBoundsException: capacity: 262144, index: 262144, put length: 1` tại `ChangeTypeVectorWriter.writeChangeType`.
- Maintainer xác nhận nguyên nhân: change type được ghi vào **page bộ nhớ đầu tiên** và không chuyển sang page thứ hai khi đầy, nên lỗi khi số dòng changelog vượt kích thước page.
- **Cùng nguyên nhân gốc với #2967.** #1906 là issue trùng (đã đóng).

**Điều kiện kích hoạt:** như #2967. Số dòng changelog của **một** batch ghi KV vượt `server.buffer.page-size` (trong issue: `capacity: 262144`, tức page 256 KB). Update/partial update sinh 2 dòng changelog mỗi bản ghi.

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** `ChangeTypeVectorWriter` ở `v1.0.0` còn nguyên `TODO`. Bản sửa là PR [#3004](https://github.com/apache/fluss/pull/3004), chưa merge. Các fix Arrow #2040, #2165 không khắc phục lỗi này.

**Giảm thiểu:** như #2967: giảm `client.writer.batch-size` hoặc tăng `server.buffer.page-size` (bình luận trong issue: `server.buffer.memory-size: 1gb`, `server.buffer.page-size: 1mb` giúp giảm). Load test ghi KV khối lượng lớn.

#### [#4000](https://github.com/apache/fluss/issues/4000): `PredicateConverter` xử lý sai `BETWEEN`, gây lỗi lúc chạy

**Vấn đề:**
- Nhánh `BETWEEN` của `PredicateConverter` truyền thẳng hai cận dạng `ValueLiteralExpression` vào `builder.between(...)` thay vì rút giá trị Java.
- `toString()` vẫn trông đúng nên test so chuỗi không phát hiện. Khi đánh giá thật, `compareLiteral` nhận đối tượng không `Comparable` và ném `RuntimeException("Unsupported type: BIGINT")`.

**Điều kiện kích hoạt (cần đồng thời):**
1. Biểu thức tới `PredicateConverter` dưới dạng hàm `BuiltInFunctionDefinitions.BETWEEN`. Chắc chắn xảy ra với Table API `Expression.between(...)`.
2. Biểu thức được đẩy xuống (`applyFilters`) và rơi vào nhánh:
   - bảng partition, `BETWEEN` trên cột partition (lọc partition); hoặc
   - bảng log có thống kê cột, `BETWEEN` trên cột có thống kê (lọc theo batch).
3. **Bảng PK không partition** (đường lookup chính của CDP) **không dính**.

> ⚠️ **Note: điều kiện mơ hồ với Flink SQL.** Bằng chứng trong issue và PR chỉ là unit test tự dựng `CallExpression`. Flink SQL (Calcite) thường đổi `x BETWEEN a AND b` thành `x >= a AND x <= b` khi lập kế hoạch, nên câu SQL `BETWEEN` **có thể không bao giờ** chạm lỗi. Chưa chạy thử để xác nhận.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi trong mã.** PR [#3947](https://github.com/apache/fluss/pull/3947) đang mở (thay đổi nhỏ: gọi `extractLiteral` cho hai cận).

**Giảm thiểu:** viết điều kiện thành `>= AND <=` (an toàn bất kể giả thuyết trên). Tránh `.between()` trong Table API.

### 🟡 Trung bình

#### [#4706](https://github.com/apache/fluss/issues/4706): Lookup trên bảng đã drop không bao giờ trả về

**Vấn đề:**
- Lookuper được tạo khi bảng còn tồn tại. Bảng bị drop, lookup tiếp theo **không bao giờ hoàn tất**: không lỗi, không timeout.
- Nguyên nhân (issue nêu, đã xác nhận trong `LookupSender.groupByLeaderAndType` của `v1.0.0`): khi `metadataUpdater.leaderFor(...)` ném lỗi (không tìm thấy leader), lookup được `reEnqueueLookup(lookup)` mà **không tăng số lần retry**, nên `client.lookup.max-retries` không bao giờ áp dụng. Chỉ `PartitionNotExistException` được hoàn tất bằng lỗi.
- **Phát hiện thêm khi đọc mã:** `client.lookup.max-retries` mặc định là **`Integer.MAX_VALUE`**, và không có option timeout cho lookup ở client. Nên kể cả khi retry được đếm, mặc định vẫn không giới hạn.
- Ghi không bị (đã sửa ở #4136 cho Rust). Rust SDK cũng bị.
- #4755 (server trả `UNKNOWN_SERVER_ERROR` thay vì `TABLE_NOT_EXIST`) là một lý do client không nhận ra bảng đã bị xóa.

**Điều kiện kích hoạt (cần đồng thời):**
1. Ứng dụng giữ một **lookuper đã tạo** cho bảng PK (dịch vụ AI agent chạy lâu, Flink lookup function).
2. Bảng bị **drop** (hoặc drop rồi tạo lại cùng tên).
3. Gọi lookup tiếp trên lookuper đó.

> ⚠️ **Note:** chưa kiểm tra trường hợp bảng bị tạo lại cùng tên (tiêu đề PR mới nói "dropped or re-created"): lookuper cũ giữ `tableId` cũ, có thể cũng treo hoặc trả lỗi khác. Chưa kiểm tra lookup có còn treo khi leader **tạm thời** không có (failover), nhưng theo cùng nhánh code thì cũng lặp không giới hạn tới khi có leader.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4757](https://github.com/apache/fluss/pull/4757) đang mở.

**Giảm thiểu:** **bắt buộc timeout ở tầng ứng dụng** (`future.get(timeout)` hoặc `orTimeout`). Có thể đặt `client.lookup.max-retries` hữu hạn (chỉ có tác dụng với các nhánh retry có đếm). Không drop/tạo lại bảng đang được agent dùng. Tạo lại lookuper khi nhận lỗi bảng không tồn tại.

#### [#4741](https://github.com/apache/fluss/issues/4741): Drop rồi tạo lại table/partition cùng tên: metadata của bảng mới có thể bị xóa nhầm

**Vấn đề:**
- Khi xử lý `UpdateMetadata`, Fluss có thể nhận diện bảng/partition bị xóa theo **đường dẫn bảng hoặc tên partition**, không theo ID.
- Ví dụ: bảng `db.t` (ID 100) có partition `p1` (ID 200); bảng bị drop nhưng thông báo xóa metadata đến **trễ**; bảng mới `db.t` (ID 101) với `p1` (ID 201) được tạo; thông báo xóa cũ đến và **xóa nhầm metadata của bảng mới**.
- Ngoài ra metadata partition cũ có thể còn trong cache của TabletServer sau khi bảng cha bị xóa/tạo lại, và bị lộ qua đường dẫn bảng mới.
- Phát hiện từ một lần CI fail (PR #4545), nguyên nhân là "possible reason".

**Điều kiện kích hoạt (cần đồng thời):**
1. **Drop rồi tạo lại** bảng hoặc partition **cùng tên**.
2. Thông báo xóa metadata của đối tượng cũ đến TabletServer **sau** khi đối tượng mới đã được tạo (thông báo bị trễ do tải, Coordinator bận, mạng chậm).

> ⚠️ **Note: điều kiện mơ hồ.** Issue nêu "a possible reason" sau khi CI fail, chưa có tái hiện xác định. Chưa đọc mã 1.0.0 để xác nhận nhánh xóa theo tên. Hậu quả cụ thể (client không thấy bảng mới, định tuyến sai) chưa được mô tả rõ. Liên quan nhưng khác #4755 (lỗi phía đọc/cache) và #4787 (race tạo partition).

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (theo issue). Người báo nhận làm PR.

**Giảm thiểu:** không drop rồi tạo lại bảng/partition cùng tên. Dùng tên có version (`profile_v2`).

#### [#4787](https://github.com/apache/fluss/issues/4787): Auto-partition race với drop table/database

**Vấn đề:**
- Auto-partition kiểm tra bảng trước khi tạo partition, nhưng việc kiểm tra và đăng ký **không nguyên tử** với DDL.
- Trình tự: task auto-partition đọc và kiểm tra table ID; luồng khác drop bảng hoặc database (có thể tạo lại bảng cùng tên, ID khác); task tiếp tục đăng ký partition **với table ID cũ**.
- Việc đăng ký partition **tự tạo znode cha còn thiếu**, nên task trễ có thể **tạo lại đường dẫn bảng/database đã xóa** mà không có metadata hợp lệ. Nếu đã có bảng thay thế, partition bị đăng ký dưới đường dẫn của nó nhưng giữ table ID cũ.
- Hậu quả: metadata và assignment partition không nhất quán, cản trở việc tạo partition sau đó, thao tác hạ nguồn lỗi.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng có **auto-partition**.
2. **Drop bảng hoặc database** đúng lúc một task auto-partition đang chạy giữa bước kiểm tra và bước đăng ký (task chạy khi bảng mới được thêm, khi Coordinator khởi động, và mỗi `auto-partition.check.interval`, mặc định 10 phút).
3. Nặng hơn nếu **tạo lại bảng cùng tên** ngay sau đó.

> ⚠️ **Note:** issue mô tả trình tự có thể xảy ra, không có log sự cố thật. Cửa sổ race có lẽ ngắn. Chưa đọc mã 1.0.0 để xác nhận.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4788](https://github.com/apache/fluss/pull/4788) chưa merge. Issue từng bị đóng rồi **mở lại**.

**Giảm thiểu:** như #4741: tránh drop/tạo lại cùng tên. Nếu phải drop bảng auto-partition, tắt auto-partition (hoặc chờ qua thời điểm kiểm tra) trước khi drop. Sau khi drop, kiểm tra ZooKeeper không còn znode của bảng.

#### [#4755](https://github.com/apache/fluss/issues/4755): Server trả sai mã lỗi cho bảng không tồn tại và dùng metadata cũ khi bảng bị tạo lại

**Vấn đề:** hai lỗi phía server, cùng xoay quanh bảng bị xóa hoặc tạo lại:
1. **Sai mã lỗi.** Metadata request cho bảng đã bị xóa: server bọc `TableNotExistException` thành `FlussRuntimeException`, trả `UNKNOWN_SERVER_ERROR` thay vì `TABLE_NOT_EXIST`. Client không biết bảng đã bị xóa (một lý do của #4706).
2. **Metadata trộn lẫn.** Sau khi xóa rồi tạo lại cùng tên, tới khi TabletServer nhận `UpdateMetadata` cho bảng mới, cache có thể **ghép thông tin bảng mới (từ ZK) với bucket/partition của bảng cũ**. Client định tuyến ghi tới bucket hoặc partition không tồn tại: ghi lỗi nhanh hoặc `flush()` treo mãi.

**Điều kiện kích hoạt:**
- **Lỗi 1:** client gửi metadata request cho bảng không tồn tại (đã xóa hoặc sai tên), và cache server không có bảng đó nên đi nhánh đọc ZK (`getTablesMetadataFromZK`).
- **Lỗi 2 (cần đồng thời):**
  1. Xóa rồi **tạo lại bảng cùng đường dẫn** (hoặc partition cùng tên).
  2. Một TabletServer đã thấy ZK mới nhưng **chưa nhận `UpdateMetadata`**. Khoảng này ngắn bình thường, dài ra khi hệ thống chịu tải.
  3. Có client hỏi hoặc ghi vào bảng trong khoảng đó.

> ⚠️ **Note:** chưa đọc phía client để biết nó xử lý `UNKNOWN_SERVER_ERROR` ra sao (retry hay ném lỗi), nên hậu quả lỗi 1 chưa rõ. Độ dài cửa sổ của lỗi 2 dưới tải production chưa đo.

**Phiên bản gặp lỗi:** `main`, mã gây lỗi có trong 1.0.0.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`.** PR [#4758](https://github.com/apache/fluss/pull/4758) đã merge (commit `185d1ff`, 2026-11-04), nhưng không có trong `v1.0.0` và `release-1.0`. bản tổng hợp ban đầu ghi "chưa merge" là cũ.

**Giảm thiểu:** không drop rồi tạo lại bảng cùng tên. Dùng tên có version (`profile_v2`).

#### [#3423](https://github.com/apache/fluss/issues/3423): Sau `ADD COLUMN`, đọc log có chiếu cột lỗi trên các batch dữ liệu cũ

**Vấn đề:**
- Batch log trên đĩa được ghi theo schema lúc ghi. Sau `ADD COLUMN`, batch cũ có ít cột hơn schema hiện tại.
- Fetch có projection chứa chỉ số cột mới thì `FileLogProjection` ở server thấy chỉ số vượt số cột của batch và trả `INVALID_COLUMN_PROJECTION`.
- **Batch read bảng PK của Flink luôn chiếu toàn bộ cột của schema hiện tại** (`ProjectionPlan`), kể cả khi người dùng không chọn cột nào, nên vừa `ADD COLUMN` xong là lỗi ngay (xác nhận trên 1.0.0 bởi bình luận của ymkim1006).
- Lỗi tự hết khi snapshot KV kế tiếp vượt qua các batch cũ (tối đa khoảng một chu kỳ `kv.snapshot.interval`, mặc định 10 phút).

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng vừa `ALTER TABLE ... ADD COLUMN`.
2. Vùng log cần đọc **vẫn chứa batch ghi theo schema cũ**: với batch read, chưa có snapshot KV mới sau điểm ADD COLUMN; với đọc log, dữ liệu cũ chưa hết hạn.
3. Fetch có projection chứa chỉ số cột mới, qua một trong hai đường:
   - **batch read bảng PK của Flink** (`FlussSource.setBatch()` + `OffsetsInitializer.full()`), không cần chọn cột: chắc chắn lỗi;
   - **đọc log có chọn cột mới** (mô tả gốc của issue).

> ⚠️ **Note:** nhánh "đọc log có chọn cột mới" suy ra từ mã, chưa tái hiện riêng. Streaming không projection được cho là không bị (theo bình luận), chưa kiểm chứng.

**Phiên bản gặp lỗi:** 0.9.0. **Đã có người gặp lại trên 1.0.0** và `main`.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4927](https://github.com/apache/fluss/pull/4927) đang mở. Hướng sửa (server hay client điền NULL) chưa thống nhất.

**Giảm thiểu:** sau `ADD COLUMN`, chờ một chu kỳ snapshot KV rồi mới chạy batch read (đối soát, export, backfill). Test schema evolution trước khi áp dụng production.

#### [#4302](https://github.com/apache/fluss/issues/4302): Java writer không chặn vi phạm NOT NULL, null bị ghi thành `""`/`0`

**Vấn đề:**
- Writer Java (một bảng và nhiều bảng) nhận giá trị **null thật** ở cột `NOT NULL` cấp cao nhất mà không từ chối trước khi encode hoặc gửi.
- Trên đường re-encode, null `STRING` thành `""`, null `INT` thành `0`. Định dạng binary khớp thì bỏ qua re-encode và gửi thẳng dòng sai. Arrow append cũng thiếu kiểm tra ở đầu vào writer.
- Delete cũng vậy với cột khóa chính: dòng có khóa null có thể tới `WriterClient.send`.
- Gốc rễ: getter giả định cột NOT NULL không null nên bỏ qua kiểm tra null.

**Điều kiện kích hoạt (cần đồng thời):**
1. Ghi bằng **Java client trực tiếp** (append, upsert hoặc delete), không qua Flink SQL.
2. Schema đích có cột `NOT NULL`.
3. Dòng đầu vào được dựng với **kiểu nullable** (ví dụ `BinaryRow`/`GenericRow` từ nguồn khác) và có **null thật** ở cột đó.

Flink sink thường đã được chặn trước bởi `table.exec.sink.not-null-enforcer` của Flink (mặc định `ERROR`).

> ⚠️ **Note:** chưa đọc mã 1.0.0 cho issue này (báo trên `main`, bản tổng hợp ban đầu ghi mã có trong 1.0.0). Chưa kiểm tra Flink `DataStream` API (`FlussSink` với `RowData` tự dựng) có đi qua not-null-enforcer hay không.

**Phiên bản gặp lỗi:** `main` (mã có trong 1.0.0).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4305](https://github.com/apache/fluss/pull/4305) đang mở.

**Giảm thiểu:** validate NOT NULL ở tầng ứng dụng trước khi gọi writer. Với Flink SQL, giữ `table.exec.sink.not-null-enforcer = ERROR`.

#### [#3362](https://github.com/apache/fluss/issues/3362): Writer không dọn thông tin partition cũ, CPU tăng dần theo thời gian chạy

**Vấn đề:**
- Mỗi partition mới được ghi tạo một mục trong bộ đệm của writer và metadata của client. Partition hết hạn và bị xóa trên server thì **các mục này vẫn ở lại**.
- `Sender` mỗi vòng lặp **duyệt toàn bộ** các mục, kể cả của partition đã chết, nên công việc mỗi vòng tăng dần theo số partition từng xuất hiện.
- Cách khắc phục duy nhất: restart job.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng có partition và job ghi **chạy rất lâu** (ngày, tuần, tháng) không restart.
2. Số partition writer từng ghi **tăng liên tục** (partition theo thời gian, partition cũ bị xóa dần). Bảng không partition hoặc số partition cố định thì không dính.
3. Mức độ tùy tốc độ sinh partition: theo ngày khoảng 365 mục/năm mỗi writer, theo giờ khoảng 8.760 mục/năm, theo phút nhiều hơn rất nhiều.

> ⚠️ **Note: mức ảnh hưởng mơ hồ.** Issue rất ngắn, không có log hay số đo CPU. Các con số trên là tính toán số học, không phải đo đạc. Chưa kiểm tra `Cluster` có tải sẵn toàn bộ partition của bảng khi khởi tạo hay không (nếu có thì bảng nhiều partition sẵn cũng dính).

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** Không có PR riêng, nằm trong umbrella #4980.

**Giảm thiểu:** partition theo ngày thay vì giờ nếu được. Restart job sink định kỳ (thường đã có qua deploy). Theo dõi CPU của Sender thread.

#### [#594](https://github.com/apache/fluss/issues/594): Bão `FetchLogRequest` khi một TabletServer sập rồi lên lại

**Vấn đề:**
- Issue chỉ có một câu và một ảnh biểu đồ, kèm bình luận "tắt idempotence (`client.writer.enable-idempotence = false`) thì không xảy ra".
- Cơ chế khớp với cả hai dữ kiện (tìm được khi đọc mã 1.0.0): follower kiểm tra batch sequence khi ghi dữ liệu sao chép. Writer state trên follower lệch với dữ liệu leader thì ném `OutOfOrderSequenceException`/`DuplicateSequenceException`.
- Nhánh xử lý hai lỗi này **không hoãn bucket**. Follower chỉ gọi `truncateToLeaderEndOffsetSnapshot`, và nếu điều kiện cắt không thỏa (`snapshot != 0 && snapshot < localLEO`) thì không làm gì. Vòng fetch kế tiếp gửi lại **ngay** đúng offset cũ, gặp đúng lỗi cũ: vòng lặp **không có thời gian chờ**.
- Kiểm tra sequence chỉ áp dụng cho batch có writer ID, khớp với "tắt idempotence thì hết". Issue #1624 (đã đóng) mô tả đúng hiện tượng này.

**Điều kiện kích hoạt (cần đồng thời):**
1. Job ghi **bật idempotence** (mặc định).
2. Replication factor ≥ 2.
3. **Writer state trên follower lệch với log của leader** cho ít nhất một writer:
   - TabletServer sập hoặc restart rồi dựng lại writer state sai (#704, #705, #2188, đã sửa);
   - writer ID hết hạn ở hai server lệch thời điểm (#1624). Trường hợp follower hết hạn trước đã được #1639 sửa. Trường hợp **leader hết hạn trước** không gây bão fetch: leader từ chối batch trước khi ghi, client nhận lỗi, reset writer và có thể làm task Flink restart;
   - log follower và leader phân kỳ sau đổi leader (#4269, chưa sửa).
4. **Điều kiện cắt log không thỏa**, ví dụ `leaderEndOffsetSnapshot = 0` (bucket chưa từng đổi leader) hoặc LEO follower ≤ snapshot.

Vòng lặp dừng khi writer hết hạn trên cả hai phía (`server.writer-id.expiration-time`, mặc định 7 ngày), khi leader đổi, hoặc khi restart follower.

> ⚠️ **Note: điều kiện mơ hồ.** Issue không nêu nguyên nhân, không có log. Cơ chế trên là suy luận từ mã và chưa chứng minh đúng là nguyên nhân của #594. Mọi nguyên nhân gây lệch writer state đã biết đều đã được sửa trong 1.0.0, nên hiện chỉ còn #4269 hoặc nguyên nhân chưa biết.

**Phiên bản gặp lỗi:** `main` thời 0.6.

**Trạng thái trong 1.0.0: ❔ Chưa xác định.** Các nguyên nhân cụ thể đã sửa: #731, #732, #1386, #1639, #2188, #4013, #2075 (backoff khi `NOT_LEADER_OR_FOLLOWER`). Cơ chế khuếch đại (không backoff) và việc thiếu leader epoch cache (#673) **vẫn còn**. Không phải "như #2749": #2749 là fetch chờ hết 30 giây (ít request), còn #594 là lặp không chờ (rất nhiều request).

**Giảm thiểu:** alert theo số `FetchLogRequest`/giây và băng thông vào giữa các TabletServer. Thấy bão thì restart follower bị ảnh hưởng.

#### [#596](https://github.com/apache/fluss/issues/596): Log của bucket bị cắt về offset 0 khi một TabletServer gặp sự cố

**Vấn đề:**
- Issue chỉ có một câu và hai ảnh (không đọc được dạng chữ), kèm bình luận "tắt idempotence thì không xảy ra".
- Trong 1.0.0, follower cắt log ở ba chỗ: khi trở thành follower (`truncateToHighWatermark`, về HW của chính nó), khi lỗi out-of-range (`fetchOffsetAndTruncate`, về LEO của leader), khi lỗi sequence (`truncateToLeaderEndOffsetSnapshot`, đã có điều kiện chặn cắt về 0 từ #731).
- Hai tình huống có thể cắt **về 0** (suy luận từ mã):
  - **(A)** file `high-watermark-checkpoint` không có mục cho bucket, HW khởi tạo bằng 0, replica thành follower thì **cắt toàn bộ log**. Leader còn sống thì chỉ phải sao chép lại, không mất dữ liệu.
  - **(B)** replica có **log rỗng** (mất đĩa) nhưng vẫn trong ISR được bầu làm leader. Follower khác cắt về 0 theo leader: **mất dữ liệu thật**.

**Điều kiện kích hoạt:**
- **Tình huống (A), cần đồng thời:**
  1. TabletServer chết đột ngột;
  2. bucket chưa từng được checkpoint HW (tạo trong vòng khoảng 5 giây trước khi chết), hoặc file checkpoint mất/hỏng;
  3. sau khi lên lại, replica được giao làm follower.
- **Tình huống (B), cần đồng thời:**
  1. một TabletServer **mất dữ liệu local** (mất PV, xóa thư mục) rồi khởi động lại khi tên nó vẫn trong ISR;
  2. leader hiện tại chết hoặc bị chuyển, và replica log rỗng được bầu. Với replication factor 3 cần thêm các replica khác không được chọn, nên rất hiếm. Replication factor 2 dễ hơn.

> ⚠️ **Note: điều kiện mơ hồ.** Không có log dạng chữ, nên không biết (A), (B) hay đường khác là nguyên nhân thật của #596. Chi tiết "tắt idempotence thì hết" gợi ý đường lỗi sequence thời 0.5, đường này đã được #731 vá. Cả hai tình huống đều chưa tái hiện.

**Phiên bản gặp lỗi:** 0.5.0.

**Trạng thái trong 1.0.0: ❔ Chưa xác định.** Nhiều fix vùng này đã vào (#676, #731, #4013), không có báo cáo lặp lại. Hai đường (A), (B) vẫn còn. Giải pháp gốc là leader epoch cache (#673), chưa có.

**Giảm thiểu:** replication factor ≥ 3, `acks=-1`, PV bền. Sau khi mất đĩa một node, cân nhắc gỡ node đó khỏi ISR (chờ nó bắt kịp) trước khi để nó có cơ hội làm leader.

#### [#709](https://github.com/apache/fluss/issues/709): Tắt nhiều TabletServer thì client nhận hàng loạt `OutOfOrderSequenceException`

**Vấn đề:**
- #676 (để tránh mất dữ liệu khi đổi leader, #674) cho follower **tự đặt HW bằng LEO của nó** ngay sau khi ghi dữ liệu sao chép. Follower có thể "biết" nhiều batch hơn client.
- Chuỗi trong issue: follower đã ghi batch 3, 4; client chưa nhận ack nên nghĩ batch cuối là 2; leader và follower cùng chết; follower lên làm leader; batch 3 gửi lại gặp không khớp, nhận `OutOfOrderSequenceException`; client coi là lỗi không phục hồi, job failover.
- 1.0.0 đã thêm nhiều nhánh xử lý `OutOfOrderSequence` "giả" ở client (coi là đã thành công hoặc retry được). Nhưng không có dấu hiệu tiến triển thì client vẫn reset writer, batch thất bại, job Flink failover.

**Điều kiện kích hoạt (cần đồng thời):**
1. Ghi **bật idempotence** (mặc định), `acks=-1`.
2. **Nhiều TabletServer tắt hoặc chết cùng lúc**, gồm cả leader và một follower của cùng bucket; hoặc leader chết khi đang có batch in-flight mà follower đã nhận.
3. Có batch **đã tới follower mà client chưa nhận ack**. Với tối đa 5 batch in-flight mỗi bucket và tải cao, gần như luôn đúng.
4. Leader mới có writer state **không khớp** với client theo kiểu không rơi vào nhánh "trùng" hoặc "retry được" của client 1.0.0.

> ⚠️ **Note:** chưa lý giải được bước 5 của issue (vì sao leader mới trả `OutOfOrderSequence` cho batch 3 thay vì coi là trùng). Điều kiện 4 phụ thuộc vào logic client mới, chưa tái hiện trên 1.0.0. Tiêu đề nói clean shutdown nhưng nội dung nói kill. Với clean shutdown có controlled shutdown (chuyển leader trước khi tắt), nên ít xảy ra hơn.

**Phiên bản gặp lỗi:** `main` thời 0.6.

**Trạng thái trong 1.0.0: 🟡 Đã giảm thiểu phía client** (#2827, #2953, #4125). Gốc rễ phía server (follower HW = LEO, TODO #673) vẫn còn.

**Giảm thiểu:** rolling restart **từng node**, chờ ISR đầy đủ. Không tắt đồng thời nhiều TabletServer. Rải replica qua nhiều node/zone để một sự cố không giết cả leader và follower.

#### [#288](https://github.com/apache/fluss/issues/288): Partition mới được tạo làm job Flink đang đọc bị lỗi "Leader not found"

**Vấn đề:**
- Khi phát hiện partition mới, enumerator gọi `listOffsets` cho các bucket. Nếu leader chưa được bầu xong, `MetadataUpdater.leaderFor` hỏi metadata **3 lần liên tiếp không chờ** rồi ném `FlussRuntimeException("Leader not found after retry 3 times ...")`.
- Ở 0.5.0, lỗi này làm job failover ở mỗi partition mới.
- Ở 1.0.0, với job **streaming có partition discovery**, enumerator nuốt lỗi (log WARN `Failed to list splits for ...`) và thử lại ở lần discovery sau (mã có `TODO: fix me in #288`). Hậu quả còn lại: partition mới bị **đọc trễ ít nhất một chu kỳ discovery**.
- Job **bounded** (batch) thì vẫn thất bại.

**Điều kiện kích hoạt (cần đồng thời):**
1. Job Flink **đọc** bảng partition (log table hoặc changelog của bảng PK partition).
2. Có partition mới được tạo: auto-partition sang ngày/giờ mới, tạo tay hoặc tạo động khi ghi.
3. Discovery chạy **đúng lúc** bucket mới chưa có leader. Dễ xảy ra hơn khi cluster lớn, Coordinator bận, ZooKeeper follower chậm (#4522) hoặc replica bị đánh dấu offline (#3011).

> ⚠️ **Note:** điều kiện 3 phụ thuộc timing (khoảng cách giữa lúc tạo partition và lúc bầu leader xong), không có ngưỡng cụ thể. Chưa tái hiện.

**Phiên bản gặp lỗi:** 0.5.0.

**Trạng thái trong 1.0.0: 🟡 Đã giảm thiểu** cho Flink streaming. Phía client vẫn chỉ thử 3 lần không backoff. PR [#479](https://github.com/apache/fluss/pull/479) đã bị đóng. Lý do giảm thiểu là enumerator nuốt lỗi, không phải #1633 như bản tổng hợp ban đầu ghi.

**Giảm thiểu:** pre-create partition (`table.auto-partition.num-precreate` ≥ 2) để partition có leader từ trước khi có dữ liệu. Với job batch, retry ở tầng scheduler.

#### [#5038](https://github.com/apache/fluss/issues/5038): Metric Coordinator bị "đóng băng" sau khi mất rồi lấy lại leadership

**Vấn đề:**
- `CoordinatorEventManager` (tạo mới mỗi nhiệm kỳ) đăng ký các gauge (`activeCoordinatorCount`, `activeTabletServerCount`, `offlineBucketCount`, `tableCount`, `bucketCount`, ...) vào `CoordinatorMetricGroup` (tạo một lần cho cả process).
- Nhiệm kỳ kết thúc thì manager bị đóng nhưng **gauge không được gỡ**. Nhiệm kỳ sau đăng ký gauge cùng tên, `AbstractMetricGroup.addMetric` **giữ gauge cũ**, bỏ gauge mới.
- Reporter đọc gauge của manager đã chết, nên giá trị **đứng yên**. Ví dụ: tăng TabletServer 9 → 12, `activeTabletServerCount` vẫn báo 9.
- **Hệ quả thêm (suy ra từ mã):** `activeCoordinatorCount` là `() -> 1`, không bị gỡ khi mất leadership, nên Coordinator **đã mất leader** (kể cả kẹt như #4362) vẫn có thể báo `activeCoordinatorCount = 1`.

**Điều kiện kích hoạt:**
1. Một Coordinator process **đã từng là leader**, mất leadership, rồi **lấy lại** mà không restart: mất ZK session do GC hoặc mạng rồi kết nối lại, leader election xoay vòng giữa các Coordinator.
2. Riêng hệ quả `activeCoordinatorCount = 1` sai: chỉ cần process **đã từng là leader** rồi mất leadership (không cần lấy lại).

Coordinator standby chưa từng làm leader thì không dính.

> ⚠️ **Note:** hệ quả `activeCoordinatorCount` là suy luận, chưa tái hiện, phụ thuộc reporter (Prometheus, JMX) có tiếp tục đọc gauge của group chưa đóng hay không. Chưa đọc diff #5039 để biết nó có xử lý riêng gauge này.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`.** Issue đã đóng nhờ [#5039](https://github.com/apache/fluss/pull/5039) (merge 04/11/2026, commit `a2e0c769`). Chưa backport.

**Giảm thiểu:** không dùng `activeCoordinatorCount` làm tín hiệu duy nhất cho "có Coordinator leader", kiểm tra node leader trong ZooKeeper hoặc gọi Admin API định kỳ. Thấy log `"Cleaning up coordinator leader services."` rồi lên leader lại trong cùng process thì restart Coordinator đó. Cân nhắc cherry-pick #5039 (thay đổi nhỏ).

#### [#4139](https://github.com/apache/fluss/issues/4139): Histogram không loại mẫu cũ, percentile P99 "đứng" ở giá trị lỗi thời

**Vấn đề:**
- `DescriptiveStatisticsHistogram` giữ bộ đệm vòng kích thước cố định (1024 mẫu), **không quan tâm thời gian**: mẫu cũ chỉ bị thay khi có mẫu mới.
- Một đợt chậm ngắn trên metric ít mẫu giữ P99 cao hàng giờ. Ví dụ trong issue: P99 = 2.000 ms cần thêm 1.015 mẫu bình thường mới xuống dưới 1 giây, khoảng **9,4 giờ** ở tốc độ 1,8 mẫu/phút.
- Metric **ngừng nhận mẫu** thì giữ giá trị cũ mãi tới khi restart. `_count` vẫn tăng, alert P99 bắn dù server khỏe.

**Điều kiện kích hoạt:** luôn có với mọi histogram dùng lớp này, nhưng **lộ rõ** khi:
1. Metric **ít mẫu**: flush log/KV trên cluster ít tải, loại request hiếm, bảng ít truy cập.
2. Vừa có **một đợt chậm ngắn**: GC dài, đĩa chậm, sự cố mạng.
3. Metric **ngừng nhận mẫu** (bảng không còn được ghi).

Metric nhiều mẫu (hàng nghìn request/giây) thì 1024 mẫu bị thay trong chưa tới một giây, gần như không lộ.

**Phiên bản gặp lỗi:** `main`, có từ ít nhất 0.9.1.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`** bằng [#4829](https://github.com/apache/fluss/pull/4829) (29/10/2026, "Expire idle histogram samples across reporters"). PR gắn issue [#4167](https://github.com/apache/fluss/pull/4167) vẫn mở.

**Giảm thiểu:** alert độ trễ dựa thêm vào metric phía client. Không alert P99 trên metric ít mẫu.

#### [#2076](https://github.com/apache/fluss/issues/2076): Helm chart chỉ quảng bá địa chỉ CLIENT, địa chỉ INTERNAL dùng Pod IP thô

**Vấn đề:**
- Chart đặt `bind.listeners` là `INTERNAL://${POD_IP}:9123, CLIENT://${POD_IP}:9124`, còn `advertised.listeners` **chỉ có** `CLIENT://<pod>.tablet-server-hs.<ns>.svc.cluster.local:9124`.
- `Endpoint.loadAdvertisedEndpoints` dùng địa chỉ bind cho listener không được quảng bá, nên địa chỉ **INTERNAL** (server gọi nhau) là **Pod IP**, đổi mỗi lần pod bị tạo lại.
- Địa chỉ này do **chart hard-code** trong `command:` và được ghi vào cuối `server.yaml`, nên `configurationOverrides` trong `values.yaml` **không ghi đè được**.
- Một người dùng 1.0.0 (bình luận ở #4256) thấy replica fetcher gọi IP cũ khoảng **10 phút** sau khi TabletServer restart, server đó nằm ngoài ISR trong lúc ấy. Đường cập nhật metadata thực ra chỉ mất vài giây (ZooKeeper → Coordinator → `UpdateMetadataRequest`), nên 10 phút nhiều khả năng do cache kết nối theo server ID (#4256), không do metadata cũ.

**Điều kiện kích hoạt:**
1. Triển khai bằng **Helm chart chính thức** trên Kubernetes.
2. Pod TabletServer hoặc Coordinator **bị tạo lại và nhận Pod IP mới**: rolling upgrade, restart, pod bị đẩy sang node khác, node hỏng.

> ⚠️ **Note:** người báo không nêu hậu quả cụ thể và chưa trả lời câu hỏi của maintainer. Hậu quả "replication chậm 10 phút" là quan sát ở #4256, **chưa chứng minh** được là do quảng bá Pod IP. Ảnh hưởng khi pod Coordinator đổi IP chưa kiểm tra.

**Phiên bản gặp lỗi:** 0.8.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi, cả `main`.** bản tổng hợp ban đầu ghi ⏭️ là sai: #4108 chỉ thêm listener EXTERNAL cho client ngoài cluster, không thêm INTERNAL.

**Giảm thiểu:** patch chart (hoặc dùng post-renderer) để quảng bá thêm `INTERNAL://<pod>.<headless-svc>...:9123`. Có thể cần `publishNotReadyAddresses: true` trên headless service. Rolling upgrade từng node, chờ ISR đầy đủ.

#### [#4522](https://github.com/apache/fluss/issues/4522): Bucket offline khoảng 60 giây ngay sau khi tạo bảng vì đọc phải ZooKeeper follower chưa cập nhật

**Vấn đề:**
1. Coordinator ghi schema vào ZooKeeper, ghi phân bổ bucket, rồi gửi `NotifyLeaderAndIsr`.
2. TabletServer tạo replica bằng cách **đọc schema từ ZooKeeper** mà không gọi `sync`. Nếu session nối với **ZK follower chưa áp dụng** lệnh ghi, đọc không thấy schema, tạo replica lỗi `SchemaNotExistException`.
3. TabletServer không tự thử lại. Coordinator đánh dấu replica **offline**.
4. Nếu **mọi** replica của bucket đều lỗi, Coordinator chờ `coordinator.offline-leader.retry-delay` (60 giây) mới thử lại.

Log trong issue: schema ghi lúc `03:27:14.831`, ba TabletServer lỗi bucket 1 lúc `.886`, bucket 1 có leader lúc `03:28:14.922`.

**Điều kiện kích hoạt (cần đồng thời):**
1. **ZooKeeper nhiều node** (bình thường trong production).
2. Server Fluss nối ZK qua địa chỉ chung (K8s Service, load balancer) hoặc danh sách nhiều node, nên session của Coordinator và TabletServer có thể nằm trên **ZK node khác nhau**.
3. **Ngay sau `CREATE TABLE`**: khoảng cách giữa ghi schema và đọc chỉ vài chục mili giây.
4. **Mọi replica** của một bucket cùng đọc trượt thì mới mất leader. Chỉ một vài replica trượt thì bucket thiếu replica khoảng 60 giây.

> ⚠️ **Note:** chưa kiểm tra việc tạo **partition mới** (auto-partition) có đọc nút ZK vừa được ghi hay không. Schema thì đã có từ lâu nên không trượt.

**Phiên bản gặp lỗi:** `main` (mã giống 1.0.0).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4524](https://github.com/apache/fluss/pull/4524) đang mở. Test của dự án chỉ chạy một ZK node nên không bắt được.

**Giảm thiểu:** sau `CREATE TABLE`, chờ mọi bucket có leader rồi mới start job. Có thể giảm `coordinator.offline-leader.retry-delay`.

#### [#4511](https://github.com/apache/fluss/issues/4511): Codec Zstd rò rỉ off-heap buffer khi nén/giải nén ném exception

**Vấn đề:**
- Cả hai codec (`ZstdArrowCompressionCodec`, `UnshadedZstdArrowCompressionCodec`) **cấp buffer đầu ra trước** rồi mới gọi Zstd.
- Buffer chỉ được `close()` khi Zstd **trả mã lỗi** hoặc (giải nén) độ dài không khớp. Nếu Zstd **ném exception**, buffer không bao giờ được giải phóng.
- Mỗi lần rò một buffer off-heap cỡ dữ liệu nén/giải nén của một cột trong một batch. Xảy ra cả đường ghi (client, WAL server) lẫn đường đọc.

**Điều kiện kích hoạt (cần đồng thời):**
1. Dùng compression **Zstd** (mặc định của Arrow log).
2. `zstd-jni` **ném exception** khi nén hoặc giải nén. Issue quan sát `ZstdException: Operation not authorized at current processing stage` khi Flink sink ghi. Các khả năng khác: dữ liệu nén hỏng, lỗi thư viện native.

> ⚠️ **Note: điều kiện mơ hồ.** Không biết điều kiện gây ra `ZstdException` trong issue; người báo nói cần điều tra riêng. Đây mới là thứ làm job thất bại, còn rò buffer là hậu quả phụ. Chưa đo lượng rò thực tế.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4513](https://github.com/apache/fluss/pull/4513) đang mở.

**Giảm thiểu:** theo dõi direct memory của TabletServer và TaskManager. Alert khi log có `ZstdException`.

#### [#513](https://github.com/apache/fluss/issues/513): `flushKv` và `updateHighWatermark` không nguyên tử, lo có khe hở "thấy log trước khi thấy KV"

**Vấn đề:**
- #179 (có trong 1.0.0) yêu cầu **KV hiển thị trước changelog**: ai đọc changelog thấy bản ghi X thì lookup key của X phải thấy giá trị tương ứng. #513 lo rằng hai bước flush KV và tăng HW không nguyên tử.
- Trong 1.0.0, mã giữ bất biến **HW ≤ `flushedLogOffset`**: HW chỉ được đẩy tới offset KV đã flush xong, phần chưa flush thì flush bất đồng bộ và đợi lần sau (`onKvFlushComplete` mới tăng HW). Lookup từ bên ngoài chỉ đọc RocksDB. Theo mã, khe hở **không còn** trên leader.
- Hệ quả có ích: với `acks=-1`, khi Flink nhận ack thì lookup của AI agent đã thấy giá trị mới.

**Điều kiện kích hoạt (theo issue):** ứng dụng đọc changelog rồi **lookup ngay** key vừa thấy (mẫu delta join).

> ⚠️ **Note: điều kiện mơ hồ.** Issue chỉ nêu mối lo thiết kế, không có kịch bản cụ thể. Theo mã 1.0.0 **không tìm thấy** kịch bản lỗi trên leader. Chưa rà các trường hợp: **chuyển leader** (HW và `flushedLogOffset` của leader mới khi vừa lên) và **historical partition**. Với `acks=1`, hoặc đọc khi chưa có ack, chưa thấy giá trị mới là hành vi bình thường, không phải lỗi.

**Phiên bản gặp lỗi:** `main` thời 0.6.

**Trạng thái trong 1.0.0: 🟡** (gần như ✅ cho đường leader). Issue chưa đóng, chưa có test xác nhận.

**Giảm thiểu:** dùng `acks=-1`. Nếu AI agent cần read-your-write, chỉ coi dữ liệu là đã ghi khi Flink sink đã nhận ack (sau checkpoint).

### 🟢 Thấp

#### [#311](https://github.com/apache/fluss/issues/311): Timeout cập nhật metadata quá ngắn khiến job ghi báo nhầm "partition không tồn tại"

**Vấn đề:**
- Ở 0.5, client cập nhật metadata partition với timeout **3 giây**. Job lớn (512 bucket, 512 sink song song) dễ vượt 3 giây. Lỗi timeout chỉ được log, bước sau kết luận partition **không tồn tại**, job failover.
- Ở 1.0.0:
  - timeout là **30 giây** (hằng số trong `MetadataUtils`, không cấu hình được);
  - lỗi timeout và lỗi retriable **vẫn bị nuốt** trong `MetadataUpdater.updateMetadata`;
  - đường ghi vào partition đi qua `DynamicPartitionCreator` (bật mặc định): nếu tưởng partition chưa có thì gọi tạo với `ignoreIfExists = true`, nên báo nhầm thường **không còn làm job lỗi**.
- Việc nuốt lỗi timeout là nguyên nhân gốc của #4928.

**Điều kiện kích hoạt:**
1. Cập nhật metadata **quá 30 giây**: TabletServer phục vụ metadata quá tải, ZooKeeper chậm (#3466), hoặc rất nhiều client cùng hỏi (job parallelism lớn khởi động cùng lúc).
2. Và hậu quả chỉ lộ ra khi rơi vào một trong các nhánh:
   - **tắt** dynamic partition creation;
   - không server nào trả lời, client rơi vào `initializeCluster` từ bootstrap;
   - đường gọi không qua `DynamicPartitionCreator`: lookup bảng partition (#2209) hoặc `getTable` (#4928).

**Phiên bản gặp lỗi:** `main` thời 0.5.

**Trạng thái trong 1.0.0: ✅ trên thực tế cho đường ghi, nhưng nên coi là 🟡.** Yêu cầu thứ hai của issue ("ném TimeoutException thay vì chỉ log") chưa làm.

**Giảm thiểu:** giữ dynamic partition creation bật. Pre-create partition. Tránh khởi động đồng thời quá nhiều job parallelism lớn.

#### [#1044](https://github.com/apache/fluss/issues/1044): Buffer của request có thể bị release nhiều lần trên server RPC

**Vấn đề:**
- Có ba nơi giải phóng buffer: `RequestProcessor.run` (luôn release trong `finally`), `NettyServerHandler.sendResponse` (release nếu request bị hủy hoặc kênh không còn active), `NettyServerHandler.channelRead` (release nếu kênh không active hoặc parse lỗi).
- Không có gì đảm bảo mỗi buffer chỉ release một lần, nên cùng một buffer vẫn có thể bị release hai lần.
- Từ trước 1.0.0, `FlussRequest.releaseBuffer()` dùng `ReferenceCountUtil.safeRelease`, nên lần thứ hai **chỉ log cảnh báo**, không crash.

**Điều kiện kích hoạt:**
1. Client **ngắt kết nối đúng lúc** server đang xử lý request của nó, hoặc gửi request khi kênh đang đóng. Hay gặp khi client timeout rồi đóng kết nối, job Flink bị kill, mạng chập chờn hoặc rolling restart client.
2. Request thuộc loại **lazily parsed** (produce log, put KV). Loại khác thì `releaseBuffer` không làm gì.

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: ✅ không còn crash, nhưng nên coi là 🟡.** Release hai lần vẫn xảy ra, chỉ là được nuốt. Mục tiêu thiết kế của issue (mỗi buffer chỉ release một lần) chưa được giải quyết. Chưa kiểm tra nguy cơ use-after-free (buffer bị release trước khi xử lý bất đồng bộ đọc xong).

**Giảm thiểu:** không cần hành động. Có thể thấy log warning về reference count khi client ngắt kết nối.

#### [#4516](https://github.com/apache/fluss/issues/4516): Coordinator coi việc xóa replica là xong dù TabletServer chưa xác nhận

**Vấn đề:** khi xử lý `StopReplicaResponse`, Coordinator có hai chỗ sai:
1. **Chỉ duyệt bucket có trong phản hồi.** Bucket được hỏi nhưng không có trong phản hồi thì không được ghi nhận gì, replica kẹt ở `ReplicaDeletionStarted` (như #3357).
2. **Xóa thất bại được thử tối đa 5 lần rồi bị đánh dấu thành công**, dù TabletServer chưa xác nhận. Bảng/partition "xóa xong" trong khi replica vẫn còn trên một TabletServer, có thể vẫn **online**.

**Điều kiện kích hoạt:**
1. Drop table hoặc drop partition, **kể cả auto-partition hết hạn** (xảy ra mỗi chu kỳ).
2. Và một trong hai:
   - **Chỗ sai 1:** phản hồi thiếu bucket. Trong 1.0.0, mọi nhánh của `stopReplicas` đều `result.add(...)`, nên có lẽ chỉ xảy ra khi lệch phiên bản hoặc có lỗi khác.
   - **Chỗ sai 2:** xóa thất bại **5 lần liên tiếp** cho cùng replica: đĩa TabletServer hỏng (replica offline), TabletServer có epoch mới hơn Coordinator (sau đổi leader hoặc Coordinator failover), lỗi quyền hoặc I/O khi xóa thư mục.

> ⚠️ **Note:** chưa tìm được đường cụ thể trong 1.0.0 làm phản hồi thiếu bucket (chỗ sai 1). Chưa tái hiện.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`). PR [#4517](https://github.com/apache/fluss/pull/4517) đang mở.

**Giảm thiểu:** kiểm tra định kỳ thư mục replica rác trên TabletServer so với metadata. Tránh Coordinator failover trùng giờ partition hết hạn.

#### [#3357](https://github.com/apache/fluss/issues/3357): Xóa bảng kẹt vĩnh viễn khi `StopReplica` không gửi tới được TabletServer

**Vấn đề:**
- Khi drop bảng, Coordinator chuyển replica sang `ReplicaDeletionStarted` và gửi `StopReplicaRequest(delete = true)`.
- Nếu request **không gửi tới được**, callback chỉ log WARN rồi `return`: không sinh sự kiện, không đổi trạng thái. Replica kẹt ở `ReplicaDeletionStarted`, không có đường nào đưa ra khỏi trạng thái này.
- Dấu hiệu: `fluss_coordinator_tableCount` cao hơn số bảng thật trong ZooKeeper. Cách thoát: restart Coordinator (#1629).
- 1.0.0 có khung retry theo TabletServer (#3391), nhưng `enqueueRequest` của khung này **không có nơi nào gọi** cho StopReplica.

**Điều kiện kích hoạt:** drop bảng hoặc partition (**kể cả auto-partition hết hạn**, xảy ra mỗi chu kỳ) đúng lúc một TabletServer giữ replica của nó đang restart/rolling upgrade, mất mạng tạm thời, hoặc vừa chết.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`). PR [#3369](https://github.com/apache/fluss/pull/3369) đã **bị đóng**, không merge. Không phải "DDL hiếm" với bảng auto-partition.

**Giảm thiểu:** tránh rolling restart trùng giờ partition hết hạn. Alert khi `tableCount` lệch với số bảng thật. Restart Coordinator để thoát.

#### [#1629](https://github.com/apache/fluss/issues/1629): Drop bảng hoặc partition khi một TabletServer đang crash thì để lại replica rác

**Vấn đề:**
- Khi drop bảng hoặc partition, nếu TabletServer giữ replica crash hoặc restart giữa chừng, nó không nhận được `StopReplicaRequest` và không phản hồi.
- Trong 1.0.0, Coordinator **bỏ qua** các replica đang chờ xóa cả khi server **chết** lẫn khi server **quay lại**. Không có retry hay quét định kỳ, nên replica trên server đó **không bao giờ bị xóa**.
- Restart Coordinator thì phần dư được dọn (theo người báo).

**Điều kiện kích hoạt:**
1. Drop bảng hoặc partition, **kể cả auto-partition hết hạn** (xảy ra định kỳ, không chỉ DDL tay).
2. Đúng lúc đó, một TabletServer giữ replica của bảng/partition đang crash, restart hoặc chưa đăng ký lại.

**Phiên bản gặp lỗi:** `main` thời 0.8.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`** bằng [#4505](https://github.com/apache/fluss/pull/4505) (15/10/2026, "Resume pending replica deletion after server restart"). bản tổng hợp ban đầu ghi ❔ là chưa đúng.

**Giảm thiểu:** tránh để partition hết hạn trùng giờ rolling restart. Sau sự cố TabletServer, kiểm tra dung lượng đĩa và restart Coordinator nếu thấy thư mục replica rác.

#### [#4599](https://github.com/apache/fluss/issues/4599): KV snapshot định kỳ "đua" với việc xóa replica, gây lỗi `registry is already closed`

**Vấn đề:**
- `Replica.dropKv()` đóng `closeableRegistryForKv`. Registry đặt `closed = true` **trước**, rồi mới đóng lần lượt tài nguyên, trong đó có `PeriodicSnapshotManager`.
- Snapshot đã lên lịch **bắt đầu chạy đúng trong khe hở này** thì đăng ký tài nguyên vào registry đã đóng và nhận `IOException`.
- `PeriodicSnapshotManager.close()` chỉ hủy future, không đồng bộ với bước khởi tạo snapshot đang chạy dưới `kvLock`.
- Người báo thấy 4 lần trong test. Xóa vẫn hoàn tất, chưa thấy hỏng snapshot. Fix của #4489 (#4492) không giải quyết cuộc đua này.

**Điều kiện kích hoạt:**
1. Drop bảng PK hoặc partition (kể cả partition hết hạn tự động) **đúng lúc** snapshot định kỳ của bucket đang bắt đầu.
2. **Suy luận thêm:** bucket bảng PK **đổi leader** (leader cũ thành follower, cũng gọi `dropKv()`) đúng lúc nó đang bắt đầu snapshot.

Khe hở rất nhỏ, nhưng xảy ra trên mọi bucket mỗi lần xóa hoặc đổi leader.

> ⚠️ **Note:** trường hợp đổi leader là suy luận từ các chỗ gọi `dropKv()`, chưa tái hiện. Log lỗi này **trùng nội dung** với dấu hiệu của #4489 (`Fail to init snapshot during triggering snapshot.`), nên dễ gây alert nhầm.

**Phiên bản gặp lỗi:** `main` (mã 1.0.0 cùng cấu trúc).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** Không có PR.

**Giảm thiểu:** không cần hành động. Khi alert theo log #4489, phân biệt bằng việc bucket có đang bị xóa hoặc đổi leader không, và snapshot có tiến triển sau đó không.

#### [#1256](https://github.com/apache/fluss/issues/1256): Client nhận exception khó hiểu khi đọc partition đang bị xóa

**Vấn đề:** Fluss chưa có cơ chế khóa đảm bảo DDL nguyên tử, nên thao tác đọc có thể thấy trạng thái nửa vời khi partition đang bị xóa. Lỗi nhận được phụ thuộc thời điểm:
1. **Lúc subscribe hoặc cập nhật metadata:** nhận `PartitionNotExistException`, là lỗi rõ ràng. 1.0.0 đã xử lý ở Flink source (bỏ split) và log fetcher (bỏ qua).
2. **Đang fetch từ local log:** nhận `LOG_OFFSET_OUT_OF_RANGE`, client ném `FetchException`, **job Flink lỗi** mà không biết lý do là partition bị xóa.
3. **Đang tải segment từ remote:** file đã bị xóa. `RemoteLogDownloader` của 1.0.0 **đưa request lại hàng đợi và thử lại không giới hạn**, mỗi lần log ERROR. Bucket kẹt tới khi partition discovery nhận ra partition đã bị xóa.

**Điều kiện kích hoạt:**
1. Một job hoặc ứng dụng **đang đọc** partition **đúng lúc** partition đó bị xóa. Hay gặp nhất: job đọc chậm (backfill, vừa restore) trên partition cũ sắp hết hạn, đúng lúc auto-partition xóa nó; hoặc xóa partition bằng tay khi job đang đọc.
2. Trường hợp 3 cần thêm: partition đã có segment trên **remote storage** và client đang đọc từ remote (đọc dữ liệu cũ).

**Phiên bản gặp lỗi:** 0.7.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** cho hai trường hợp 2 và 3. PR [#1257](https://github.com/apache/fluss/pull/1257) đã **bị đóng, không merge**. Lỗi tải remote thử lại vô hạn chỉ được sửa trên `main` (#1752).

**Giảm thiểu:** để retention partition dài hơn độ trễ tối đa của job đọc. Không xóa partition bằng tay khi còn job đọc nó.

#### [#1819](https://github.com/apache/fluss/issues/1819): Cùng một giá trị `TIME`, lookup và scan trả kết quả khác nhau

**Vấn đề:**
- `TIME()` mặc định là `TIME(0)`, được ánh xạ sang Arrow `Time(SECOND)`. Khi ghi log, `ArrowTimeWriter` **chia 1000** số mili giây, nên scan log trả `36610000` thay vì `36610123`.
- Đường ghi KV lưu nguyên số mili giây, nên lookup trả `36610123`. Hai đường đọc **lệch nhau**.
- Gốc rễ: client được ghi giá trị chính xác hơn kiểu khai báo mà không bị chuẩn hóa.

**Điều kiện kích hoạt (cần đồng thời):**
1. Cột kiểu **`TIME` độ chính xác 0** (`TIME()` hoặc `TIME(0)`, là mặc định).
2. Giá trị ghi vào có **phần mili giây khác 0**.
3. Đọc cùng dữ liệu qua **hai đường**: lookup (KV) và scan log/changelog (Arrow). Bảng log thuần chỉ thấy "mất mili giây", không thấy lệch.

> ⚠️ **Note:** chỉ chắc chắn khi ghi trực tiếp bằng Java client (như test trong issue). Với Flink SQL, Flink thường làm tròn `TIME(0)` về giây trước khi ghi, nên có thể không gặp. Chưa kiểm chứng.

**Phiên bản gặp lỗi:** `main` thời 0.8.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#1822](https://github.com/apache/fluss/pull/1822) đang mở.

**Giảm thiểu:** khai báo `TIME(3)` (dùng `TimeMilliVector`, log và KV khớp), hoặc dùng `TIMESTAMP`.

#### [#4295](https://github.com/apache/fluss/issues/4295): Khóa chính TIMESTAMP trước 1970 không giải mã được ở bộ giải mã khóa Iceberg

**Vấn đề:**
- `IcebergKeyDecoder` tách micro giây bằng `micros / 1000` và `micros % 1000`. Với số âm, `%` cho phần dư âm, phần nano nằm ngoài 0–999.999 mà `TimestampNtz` yêu cầu, nên ném `IllegalArgumentException`.
- Ví dụ: `1969-12-31 23:59:59.999999` (−1 µs) lỗi, còn −1.000 µs thì không.
- Decoder này **chỉ** được dùng ở đường **lookup kèm insert-if-not-exists** (`KeyRecordBatch`) cho bảng lake format Iceberg.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng PK có `table.datalake.format = iceberg`, mã hóa khóa kiểu Iceberg.
2. Khóa chính có cột `TIMESTAMP` độ chính xác micro giây.
3. Giá trị **trước 1970** và phần micro giây **không chia hết cho 1000**.
4. Dùng **lookup kèm insert-if-not-exists** với khóa chưa tồn tại.

**Phiên bản gặp lỗi:** `main`.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã fix trên `main`.** Issue đã đóng nhờ [#4296](https://github.com/apache/fluss/pull/4296) (merge 06/11/2026, commit `a79175e3`, dùng `TimestampNtz.fromMicros()`). bản tổng hợp ban đầu ghi "PR chưa merge" là cũ. Phạm vi hẹp, nên chuyển sang **nhóm B (Iceberg)**.

**Giảm thiểu:** không dùng `TIMESTAMP(6)` trước 1970 làm khóa chính trên bảng Iceberg. CDP gần như không gặp.

#### [#4548](https://github.com/apache/fluss/issues/4548): Auto-partition từ chối mọi partition QUARTER nhưng chấp nhận ngày không tồn tại

**Vấn đề:** `isValidPartitionTime` chỉ gọi `DateTimeFormatter.ofPattern(format).parse(time)`. Có hai lỗi:
1. **QUARTER không bao giờ hợp lệ.** Mẫu `yyyyQ` không phân tích được `20264`, vì trường năm độ dài thay đổi "ăn" hết chữ số. Kể cả quý do chính Fluss sinh ra cũng bị từ chối.
2. **Ngày không tồn tại được chấp nhận** với resolver SMART: `20240230`, `20230229`, `20240431`, giờ `2024010124` đều qua, nên có thể tạo partition sai tên.

Hàm này cũng từ chối partition **cũ hơn retention**, so theo đồng hồ của máy chạy client (liên quan #4726).

**Điều kiện kích hoạt:**
- **Lỗi 1:** bảng `table.auto-partition.time-unit = QUARTER`, và một trong các thao tác: tạo partition bằng tay (`Admin#createPartition`); ghi vào partition chưa tạo sẵn (dynamic partition creation lỗi); lookup partition quý đã qua (không đi đường partition lịch sử).
- **Lỗi 2:** áp dụng cho **mọi** đơn vị (cả DAY, HOUR). Tạo partition bằng tay hoặc ghi dữ liệu có giá trị partition **sai lịch** vào bảng auto-partition.

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4549](https://github.com/apache/fluss/pull/4549) đang mở.

**Giảm thiểu:** không dùng QUARTER (dùng MONTH). Validate giá trị cột partition trong Flink trước khi sink.

#### [#4538](https://github.com/apache/fluss/issues/4538): `Decimal#hashCode` không nhất quán với `Decimal#equals`

**Vấn đề:**
- `Decimal.equals` so theo **giá trị số** (`1.0` bằng `1.00`), nhưng `hashCode` trả `toBigDecimal().hashCode()`, **phụ thuộc scale** (311 và 3102 trong ví dụ).
- Cho `1.0` vào `HashSet` rồi tìm `1.00` sẽ không thấy.
- Các bộ chuyển đổi chính thức (POJO, Flink) chuẩn hóa scale theo kiểu cột, nên trong luồng dữ liệu bình thường lỗi khó lộ.

**Điều kiện kích hoạt:** hai `Decimal` **bằng giá trị nhưng khác scale** nằm chung trong cấu trúc dựa trên hash:
- code ứng dụng tự tạo `Decimal.fromBigDecimal(...)` với scale khác nhau rồi dùng làm khóa `HashMap`/`HashSet`;
- so sánh hoặc khử trùng giá trị giữa hai cột khác kiểu (`DECIMAL(10,2)` và `DECIMAL(10,4)`).

**Phiên bản gặp lỗi:** 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4539](https://github.com/apache/fluss/pull/4539) đang mở.

**Giảm thiểu:** không dùng `Decimal` của Fluss làm khóa hash trong ứng dụng, hoặc chuẩn hóa scale trước.

#### [#5021](https://github.com/apache/fluss/issues/5021): Ba lỗi nhỏ ở Java client: mô tả SASL bị đảo, `lastPollSecondsAgo` sai trước lần poll đầu, mã lỗi lạ bị mất

**Vấn đề:**
1. **Mô tả bị đảo:** `client.security.sasl.username` mô tả là password và ngược lại. Tài liệu website chép lại phần sai.
2. **`lastPollSecondsAgo`:** tính bằng `(now - lastPollMs) / 1000` với `lastPollMs` ban đầu là `0`, nên trước lần `poll()` đầu metric báo **số giây từ 1970** (khoảng 1,8 tỷ). Alert "consumer treo" bắn mỗi lần scanner khởi động.
3. **Mã lỗi lạ bị mất:** `Errors.forCode(code)` ánh xạ mã client không biết thành `UNKNOWN_SERVER_ERROR`, chỉ log WARN. Exception ứng dụng nhận **không có mã gốc**.

**Điều kiện kích hoạt:**
1. Người cấu hình đọc mô tả option trong tài liệu.
2. **Mỗi lần scanner khởi động** (Flink source start, restore, split mới), trong khoảng từ lúc đăng ký metric tới lần `poll()` đầu, nếu reporter thu metric đúng lúc này.
3. Server trả **mã lỗi mới** mà client chưa biết, thường khi **client cũ hơn server** (server đã nâng cấp, job Flink chưa).

**Phiên bản gặp lỗi:** `main`. Cả ba có trong 1.0.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#5044](https://github.com/apache/fluss/pull/5044) đang mở. Nằm trong umbrella #4980. Không ảnh hưởng dữ liệu.

**Giảm thiểu:** bỏ qua alert `lastPollSecondsAgo` trong vài phút đầu sau khi job khởi động, hoặc lọc giá trị lớn bất thường. Nâng client cùng lúc với server.

#### [#777](https://github.com/apache/fluss/issues/777): Bảng 1 replica: fetch đang chờ dữ liệu chỉ hoàn tất khi hết thời gian chờ

**Vấn đề:**
- #768 (sửa deadlock #762) gỡ `tryCompleteDelayedOperations` khỏi `Replica#appendRecordsToLeader()`. Sau khi ghi, `maybeIncrementLeaderHW` vẫn được gọi nhưng kết quả bị bỏ, và **không ai đánh thức** delayed fetch đang chờ.
- Delayed fetch chỉ còn được đánh thức khi: follower fetch (`updateFollowerFetchState`), KV flush xong (`onKvFlushComplete`), trở thành leader, callback AdjustIsr.
- Bảng 1 replica không có follower fetch, nên với **bảng log**, consumer chờ đủ `maxWaitMs` (mặc định 500 ms) mới nhận được dữ liệu dù dữ liệu đã sẵn sàng.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng có **replication factor 1**.
2. Rõ nhất với **bảng log**. Bảng PK được đánh thức khi KV flush xong.
3. Consumer đọc **ở đuôi log** (đã bắt kịp), gửi fetch khi chưa có dữ liệu mới.

Với replica ≥ 3, follower fetch đánh thức delayed fetch, nên gần như không gặp (có thể trễ thêm một chút tùy lúc follower fetch).

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (đã xác nhận trong mã, bình luận gần đây xác nhận còn trên `main`). bản tổng hợp ban đầu ghi ❔ là chưa đúng. Không có PR.

**Giảm thiểu:** production dùng replication factor ≥ 3. Môi trường dev/test 1 replica sẽ thấy độ trễ khoảng 500 ms, không phải lỗi hiệu năng thật.

#### [#2653](https://github.com/apache/fluss/issues/2653): Đặt `table.log.ttl = -1` (giữ log vô hạn) bị từ chối

**Vấn đề:**
- Tài liệu thời 0.9 nói có thể đặt `-1`, nhưng trình phân tích Duration không nhận dấu `-`, nên báo `InvalidConfigException`.
- 1.0.0 vẫn từ chối `-1`, nhưng mô tả cấu hình ghi rõ **`'0ms'` tắt xóa theo TTL**. Mã xóa log local (`LogTablet.isSegmentExpired`) và remote (`RemoteLogTablet.expiredRemoteLogSegments`) đều coi `ttl <= 0` là không hết hạn.

**Điều kiện kích hoạt:** đặt `'table.log.ttl' = '-1'` (theo tài liệu cũ hoặc thói quen từ Kafka `retention.ms = -1`). Lỗi xảy ra ngay lúc tạo hoặc sửa bảng, không ảnh hưởng dữ liệu.

**Phiên bản gặp lỗi:** `main` thời 0.9.

**Trạng thái trong 1.0.0: 🟡** `-1` vẫn bị từ chối (PR [#2621](https://github.com/apache/fluss/pull/2621) đang mở), nhưng có cách chính thức là `'0ms'`.

**Giảm thiểu:** dùng `'table.log.ttl' = '0ms'`.

#### [#1854](https://github.com/apache/fluss/issues/1854): Container Docker của Fluss chạy bằng root

**Vấn đề:**
- Image `fluss` tạo user `fluss` (UID/GID 9999) và `chown /opt/fluss`, nhưng **không có `USER`**, và entrypoint `exec` thẳng script khởi động, không `gosu`. Coordinator và TabletServer chạy bằng **root**.
- Helm chart 1.0.0 không đặt `securityContext`.
- Hướng sửa đã thống nhất trong bình luận (`chown` + `gosu`), nhưng chưa có PR.
- **Phát hiện thêm khi đọc chart (nguy hiểm hơn):** dữ liệu TabletServer mặc định nằm trên `emptyDir` (`tablet.storage.enabled: false`), và `remote.data.dir` trỏ vào đĩa local của pod.

**Điều kiện kích hoạt:** mọi lần chạy image `fluss` chính thức (Docker, docker-compose, Helm chart). Không có điều kiện đặc biệt.

**Phiên bản gặp lỗi:** 0.7.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (đã xác nhận trong mã, cả `main`). bản tổng hợp ban đầu ghi ❔ là chưa đúng.

**Giảm thiểu:** đặt `securityContext.runAsUser: 9999` **kèm `fsGroup: 9999`** và kiểm tra quyền ghi `data.dir`. Bật `tablet.storage.enabled` (PVC) và đặt `remote.data.dir` lên object store.

#### [#3304](https://github.com/apache/fluss/issues/3304): `currentFetchEventTimeLag` báo thấp hơn thực tế khi các bucket có độ trễ chênh nhau

**Vấn đề:**
- Mỗi lần fetch, mã lấy timestamp **lớn nhất** của bản ghi cuối mỗi bucket (`Math.max`) rồi báo `fetchTimestamp − max`. Lag được báo là lag **nhỏ nhất** giữa các bucket.
- Ví dụ: một bucket đã bắt kịp và một bucket trễ 2 giờ thì `currentFetchEventTimeLag` gần 0, còn `currentEmitEventTimeLag` khoảng 2 giờ.

**Điều kiện kích hoạt (cần đồng thời):**
1. Một subtask Flink source đọc **nhiều bucket hoặc partition**.
2. Tiến độ giữa các bucket **chênh lệch lớn**: backfill partition cũ song song partition mới, một bucket nghẽn hoặc kẹt (#1256), partition mới đọc từ earliest (#3548).

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#3356](https://github.com/apache/fluss/pull/3356) đang mở.

**Giảm thiểu:** dùng metric `pendingRecords` (tổng high watermark − offset đang đọc, đã có trong 1.0.0) để alert độ trễ.

#### [#1037](https://github.com/apache/fluss/issues/1037): `currentEmitEventTimeLag` của Flink source luôn cao khi đọc bảng nhiều partition

**Vấn đề:**
- `currentEmitEventTimeLag` = thời điểm hiện tại trừ **timestamp ghi vào Fluss** của bản ghi vừa phát ra. Metric cao nghĩa là source đang phát bản ghi cũ.
- Với bảng nhiều partition, metric cao có thể là **đúng**: đang đọc lại partition cũ, partition mới được đọc từ đầu sau khi phát hiện (#3548), hoặc một bucket chậm làm bản ghi cũ xen kẽ.
- Chỉ bất thường nếu job **đã bắt kịp mọi partition** mà metric vẫn cao.

**Điều kiện kích hoạt:** đọc bảng có nhiều partition bằng Flink source (issue không nêu thêm).

> ⚠️ **Note: điều kiện mơ hồ.** Issue chỉ có một câu và một ảnh biểu đồ, không nêu cấu hình, số partition hay con số cụ thể. Chưa phân biệt được "metric cao hợp lý" (đang đọc dữ liệu cũ) với "metric sai". Chưa tái hiện, chưa kết luận được đây có phải lỗi.

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: ❔ Chưa xác định.** Không có PR. Không thấy thay đổi nào sửa trực tiếp.

**Giảm thiểu:** không alert chỉ dựa vào metric này. Đối chiếu với lag theo offset của từng bucket.

#### [#764](https://github.com/apache/fluss/issues/764): Máy trạng thái replica log ERROR khi replica đang xóa bị đẩy sang "offline"

**Vấn đề:**
1. Coordinator tạo bảng/partition, gửi `NotifyLeaderAndIsr`.
2. Bảng bị drop ngay sau đó: Coordinator xóa node ZK của bảng, chuyển replica sang `ReplicaDeletionStarted`.
3. TabletServer xử lý `NotifyLeaderAndIsr` **muộn**, không thấy schema trong ZK, trả `StorageException`.
4. Coordinator coi mọi bucket lỗi là offline, cố chuyển replica sang `OfflineReplica`. Chuyển trạng thái không hợp lệ nên bị từ chối, kèm **2 dòng log ERROR**.

Không có hậu quả chức năng: từ chối là đúng, việc xóa vẫn tiếp tục. Vấn đề là **log ERROR gây nhiễu**.

**Điều kiện kích hoạt:** drop bảng hoặc partition **ngay sau khi tạo**, trước khi TabletServer kịp xử lý `NotifyLeaderAndIsr`. Hay gặp trong test, hoặc khi tạo rồi xóa partition liên tục; với CDP vận hành thường thì hiếm.

**Phiên bản gặp lỗi:** 0.6.0.

**Trạng thái trong 1.0.0: ⏭️ Còn lỗi, đã có fix trên `main`.** Commit [#4505](https://github.com/apache/fluss/pull/4505) (15/10/2026) thêm điều kiện bỏ qua phản hồi lỗi tới muộn của replica đang xóa. PR [#795](https://github.com/apache/fluss/pull/795) đã bị đóng.

**Giảm thiểu:** loại dòng ERROR này khỏi alert log.

#### [#765](https://github.com/apache/fluss/issues/765): Máy trạng thái bucket log ERROR khi bucket đã xóa bị đẩy sang "offline"/"online"

**Vấn đề:**
- Log trong issue: `Table bucket ... should be one of [OnlineBucket, NewBucket, OfflineBucket] states before moving to OfflineBucket state. Instead it is in NonExistentBucket.` cùng cặp lỗi tương tự cho `OnlineBucket`.
- Đây là cặp chuyển OfflineBucket rồi OnlineBucket mà `onReplicaBecomeOffline` làm cho bucket có leader là replica vừa bị coi là offline. Bucket đã bị xóa (`NonExistentBucket`), nên cả hai lần bị từ chối kèm ERROR.
- Cùng gốc với #764: phản hồi `NotifyLeaderAndIsr` lỗi tới muộn sau khi bảng/partition đã bị xóa (suy luận từ mã). Không có hậu quả chức năng.

**Điều kiện kích hoạt:** drop bảng hoặc partition (kể cả hết hạn tự động) trong khi có phản hồi `NotifyLeaderAndIsr` lỗi **tới muộn** cho bucket của nó, sau khi bucket đã sang `NonExistentBucket`. Test trong issue dùng bảng auto-partition theo năm.

> ⚠️ **Note:** issue chỉ dán log, không phân tích. Kịch bản "phản hồi tới muộn" là suy luận từ mã.

**Phiên bản gặp lỗi:** khoảng 0.6.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi**, có thể đã được giải quyết trên `main` bởi #4505 (cùng thay đổi với #764, #1629), chưa xác nhận.

**Giảm thiểu:** loại dòng ERROR này khỏi alert log.

#### [#898](https://github.com/apache/fluss/issues/898): Drop bảng partition chưa có partition nào thì bảng còn sót trong Coordinator

**Vấn đề:**
- Việc xóa bảng kết thúc khi phản hồi xóa của bucket cuối cùng gọi `resumeDeletions`. Bảng partition **chưa có partition** thì không có bucket, không có phản hồi, bảng **nằm mãi** trong hàng đợi xóa, `tableCount` lệch.
- Chỉ được dọn khi một lần xóa bảng khác tình cờ gọi `resumeDeletions` hoặc Coordinator restart.
- Trong 1.0.0, `processDropTable` **chủ động gọi `resumeDeletions()`** cho bảng partition, nên bảng được hoàn tất xóa ngay.

**Điều kiện kích hoạt (bản cũ 0.7–0.9):** tạo bảng partition rồi drop **trước khi** có partition nào (script tự động, test, hoặc auto-partition tắt).

**Phiên bản gặp lỗi:** `main` thời khoảng 0.7.

**Trạng thái trong 1.0.0: ✅ Đã khắc phục** bằng commit [#3482](https://github.com/apache/fluss/pull/3482) (14/06/2026), dù issue và PR [#987](https://github.com/apache/fluss/pull/987) vẫn mở. Không phải lỗi "log ERROR" như bản tổng hợp ban đầu phân loại.

**Giảm thiểu:** không cần.

#### [#321](https://github.com/apache/fluss/issues/321): Rò rỉ bộ nhớ Arrow khi writer bị đóng trước khi gửi hết batch

**Vấn đề:**
- `ArrowWriter` của một batch chỉ được trả về pool khi batch được **gửi đi**. `ArrowWriterPool.close()` chỉ đóng writer **đang rảnh**.
- Force close khi còn batch chưa gửi thì `ArrowWriter` của các batch đó không được đóng, Arrow ném `Memory was leaked by query` (một người dùng Flink thấy khoảng **28 MB** mỗi lần).
- 1.0.0 làm lỗi nặng hơn mức "chỉ log" (suy luận từ mã):
  1. Flink sink **luôn** đóng connection với `Duration.ZERO` (force close), nên mỗi lần task đóng là một lần force close.
  2. Allocator Arrow phía client dùng **native memory qua `Unsafe`**. `RecordAccumulator.destroyResources()` ném lỗi ở `bufferAllocator.close()` thì `chunkedFactory.close()` không chạy, mà kể cả chạy cũng không giải phóng chunk đang bị giữ. Bộ nhớ **rò thật**.

**Điều kiện kích hoạt:**
1. Writer bị **đóng khi còn batch chưa rút ra gửi**:
   - **Flink sink:** mỗi lần task đóng (failover, cancel, stop-with-savepoint, rescale) mà còn bản ghi ghi sau lần flush checkpoint cuối chưa gửi. Với tải cao, gần như luôn đúng.
   - **Java client:** `connection.close(Duration.ZERO)` hoặc timeout nhỏ.
2. Đóng graceful (`connection.close()` không tham số) thì không rò.

> ⚠️ **Note:** chưa đo được lượng rò thực tế trên TaskManager và chưa tái hiện. Rủi ro OOMKilled phụ thuộc tần suất failover và việc TaskManager có được tái sử dụng sau failover hay không.

**Phiên bản gặp lỗi:** 0.5. Gặp lại năm 2025 trên Flink.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (có `TODO` trong mã). Không có PR. Nằm trong umbrella #4980. bản tổng hợp ban đầu xếp 🟢, nên nâng lên 🟡 cho tới khi đo được.

**Giảm thiểu:** theo dõi native/direct memory của TaskManager qua nhiều lần failover. Hạn chế failover lặp lại. Restart TaskManager định kỳ nếu thấy bộ nhớ tăng dần.

#### [#3223](https://github.com/apache/fluss/issues/3223): TabletServer vẫn recover log của bucket thuộc partition đã bị xóa

**Vấn đề:**
- Ở 0.9.0, TabletServer recover mọi thư mục tablet trên đĩa, kể cả của partition đã xóa. Nếu thư mục đó hỏng (ví dụ sau khi đầy đĩa, #3224), recovery lỗi và server không lên.
- Ở 1.0.0, `LogManager.loadLog` đọc schema bảng và đăng ký partition trên ZooKeeper **trước khi** recover. Bảng hoặc partition không còn (hoặc partition ID khác vì bị tạo lại) thì log `"Table or partition for {} has already been dropped, the residual data will be removed."`, xóa thư mục log và KV, rồi tiếp tục.

**Điều kiện kích hoạt (bản cũ):** TabletServer khởi động khi trên đĩa còn thư mục của partition hoặc bảng đã xóa, và thư mục đó hỏng.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ✅ Đã khắc phục** bằng commit [#3388](https://github.com/apache/fluss/pull/3388) (07/06/2026, sửa #3387), dù issue và PR #3225 vẫn mở. bản tổng hợp ban đầu ghi "không có fix" là sai. Recovery **vẫn** có thể lỗi với partition **còn tồn tại** mà dữ liệu hỏng (#3224, #2943).

**Giảm thiểu:** không cần. Lưu ý rủi ro ngược (suy ra từ mã, chưa kiểm chứng): nếu ZooKeeper trả lỗi sai lúc khởi động, dữ liệu có thể bị coi là rác.

#### [#4116](https://github.com/apache/fluss/issues/4116): Các uber-jar `fluss-fs-*` lộ jackson và guava không relocate

**Vấn đề:**
- Cả 7 plugin filesystem của 1.0.0 chứa hàng nghìn lớp `com.fasterxml.*` (jackson-databind 2.15.3) và `com.google.*` (guava) không đổi package. Shade plugin của từng module chỉ relocate `org.apache.commons`.
- **Server:** jar đặt đúng trong `plugins/` được cách ly bằng plugin classloader, không đè thư viện server.
- **Client:** `FlussConnection` gọi `FileSystem.initialize(conf, null)`, **không có plugin manager**, nên filesystem được tìm bằng `ServiceLoader` trên **classpath của ứng dụng**. Jar `fluss-fs-*` buộc phải nằm chung với thư viện ứng dụng.

**Điều kiện kích hoạt (cần đồng thời):**
1. Client cần đọc **remote storage**: đọc log cũ đã tier, Flink batch scan bảng PK (đọc KV snapshot), union read với lake. Server chỉ dính khi đặt jar vào `lib/`.
2. Ứng dụng hoặc connector khác mang **jackson/guava khác phiên bản** và dùng API không tương thích.

> ⚠️ **Note:** chưa tái hiện xung đột cụ thể. Có xảy ra hay không phụ thuộc hoàn toàn vào dependency của từng ứng dụng.

**Phiên bản gặp lỗi:** `main`. Người báo đã đo lại trên artifact 1.0.0 ở Maven Central.

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4073](https://github.com/apache/fluss/pull/4073) và [#4144](https://github.com/apache/fluss/pull/4144) chưa merge.

**Giảm thiểu:** như #4072. Với job Flink, đặt jar filesystem vào `lib/` của Flink và kiểm tra xung đột với connector khác.

#### [#4072](https://github.com/apache/fluss/issues/4072): Uber-jar `fluss-fs-s3` chứa khoảng 1.114 lớp jackson không relocate

**Vấn đề:**
- `fluss-fs-s3` gói `hadoop-common` và AWS SDK **không relocate**, nên khoảng 1.114 lớp jackson nằm ở `com/fasterxml/*` (databind khoảng 800, core 200, cbor 50, annotation 20, MRJ 25).
- Đặt cùng classpath với jackson khác phiên bản của ứng dụng thì có thể đè lớp, gây `NoSuchMethodError`.
- Là trường hợp riêng của #4116. Phía client, filesystem được nạp **từ classpath** (không qua plugin), nên với S3 đây là rủi ro mặc định.

**Điều kiện kích hoạt (cần đồng thời):**
1. **Client** (job Flink, service Java) dùng remote storage **S3/MinIO** và cần đọc dữ liệu trên đó: log cũ đã tier, KV snapshot khi batch scan, union read. Server chỉ dính khi đặt jar sai chỗ (`lib/` thay vì `plugins/`).
2. Classpath có **jackson phiên bản khác** được dùng với API không tương thích.

**Phiên bản gặp lỗi:** `main`. Có trong artifact 1.0.0 (theo #4116).

**Trạng thái trong 1.0.0: ❌ Còn lỗi.** PR [#4073](https://github.com/apache/fluss/pull/4073) đang mở.

**Giảm thiểu:** kiểm tra `mvn dependency:tree`, đồng bộ phiên bản jackson với 2.15.x, hoặc tự relocate khi đóng fat jar.

### Đã đánh giá lại: không ảnh hưởng tới CDP hoặc đã khắc phục

#### [#3228](https://github.com/apache/fluss/issues/3228): Job Flink lỗi `OutOfRange` vĩnh viễn khi resume sau gián đoạn dài

> **Đánh giá: không ảnh hưởng tới CDP.** Lý do như #3084: Flink job của CDP không đọc log table/changelog.

**Vấn đề:**
- Flink source lưu offset trong checkpoint. Job dừng đủ lâu thì dữ liệu ở offset đó không còn trong Fluss, server trả `LOG_OFFSET_OUT_OF_RANGE`.
- Client ném `FetchException` (`AbstractLogFetchCollector.handleInitializeErrors`), subtask fail, Flink restore lại cùng offset cũ và lỗi tiếp: **không bao giờ tự phục hồi**.
- Không có tùy chọn kiểu `auto.offset.reset`. `scan.startup.mode` chỉ áp dụng lần khởi động đầu.
- Với bảng bật lake, dữ liệu còn trong lake nhưng client không fallback sang lake. Trong 1.0.0, Coordinator không bao giờ gửi lake `logStartOffset` cho TabletServer, nên `EARLIEST` chỉ tính local/remote (xem #4676).

**Điều kiện kích hoạt (cần đồng thời):**
1. Flink job **dừng/lỗi đủ lâu** để offset trong checkpoint/savepoint tụt sau log start offset của bucket.
2. Log ở offset đó **đã bị xóa khỏi local** (hết `table.log.ttl`, hoặc đã tier sang lake rồi bị dọn).
3. Offset đó **không còn ở remote log**. Còn ở remote thì client đọc được.
4. Job resume bằng offset đã lưu.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: ❌ Còn lỗi** (cả `main`). PR [#3308](https://github.com/apache/fluss/pull/3308) (fallback sang lake) đang mở, và không áp dụng cho changelog bảng PK (`isChangeLog`). Khi lên 1.1, `log.retention.roll-active-segment.enabled` mặc định `true` làm rủi ro tăng nếu TTL ngắn.

**Giảm thiểu:** `table.log.ttl` dài hơn thời gian gián đoạn tối đa chấp nhận được. Runbook: khởi động lại job không dùng state (hoặc từ timestamp) khi gặp lỗi này.

#### [#3084](https://github.com/apache/fluss/issues/3084): Một bucket kẹt âm thầm khi tải remote log, restart thì `OutOfRange`

> **Đánh giá: không ảnh hưởng tới CDP.** Lý do: Flink job của CDP không đọc changelog hay log table.

**Vấn đề:**
- Job backfill chạy ổn khoảng một tuần. Restart từ checkpoint thì một subtask lỗi `FetchException: The fetching offset 1546148 is out of range ... we only have log segments from offset 2199904`. Chỉ bucket 50 đứng yên, các bucket khác đã bắt kịp.
- Giả thuyết (contributor đưa ra, người báo đồng ý, chưa xác nhận): việc tải segment remote bị **treo âm thầm**, offset của split không tiến, log trên server hết hạn dần. Tới khi restart, offset trong checkpoint không còn.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng bật remote log và job đọc offset **cũ** nằm ở remote (backfill, job tụt lại).
2. Việc tải segment remote gặp sự cố (lỗi S3/HDFS, mạng, deadlock semaphore như #3085). Ở 1.0.0, lỗi tải bị **thử lại mãi**, không báo lỗi.
3. Job không dừng: các bucket khác vẫn tiến nên trông như đang chạy.
4. Job restart từ checkpoint khi log của offset kẹt đã hết hạn (`table.log.ttl`), rồi lặp lỗi như #3228.

> ⚠️ **Note: điều kiện mơ hồ.** Người báo không có log của ngày bị kẹt, nên nguyên nhân bước 2 chỉ là giả thuyết.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: 🟡 Nguyên nhân gốc đã giảm thiểu, `main` có thêm fix.**
- Có trong 1.0.0: #3104 (bắt mọi `Throwable` khi tải remote file, fix #3085, backport 0.9.1 qua #3136), #2786, #3816.
- Chỉ trên `main` (merge 2026-09-22): [#1752](https://github.com/apache/fluss/pull/1752) ném exception sau vài lần retry thất bại.
- Khi offset thực sự đã hết hạn thì vẫn fail vĩnh viễn như #3228.

**Giảm thiểu:** theo dõi offset từng bucket và metric `remoteFetchErrorCount`. Đặt `table.log.ttl` dài hơn thời gian gián đoạn tối đa.

#### [#2749](https://github.com/apache/fluss/issues/2749): Fetch log request timeout lặp lại khi một TabletServer restart

> **Đánh giá: không ảnh hưởng tới CDP.** Đây là hành vi đúng khi leader không phản hồi, không phải lỗi gây hại cho CDP.

**Vấn đề:**
- `ReplicaFetcherThread` gửi `FetchLogRequest` và chờ `future.get(30, SECONDS)` với 30 giây **hard-code**. Leader đang restart không trả lời thì follower chờ đủ 30 giây, log WARN `TimeoutException`, đánh dấu mọi bucket của luồng đó lỗi, nghỉ 1 giây (`log.replica.fetch.backoff-interval`) rồi thử lại: log lặp mỗi khoảng 31 giây.
- Hai tham số người báo thử (`log.replica.fetcher-number`, `log.replica.fetch.wait-max-time`) không liên quan tới thời hạn 30 giây.
- Điều đáng cải thiện chỉ là: timeout không cấu hình được, chờ thay vì fail nhanh khi kết nối chết, cảnh báo kèm stack trace lặp lại.

**Điều kiện kích hoạt (cần đồng thời):**
1. Một TabletServer là leader của một số bucket đang **restart hoặc không phản hồi**.
2. Request fetch không có phản hồi **và không có lỗi ngắt kết nối** trong 30 giây.
3. Tình trạng kéo dài tới khi leader phản hồi lại hoặc Coordinator chuyển leadership.

> ⚠️ **Note:** issue chỉ có một đoạn log, không nêu hậu quả. "Độ trễ replication tăng" trong bản tổng hợp ban đầu là suy luận. Không biết vì sao request không nhận được lỗi ngắt kết nối khi leader restart.

**Phiên bản gặp lỗi:** 0.8.0.

**Trạng thái trong 1.0.0: ❌ (hành vi giữ nguyên), nhưng không ảnh hưởng.** Không có PR.

**Giảm thiểu:** dùng controlled shutdown khi restart để leadership được chuyển trước. Không alert theo dòng WARN này.

#### [#3011](https://github.com/apache/fluss/issues/3011): Replica bị đánh dấu offline mãi, bucket mất leader vĩnh viễn

> **Đánh giá: đã khắc phục trong 1.0.0.** Kịch bản "vĩnh viễn" đã được khắc phục trong 1.0.0.

**Vấn đề:**
- Ở 0.9.0: bucket chưa có leader, Coordinator vẫn gửi `NotifyLeaderAndIsr(leader = -1)` cho follower. TabletServer coi là lỗi, trả `STORAGE_EXCEPTION`. Coordinator đánh dấu replica **offline**, các lần bầu sau không còn ứng viên. Không có cơ chế gỡ dấu, bucket mất leader tới khi restart.
- Ở 1.0.0:
  - TabletServer bỏ qua im lặng khi `leaderId == NO_LEADER` ([#3671](https://github.com/apache/fluss/pull/3671));
  - Coordinator tự gỡ dấu offline và bầu lại sau 1 phút (`retryableOfflineReplicasOnLiveTabletServers`, [#3539](https://github.com/apache/fluss/pull/3539)).
- Phần còn lại: **mọi lỗi** trong phản hồi `NotifyLeaderAndIsr` (kể cả tạm thời) vẫn đánh dấu offline, bucket có thể mất leader **khoảng 60 giây**.

**Điều kiện kích hoạt:**
- **0.9.0:** bucket rơi vào trạng thái không có leader: mọi replica ISR cùng chết, leader chết mà replica khác chưa đủ điều kiện, ngay sau khi tạo bảng (#4522).
- **1.0.0 (phần còn lại, mất leader khoảng 60 giây):** một replica là **ứng viên duy nhất** trả lỗi trong `NotifyLeaderAndIsr`: `FencedLeaderEpochException` trong `Replica.makeFollower`, `SchemaNotExistException` khi ZK follower chậm (#4522), bị từ chối vì disk write protection (#3539).

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: 🟡 (kịch bản chính ✅).** PR gốc [#3012](https://github.com/apache/fluss/pull/3012) đã bị đóng. Không có PR cho phần "chỉ coi lỗi nghiêm trọng là offline".

**Giảm thiểu:** alert khi `offlineBucketCount > 0` kéo dài hơn 1–2 phút (lưu ý #5038). Có thể giảm `coordinator.offline-leader.retry-delay`.

#### [#3466](https://github.com/apache/fluss/issues/3466): Partition hết hạn gây bão `GET_METADATA` vào ZooKeeper

> **Đánh giá: đã khắc phục trong 1.0.0.** Lưu ý: phân tích mã cho thấy mới **giảm một phần** (xem trạng thái).

**Vấn đề:**
- Partition bị xóa (hết hạn) nhưng client Flink còn tham chiếu (reader giữ bucket, writer có batch). Các task nhận `PartitionNotExistException` rồi **hỏi lại metadata liên tục**.
- TabletServer không có partition trong cache nên **hỏi ZK** mỗi lần. Enumerator chỉ biết partition bị xóa ở lần liệt kê kế tiếp (tối đa `scan.partition.discovery.interval`, mặc định 1 phút).
- `P` task × `N` partition có thể sinh cỡ `P × N` lượt đọc ZK. Issue ước tính P = 100, N = 100 cho ≥ 10.000 lượt đọc ZK đồng thời.

**Điều kiện kích hoạt (cần đồng thời):**
1. Bảng có partition **bị xóa** (auto-partition hết hạn, `DROP PARTITION`).
2. Có client còn tham chiếu partition vừa xóa: reader đang giữ bucket, hoặc writer có batch/ghi tiếp (dữ liệu trễ).
3. **Quy mô đủ lớn:** nhiều task (`P`) và nhiều partition cùng hết hạn (`N`).
4. Phía đọc kéo dài tới lần discovery kế tiếp. Phía ghi (bảng không bật historical partition) không có điểm dừng rõ (#4726).

Partition theo **giờ** thì lặp lại đều đặn.

> ⚠️ **Note:** các con số tải là ước tính trong issue, chưa tái hiện. Chưa đo mức giảm thực tế của #3511.

**Phiên bản gặp lỗi:** 0.9.0.

**Trạng thái trong 1.0.0: 🟡 Đã giảm một phần.** 1.0.0 có negative cache [#3511](https://github.com/apache/fluss/pull/3511), nhưng chỉ cho request theo **partition ID** (phía đọc, `LogFetcher`). Request theo **tên partition** (phía ghi, `Sender`/`WriterClient`) **chưa có**. #3511 liên kết #3510, không phải #3466.

**Giảm thiểu:** giám sát request rate của ZK vào thời điểm partition hết hạn. Lọc dữ liệu trễ trước khi sink. Partition theo ngày thay vì giờ nếu được.
