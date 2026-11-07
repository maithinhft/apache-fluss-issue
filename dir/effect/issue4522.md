# Issue #4522 — Bucket "offline" khoảng 60 giây ngay sau khi tạo bảng, vì TabletServer đọc phải ZooKeeper follower chưa cập nhật

Link: https://github.com/apache/fluss/issues/4522 — Phiên bản được báo: `main` (người báo: morazow, 29/09/2026, tức ngay sau khi 1.0.0 phát hành). Issue **rất chi tiết**:
- log có mốc thời gian từ một cluster thật (ZooKeeper 3 node, 3 TabletServer, 1 Coordinator, chạy trên Kubernetes);
- phân tích nguyên nhân;
- đề xuất sửa.

Không có bình luận. Trạng thái: còn mở. PR [#4524](https://github.com/apache/fluss/pull/4524) (cùng tác giả) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và xác nhận: đường đọc schema không gọi `sync`, lỗi biến thành "replica offline" như thế nào, và vì sao mất đúng 60 giây (cơ chế retry của #3011/#3539). Mình **chưa tái hiện**. Theo issue, test của dự án chỉ chạy với **một** ZooKeeper nên không bao giờ bắt được lỗi này.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **ZooKeeper ensemble** | Cụm ZooKeeper nhiều node (thường 3 hoặc 5). Một node là **leader** (xử lý mọi lệnh ghi), các node khác là **follower**. Client kết nối vào **bất kỳ** node nào và đọc từ chính node đó. |
| **Follower lag (ZooKeeper)** | Follower áp dụng một lệnh ghi đã commit **muộn hơn** leader vài mili giây, đặc biệt follower không nằm trong quorum ghi. Client đọc qua follower đó có thể **chưa thấy** dữ liệu vừa được node khác ghi. |
| **`sync`** | Lệnh ZooKeeper buộc node đang phục vụ client **bắt kịp leader** trước lần đọc kế tiếp. Không gọi `sync` thì ZooKeeper chỉ đảm bảo "mỗi client tự thấy dữ liệu của chính mình theo thứ tự", không đảm bảo thấy dữ liệu của client **khác**. |
| **Schema (Fluss)** | Định nghĩa cột của bảng. Coordinator lưu trong ZooKeeper tại `.../tables/<bảng>/schemas/<id>`. |
| **`NotifyLeaderAndIsr`** | Lệnh Coordinator gửi TabletServer: "tạo hoặc nhận replica của bucket này, leader là X". |
| **`SchemaNotExistException`** | Lỗi "không tìm thấy schema của bảng". |
| **`coordinator.offline-leader.retry-delay`** | Thời gian Coordinator chờ trước khi thử bầu lại leader cho bucket mà mọi replica đều bị đánh dấu offline. Mặc định **60 giây** (xem #3011). |
| **`offlineBucketCount`** | Metric Coordinator: số bucket không có leader. |

## Tóm tắt

Ngay sau `CREATE TABLE`:
1. Coordinator ghi schema vào ZooKeeper, rồi ghi phân bổ bucket, rồi gửi `NotifyLeaderAndIsr` cho các TabletServer.
2. Mỗi TabletServer tạo replica bằng cách **đọc schema từ ZooKeeper**. Nếu session của nó đang nối với một **ZK follower chưa áp dụng** lệnh ghi schema, lệnh đọc **không thấy** schema, và tạo replica thất bại với `SchemaNotExistException`.
3. TabletServer **không tự thử lại**. Nó trả lỗi cho bucket đó, và Coordinator đánh dấu replica **offline**.
4. Nếu **mọi** replica của một bucket đều gặp chuyện này, Coordinator không bầu được leader, và **chờ 60 giây** (`offline-leader.retry-delay`) mới thử lại.

Trong log của issue: schema ghi lúc `03:27:14.831`, ba TabletServer đều lỗi bucket 1 lúc `.886` (55 ms sau). Bucket 0 của cùng bảng tạo thành công 2 ms sau đó, không có lệnh ghi nào xen giữa. `offlineBucketCount` lên 1 khoảng 60 giây, rồi lúc `03:28:14.922` bucket 1 mới có leader.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm:
  - `TabletManagerBase.getTableInfo` (dùng chung cho `ReplicaManager`, `LogManager`, `KvManager`);
  - `ReplicaManager.maybeCreateReplica`;
  - `CoordinatorEventProcessor` (đánh dấu offline và retry).
- **Đã xác nhận trong mã `v1.0.0`:**
  - `TabletManagerBase.getTableInfo(zkClient, tablePath)`:
    ```java
    int schemaId = zkClient.getCurrentSchemaId(tablePath);
    Optional<SchemaInfo> schemaInfoOpt = zkClient.getSchemaById(tablePath, schemaId);
    if (!schemaInfoOpt.isPresent()) {
        throw new SchemaNotExistException(String.format(
            "Failed to load table '%s': Table schema not found in zookeeper metadata.", tablePath));
    }
    ```
    Không có `sync` trước khi đọc. Theo issue, trên follower chưa cập nhật, `getChildren(schemas)` trả danh sách rỗng, nên `getCurrentSchemaId` trả 0, và `getSchemaById(0)` không thấy gì.
  - Hàm này được gọi từ ba chỗ: `ReplicaManager.java:2483` (trong `maybeCreateReplica`), `LogManager.java:438` và `KvManager.java:684`. Vì vậy cả lúc nạp tablet cũng có cùng rủi ro, đúng như issue nói.
  - `maybeCreateReplica` bọc trong `try { ... } catch (Exception e) { LOG.error("Error while checking and creating replica", e); }` và trả `Optional.empty()`.
  - Nơi gọi (`ReplicaManager.java:2226`) đổi `empty` thành `StorageException("The replica %s for table bucket %s is offline while notify leader and isr")`. Kết quả cho bucket đó là **lỗi**.
  - Phía Coordinator (xem #3011): mọi bucket có lỗi trong phản hồi `NotifyLeaderAndIsr` đều bị `onReplicaBecomeOffline`. Khi mọi replica offline thì *"Elect result is empty"*, và `scheduleOfflineLeaderRetryIfNeeded` lên lịch thử lại sau `coordinator.offline-leader.retry-delay` (60 giây). Log `"Offline leader retry task scheduled after 60000 ms"` trong issue khớp đúng với đoạn mã này.
- **Hướng sửa (issue và PR #4524):** khi không thấy schema thì gọi `sync` trên đường dẫn `schemas` rồi đọc lại. Chỉ tốn thêm một lượt ở nhánh lỗi. Thêm cluster ZooKeeper 3 node vào test.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **ZooKeeper nhiều node** (bình thường trong production). Với 1 node ZooKeeper thì không thể đọc dữ liệu cũ.
2. **Server Fluss nối vào ZooKeeper qua một địa chỉ chung** (ví dụ Kubernetes Service hoặc load balancer), nên session của TabletServer và Coordinator có thể nằm trên **các ZK node khác nhau**. Với chuỗi `zookeeper.address` liệt kê nhiều node, Curator cũng chọn ngẫu nhiên, nên vẫn có thể khác node (suy luận).
3. **Ngay sau khi tạo bảng mới**: khoảng cách giữa lúc ghi schema và lúc TabletServer đọc chỉ vài chục mili giây.
4. **Mọi replica của một bucket cùng đọc trượt** thì mới mất leader. Nếu chỉ một vài replica trượt, bucket vẫn có leader nhưng thiếu replica trong khoảng 60 giây.
- **Về partition mới (auto-partition):** schema đã có trong ZooKeeper từ lâu nên đọc schema không trượt. Nhưng mình **chưa kiểm tra** việc tạo partition có đọc nút ZooKeeper nào vừa được ghi hay không.

**Phiên bản:** báo trên `main` (code giống `1.0.0`). Theo cơ chế thì đã có từ các bản trước.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người tạo bảng mới rồi **ghi hoặc đọc ngay**, trên cluster có ZooKeeper nhiều node.

**Hậu quả:**
1. **Bucket không có leader khoảng 60 giây** sau `CREATE TABLE`. Ghi vào key thuộc bucket đó lỗi hoặc retry; lookup lỗi hoặc treo tới timeout.
2. **Job Flink bắt đầu ngay sau khi tạo bảng** (thường gặp trong pipeline tự động: tạo bảng → submit job) có thể failover. Nếu job đọc, có thể gặp "Leader not found" (#288).
3. **Replica thiếu tạm thời:** nếu chỉ một số replica trượt, bucket chạy với ít replica hơn trong khoảng 60 giây.
4. **Tự hết sau khoảng 60 giây.** Không mất dữ liệu (bảng mới chưa có dữ liệu).
5. Lỗi phụ thuộc thời gian nên **không ổn định**: có lần gặp, có lần không, nên khó chẩn đoán.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4522 trong `relate_issue.md` ghi: *"ZK follower chậm: bucket offline ~60s ngay sau khi tạo bảng | main (code có trong 1.0.0) | ❌ PR #4524 chưa merge | Chỉ ngay sau `CREATE TABLE`. Chờ bảng sẵn sàng rồi mới start job"*. Đối chiếu:
- **Đúng** về cơ chế, trạng thái và phạm vi.
- **Bổ sung:** con số 60 giây chính là `coordinator.offline-leader.retry-delay` (cơ chế ở #3011), nên **cấu hình được**. Ngoài ra `LogManager` và `KvManager` cũng gọi cùng hàm đọc schema.

## 5. Mức ảnh hưởng với CDP

- CDP thường tạo bảng **ít khi** (lúc thiết kế schema), nhưng có thể có pipeline tạo bảng tự động (bảng feature hoặc segment mới, bảng `profile_v2` khi đổi schema theo khuyến nghị ở #4741). Rủi ro nằm ở **phút đầu tiên** của mỗi bảng mới.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Sau `CREATE TABLE`, chờ bảng sẵn sàng** rồi mới start job ghi hoặc đọc: kiểm tra qua Admin API hoặc metadata rằng **mọi bucket đã có leader**. Đừng chỉ chờ cố định vài giây.
- **Giảm `coordinator.offline-leader.retry-delay`** (ví dụ 10 giây) để rút ngắn thời gian mất leader khi bị trượt. Cái giá là Coordinator thử lại thường xuyên hơn khi replica hỏng thật (xem #3011).
- **Cấu hình ZooKeeper để giảm độ trễ follower:** follower khỏe, mạng thấp trễ. Chỉ là giảm xác suất, không loại bỏ được.
- **Alert** theo log `"Error while checking and creating replica"` kèm `SchemaNotExistException` trên TabletServer, và theo `offlineBucketCount` (lưu ý #5038).

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện với ZooKeeper 3 node.
- Mình chưa kiểm tra Curator của Fluss chọn ZK node thế nào khi `zookeeper.address` là danh sách nhiều node.
- Mình chưa kiểm tra đường tạo **partition mới** hoặc **ALTER TABLE (đổi schema)** có thể đọc trượt tương tự hay không. Ví dụ sau `ADD COLUMN`, một TabletServer đọc trượt có thể thấy schema ID cũ. Mình chưa xác minh điều này có xảy ra không.
- Mình chưa đọc diff của PR #4524.
