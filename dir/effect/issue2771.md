# Issue #2771 — Coordinator ném NPE trong `releaseBucket` khi Flink trả lại KV snapshot lease cho một bảng không còn trong lease

Link: https://github.com/apache/fluss/issues/2771 — Phiên bản được báo: `main` thời 0.9 (người báo: juntaozhang, 02/03/2026). Issue có stack trace `NullPointerException ... because "tableLease" is null` tại `KvSnapshotLeaseHandler.releaseBucket(KvSnapshotLeaseHandler.java:141)`. Kịch bản người báo nêu: job **Flink Delta Join**, sau khi checkpoint xong, `notifyCheckpointComplete` gửi yêu cầu release. Fluss **đã thực hiện xong** việc release, nhưng **phản hồi bị lỗi**, nên Flink gửi lại và **lần nào cũng thất bại**. Không có bình luận. Trạng thái: còn mở. PR [#2772](https://github.com/apache/fluss/pull/2772) **đang mở**.
**Nhóm B:** chỉ khi có job Flink đọc **bảng PK** (snapshot + log), ví dụ Delta Join hoặc source thường của bảng PK. Đây là các job dùng **KV snapshot lease** (có từ 0.9).

> **Mức chắc chắn:** cao, và kết luận là **đã sửa trong 1.0.0**. Mình đọc mã `v1.0.0` và thấy `KvSnapshotLeaseHandler.releaseBucket` đã có `if (tableLease == null) return NO_SNAPSHOT_ID;`. Đoạn kiểm tra này đến từ commit [#3661](https://github.com/apache/fluss/pull/3661) (16/07/2026, có trong tag `v1.0.0`). Commit đó còn cho client **tự retry** các thao tác lease. Issue vẫn mở trên GitHub, có lẽ vì #3661 không liên kết tới nó. Mình chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **KV snapshot** | Bản chụp RocksDB của một bucket bảng PK, lưu trên remote storage. Job Flink đọc bảng PK bằng cách **đọc snapshot trước, rồi đọc log** từ offset của snapshot. |
| **KV snapshot lease** | "Hợp đồng giữ chỗ" do client đăng ký với Coordinator: *"tôi đang đọc snapshot X của bucket Y, đừng xóa nó"*. Không có lease thì snapshot cũ có thể bị dọn (theo `kv.snapshot.num-retained`) giữa lúc job đang đọc. |
| **`scan.kv.snapshot.lease.id`** | Option Flink: ID của lease. Mặc định là **UUID ngẫu nhiên**. |
| **`scan.kv.snapshot.lease.duration`** | Option Flink: lease tự hết hạn sau bao lâu nếu không được trả. Mặc định **1 ngày**. |
| **`releaseSnapshots` / `releaseBucket`** | Client báo *"đã đọc xong snapshot của các bucket này, trả lại"*. Coordinator xóa bucket khỏi lease, giảm bộ đếm tham chiếu snapshot. Khi lease trống thì xóa cả lease. |
| **`KvSnapshotTableLease`** | Phần của lease ứng với một bảng: mảng `snapshotId` theo bucket (bảng thường) hoặc theo partition rồi bucket (bảng partition). |
| **Release lặp (idempotent)** | Gửi release hai lần cho cùng bucket. Lần hai phải được coi là "không có gì để làm", không được lỗi. |

## Tóm tắt

Khi job Flink trả lại bucket **cuối cùng** của một bảng trong lease, Coordinator xóa luôn phần của bảng đó (`tableIdToTableLease.remove(tableId)`). Nếu **phản hồi** của lần release này không tới được Flink (timeout, mất kết nối, Coordinator chuyển đổi), Flink **gửi lại** release cho cùng bucket. Ở bản bị lỗi, `tableIdToTableLease.get(tableId)` trả `null` và gây **NPE**. Coordinator trả `UnknownServerException`, Flink đưa bucket **vào lại hàng đợi** và gửi lại ở checkpoint sau, nên **lỗi lặp vô hạn**.

Trong `v1.0.0`, `releaseBucket` trả `NO_SNAPSHOT_ID` khi không tìm thấy bảng trong lease, nên release lặp **thành công lặng lẽ**.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-server`: `coordinator/lease/KvSnapshotLeaseHandler`, `coordinator/lease/KvSnapshotLeaseManager`;
  - `fluss-flink`: `source/enumerator/FlinkSourceEnumerator.notifyCheckpointComplete`.
- **Đã xác nhận trong mã `v1.0.0` (server):**
  ```java
  public long releaseBucket(TableBucket tableBucket) {
      ...
      KvSnapshotTableLease tableLease = tableIdToTableLease.get(tableId);
      if (tableLease == null) {
          return NO_SNAPSHOT_ID;                       // ← chống NPE, đến từ #3661
      }
      bucketIndex = partitionId == null ? tableLease.getBucketSnapshots()
                                        : tableLease.getBucketSnapshots(partitionId);
      if (bucketIndex != null) {
          if (bucketId >= bucketIndex.length) return NO_SNAPSHOT_ID;
          snapshotId = bucketIndex[bucketId];
          bucketIndex[bucketId] = NO_SNAPSHOT_ID;
          ... nếu mọi bucket đều NO_SNAPSHOT_ID → tableIdToTableLease.remove(tableId) ...
      }
      return snapshotId;
  }
  ```
  Các nhánh "không có gì" khác (partition không có trong lease, bucket ngoài mảng) cũng trả `NO_SNAPSHOT_ID`.
- `KvSnapshotLeaseManager.release(leaseId, buckets)`:
  - nếu **lease không tồn tại** thì `return` (không lỗi);
  - chỉ giảm `leasedBucketCount` và tham chiếu snapshot khi `snapshotId != NO_SNAPSHOT_ID`, nên release lặp **không giảm đếm hai lần**;
  - lease trống thì `dropLease`, còn không thì `metadataManager.updateLease` (ghi ZooKeeper).
- **Lịch sử (đã xem `git log` của file đến `v1.0.0`):**
  - #2179 (*"Support kv snapshot lease"*) thêm tính năng, chưa có kiểm tra null;
  - **#3661** (16/07/2026, *"Retry KV snapshot lease operations after metadata refresh"*) thêm đúng 3 dòng `if (tableLease == null) return -1L;`. Mô tả commit: *"Make repeated snapshot release safe when the lease still exists but the target table has already been removed. This keeps retries transparent to the enumerator during job recovery or transient network failures."* Commit này cũng cho `KvSnapshotLeaseImpl` phía client dùng `RetryableGatewayClientProxy`, tức **tự retry** khi metadata Coordinator cũ;
  - #3928 chỉ đổi `-1L` thành hằng `NO_SNAPSHOT_ID`.
- **Đã xác nhận trong mã `v1.0.0` (Flink):** `notifyCheckpointComplete` gọi `createKvSnapshotLease(id, duration).releaseSnapshots(consumed).get()`. Lỗi khác `UnsupportedVersionException` thì log ERROR *"Failed to release kv snapshot lease. These snapshots need to re-enqueue"* và **đưa lại bucket vào hàng đợi** với checkpoint hiện tại. Đây là lý do lỗi ở bản cũ **lặp mãi**.

## 2. Khi nào lỗi xảy ra

**Ở bản bị lỗi** (trước #3661, tức 0.9 và `main` trước 16/07/2026), cần:
1. Job Flink đọc bảng PK, có KV snapshot lease.
2. Release bucket **cuối cùng** của một bảng (hoặc partition cuối của bảng partition) thành công ở server, nhưng client **không nhận được phản hồi**.
3. Client gửi lại.

**Ở `v1.0.0`:** không xảy ra NPE nữa. Điều kiện trên chỉ dẫn tới một lần release lặp vô hại.

## 3. Tác động đến ai, làm sai lệch gì

**Ở bản bị lỗi:**
- Log ERROR lặp **mỗi checkpoint** phía Flink, và log lỗi phía Coordinator.
- Mỗi checkpoint lại gửi một RPC thất bại.
- **Không mất dữ liệu:** release đã thực hiện xong ở lần đầu. "Lệch trạng thái" chỉ là Flink nghĩ bucket chưa được trả.

**Ở `v1.0.0`:** không còn tác động.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Flink Delta Join | #2771: NPE `releaseBucket` | 🟡 | ❌"* trong `relate_issue.md`. Đối chiếu:
- **Sai trạng thái:** nên là **✅ đã sửa trong 1.0.0**, nhờ #3661.
- **Phạm vi rộng hơn Delta Join:** đường release này nằm trong `FlinkSourceEnumerator`, dùng chung cho **mọi** job Flink đọc bảng PK theo chế độ snapshot + log. Delta Join chỉ là kịch bản người báo gặp.

## 5. Mức ảnh hưởng với CDP

- **Không còn ảnh hưởng** khi chạy server và connector `1.0.0`.

**Lưu ý vận hành (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Dùng **cùng phiên bản** server và connector Flink 1.0.0. Bản sửa nằm ở server; phần retry nằm ở client.
- Nếu đặt `scan.kv.snapshot.lease.id` thủ công, mỗi job nên có **ID riêng**. Hai job dùng chung một lease ID sẽ trả snapshot của nhau (suy luận từ mã: lease được tra theo ID).
- Theo dõi log Flink *"Failed to release kv snapshot lease"*. Nếu lặp liên tục thì đó là vấn đề khác, cần điều tra.
- Lease không được trả (job chết) sẽ giữ snapshot tới hết `scan.kv.snapshot.lease.duration` (mặc định 1 ngày), làm remote storage tốn dung lượng hơn. Nên tính vào dung lượng.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test tái hiện (release hai lần bucket cuối) trên 1.0.0. Mình chỉ đọc mã và commit, không đọc test thêm trong #3661.
- Mình chưa đọc diff của PR #2772 (đang mở). Có thể nó sửa cùng chỗ, và trở nên thừa.
- Mình chưa kiểm tra khi **Coordinator failover** giữa chừng: lease được nạp lại từ ZooKeeper (`metadataManager.getLease`) có đúng trạng thái sau release hay không.
