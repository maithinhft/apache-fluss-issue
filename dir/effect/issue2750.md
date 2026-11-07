# Issue #2750 — Server nhận yêu cầu `PRODUCE_LOG` (ghi log trực tiếp) vào bảng PK, bỏ qua RocksDB

Link: https://github.com/apache/fluss/issues/2750 — Phiên bản được báo: `main` thời 0.9 (người báo: loserwang1024, 28/02/2026). Issue một dòng: *"when PRODUCE_LOG, we don't check whether a table is log table or pk table."* Có 2 bình luận:
- một contributor đề xuất hai lớp kiểm tra: kiểm tra `hasPrimaryKey` trong `TabletService.produceLog` và `putKv`, cộng một lớp dự phòng trong `ReplicaManager.appendToLocalLog`;
- người báo đồng ý.

Trạng thái: còn mở. PR [#2752](https://github.com/apache/fluss/pull/2752) **đang mở** (chưa merge).
**Nhóm B:** chỉ liên quan khi có client **gửi sai loại yêu cầu** (bug client, client tự viết, client bản cũ hoặc độc hại). Client Java chuẩn ghi bảng PK bằng `PUT_KV`.

> **Mức chắc chắn:** cao, và kết luận là **đã được chặn trong 1.0.0**, dù không bằng PR #2752. Mình đọc mã `v1.0.0`:
> - `TabletService.produceLog` **không** kiểm tra bảng PK, đúng như issue;
> - nhưng `Replica.appendRecordsToLeader`, nơi mọi `PRODUCE_LOG` phải đi qua để ghi, **đã có** đoạn `if (isKvTable()) throw new InvalidPartitionException("Produce-log request must not target a primary-key table.")`.
>
> Mình **chưa xác định** commit nào thêm đoạn này, vì bản clone git trong scratchpad đã mất. Mình chưa chạy test.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`PRODUCE_LOG`** | RPC ghi bản ghi **thẳng vào log** của bucket. Dùng cho **bảng log** (append-only). |
| **`PUT_KV`** | RPC ghi vào **bảng PK**: TabletServer cập nhật **RocksDB** (trạng thái khóa → giá trị) rồi sinh **changelog** (`+I`, `-U`, `+U`, `-D`) vào log. Log và KV luôn khớp nhau. |
| **Bảng PK có log** | Log của bảng PK là **changelog do KV sinh ra**. Ghi thẳng vào log mà không qua KV thì **log và KV lệch nhau**. |
| **`isKvTable()`** | `Replica` biết bucket thuộc bảng PK (có `KvTablet`) hay không. |

## Tóm tắt

Nếu một client gửi `PRODUCE_LOG` vào bucket của **bảng PK**, server cũ sẽ **ghi thẳng** bản ghi vào changelog mà **không cập nhật RocksDB**. Hậu quả:
- người đọc changelog (Flink streaming) thấy thay đổi **không có thật** trong trạng thái KV;
- lookup và snapshot (đọc từ KV) **không thấy** thay đổi đó;
- khi phục hồi KV từ log, có thể áp dụng các bản ghi "lạ".

Trong `v1.0.0`, `Replica.appendRecordsToLeader` **từ chối** với `InvalidPartitionException`, nên ghi sai loại **không lọt** vào log.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `tablet/TabletService.produceLog`, `replica/ReplicaManager.appendToLocalLog`, `replica/Replica.appendRecordsToLeader`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `TabletService.produceLog`: `authorizeTable(WRITE, tableId)`, rồi kiểm tra định tuyến bucket (`collectRoutingErrors`), rồi chuyển xuống `ReplicaManager`. **Không** kiểm tra khóa chính.
  - `ReplicaManager.appendToLocalLog`: với mỗi bucket, `getReplicaOrException(tb)` rồi `replica.appendRecordsToLeader(records, requiredAcks)`. Exception được bắt và chuyển thành lỗi theo bucket.
  - `Replica.appendRecordsToLeader`:
    ```java
    if (!isLeader()) throw new NotLeaderOrFollowerException(...);
    // Primary-key writes must go through PUT_KV so the log and local KV state are
    // updated together. PRODUCE_LOG is only valid for append-only tables.
    if (isKvTable()) {
        throw new InvalidPartitionException("Produce-log request must not target a primary-key table.");
    }
    validateInSyncReplicaSize(requiredAcks);
    appendInfo = logTablet.appendAsLeader(memoryLogRecords);
    ```
  - **Chiều ngược lại** (`PUT_KV` vào bảng log): `Replica.putRecordsToLeader` có `checkNotNull(kv, "KvTablet for the replica to put kv records shouldn't be null.")`. Yêu cầu cũng **bị từ chối**, chỉ là thông báo lỗi kém rõ hơn.
- **Ghi chú:** loại exception được dùng (`InvalidPartitionException`) **không khớp ngữ nghĩa** (không liên quan partition). Đó chỉ là chuyện thông báo lỗi.

## 2. Khi nào lỗi xảy ra

**Ở bản bị lỗi** (0.9 và `main` trước khi có đoạn kiểm tra): một client gửi `PRODUCE_LOG` tới bucket của bảng PK. Client Java chuẩn **không** làm vậy. Rủi ro đến từ:
- client ngôn ngữ khác (Rust, Python, C++) có bug;
- client tự viết theo giao thức;
- lỗi cấu hình hoặc người dùng có quyền WRITE cố ý ghi sai.

**Ở `v1.0.0`:** bị từ chối.

## 3. Tác động đến ai, làm sai lệch gì

**Ở bản bị lỗi:**
- **Changelog lệch KV.** Đây là loại hỏng dữ liệu âm thầm: người tiêu thụ changelog và người lookup thấy dữ liệu khác nhau.

**Ở `v1.0.0`:** yêu cầu sai bị trả lỗi theo bucket, **không ghi**. Client nhận exception.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Rebalance / Typed API / JMX | ..., #2750 | 🟢 | ❌"*. Đối chiếu:
- **Sai trạng thái:** nên là **✅ (đã chặn trong 1.0.0)**, vì `Replica.appendRecordsToLeader` đã có kiểm tra `isKvTable()`. Issue và PR #2752 vẫn mở, có lẽ vì bản sửa đến từ thay đổi khác.
- Phân tích cũ của mình (chỉ nhìn `TabletService.produceLog`) đã **bỏ sót** lớp kiểm tra này.

## 5. Mức ảnh hưởng với CDP

- **Không còn ảnh hưởng** khi server chạy `1.0.0`.

**Lưu ý vận hành (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Giới hạn quyền **WRITE** theo bảng bằng ACL (nếu bật authorization), để chỉ service đúng mới ghi được.
- Nếu dùng client ngôn ngữ khác (Python, Rust) để ghi bảng PK, test rằng chúng dùng **upsert** (`PUT_KV`), không phải append.

## 6. Những điều chưa kiểm chứng

- Mình chưa xác định commit hoặc PR đã thêm kiểm tra `isKvTable()` vào `appendRecordsToLeader`, và chưa kiểm tra nó vào trước hay sau 0.9.
- Mình chưa chạy test gửi `PRODUCE_LOG` vào bảng PK trên 1.0.0.
- Mình chưa kiểm tra đường ghi của **follower** (`appendRecordsToFollower` không có kiểm tra, nhưng follower chỉ sao chép từ leader nên không cần).
