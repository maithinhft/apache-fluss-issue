# Issue #4599 — Việc khởi tạo KV snapshot định kỳ "đua" với việc xóa replica, gây lỗi `registry is already closed`

Link: https://github.com/apache/fluss/issues/4599 — Phiên bản được báo: `main` (người báo: fxbing, 12/10/2026). Issue có **tái hiện xác định** dựng trên `ReplicaTest` với Replica, RocksDB và registry thật, chạy trên cả bản gốc và nhánh của PR #4492 (fix của #4489), kèm bảng kết quả. Có hai bình luận: một người xin nhận làm, người báo trả lời là không làm. Không có PR. Trạng thái: còn mở.

> **Mức chắc chắn:** cao về cơ chế, vì có tái hiện xác định của người báo. Người báo ghi rõ: *"Deletion completed, and no snapshot corruption was established"*, tức việc xóa vẫn hoàn tất và **chưa thấy hỏng snapshot**. Mình đã đọc mã `v1.0.0`, và thấy thêm một điểm (suy luận, chưa tái hiện): `dropKv()` không chỉ chạy khi xóa replica mà **còn chạy khi replica đổi vai trò leader ↔ follower**, nên cuộc đua có thể xảy ra cả lúc đổi leader.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **KV snapshot** | Bản chụp định kỳ trạng thái RocksDB của một bucket bảng PK, đẩy lên remote storage để phục hồi khi mất máy. Chu kỳ là `kv.snapshot.interval`. |
| **`PeriodicSnapshotManager`** | Bộ lập lịch snapshot của mỗi bucket leader. Lịch theo kiểu chuỗi: mỗi lần `triggerSnapshot` chạy xong mới hẹn lần sau (xem #4489). |
| **`initSnapshot`** | Bước đầu của snapshot, chạy đồng bộ: chuẩn bị RocksDB checkpoint và đăng ký tài nguyên để dọn sau. |
| **`CloseableRegistry`** | "Sổ" các tài nguyên cần đóng khi KV của replica bị hủy. Khi sổ đã đóng, mọi lần đăng ký mới đều bị từ chối bằng `IOException: Cannot register Closeable, registry is already closed. Closing argument.` |
| **`dropKv()`** | Hàm trong `Replica` hủy KV tablet của bucket: đóng `closeableRegistryForKv`, đóng RocksDB, xóa khỏi `KvManager`. |
| **Race condition (tranh chấp)** | Hai việc chạy song song không được đồng bộ, nên kết quả phụ thuộc thứ tự ngẫu nhiên. |

## Tóm tắt

`Replica.dropKv()` đóng `closeableRegistryForKv`. Registry **đặt `closed = true` trước**, rồi mới đóng lần lượt các tài nguyên đã đăng ký, trong đó có `PeriodicSnapshotManager`. Nếu một lần snapshot đã lên lịch **bắt đầu chạy đúng trong khe hở này**, nó cố đăng ký tài nguyên vào registry đã đóng và nhận `IOException`. Thêm vào đó, `PeriodicSnapshotManager.close()` chỉ hủy future đã lên lịch, **không đồng bộ** với bước khởi tạo snapshot đang chạy dở dưới `kvLock`.

Người báo thấy 4 lần như vậy khi xóa bảng trong test. Việc xóa vẫn hoàn tất, và chưa thấy hỏng snapshot. Fix của #4489 (#4492) **không** giải quyết cuộc đua này.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `replica/Replica.dropKv`, `kv/snapshot/PeriodicSnapshotManager` (`triggerSnapshot`, `close`), `utils/CloseableRegistry`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `PeriodicSnapshotManager.close()`:
    ```java
    synchronized (this) {
        started = false;
        if (scheduledTask != null && !scheduledTask.isDone()) { scheduledTask.cancel(true); }
    }
    ```
    Chỉ đặt cờ và hủy future. Một lần `triggerSnapshot` **đã vào bên trong** (đã qua kiểm tra `if (started)`) vẫn chạy tiếp tới `target.initSnapshot()`.
  - `triggerSnapshot()`: nếu `initSnapshot()` ném exception thì `LOG.error("Fail to init snapshot during triggering snapshot.", e); return;`. Đây chính là chỗ của #4489 (không hẹn lần sau). Trong ngữ cảnh xóa replica, `return` là vô hại vì replica đang bị hủy. Nhưng **dòng log ERROR giống hệt** với dấu hiệu của #4489.
  - `Replica.dropKv()`: đóng các scanner, rồi `closeableRegistry.unregisterCloseable(closeableRegistryForKv)` và `IOUtils.closeQuietly(closeableRegistryForKv)`, rồi `kvManager.dropKv(tableBucket)`. Comment ghi *"Runs under leaderIsrUpdateLock(W)"*.
- **Phát hiện thêm (đã đọc mã, suy luận về hệ quả):** `dropKv()` được gọi từ **bốn chỗ** trong `Replica` của 1.0.0:
  1. `delete()` (xóa replica, đúng kịch bản của issue);
  2. khi **trở thành leader mới** (comment: *"if it's become new leader, we must first destroy the old kv tablet ... Otherwise ... data loss"*);
  3. khi **chuyển từ leader sang follower** (comment: *"it should be from leader to follower, we need to destroy the kv tablet"*);
  4. một chỗ khác ở dòng khoảng 792 mà mình chưa xác định ngữ cảnh.

  Vì vậy cùng cuộc đua giữa snapshot và việc đóng registry có thể xảy ra **mỗi lần một bucket bảng PK đổi leader**: rolling restart, rebalance, failover. Mình chưa tái hiện trường hợp này.

## 2. Khi nào lỗi xảy ra

- **Theo issue:** drop bảng PK hoặc partition (gồm partition hết hạn tự động) đúng lúc snapshot định kỳ của bucket đang bắt đầu.
- **Theo suy luận thêm:** bucket bảng PK đổi leader (leader cũ thành follower) đúng lúc nó đang bắt đầu snapshot.
- Khe hở rất nhỏ (giữa `closed = true` và việc đóng manager), nhưng xảy ra trên **mọi bucket** mỗi lần xóa hoặc đổi leader.

**Phiên bản:** báo trên `main`. Mã `v1.0.0` có cùng cấu trúc.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster có bảng PK, khi xóa bảng hoặc partition, hoặc khi đổi leader.

**Hậu quả:**
1. **Log ERROR** `Fail to init snapshot during triggering snapshot.` kèm `IOException: Cannot register Closeable, registry is already closed`.
2. **Gây nhiễu cho việc giám sát #4489:** nếu bạn alert theo dòng `Fail to init snapshot` để phát hiện snapshot bị dừng vĩnh viễn (#4489), các dòng từ cuộc đua này là **alert giả**. Cần phân biệt bằng nguyên nhân (`registry is already closed`) và ngữ cảnh (bucket vừa bị xóa hoặc vừa đổi leader).
3. **Không có bằng chứng hỏng dữ liệu hoặc snapshot** (theo người báo). Mình chưa kiểm tra tài nguyên đã tạo dở trong `initSnapshot` (ví dụ thư mục RocksDB checkpoint) có bị bỏ lại trên đĩa hay không.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4599 trong `relate_issue.md` ghi: *"❌ | Chỉ log lỗi"*. Đối chiếu:
- **Đúng** theo những gì người báo quan sát.
- **Bổ sung:**
  - Có thể xảy ra cả khi **đổi leader**, không chỉ khi xóa (suy luận từ các chỗ gọi `dropKv()`).
  - Log lỗi này **trùng nội dung** với dấu hiệu của #4489, nên ảnh hưởng tới cách alert.

## 5. Mức ảnh hưởng với CDP

- **Thấp** về dữ liệu.
- **Có ảnh hưởng tới giám sát**: CDP nên alert #4489 (snapshot dừng), nhưng cần lọc nhiễu từ issue này.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi alert snapshot (cho #4489), **đừng dựa vào dòng log `Fail to init snapshot` đơn lẻ**. Thay vào đó, alert theo **thời điểm snapshot cuối của từng bucket leader** không tiến quá N lần `kv.snapshot.interval`.
- Nếu vẫn muốn dùng log, loại các dòng mà nguyên nhân là `registry is already closed`.
- Định kỳ kiểm tra thư mục tạm của snapshot hoặc checkpoint trên đĩa TabletServer có bị tồn đọng không (chưa xác minh là có).

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện trường hợp đổi leader.
- Mình chưa kiểm tra `initSnapshot` để lại gì trên đĩa khi bị lỗi giữa chừng.
- Mình chưa xác định ngữ cảnh lời gọi `dropKv()` thứ tư trong `Replica`.
- Mình chưa kiểm tra `initSnapshot` có giữ `leaderIsrUpdateLock` hay không, tức `dropKv()` (chạy dưới write lock) có thực sự chạy song song được với nó hay không. Người báo đã tái hiện được với đường xóa, nên ít nhất ở đường đó là có.
