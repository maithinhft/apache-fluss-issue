# Issue #4566 — KV recovery thất bại khi toàn bộ dữ liệu local của TabletServer bị mất

Link: https://github.com/apache/fluss/issues/4566 — Phiên bản được báo: `1.0.0`

## Tóm tắt

Một bảng khóa chính (PK/KV) lưu trạng thái ở hai nơi: **KV** (RocksDB, cục bộ trên TabletServer, có KV snapshot định kỳ đẩy lên remote storage) và **log/WAL** (cục bộ, cũ hơn thì được đẩy lên remote). Khi TabletServer mất hết dữ liệu local nhưng ZooKeeper và remote storage còn nguyên, lẽ ra bảng phải tự khôi phục từ **KV snapshot + phần WAL còn trên remote**. Issue nói việc này có thể **thất bại**, vì:

1. Log local được tạo mới, rỗng, bắt đầu từ offset 0, trong khi KV snapshot ghi một offset cao hơn (ví dụ 1000). Bước khôi phục KV phải đọc log từ offset 1000, nhưng log local không có offset đó và ranh giới log không được khôi phục từ trạng thái bền trên remote.
2. Sau khi một lần khôi phục thất bại, trạng thái KV khởi tạo dở vẫn còn đăng ký, nên các lần retry sau đó thất bại với lỗi `Duplicate kv tablet directories`, che mất lỗi gốc.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, phần khôi phục KV khi một replica trở thành leader.
- **Các lớp liên quan** (đã đọc tại tag `v1.0.0`):
  - `Replica` (`createKv()`, `initKvTablet()`, `recoverKvTablet()`): `onBecomeNewLeader()` gọi `dropKv()` rồi `createKv()`. `initKvTablet()` tải KV snapshot mới nhất, đặt `restoreStartOffset = completedSnapshot.getLogOffset()`, rồi gọi `recoverKvTablet(restoreStartOffset, ...)`.
  - `KvRecoverHelper.recover()` / `readLogRecordsAndApply(...)`: đọc log từ `restoreStartOffset` và áp dụng vào KV. Nó chỉ lấy dữ liệu từ remote khi `nextFetchOffset < localLogStartOffset`; ngược lại nó đọc thẳng từ log local bằng `logTablet.read(nextFetchOffset, ...)`.
  - `KvManager.loadKv(...)`: ném `IllegalStateException("Duplicate kv tablet directories ...")` nếu bucket đó đã có KV tablet đang đăng ký trong `currentKvs`.
- **Cơ chế lỗi 1 (ranh giới log):** log local rỗng nên `localLogStartOffset = 0` và log end offset cũng là 0. Giả sử snapshot ở offset S > 0. Vì S không nhỏ hơn 0, `KvRecoverHelper` không chuyển sang đọc remote mà gọi `logTablet.read(S, ...)` trên một log rỗng. Offset S nằm ngoài phạm vi của log local, nên bước này không thể đọc được (mình suy ra ngoại lệ sẽ là lỗi offset ngoài phạm vi, chưa chạy thử để xem đúng loại exception). Đây là lúc "ranh giới log local" không được dựng lại từ trạng thái trên remote.
- **Cơ chế lỗi 2 (retry bị che lỗi):** `Replica.createKv()` gọi `initKvTablet()` tối đa `INIT_KV_TABLET_MAX_RETRY_TIMES = 5` lần trong một vòng lặp và **không gọi `dropKv()` giữa các lần thử**, chỉ gọi sau khi hết lượt. Lần thử đầu đã tải snapshot và `loadKv` (đăng ký KV tablet) rồi mới thất bại ở bước đọc log. Các lần thử 2 đến 5 gọi lại `loadKv` cho cùng bucket nên gặp `Duplicate kv tablet directories`. Exception cuối cùng mà nơi gọi nhận được (`KvStorageException: Failed to create KV tablet ... after 5 attempts`) có nguyên nhân là lỗi trùng thư mục này, còn lỗi thật ở lần thử đầu chỉ nằm trong dòng log `WARN` đầu tiên. (Phần này mình suy ra từ mã nguồn; chưa chạy thực tế.)

## 2. Khi nào lỗi xảy ra

Phải thỏa đồng thời (theo bước tái hiện trong issue):

1. **Bảng khóa chính** có nhiều replica.
2. **Đã có ít nhất một KV snapshot hoàn tất** và phần WAL cần thiết đã được upload lên remote storage.
3. **Mất toàn bộ dữ liệu local của TabletServer**: file KV, WAL, checkpoint và marker tắt máy sạch (clean-shutdown marker). ZooKeeper và remote storage vẫn còn.
4. Có thêm dữ liệu được ghi **sau** snapshot và sau phần WAL đã upload, chỉ tồn tại trên các replica local đã mất. Issue chấp nhận mất phần này, nhưng yêu cầu bảng vẫn phải khôi phục được về một mốc nhất quán.
5. Khởi động lại cluster và truy cập bảng; khi replica trở thành leader sẽ chạy `createKv()`.

Kịch bản thực tế dễ gặp: triển khai trên Kubernetes dùng `emptyDir` hoặc ổ đĩa tạm thay vì volume bền, mất cả node, hoặc cài lại toàn bộ TabletServer mà không giữ đĩa. Với mỗi replica, mất đĩa của riêng nó không gây lỗi này, vì còn replica khác giữ dữ liệu; lỗi chỉ lộ ra khi **mọi** replica của bucket đều mất dữ liệu local.

Không ảnh hưởng: log table (không có KV), bảng mà ít nhất một replica còn nguyên dữ liệu local, và sự cố khởi động lại thông thường khi đĩa còn.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành cluster production có bảng khóa chính, trong kịch bản khôi phục sau thảm họa (mất toàn bộ đĩa của các TabletServer, hoặc di chuyển cluster sang hạ tầng mới nhưng giữ ZooKeeper và remote storage).

**Hậu quả:**

1. **Bảng không phục hồi được dù dữ liệu bền vẫn còn đủ.** Bucket không có leader hoạt động, nên không đọc/ghi được các bảng PK bị ảnh hưởng. Mình chưa kiểm tra chính xác bộ điều phối xử lý ngoại lệ ném ra từ `createKv()` như thế nào (ví dụ replica có bị đánh dấu offline và thử lại hay không) nên chưa xác định được trạng thái cuối cùng của bucket.
2. **Lỗi gốc bị che.** Người vận hành chỉ thấy `Duplicate kv tablet directories` (kèm hướng dẫn xóa thư mục thủ công, không đúng nguyên nhân), dễ xử lý sai.
3. **Không có sai lệch dữ liệu âm thầm** trong lỗi này: bảng thất bại rõ ràng thay vì trả dữ liệu sai. Rủi ro nằm ở tính sẵn sàng và thời gian khôi phục, không phải tính đúng đắn.

**Điều cần làm tốt hơn (theo mong đợi trong issue):** khi có snapshot và WAL remote cần thiết thì bảng phải khôi phục đến một mốc bền nhất quán rồi tiếp tục đọc, ghi, nhân bản, snapshot và tiering. Dữ liệu ghi sau mốc đó, kể cả dữ liệu đã được xác nhận nhưng chỉ nằm trên các replica đã mất, có thể mất. Nếu WAL cần thiết bị thiếu hoặc cắt cụt thì phải báo lỗi rõ ràng, không được tạo ra trạng thái KV thiếu dữ liệu.

**Gợi ý cho production:** dùng volume bền (PersistentVolume) cho thư mục dữ liệu của TabletServer; không dựa vào remote storage một mình để khôi phục sau mất toàn bộ đĩa cho đến khi issue này được sửa; nên diễn tập kịch bản mất đĩa trên môi trường thử.
