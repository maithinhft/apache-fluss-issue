# Issue #2209 — Client bị chặn (và có thể deadlock) khi cập nhật metadata đồng bộ lúc lookup bảng partition

Link: https://github.com/apache/fluss/issues/2209 — Phiên bản được báo: `0.8.0`. PR sửa: [#2212](https://github.com/apache/fluss/pull/2212), chưa merge (theo `relate_issue.md`).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Metadata (phía client)** | "Bản đồ" cluster mà client giữ trong bộ nhớ: bảng có những partition nào, mỗi partition có `partitionId` gì, mỗi bucket nằm ở TabletServer nào. Client cần bản đồ này để biết gửi yêu cầu lookup tới đâu. |
| **Bảng partition** | Bảng chia thành nhiều partition (thường theo ngày). Để tra một khóa, client phải đổi *tên partition* của khóa đó (ví dụ `2026-11-05`) thành `partitionId`. |
| **Cập nhật metadata** | Client gửi một RPC `GET_METADATA` hỏi server rồi dựng lại bản đồ. Cần làm khi bản đồ chưa có partition cần dùng. |
| **Đồng bộ (synchronous)** | Thread gọi hàm **đứng chờ tại chỗ** cho tới khi có kết quả. Ngược lại, bất đồng bộ trả về `CompletableFuture` và thread đi làm việc khác. |
| **Netty IO thread** | Netty là thư viện mạng mà client Fluss dùng. Nó có một số ít thread cố định (event loop) làm hai việc: đọc dữ liệu từ socket và chạy các callback khi có phản hồi. Phản hồi của server chỉ được xử lý khi có một IO thread rảnh. (Đây là hành vi chung của Netty; mình chưa đọc cấu hình số IO thread của client Fluss.) |
| **Deadlock** | Hai bên chờ nhau nên không ai tiến lên được. |

## Tóm tắt

Khi tra cứu (lookup) trên bảng partition, nếu client chưa biết `partitionId` của khóa, nó gọi một hàm **đồng bộ** để hỏi server metadata. Hàm này chặn thread đang gọi cho tới khi phản hồi về. Nếu thread bị chặn lại chính là một Netty IO thread, và phản hồi của server cũng phải được chính các IO thread đó xử lý, thì có thể xảy ra tình huống: tất cả IO thread đều đang chờ phản hồi, còn phản hồi thì không có IO thread rảnh để nhận. Cả client đứng im.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, đường tra cứu (lookup) và cập nhật metadata.
- **Các lớp liên quan** (đã đọc tại tag `v1.0.0`):
  - `AbstractLookuper.resolvePartitionRouting(partitionName)`: gọi `metadataUpdater.checkAndUpdatePartitionMetadata(partitionPath)` rồi đọc `partitionId`. Được `PrimaryKeyLookuper.lookup(...)` gọi trước khi gửi yêu cầu.
  - `MetadataUpdater.updateMetadata(...)`: bọc việc gửi RPC trong `synchronized (this)`, nên mọi lần cập nhật metadata trong cùng một client **xếp hàng qua một khóa duy nhất**.
  - `MetadataUtils.sendMetadataRequestAndRebuildCluster(...)`: có dòng `.get(30, TimeUnit.SECONDS)` (khoảng dòng 167), tức thread gọi **đứng chờ tối đa 30 giây** cho phản hồi. Cạnh đó còn comment `TODO currently, we don't have timeout logic in ...`.
- **Cơ chế lỗi** theo issue: `PrimaryKeyLookuper.lookup()` gọi đồng bộ `getPartitionId()` trên Netty IO thread, chặn thread này trong lúc chờ metadata. Khi nhiều request cùng cần metadata, mọi IO thread có thể cùng bị chặn, không còn thread nào xử lý phản hồi metadata, nên deadlock.
- **Hướng sửa trong issue:** làm cập nhật metadata hoàn toàn bất đồng bộ (`checkAndUpdatePartitionMetadataAsync()`, `updateMetadataAsync()` trả `CompletableFuture`), gom nhiều yêu cầu metadata đồng thời thành một RPC, và khử trùng lặp (nhiều yêu cầu cho cùng bảng/partition dùng chung một future).
- **Trạng thái trong `v1.0.0`:** các đoạn mã đồng bộ trên **vẫn còn**. PR #2212 chưa merge.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Bảng có partition** (với bảng không partition, `resolvePartitionRouting` không được gọi).
2. **Client chưa có metadata của partition cần tra**, ví dụ: vừa khởi động; vừa sang ngày mới nên có partition mới; partition vừa được tạo hoặc bị xóa.
3. **Nhiều lookup đồng thời** cùng gặp thiếu metadata, tức tải đồng thời cao.
4. **Với dạng nặng nhất (deadlock cả IO thread):** `lookup()` được gọi **từ bên trong một Netty IO thread**. Điều này xảy ra khi code của bạn gọi `lookup()` trong callback của một future mà Netty hoàn tất, ví dụ `.thenCompose(...)` nối tiếp từ kết quả của lookup hoặc lần ghi trước.

**Điều mình đã kiểm tra về đường Flink:** `FlinkAsyncLookupFunction.asyncLookup` ở `v1.0.0` gọi `lookuper.lookup(...)` từ thread của Flink, và callback `whenComplete` chỉ hoàn tất future kết quả, không gọi lookup lần nữa. Trong mã có chú thích rằng không được gọi `lookuper.lookup()` trong callback vì lookuper không an toàn đa luồng. Vì vậy, với Flink async lookup chuẩn, lookup **không** chạy trên Netty IO thread. Phiên bản bạn gặp là nhẹ hơn: thread của Flink bị chặn khi thiếu metadata (xem mục 3).

**Về "treo vĩnh viễn":** issue báo trên `0.8.0`, mô tả treo vô thời hạn. Ở `v1.0.0` việc chờ có giới hạn 30 giây. Khi quá hạn sẽ có `TimeoutException`; `updateMetadata` bắt lỗi này như lỗi có thể thử lại, ghi cảnh báo và đánh dấu server là không khả dụng. Mình suy ra mỗi lần thiếu metadata có thể tốn tới khoảng 30 giây và các lần cập nhật khác phải xếp hàng sau khóa `synchronized`, nhưng chưa chạy thực tế nên chưa khẳng định được hành vi cuối cùng. Phiên bản nào bắt đầu có timeout 30 giây thì mình chưa kiểm tra.

**Mình chưa kiểm tra:** việc các phản hồi RPC của client Fluss có thực sự được hoàn tất trên chính các IO thread đó hay trên một executor riêng; điều này quyết định deadlock IO thread có tái hiện được hay không.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:**
- Ứng dụng dùng Fluss Java client để lookup bảng partition với tải đồng thời cao, nhất là **ứng dụng tự viết (ví dụ AI agent) nối chuỗi lookup bằng callback**: đây là dạng có nguy cơ deadlock IO thread.
- Job Flink dùng lookup join trên bảng partition: ít nguy cơ deadlock, nhưng thread xử lý của Flink có thể bị chặn khi thiếu metadata.

**Hậu quả:**
1. **Client treo hoặc đứng khựng.** Dạng nặng: cả client không xử lý được mọi phản hồi mạng, kể cả của các lookup và ghi khác trên cùng client. Dạng nhẹ: các lookup bị chậm hàng chục giây.
2. **Tăng độ trễ hàng loạt** khi cả đoàn lookup xếp hàng chờ cùng một khóa metadata.
3. **Không có sai lệch dữ liệu được ghi nhận.** Đây là lỗi về tính sẵn sàng và độ trễ, không phải dữ liệu sai.
4. **Dễ xuất hiện đúng lúc nhạy cảm:** khi sang ngày mới (partition mới), sau khi restart ứng dụng, hoặc sau khi partition hết hạn; và khó tái hiện vì cần tải đồng thời.

**Biện pháp (từ phân tích hiện tại):**
- Ưu tiên bảng context cho AI agent là **PK không partition**, vì nó không đi qua đoạn đồng bộ này.
- Không gọi `lookup()` bên trong callback của một future do Fluss client hoàn tất; chuyển sang thread pool riêng của ứng dụng trước khi gọi.
- **Khởi động ấm (warm-up):** lookup thử các partition hiện tại ngay khi khởi động để nạp metadata trước khi nhận tải.
- Đặt timeout ở tầng ứng dụng cho mọi lookup.
- Nếu cần fix sớm, cân nhắc tự build kèm patch của PR #2212 sau khi đánh giá.

## Đính chính cho `relate_issue.md`

Dòng #2209 trong `relate_issue.md` đang ghi "Đúng luồng AI agent / Flink async lookup" và xếp 🔴 Rất cao. Theo kết quả đọc mã ở trên, cách viết này quá rộng: với Flink async lookup chuẩn, lookup không chạy trên Netty IO thread. Mình đề nghị sửa thành "chủ yếu ảnh hưởng ứng dụng agent tự viết gọi lookup nối tiếp trong callback", và xem lại mức độ (có thể hạ xuống 🟠) cho khớp với phân tích này.
