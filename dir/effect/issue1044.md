# Issue #1044 — Buffer của request có thể bị giải phóng (release) nhiều lần trên server RPC

Link: https://github.com/apache/fluss/issues/1044 — Phiên bản được báo: `0.6.0` (người báo: loserwang1024, 09/06/2025, nhãn `priority=critical`). Issue chỉ liệt kê **ba chỗ** giải phóng buffer và nói *"không có gì đảm bảo buffer của một request chỉ bị release một lần"*. Có một bình luận cc maintainer. Không có PR liên kết. Trạng thái: còn mở.

> **Mức chắc chắn:** trung bình–cao. Mình đã đọc các điểm giải phóng buffer trong `fluss-rpc` của `v1.0.0`:
> - **Vẫn có** những đường mà cùng một buffer bị release hai lần.
> - Lần release thứ hai dùng `ReferenceCountUtil.safeRelease`, nên **không ném lỗi** mà chỉ log cảnh báo.
>
> Mình **chưa kiểm tra** nguy cơ khó thấy hơn: buffer bị release **trước khi** xử lý bất đồng bộ đọc xong (use-after-free).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Netty / `ByteBuf`** | Netty là thư viện mạng của Fluss. `ByteBuf` là vùng nhớ (thường off-heap, lấy từ pool) chứa dữ liệu nhận qua mạng. |
| **Reference counting** | Mỗi `ByteBuf` có bộ đếm tham chiếu. `release()` giảm đi 1, và về 0 thì vùng nhớ **trả lại pool**, có thể được cấp cho request khác. |
| **Release hai lần** | Gọi `release()` khi bộ đếm đã là 0. Netty ném `IllegalReferenceCountException`. |
| **`ReferenceCountUtil.safeRelease`** | Hàm Netty: thử release, và nếu lỗi thì **chỉ log WARN**, không ném. |
| **Lazily parsed message** | Với request lớn (ví dụ produce log, put KV), Fluss **không sao chép** dữ liệu ra khỏi `ByteBuf` mà đọc thẳng từ đó. Vì vậy buffer phải sống tới khi xử lý xong. |
| **Use-after-free** | Dùng vùng nhớ đã được trả lại pool. Có thể đọc phải dữ liệu của request khác, tức hỏng dữ liệu. Đây là hậu quả **nặng** hơn nhiều so với release hai lần. |
| **`RequestProcessor`** | Luồng phía server lấy request khỏi hàng đợi và gọi hàm RPC tương ứng. |

## Tóm tắt

Issue chỉ ra ba nơi giải phóng buffer: `NettyClientHandler#channelRead`, `NettyServerHandler#sendResponse` và `RequestProcessor#run`. Trong `1.0.0` (tên lớp như cũ):
- `RequestProcessor` **luôn** gọi `request.releaseBuffer()` trong `finally` sau khi gọi hàm RPC.
- `NettyServerHandler.sendResponse` **cũng** gọi `request.releaseBuffer()` nếu request bị đánh dấu hủy, hoặc nếu kênh đã không còn active khi trả lời.
- `NettyServerHandler.channelRead` gọi `buffer.release()` trực tiếp nếu kênh không active hoặc parse lỗi.

Vì vậy cùng một buffer vẫn có thể bị release hai lần. Từ trước 1.0.0, `FlussRequest.releaseBuffer()` đã dùng `ReferenceCountUtil.safeRelease`, nên lần thứ hai **chỉ ra log cảnh báo**, không làm hỏng xử lý.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-rpc`, gồm `netty/server/FlussRequest`, `netty/server/NettyServerHandler`, `netty/server/RequestProcessor`, `netty/client/NettyClientHandler`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlussRequest.releaseBuffer()`:
    ```java
    if (message.isLazilyParsed()) {
        ReferenceCountUtil.safeRelease(buffer);
    }
    ```
    Chỉ release khi message được parse kiểu lazy, và dùng `safeRelease`.
  - `RequestProcessor` (vòng xử lý request):
    ```java
    try { processRequest(request); }
    catch (Throwable t) { LOG.error("Error while processing request.", t); }
    finally {
        // this releases the lazily parsed buffer (e.g. for produce request).
        // RpcGateway shouldn't hold the reference to the request/ByteBuf after
        // the invoking of RPC method, i.e. async execution shouldn't retain the request/ByteBuf.
        request.releaseBuffer();
    }
    ```
  - `NettyServerHandler.sendResponse(ctx, request)` chạy khi future phản hồi hoàn tất (`future.whenCompleteAsync(..., ctx.executor())`):
    - `if (request.cancelled()) request.releaseBuffer();`
    - nếu `!state.isActive()` thì `request.releaseBuffer();`
  - `NettyServerHandler.channelRead`: nếu kênh không active (`request.fail(new NetworkException("Channel is inactive")); needRelease = true;`) hoặc parse lỗi, thì trong `finally` gọi `buffer.release()`. Đây là release **trực tiếp**, không qua `safeRelease`. Trong nhánh kênh không active, request **đã được đưa vào** `requestChannel` trước đó, nên `RequestProcessor` sẽ release thêm lần nữa (bằng `safeRelease`).
  - Mình tìm lời gọi `FlussRequest.cancel()` trong `fluss-rpc/src/main` và `fluss-server/src/main` và **không thấy**. Nhánh "cancelled" trong `sendResponse` có vẻ không bao giờ chạy.
- **Các đường release hai lần còn lại (suy ra từ mã):**
  1. Kênh **mất kết nối** khi request đang xử lý: `RequestProcessor` release trong `finally`, rồi khi phản hồi xong thì `sendResponse` thấy kênh không active và release lần nữa. Lần hai bị `safeRelease` bắt.
  2. Request đến khi kênh **đã không active**: `channelRead` release trực tiếp, rồi `RequestProcessor` release lần nữa. Lần hai bị `safeRelease` bắt.
- **Nguy cơ use-after-free (chưa kiểm chứng):** comment trong `RequestProcessor` đặt quy ước *"async execution shouldn't retain the request/ByteBuf"*. Nếu một hàm RPC nào đó **giữ tham chiếu** tới dữ liệu lazily parsed để xử lý bất đồng bộ (ví dụ trên luồng khác sau khi hàm trả future), thì buffer đã bị trả lại pool trong lúc vẫn còn được đọc. Mình **chưa rà** các hàm RPC để kiểm tra quy ước này có được tuân thủ hay không.

## 2. Khi nào lỗi xảy ra

- **Release hai lần:** khi client **ngắt kết nối** trong lúc server đang xử lý request của nó, hoặc gửi request ngay lúc kênh đang đóng. Thường gặp khi:
  - client timeout và đóng kết nối;
  - client (job Flink) bị kill;
  - mạng chập chờn;
  - rolling restart client.
- Chỉ ảnh hưởng request **lazily parsed** (ví dụ produce log, put KV), vì `releaseBuffer` không làm gì với loại khác.

**Phiên bản:** báo trên `0.6.0`. Các đường trên vẫn còn ở `1.0.0`, nhưng đã có `safeRelease`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** TabletServer và Coordinator, khi client ngắt kết nối giữa chừng.

**Hậu quả trên 1.0.0:**
1. **Log WARN của Netty** về `refCnt` hoặc release thất bại (từ `safeRelease`). Không làm sập luồng xử lý (suy ra từ `safeRelease`).
2. Đường `channelRead` dùng `buffer.release()` **trực tiếp**. Nếu buffer đã bị release ở chỗ khác trước đó thì nó sẽ ném `IllegalReferenceCountException`, nhưng exception đó nằm trong `finally` của `channelRead`, nên được Netty bắt ở `exceptionCaught` và có thể **đóng kênh**. Mình chưa thấy kịch bản cụ thể dẫn tới thứ tự này.
3. **Nếu có use-after-free** (chưa kiểm chứng): hỏng dữ liệu của request khác. Đây là rủi ro nặng nhất nhưng không có bằng chứng.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #1044 trong `relate_issue.md` ghi: *"✅ `FlussRequest.releaseBuffer` dùng `ReferenceCountUtil.safeRelease` (#1043), không còn crash | Có thể có log warning"*. Đối chiếu:
- **Đúng về mặt "không crash"** ở đường `releaseBuffer`.
- **Chính xác hơn:** release hai lần **vẫn xảy ra**, chỉ là được nuốt. Issue (đảm bảo mỗi buffer chỉ release một lần) **chưa được giải quyết** về thiết kế. Nên ghi **🟡** thay vì ✅.
- Mình không xác minh được "#1043" là commit nào. Mình chỉ thấy `safeRelease` trong mã.

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Chủ yếu là log cảnh báo khi client ngắt kết nối.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Không cần biện pháp riêng.
- Khi lọc log TabletServer, coi các WARN của Netty về `refCnt` hoặc `IllegalReferenceCount` xảy ra **cùng lúc** với client ngắt kết nối là triệu chứng đã biết.
- Nếu thấy chúng xuất hiện **không kèm** client ngắt kết nối, hoặc thấy dấu hiệu hỏng dữ liệu (CRC lỗi, `CorruptRecordException`), thì ghi nhận và báo upstream, vì đó có thể là dấu hiệu use-after-free.

## 6. Những điều chưa kiểm chứng

- Mình chưa rà các hàm RPC (đặc biệt `produceLog`, `putKv`) để xem có giữ tham chiếu buffer sau khi hàm trả về hay không.
- Mình chưa kiểm tra phía client (`NettyClientHandler`) có đường release hai lần tương tự hay không.
- Mình chưa tái hiện log cảnh báo.
- Mình chưa xác minh commit đã đưa `safeRelease` vào.
