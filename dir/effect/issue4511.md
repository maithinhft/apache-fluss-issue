# Issue #4511 — Bộ nén Zstd của Arrow rò rỉ bộ nhớ off-heap khi lệnh nén hoặc giải nén ném exception

Link: https://github.com/apache/fluss/issues/4511 — Phiên bản được báo: `0.9.0` (người báo: loserwang1024, 29/09/2026). Issue ngắn, chỉ rõ chỗ lỗi và hướng sửa, kèm một lỗi quan sát được khi chạy Flink sink: `com.github.luben.zstd.ZstdException: Operation not authorized at current processing stage`. Người báo nói rõ **nguyên nhân của ZstdException cần điều tra riêng**; issue này chỉ nói về việc **rò buffer** khi có exception. Trạng thái: còn mở. PR [#4513](https://github.com/apache/fluss/pull/4513) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã đọc cả hai codec trong `v1.0.0` (`ZstdArrowCompressionCodec` và `UnshadedZstdArrowCompressionCodec`). Lỗi đúng như mô tả. Mình **không biết** vì sao Zstd ném `Operation not authorized at current processing stage`, và chưa đo lượng bộ nhớ rò thực tế.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Arrow log format** | Định dạng log mặc định của Fluss (`table.log.format = ARROW`). Dữ liệu được ghi theo cột và **nén theo từng cột**. |
| **Zstd** | Thuật toán nén. **Mặc định** cho log Arrow (`table.log.arrow.compression.type = ZSTD`, mức nén `table.log.arrow.compression.zstd.level = 3`). Fluss gọi thư viện native `zstd-jni` (`com.github.luben.zstd`). |
| **Off-heap / direct memory** | Bộ nhớ nằm ngoài heap Java. **GC không tự thu hồi** vùng này; phải giải phóng bằng tay (`close()`). Bị giới hạn bởi `-XX:MaxDirectMemorySize` hoặc bởi bộ cấp phát của Arrow. |
| **`ArrowBuf` / `BufferAllocator`** | `ArrowBuf` là một vùng nhớ off-heap do `BufferAllocator` của Arrow cấp. Allocator **đếm** số byte đang cấp. Khi allocator đóng mà còn byte chưa trả, Arrow báo `IllegalStateException: Memory was leaked by query`. |
| **`doCompress` / `doDecompress`** | Hai hàm của codec: nén một buffer cột khi **ghi** batch, và giải nén khi **đọc** batch. |
| **`Zstd.isError(...)`** | Cách `zstd-jni` báo lỗi qua **giá trị trả về**. Ngoài ra thư viện cũng có thể **ném exception** trực tiếp. Đây là trường hợp bị bỏ sót. |
| **`try/finally`** | Cấu trúc Java đảm bảo đoạn dọn dẹp luôn chạy, kể cả khi có exception. |

## Tóm tắt

Cả hai codec Zstd đều **cấp buffer đầu ra trước** rồi mới gọi Zstd. Buffer đầu ra chỉ được `close()` ở hai nhánh: khi Zstd **trả mã lỗi** (`Zstd.isError`), và (riêng giải nén) khi độ dài giải nén không khớp. Nếu `Zstd.compressDirectByteBuffer` hoặc `Zstd.decompressDirectByteBuffer` **ném exception**, code nhảy ra ngoài mà không đi qua các nhánh đó, và **buffer đầu ra không bao giờ được giải phóng**. Mỗi lần như vậy rò một buffer off-heap có kích thước khoảng bằng dữ liệu nén hoặc giải nén của **một cột trong một batch**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, gồm `compression/ZstdArrowCompressionCodec` (bản Arrow đã shade, dùng cho đọc ghi log chính) và `compression/UnshadedZstdArrowCompressionCodec` (bản Arrow không shade, dùng qua `UnshadedArrowReadUtils` trong `LogRecordReadContext`).
- **Đã xác nhận trong mã `v1.0.0` (`ZstdArrowCompressionCodec`):**
  - `doCompress`:
    ```java
    ArrowBuf compressedBuffer = allocator.buffer(compressedSize);      // cấp trước
    ...
    long bytesWritten = Zstd.compressDirectByteBuffer(...);            // có thể ném
    if (Zstd.isError(bytesWritten)) {
        compressedBuffer.close();                                      // chỉ đóng ở đây
        throw new RuntimeException("Error compressing: " + ...);
    }
    ```
  - `doDecompress`: `ArrowBuf uncompressedBuffer = allocator.buffer(decompressedLength);` rồi `Zstd.decompressDirectByteBuffer(...)`. Buffer chỉ được đóng khi `Zstd.isError(...)` hoặc khi `decompressedLength != decompressedSize`.
  - Không có `try/finally` ở cả hai hàm. `UnshadedZstdArrowCompressionCodec` có cấu trúc **y hệt**.
- **Codec được dùng ở đâu (đã đọc mã):** qua `ArrowCompressionFactory` và `ArrowUtils` (dùng `FlussVectorLoader(schemaRoot, ArrowCompressionFactory.INSTANCE)` khi đọc batch). Vì vậy:
  - **nén** chạy khi build batch Arrow, cả ở **client** (writer gửi batch log) lẫn ở **server** (dựng WAL hoặc changelog của bảng PK, xem #2967);
  - **giải nén** chạy khi **đọc** batch: scanner phía client, và các chỗ phía server cần đọc nội dung batch.
- **Hướng sửa (issue):** dùng `try/finally` kèm cờ "thành công" ngay sau khi cấp buffer đầu ra. Lỗi thì đóng buffer; thành công thì chuyển quyền sở hữu cho bên gọi.

## 2. Khi nào lỗi xảy ra

- Chỉ khi `zstd-jni` **ném exception** trong lúc nén hoặc giải nén. Bình thường thì không.
- Exception quan sát được trong issue: `ZstdException: Operation not authorized at current processing stage`, khi **Flink sink ghi**. Mình không biết điều kiện gây ra nó. Thông báo này của zstd thường gắn với việc dùng sai trạng thái của một "context" nén, nhưng đó chỉ là hiểu biết chung về zstd, mình chưa kiểm chứng với Fluss.
- Các khả năng khác làm Zstd ném lỗi khi giải nén: dữ liệu nén bị hỏng (đọc file hỏng, truyền mạng lỗi), hoặc lỗi thư viện native (suy luận).

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` y hệt nên vẫn còn lỗi.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** mọi bảng dùng log Arrow với Zstd (mặc định), **chỉ khi** Zstd ném exception.

**Hậu quả:**
1. **Trước hết là lỗi gốc:** batch không nén hoặc giải nén được, nên ghi hoặc đọc batch đó thất bại. Phía Flink sink thì task lỗi và failover (đúng như quan sát trong issue).
2. **Rò direct memory:** mỗi lần rò một buffer.
   - Với **job Flink**: nếu lỗi lặp lại (task failover rồi chạy lại trên cùng TaskManager), direct memory của TaskManager tăng dần, có thể dẫn tới `OutOfMemoryError: Direct buffer memory` hoặc TaskManager bị kill. Khi allocator của client đóng, có thể thấy `Memory was leaked by query` (giống #321). Đây là suy luận.
   - Với **TabletServer**: allocator phía server sống suốt đời process, nên phần rò **không bao giờ được trả** cho tới khi restart (suy luận).
3. **Không làm sai dữ liệu đã ghi.** Batch lỗi không được ghi hoặc đọc.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4511 trong `relate_issue.md` ghi: *"❌ PR #4513 chưa merge | Zstd là compression mặc định của Arrow log. Chỉ rò khi có lỗi. Theo dõi direct memory"*. Đối chiếu:
- **Đúng hoàn toàn.**
- **Bổ sung:** lỗi xảy ra ở cả đường **ghi** (client và WAL phía server) lẫn đường **đọc**. Phần đáng lo hơn là **nguyên nhân của ZstdException** (vì sao Zstd lỗi khi Flink ghi). Điều đó chưa được điều tra, và nó mới là thứ làm job thất bại.

## 5. Mức ảnh hưởng với CDP

- Trong điều kiện bình thường: **không có tác động**.
- Nếu một ngày gặp `ZstdException`: job ghi profile hoặc sự kiện failover, và nếu lặp lại nhiều thì TaskManager hoặc TabletServer có thể cạn direct memory.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Theo dõi direct memory** của TaskManager Flink (metric `Status.JVM.Memory.Direct.*`) và của TabletServer. Alert khi tăng đều.
- **Alert theo log** chứa `ZstdException` hoặc `Error compressing` hay `Error decompressing`. Khi thấy, **restart** process bị ảnh hưởng để thu hồi bộ nhớ, và thu thập thông tin để báo upstream (phiên bản `zstd-jni`, dữ liệu, mức nén).
- **Cấu hình restart strategy** của Flink để failover lặp lại không chạy mãi trên cùng TaskManager đang rò. Ví dụ dùng `failure-rate`, hoặc giới hạn số lần.
- Nếu gặp ZstdException **lặp lại** với một bảng, có thể đổi nén cho bảng mới sang `LZ4_FRAME` (`'table.log.arrow.compression.type' = 'LZ4_FRAME'`). Cách này né được cả lỗi gốc lẫn chỗ rò, nhưng thay đổi tỷ lệ nén và CPU, nên chỉ nên làm khi thật sự gặp lỗi. Mình chưa kiểm tra codec LZ4 có cùng kiểu rò hay không.

## 6. Những điều chưa kiểm chứng

- Nguyên nhân của `ZstdException: Operation not authorized at current processing stage`: chưa ai điều tra.
- Mình chưa đo lượng bộ nhớ rò mỗi lần, và chưa kiểm tra allocator phía server có giới hạn để cảnh báo sớm hay không.
- Mình chưa kiểm tra codec LZ4 (`Lz4ArrowCompressionCodec` nếu có) có cùng kiểu rò hay không.
- Mình chưa xác định chính xác những chỗ phía server giải nén batch.
