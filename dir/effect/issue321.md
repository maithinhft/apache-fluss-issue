# Issue #321 — Rò rỉ bộ nhớ Arrow khi writer bị đóng trước khi kịp gửi hết batch

Link: https://github.com/apache/fluss/issues/321 — Phiên bản được báo: `0.5` (người báo: loserwang1024, 13/01/2025, repo cũ `alibaba/fluss`). Issue có test tái hiện (gọi `sender.forceClose()` khi còn batch trong accumulator) cho ra `IllegalStateException: Memory was leaked by query. Memory leaked: (24576)`, kèm phân tích. Có một bình luận (07/2025) từ người gặp lại trong **job Flink**: `Memory was leaked by query. Memory leaked: (29574584)` (khoảng 28 MB) khi `FlinkSinkWriter` đóng connection. Không có PR. Issue được nhắc trong issue tổng #4980. Trạng thái: còn mở.

> **Mức chắc chắn:** trung bình–cao. Mình đã đọc đường đóng writer của `v1.0.0` (Flink sink → `FlussConnection` → `WriterClient` → `Sender` → `RecordAccumulator`):
> - Lỗi gốc **vẫn còn**: batch chưa gửi không được trả `ArrowWriter` khi force close, và có `TODO` xác nhận.
> - Hai điểm làm lỗi **nặng hơn** mức "chỉ log" (suy luận từ mã, chưa đo):
>   1. Flink sink của 1.0.0 **luôn** đóng connection với timeout 0, nên **mỗi lần task Flink đóng** (failover, cancel) là một lần force close.
>   2. Bộ cấp phát Arrow phía client của 1.0.0 dùng **native memory qua `Unsafe`**, và các chunk còn bị giữ **không bao giờ được giải phóng**, nên đây là rò rỉ thật, không chỉ là cảnh báo.
>
> Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Writer / Sender / Accumulator** | Phía client: ứng dụng ghi vào **accumulator** (gom bản ghi thành batch). Một **sender thread** rút batch ra gửi lên TabletServer. |
| **`ArrowWriter` / `ArrowWriterPool`** | Bộ ghi batch dạng Arrow, giữ các buffer off-heap. Dùng xong phải **trả về pool** (recycle). Khi pool đóng, chỉ các writer **đang rảnh** được đóng. |
| **`BufferAllocator` (Arrow)** | Bộ cấp phát của Arrow. Khi `close()` mà còn buffer chưa trả, nó ném `IllegalStateException: Memory was leaked by query`. |
| **`ChunkedAllocationManager`** | Cách cấp phát của Fluss client (từ #3026, có trong 1.0.0): lấy các **chunk** lớn (mặc định 4 MB) rồi chia nhỏ. Chunk chỉ được giải phóng khi **mọi phần chia nhỏ** đã trả lại. |
| **`Unsafe.allocateMemory`** | Cấp bộ nhớ native bằng API nội bộ của JVM. **Không** bị giới hạn bởi `-XX:MaxDirectMemorySize` và **không** hiện trong metric direct memory của JVM. |
| **Force close** | Đóng writer **ngay**, không chờ gửi hết batch còn lại. |
| **Graceful close** | Đóng có chờ, gửi hết dữ liệu còn lại (tới hết timeout). |

## Tóm tắt

`ArrowWriter` của một batch chỉ được trả về pool khi batch được **gửi đi** (lúc build hoặc serialize). Khi đóng writer, `ArrowWriterPool.close()` chỉ đóng các writer **đang rảnh** (`freeWriters`). Nếu writer bị **force close** khi còn batch **chưa được rút ra gửi**, `ArrowWriter` của các batch đó không được trả về và không được đóng. Khi allocator đóng, Arrow phát hiện còn bộ nhớ chưa trả, và ném `Memory was leaked by query`.

Trong 1.0.0:
- **Flink sink luôn đóng connection với `Duration.ZERO`**, tức force close (để failover không bị chặn khi cluster Fluss không phản hồi). Vì vậy kịch bản này xảy ra ở **mỗi lần task sink đóng mà còn batch chưa gửi**.
- `RecordAccumulator.destroyResources()` đóng theo thứ tự: pool buffer → pool Arrow writer → `bufferAllocator.close()` → `chunkedFactory.close()`. Nếu `bufferAllocator.close()` ném lỗi do rò, `chunkedFactory.close()` **không chạy**. Mà ngay cả khi chạy, `ChunkedFactory.close()` cũng **không giải phóng** chunk còn phần chia nhỏ đang bị giữ. Vậy bộ nhớ native của các batch chưa gửi **rò thật** khỏi process.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-client`: `write/WriterClient`, `write/Sender`, `write/RecordAccumulator`;
  - `fluss-common`: `row/arrow/ArrowWriterPool`, `shaded/arrow/.../memory/ChunkedAllocationManager`;
  - `fluss-flink`: `sink/writer/FlinkSinkWriter`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkSinkWriter.close()`:
    ```java
    // Close the connection immediately without waiting for pending write requests:
    // all records that must be persisted have already been flushed on checkpoints ...
    // A graceful close could block the task indefinitely when the Fluss cluster is unavailable ...
    connection.close(Duration.ZERO);
    ```
  - `WriterClient.close(timeout)`: `sender.initiateClose()`, chờ `ioThreadPool` tối đa `timeoutMs`. Nếu chưa xong thì `sender.forceClose()` rồi `ioThreadPool.shutdownNow()`. Với timeout 0 thì force close ngay.
  - `Sender.run()` (phần tắt):
    ```java
    while (!forceClose && accumulator.hasUnDrained()) { runOnce(); }   // gửi nốt chỉ khi KHÔNG force close
    ...
    finally { ... destroyResources(); }
    // TODO if force close failed, add logic to abort incomplete batches.
    ```
  - `RecordAccumulator.destroyResources()`:
    ```java
    resourcesDestroyed = true;
    writerBufferPool.close();
    arrowWriterPool.close();     // chỉ đóng freeWriters
    bufferAllocator.close();     // ném "Memory was leaked" nếu còn buffer chưa trả
    chunkedFactory.close();      // không chạy nếu dòng trên ném lỗi
    ```
  - `ArrowWriterPool.close()`: duyệt `freeWriters`, gọi `writer.root.close()`, rồi `freeWriters.clear()`. Writer đang được batch giữ **không** nằm trong `freeWriters`.
  - `ChunkedAllocationManager`: cấp chunk bằng `MemoryUtil.UNSAFE.allocateMemory(...)`. `ChunkedFactory.close()` chỉ `destroy()` các chunk **rỗng** (`freeChunks`, và `activeChunk` khi `subAllocCount == 0`).
- **Trên `main`:** commit #4448 (29/09/2026, *"Replace Unsafe with Netty direct memory in ChunkedAllocationManager and expose metrics"*) chuyển sang direct memory của Netty và thêm metric. Commit này **không có trong 1.0.0**, và không sửa việc batch chưa gửi bị bỏ rơi. Mình chưa đọc diff.

## 2. Khi nào lỗi xảy ra

- Writer bị **đóng khi còn batch chưa rút ra gửi**:
  - **Flink sink:** mỗi lần task đóng (failover, cancel job, stop-with-savepoint, rescale) nếu còn bản ghi ghi sau lần flush checkpoint cuối mà chưa được gửi. Với tải cao, điều này **gần như luôn đúng** (suy luận).
  - **Ứng dụng Java client:** gọi `connection.close(Duration.ZERO)` hoặc timeout nhỏ.
- Đóng graceful (`connection.close()` không tham số dùng `Long.MAX_VALUE`) thì gửi hết trước khi đóng, nên không rò (theo mã).

**Phiên bản:** báo trên `0.5`. Gặp lại năm 2025 trên Flink. Mã `v1.0.0` vẫn còn đường này.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** chủ yếu **job Flink ghi vào Fluss**, đặc biệt job hay failover.

**Hậu quả:**
1. **Log lỗi** `Memory was leaked by query` khi task đóng, như bình luận năm 2025.
2. **Rò bộ nhớ native trong TaskManager** (suy luận từ mã): mỗi lần đóng có thể rò tới **lượng dữ liệu đang chờ trong accumulator**, giới hạn bởi `client.writer.buffer.memory-size` (mặc định **64 MB** mỗi writer), làm tròn lên theo chunk 4 MB. Flink **tái sử dụng TaskManager** qua các lần failover, nên nếu job failover liên tục, bộ nhớ native của TaskManager **tăng dần**.
3. **Khó phát hiện:** bộ nhớ cấp bằng `Unsafe` **không** hiện trong metric direct memory của JVM, và không bị `MaxDirectMemorySize` chặn. Triệu chứng có thể là **container bị Kubernetes kill vì vượt memory limit (OOMKilled)**, không phải `OutOfMemoryError` của Java (suy luận).
4. **Không mất dữ liệu đã checkpoint:** batch bị bỏ sẽ được Flink phát lại từ checkpoint, đúng như comment trong `FlinkSinkWriter`.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` gom #321 vào ghi chú cuối mục 4.4 (*"Rò rỉ khi đóng sender"*, 🟢, *"Không thấy fix nào trong 1.0.0"*). Đối chiếu:
- **Không có fix**: đúng.
- **Mức 🟢 có thể thấp:** với Flink sink, force close xảy ra **mỗi lần task đóng**, và phần rò là native memory không được thu hồi. Với job hay failover thì đây là rủi ro **làm TaskManager bị OOMKilled dần dần**. Nên coi là **🟡** cho tới khi đo được.

## 5. Mức ảnh hưởng với CDP

- Job Flink ghi profile hoặc sự kiện vào Fluss chạy liên tục. Bình thường ít failover, nên ảnh hưởng nhỏ. Nhưng **khi cluster có sự cố** (job failover nhiều lần trong ngày), TaskManager có thể phình bộ nhớ.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Theo dõi RSS hoặc memory của container TaskManager** (không chỉ heap và direct). Alert khi tăng đều sau mỗi lần failover.
- **Chừa headroom** cho native memory trong cấu hình Flink (`taskmanager.memory.task.off-heap.size` hoặc `jvm-overhead`) để hấp thụ phần rò giữa các lần restart TaskManager.
- **Restart TaskManager** (không chỉ job) sau một chuỗi failover dài, để thu hồi bộ nhớ.
- **Giảm `client.writer.buffer.memory-size`** nếu không cần buffer lớn, để giới hạn lượng rò mỗi lần.
- Ứng dụng Java client (AI agent ghi dữ liệu nếu có): đóng connection bằng `close()` (graceful), không dùng `close(Duration.ZERO)`, trừ khi buộc phải đóng nhanh.

## 6. Những điều chưa kiểm chứng

- Mình chưa đo lượng rò thực tế mỗi lần task Flink đóng.
- Mình chưa kiểm tra exception ở `bufferAllocator.close()` (trong `finally` của sender thread) được log hay nuốt.
- Mình chưa kiểm tra việc Flink tái sử dụng classloader hoặc TaskManager có làm phần rò tích lũy thật hay không, hay có cơ chế dọn khác.
- Mình chưa đọc diff của #4448 trên `main`.
