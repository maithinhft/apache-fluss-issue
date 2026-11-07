# Issue #2198 — Flink SQL Gateway chạy lâu với Fluss catalog bị rò thread `globalEventExecutor` (giữ classloader)

Link: https://github.com/apache/fluss/issues/2198 — Phiên bản được báo: `0.8.0` (người báo: loserwang1024, 18/12/2025). Issue ngắn:
- triệu chứng: Flink SQL Gateway chạy lâu với Fluss thì **rò thread**; thread dump có **rất nhiều** thread tên `globalEventExecutor-2-x` thuộc Netty đã shade của Fluss (`org.apache.fluss.shaded.netty4...`);
- người báo cho rằng đây là vấn đề [netty/netty#14615](https://github.com/netty/netty/issues/14615): Netty nằm trên classpath chính, trong hệ thống tạo **classloader tạm** (cho plugin), sẽ giữ lại các classloader tạm đó. Connector Fluss nằm ở classpath chính, còn Gateway tạo classloader tạm cho mỗi phiên.

Không có bình luận, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi dùng **Flink SQL Gateway** (hoặc dịch vụ chạy lâu tạo nhiều phiên hay classloader) với **Fluss catalog**.

> **Mức chắc chắn:** trung bình. Mình đã xác nhận trong mã `v1.0.0`:
> - Fluss dùng **Netty 4.1.104.Final** (shade), cũ hơn thời điểm netty#14615 được mở;
> - client Fluss tắt Netty bằng `shutdownGracefully().addListener(...)`.
>
> Cơ chế "listener của `terminationFuture` chạy trên `GlobalEventExecutor`, thread này kế thừa context classloader của thread khởi tạo" là **kiến thức về Netty**. Mình không đọc mã Netty, và chưa tái hiện trên Gateway.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Flink SQL Gateway** | Dịch vụ chạy lâu nhận câu SQL từ nhiều client (JDBC, REST). Mỗi phiên (session) có thể tạo **classloader riêng** để nạp jar của người dùng. |
| **Classloader tạm (ephemeral)** | Classloader tạo cho một phiên rồi bỏ đi khi phiên kết thúc. Nếu còn **bất kỳ** tham chiếu nào tới nó, mọi class nó đã nạp **không được thu hồi**, gây rò Metaspace. |
| **Context classloader (TCCL)** | Classloader gắn với mỗi thread. Thread mới **kế thừa** TCCL của thread tạo ra nó. |
| **`GlobalEventExecutor`** | Executor dùng chung (singleton) của Netty. Nó **tự khởi động một thread** khi có việc, và tự dừng khi rảnh khoảng 1 giây. Thread đó tên `globalEventExecutor-<pool>-<n>`. |
| **`shutdownGracefully` / `terminationFuture`** | Khi tắt `EventLoopGroup`, Netty trả một future. Listener gắn vào future đó được gọi trên `GlobalEventExecutor` (kiến thức về Netty). |
| **Netty shade** | Fluss đóng gói Netty dưới package `org.apache.fluss.shaded.netty4`, để không đụng Netty của Flink. |

## Tóm tắt

Mỗi khi Fluss catalog hoặc connection được **đóng** trong một phiên Gateway, client Fluss gọi `shutdownGracefully()` trên event loop và gắn listener. Netty chạy listener đó bằng `GlobalEventExecutor`, nên **khởi động thread** `globalEventExecutor`. Thread này **kế thừa TCCL** là classloader tạm của phiên, và với bản Netty cũ, Netty còn giữ tham chiếu. Kết quả: classloader của phiên đã kết thúc **không được thu hồi**, và thread cùng Metaspace **tăng dần** theo số phiên. Sau thời gian dài, Gateway có thể hết Metaspace hoặc bị chậm.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-rpc`: `netty/client/NettyClient.close()`, `netty/NettyUtils.shutdownGroup`;
  - Netty shade (`fluss-shaded-netty`);
  - `fluss-flink`: `catalog/FlinkCatalog.close()`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `pom.xml`: `<netty.version>4.1.104.Final</netty.version>`, dùng qua `fluss-shaded-netty` `${netty.version}-1.0-incubating`.
  - `NettyUtils.newEventLoopGroup`: `EpollEventLoopGroup` hoặc `NioEventLoopGroup` với `new DefaultThreadFactory(threadName, true)` (thread daemon).
  - `NettyUtils.shutdownGroup`:
    ```java
    group.shutdownGracefully()
         .addListener(finished -> { if (finished.isSuccess()) { ... } ... });
    ```
  - `NettyClient.close()`: đóng từng kết nối rồi `shutdownFutures.add(NettyUtils.shutdownGroup(eventGroup))`.
  - `FlinkCatalog` có `close()`. Mình đã thấy phương thức, nhưng chưa lần đầy đủ chuỗi đóng `Connection` và `NettyClient`.
- **Không có thay đổi liên quan** trong 1.0.0 theo những gì mình thấy: vẫn Netty 4.1.104, và không có thiết lập TCCL quanh `shutdownGracefully`.

## 2. Khi nào lỗi xảy ra

- Dịch vụ **chạy lâu** tạo **nhiều classloader tạm** và **nhiều lần tạo hoặc đóng** client Fluss. Điển hình là Flink SQL Gateway với Fluss catalog, hoặc một web service tự nạp connector theo phiên.
- Connector Fluss nằm trên **classpath chính** (`lib/` của Flink), như issue mô tả.
- Job Flink thông thường (TaskManager) ít bị hơn, vì số lần tạo client ít. Mình chưa kiểm tra.

**Phiên bản:** báo trên `0.8.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

- **Rò thread và rò classloader (Metaspace)** trên tiến trình Gateway, tăng dần theo thời gian.
- Lâu dài có thể gây `OutOfMemoryError: Metaspace` hoặc GC nặng, khiến Gateway **phải restart**.
- **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Flink SQL Gateway / Flink 2.2 catalog | #2198 (rò classloader), #3975 | 🟢 | ❌"*. Đối chiếu:
- **Đúng** về trạng thái ❌.
- **Bổ sung:** nguyên nhân nằm ở Netty 4.1.104 (bản cũ). Hướng sửa có thể là **nâng Netty** hoặc đặt TCCL về classloader của Fluss khi tắt event loop (suy luận).

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu CDP dùng **Flink SQL Gateway** (hoặc Zeppelin, Kyuubi, notebook) để truy vấn ad-hoc vào Fluss, điều hay gặp khi analyst hoặc AI agent chạy SQL.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Theo dõi** số thread `globalEventExecutor` (bằng `jstack` hoặc metric JVM thread count) và **Metaspace** của tiến trình Gateway. Đặt alert khi tăng liên tục.
- **Restart Gateway định kỳ** (ví dụ hằng đêm) nếu thấy tăng.
- Đặt `-XX:MaxMetaspaceSize` để lỗi lộ ra sớm và rõ, thay vì làm chậm cả máy.
- Hạn chế tạo hoặc đóng catalog liên tục: dùng **catalog cấu hình sẵn** (ví dụ trong `sql-gateway` init SQL) thay vì `CREATE CATALOG` ở mỗi phiên. Mình chưa kiểm chứng việc này giảm rò.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện trên Flink SQL Gateway 1.20 hoặc 2.x với Fluss 1.0.0.
- Mình chưa kiểm tra netty#14615 được sửa ở bản Netty nào, và cách sửa đó có giải quyết trường hợp này không.
- Mình chưa kiểm tra Arrow (dùng Netty allocator của Arrow) hay các thành phần khác có cùng kiểu rò hay không.
- Mình chưa kiểm tra `main` có nâng Netty hay chưa.
