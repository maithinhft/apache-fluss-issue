# Issue #4726 — Writer: batch ghi vào auto-partition đã hết hạn bị thử lại vô hạn, không bao giờ hoàn tất

Link: https://github.com/apache/fluss/issues/4726 — Phiên bản được báo: `1.0.0`. Không có bình luận. PR sửa: [#4727](https://github.com/apache/fluss/pull/4727) *"[client] Abort pending writes for expired auto partitions in sender"*, **chưa merge** (sửa `Sender`, `RecordAccumulator`, `SenderTest`). Người báo nhận làm PR. Liên quan: [#2940](https://github.com/apache/fluss/issues/2940) (đã đóng), thêm bước kiểm tra thời gian partition khi tạo partition động.

> **Mức chắc chắn:** issue được viết dựa trên phân tích mã, không kèm log sự cố thật. Mình đã đọc `Sender` và `RecordAccumulator` của `v1.0.0` và xác nhận cơ chế (mục 1). Mình **chưa chạy tái hiện** và chưa xác minh một số hệ quả phụ (đánh dấu rõ bên dưới).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Auto-partition (partition tự động)** | Bảng chia partition theo thời gian (ví dụ theo ngày). Fluss tự tạo partition mới sắp tới và **tự xóa partition quá hạn** theo cấu hình giữ lại `table.auto-partition.num-retention`. |
| **Cửa sổ giữ lại (retention window)** | Khoảng các partition gần nhất được giữ. Partition cũ hơn cửa sổ bị coi là hết hạn và bị xóa, và **không bao giờ được tạo lại**. |
| **Writer / `Sender`** | Phía client ghi dữ liệu: ứng dụng gọi `send` để đưa bản ghi vào bộ đệm; luồng nền `Sender` lấy các batch ra gửi lên server. |
| **Accumulator / batch** | Bộ đệm trong writer gom bản ghi theo từng partition/bucket thành batch trước khi gửi (xem [issue2967.md](issue2967.md)). |
| **Unknown leader (chưa biết leader)** | Batch nhắm tới một bucket mà client chưa có thông tin leader trong metadata. `Sender` sẽ làm mới metadata để tìm leader. |
| **`PartitionNotExistException`** | Lỗi báo partition không tồn tại (chưa được tạo, đã bị xóa, hoặc metadata chưa lan tới client). |
| **`InvalidPartitionException`** | Lỗi báo partition **không hợp lệ** theo quy tắc thời gian của auto-partition (ví dụ đã nằm ngoài cửa sổ giữ lại). |
| **`validateAutoPartitionTime`** | Hàm kiểm tra một partition có nằm trong khoảng thời gian hợp lệ của bảng auto-partition hay không; ném `InvalidPartitionException` nếu không. |
| **Historical partition** | Tùy chọn bảng `table.datalake.historical-partition.enabled` (mặc định `false`; theo mô tả trong `v1.0.0` hiện chỉ hỗ trợ bảng auto-partition **Paimon** có đúng một khóa partition). Khi bật, Coordinator tạo và giữ một **partition hệ thống đặc biệt** (trong Fluss, tên là `HISTORICAL_PARTITION_VALUE`) để nhận bản ghi ghi vào partition đã hết hạn, và (với bảng PK) để lookup partition đã hết hạn chuyển sang lake. Mặc định tắt thì ghi vào partition hết hạn bị từ chối. Client **không** kiểm tra dữ liệu có ở lakehouse hay không; nó chỉ kiểm tra partition hệ thống này có tồn tại trong metadata của Fluss hay không (xem mục 4). |
| **Write future / callback** | Đối tượng mà ứng dụng dùng để biết một lần ghi thành công hay lỗi. Nếu batch không bao giờ hoàn tất thì future không bao giờ hoàn tất. |

## Tóm tắt

Từ #2940, nếu ứng dụng ghi vào một partition **đã hết hạn**, `WriterClient#send` ném `InvalidPartitionException` ngay (đồng bộ), nên lỗi rõ ràng. Nhưng nếu bản ghi đã vào bộ đệm lúc partition **còn hợp lệ**, rồi partition mới hết hạn **trước khi batch được gửi**, thì khi `Sender` xử lý batch, nó coi đó là "unknown leader" và làm mới metadata. (Batch ở trạng thái "unknown leader" vì lúc đó partition đang chờ tạo hoặc metadata chưa tới client. Hai lý do này giải thích vì sao batch chưa gửi được ngay, chứ không phải vì sao partition hết hạn.) Lần làm mới ném `PartitionNotExistException`. Với bảng **không có historical partition**, `Sender` bỏ qua ngoại lệ này và **giữ nguyên batch trong hàng đợi**. Vì partition đã hết hạn không bao giờ được tạo lại, vòng này lặp mãi: client làm mới metadata liên tục (ngoại lệ bị bắt ngay trong `Sender.runOnce` nên không dừng luồng gửi, xem mục 5), và các write future của batch đó **không bao giờ hoàn tất**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, phần writer: `Sender`, `RecordAccumulator`, `WriterClient`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `Sender.runOnce` (khoảng dòng 255–270): nếu `readyCheckResult.unknownLeaderTables` không rỗng thì gọi `metadataUpdater.updatePhysicalTableMetadata(...)`. Nếu ném `PartitionNotExistException` thì gọi `handlePartitionNotExistException(...)`. Có comment `TODO: this try-catch is not needed when we don't update metadata for unready partitions`.
  - `handlePartitionNotExistException` (khoảng dòng 810–825): với mỗi partition, nếu `accumulator.isHistoricalPartitionEnabled(...)` là **false** thì chỉ `continue`, tức **bỏ qua**, không hủy batch, không hoàn tất future. Chỉ bảng có historical partition mới được xử lý tiếp (`handleMissingPartition`).
  - Không có giới hạn thời gian cho batch đã vào hàng đợi: `Sender` có comment `TODO add logic for batch expire` (khoảng dòng 286) và `RecordAccumulator` có `TODO add deliveryTimeoutMs to report success or failure on record delivery` (dòng 149). Nghĩa là batch không tự hết hạn sau một thời gian.
  - Việc kiểm tra ném lỗi đồng bộ cho bản ghi **mới** nằm ở `WriterClient` (dùng `generateAutoPartitionTime` với `numToRetain`); phần này đúng như issue mô tả, nhưng không áp dụng cho batch **đã nằm** trong bộ đệm.
- **Cách sửa đề xuất (issue và PR #4727):** khi làm mới metadata trả `PartitionNotExistException`, với bảng không có historical partition thì gọi `validateAutoPartitionTime`:
  - Nếu ném `InvalidPartitionException`: **hủy chỉ các batch của partition đó** và hoàn tất callback/future với lỗi.
  - Nếu không: giữ batch chờ, vì partition có thể đang được tạo hoặc metadata chưa lan.
  - Làm mới nhắm vào đúng một đường dẫn vật lý, để vẫn chạy được khi client còn cache partition ID nhưng metadata bucket đã cũ.
  - Ghi chú của issue: `PartitionNotExistException` một mình **không đủ** để kết luận partition mất vĩnh viễn; chỉ khi `validateAutoPartitionTime` xác nhận không hợp lệ mới được bỏ cuộc.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Bảng auto-partition**, và **không có historical partition** (không dùng lakehouse theo cách bật tính năng đó).
2. **Có bản ghi đã nằm trong bộ đệm của writer** nhắm tới một partition lúc partition đó **còn hợp lệ** (nằm trong cửa sổ giữ lại), nhưng batch **chưa gửi được ngay** vì client chưa biết leader: partition đang chờ được tạo, hoặc metadata của nó chưa tới client. Do đó batch ở lại trong bộ đệm một lúc.
3. **Partition hết hạn trước khi batch được gửi.** Ví dụ sự kiện **đến trễ** của một ngày sắp rơi khỏi cửa sổ, và việc gửi bị trì hoãn (batch chưa đầy, `batch-timeout` chưa tới, hoặc server chậm).
4. `Sender` làm mới metadata cho partition đó và nhận `PartitionNotExistException`.

**Dấu hiệu:** hoạt động làm mới metadata liên tục (request metadata lặp lại ở phía server/Coordinator); theo issue là có log `PartitionNotExistException` lặp lại, nhưng trong mã `v1.0.0` mình thấy lỗi này chỉ được ghi ở mức `DEBUG` (xem mục 5), nên với cấu hình log mặc định có thể **không thấy gì**; một số batch/ghi không bao giờ hoàn tất; throughput hoặc độ trễ của writer xấu đi.

**Phiên bản:** báo trên `1.0.0`, mình đọc mã `v1.0.0` và thấy cùng cấu trúc, nên lỗi còn trong `1.0.0`. PR #4727 chưa merge.

**Khoảng thời gian cửa sổ hẹp:** lỗi chỉ xảy ra khi partition hết hạn **giữa** lúc bản ghi vào bộ đệm và lúc batch được gửi. Với tải bình thường, khoảng này ngắn. Dữ liệu đến trễ hơn cửa sổ giữ lại sẽ bị từ chối ngay bằng `InvalidPartitionException` (đồng bộ), không rơi vào lỗi này. Đây là suy luận của mình từ mô tả issue và mã, chưa tái hiện.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** ứng dụng dùng Fluss writer (Flink sink, Java client) ghi vào **bảng auto-partition không bật historical partition**, nhất là bảng sự kiện theo ngày có dữ liệu đến trễ gần mép cửa sổ giữ lại.

**Hậu quả:**
1. **Batch bị kẹt vĩnh viễn:** write future của các bản ghi trong batch không bao giờ hoàn tất.
2. **Lãng phí tài nguyên:** `Sender` liên tục làm mới metadata (tăng tải lên Coordinator/ZooKeeper). Issue nói log bị spam, nhưng theo mã `v1.0.0` đường này chỉ log `DEBUG` (mục 5). Batch kẹt vẫn nằm trong bộ nhớ của writer. Mức chiếm buffer thực tế và việc nó có chặn các batch khác hay không mình **chưa kiểm chứng**; issue chỉ nói các partition khác "không bị ảnh hưởng" khi sửa đúng.
3. **Có thể làm treo `flush`/checkpoint:** nếu ứng dụng chờ mọi write future hoàn tất (ví dụ Flink sink flush khi checkpoint) thì có thể không tiến lên được. Đây là **suy luận** của mình (batch không hoàn tất ⇒ flush chờ), chưa kiểm chứng trong mã Flink sink.
4. **Dữ liệu của các bản ghi kẹt không được ghi.** Vì partition đã hết hạn nên về logic dữ liệu đó đáng ra phải bị từ chối; vấn đề là nó bị im lặng thay vì báo lỗi.
5. **Không có dữ liệu sai âm thầm** được ghi nhận, nhưng có **kẹt âm thầm** (không báo lỗi lên ứng dụng).

**Mức ảnh hưởng với CDP:** liên quan nếu bạn có bảng PK hoặc bảng sự kiện **auto-partition theo ngày** và có sự kiện đến trễ. Với bảng PK không partition thì không áp dụng.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Lọc dữ liệu trễ trước khi sink** trong Flink: bỏ hoặc chuyển sang luồng khác những sự kiện có thời gian nằm gần/ngoài cửa sổ giữ lại.
- **Đặt retention partition (`table.auto-partition.num-retention`) dài hơn độ trễ tối đa** có thể có của dữ liệu cộng thời gian xử lý sự cố, để partition đích không hết hạn khi dữ liệu còn trong bộ đệm.
- Giám sát: tần suất request metadata từ client (vì log mặc định có thể không hiện lỗi này), bật log `DEBUG` cho `MetadataUpdater` khi điều tra, throughput sink về 0, và checkpoint/flush kéo dài.
- Khi gặp tình trạng này, khởi động lại writer/job để bỏ các batch kẹt (chấp nhận mất các bản ghi thuộc partition đã hết hạn).
- Theo dõi PR #4727.

## 4. Bổ sung: nếu bảng bật historical partition mà partition lịch sử cũng không có thì sao

(Đã đọc `WriterClient.resolveHistoricalWriteTarget` và `Sender.handleMissingPartition` ở `v1.0.0`.)

- Client **không hỏi lakehouse**. Cái nó kiểm tra là partition hệ thống "historical" của bảng có trong metadata Fluss hay không (`checkAndUpdatePartitionMetadata`).
- **Khi gọi `send` (đồng bộ):** nếu partition gốc đã hết hạn và partition lịch sử cũng không tồn tại, `WriterClient` ném `PartitionNotExistException("Historical partition ... does not exist.")`. Có `TODO` trong mã về việc chỉ kích hoạt đích này sau khi có giao thức đảm bảo mọi bản ghi đã nhận đều đọc được từ lake (PR #3820).
- **Khi batch đã nằm trong bộ đệm:** `Sender.handleMissingPartition` xử lý theo thứ tự:
  1. Nếu đích là chính partition lịch sử mà metadata xác nhận không còn: **hủy batch** với `PartitionNotExistException` ("Cannot continue historical writes ... because refreshed metadata confirms that the partition no longer exists").
  2. Nếu còn request đang bay tới partition gốc: **hủy batch** với `PartitionNotExistException` ("Cannot safely reroute writes ... could break idempotence").
  3. Ngược lại thử chuyển hàng đợi sang partition lịch sử; nếu partition lịch sử không tồn tại thì **hủy batch** với `PartitionNotExistException` ("Cannot reroute writes ... because historical write target ... does not exist").
- Tóm lại với bảng bật tính năng này, trường hợp này cho **`PartitionNotExistException` rõ ràng**, không kẹt vô hạn. Vòng lặp vô hạn của issue #4726 chỉ xảy ra với bảng **không** bật tính năng.
- Mình **không biết** điều gì xảy ra khi partition lịch sử có nhưng dữ liệu tương ứng không có trong lake: đó là xử lý ở phía server, mình chưa đọc.

## 5. Bổ sung: vì sao ném `PartitionNotExistException` mà vòng lặp vẫn chạy

(Đã đọc `Sender.runOnce`, `Sender.run`, `RecordAccumulator.bucketReady`, `MetadataUpdater.updateMetadata` ở `v1.0.0`.)

- **Ngoại lệ được bắt ngay tại chỗ, không đi lên trên.** Trong `sendWriteData`, lời gọi `metadataUpdater.updatePhysicalTableMetadata(...)` nằm trong `try/catch`. Nếu là `PartitionNotExistException` thì chỉ gọi `handlePartitionNotExistException(...)`. Với bảng không bật historical partition, hàm này `continue` mà không làm gì. Vì không có `throw`, `runOnce` chạy tiếp bình thường.
- **Ngay cả khi ngoại lệ thoát ra,** vòng `while (running)` trong `Sender.run` bắt `Exception`, ghi `LOG.error` rồi lặp tiếp. Luồng gửi không dừng vì một ngoại lệ.
- **Không có bước nào hủy batch.** Một batch chỉ bị hủy khi có lệnh tường minh (`abortBatches`, `maybeAbortBatches`); với trường hợp này chỉ bảng bật historical partition mới gọi `abortBatches`. Và batch cũng không tự hết hạn (`TODO add logic for batch expire`, `TODO add deliveryTimeoutMs`).
- **Vì sao lặp lại:** mỗi vòng, `RecordAccumulator.bucketReady` thấy partition của batch không có trong metadata (`partitionId == null`) và đưa nó vào `unknownLeaderTables`. Sau đó `Sender` lại làm mới metadata, lại nhận `PartitionNotExistException`, lại bỏ qua. Vì không có node nào sẵn sàng, nó chờ `awaitNextReadyCheck` rồi lặp.
- **Đây là thiết kế có chủ đích, không phải ngẫu nhiên:** `PartitionNotExistException` có thể chỉ là tạm thời (partition đang được tạo hoặc metadata chưa lan), nên mã không dám hủy batch chỉ dựa vào lỗi này. Chỗ thiếu là không có bước phân biệt "tạm thời" với "hết hạn vĩnh viễn", đó chính là điều PR #4727 bổ sung bằng `validateAutoPartitionTime`.
- **Về log:** `MetadataUpdater.updateMetadata` ghi `PartitionNotExistException` ở mức `LOG.debug("Failed to update metadata because the partition does not exist", t)`. `handlePartitionNotExistException` cũng không log gì trong nhánh bỏ qua. Nên ở mức log mặc định (INFO), tình trạng này **có thể im lặng hoàn toàn**, khác với mô tả "log lỗi lặp lại" của issue. Mình chưa kiểm tra cấu hình log mặc định của bản phân phối cụ thể của bạn.
