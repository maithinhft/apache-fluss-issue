# Issue #3466 — Partition hết hạn khiến hàng loạt client hỏi lại metadata, dồn tải xuống ZooKeeper

Link: https://github.com/apache/fluss/issues/3466 — Phiên bản được báo: `0.9.0`. Tiêu đề: *"[server][client] Partition expiration storms ZooKeeper via repeated GET_METADATA on non-existent partitions until enumerator unsubscribes"*. Trạng thái: **còn mở**. Người báo (loserwang1024) tự nhận làm PR. Có 1 bình luận (14/09) của fxbing, người này xin nhận làm phần client.

> **Lưu ý quan trọng (khác với `relate_issue.md`):** dòng #3466 viết "1.0.0 có negative cache ([#3511](https://github.com/apache/fluss/pull/3511)), giảm tải đáng kể". Khi đối chiếu mã `v1.0.0`, mình thấy #3511 **chỉ xử lý một trong hai đường hỏi metadata** (đường hỏi theo partition **ID**, chủ yếu do bên đọc dùng). Đường hỏi theo partition **tên** (do bên ghi dùng) **không có** negative cache. Và #3511 liên kết với issue **#3510**, không phải #3466. Chi tiết ở mục 4.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Partition** | Bảng chia theo giá trị cột (ví dụ theo ngày/giờ). Mỗi partition có **tên** (ví dụ `20260610`) và **ID** (số do Fluss cấp). |
| **Auto-partition expiration** | Fluss tự xóa partition cũ khi vượt số partition giữ lại (retention). Sau khi xóa, partition đó **không còn tồn tại**. |
| **ZooKeeper (ZK)** | Dịch vụ lưu metadata của cluster Fluss (bảng, partition, bucket, leader...). Nếu bị dồn quá nhiều request, cả cluster có thể chậm hoặc mất ổn định. |
| **Metadata / `GET_METADATA` (`MetadataRequest`)** | Client hỏi server "bảng/partition này có những bucket nào, leader ở đâu". Client cần thông tin này trước khi đọc hoặc ghi. |
| **TabletServer metadata cache** | Bản sao metadata nằm trong bộ nhớ của TabletServer. Nếu có trong cache thì trả ngay, **không cần hỏi ZK**. Không có thì phải hỏi ZK. |
| **`PartitionNotExistException`** | Lỗi server trả khi partition được hỏi không tồn tại. |
| **`MetadataUpdater`** | Thành phần phía client giữ metadata và gọi `GET_METADATA` để làm mới. Mỗi reader/writer task của Flink có một cái riêng. |
| **Negative cache (cache phủ định)** | Nhớ kết quả "không tồn tại" trong một thời gian, lần hỏi sau trả luôn lỗi mà không hỏi ZK nữa. |
| **Fan-out / khuếch đại** | Một request đầu vào sinh ra nhiều request ở tầng sau (ở đây: một `GET_METADATA` sinh nhiều lần đọc ZK). |
| **Enumerator (Flink source)** | Thành phần điều phối của Flink source, chạy một bản, định kỳ liệt kê partition và giao bucket cho các reader. |
| **`scan.partition.discovery.interval`** | Chu kỳ enumerator kiểm tra partition mới/đã xóa. Mặc định 1 phút theo issue (mình chưa đọc lại giá trị này trong mã). |
| **`PartitionsRemovedEvent`** | Sự kiện enumerator gửi cho reader để báo "các partition này đã bị xóa, ngừng đọc". Chỉ gửi sau lần liệt kê kế tiếp. |
| **`Sender`** | Luồng nền phía client gửi batch ghi tới server. Cần biết leader của bucket, nên phải có metadata. |

## Tóm tắt

Khi partition bị xóa (hết hạn), client Flink vẫn còn tham chiếu tới nó (reader đang giữ bucket, writer đang có batch). Các task này nhận `PartitionNotExistException` rồi **hỏi lại metadata liên tục**. TabletServer không có partition trong cache nên **hỏi ZK** mỗi lần. Enumerator chỉ biết partition đã bị xóa ở lần liệt kê kế tiếp (tối đa `scan.partition.discovery.interval`, mặc định 1 phút) rồi mới báo reader ngừng. Trong thời gian đó, `P` task × `N` partition thiếu có thể sinh cỡ `P × N` lượt đọc ZK. Với P=100, N=100, issue ước tính ≥ 10.000 lượt đọc ZK đồng thời, đủ làm quá tải ZK. Các con số này là **ước tính trong issue**, mình chưa tái hiện.

## 1. Lỗi nằm ở thành phần nào

Issue chỉ ra ba nguyên nhân gốc. Mình đối chiếu từng cái với mã `v1.0.0`:

1. **Server không nhớ rằng partition đã được chứng minh là không tồn tại** (`fluss-server`).
   - Mã `v1.0.0` có `PartitionNegativeCache` (từ #3511), khóa là **partition ID** (`Cache<Long, Boolean>`), TTL 10 phút tính từ lần truy cập cuối, tối đa 10.000 phần tử. Giá trị mặc định cố định trong mã, mình không thấy tham số cấu hình.
   - Nó được dùng trong `RpcServiceBase.processMetadataRequest` ở **nhánh request theo partition ID** (`request.getPartitionsIds()`).
   - **Nhánh request theo partition tên** (`request.getPartitionsPathsList()`) vẫn đi `getPartitionMetadataFromCache` rồi `getPartitionsMetadataFromZK` (không có negative cache). Hàm này gọi `zkClient.getPartitionIds(partitionPaths)`, đọc dữ liệu ZK cho từng đường dẫn partition, rồi ném `PartitionNotExistException` nếu thiếu.
2. **Mỗi request fan-out ra nhiều lượt đọc ZK** (`ZkBasedMetadataProvider`, `ZooKeeperClient`).
   - Đã xác nhận: `getPartitionIds` đọc từng đường dẫn (`getDataInBackground`) cho mọi partition được hỏi, **kể cả khi có partition thiếu**.
   - Phần "bỏ qua ZK với partition đã có trong cache" mà issue đề xuất thì mã `v1.0.0` **đã làm sẵn**: `processMetadataRequest` chỉ gửi các partition **không có trong cache** (`unknownPartitions`) xuống ZK.
3. **Client không tự dừng khi gặp `PartitionNotExistException`** (`fluss-client`, `fluss-flink`).
   - Reader: `LogFetcher` bắt `PartitionNotExistException` khi cập nhật metadata và chỉ log WARN "ignore it", không hủy đăng ký bucket. Reader chỉ ngừng khi enumerator gửi `PartitionsRemovedEvent`.
   - Writer: `Sender.handlePartitionNotExistException` bỏ qua (`continue`) với bảng không bật historical partition, nên batch cứ chờ. Đây cũng là nội dung comment của fxbing ("flush() không hoàn tất") và trùng với #4726.
   - Mình không thấy trong `v1.0.0` bản sửa nào làm client tự dừng. Mình **chưa** kiểm tra từng nhánh của `MetadataUpdater` để khẳng định không có nhánh nào dừng sớm.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Bảng có partition và bị xóa partition** (auto-partition hết hạn, hoặc `DROP PARTITION`).
2. **Có client còn tham chiếu partition vừa bị xóa**: reader đang giữ bucket của nó, hoặc writer đang có batch/ghi tiếp vào nó (ví dụ dữ liệu trễ).
3. **Quy mô đủ lớn**: nhiều task (`P` cao) và nhiều partition cùng hết hạn (`N` cao). Với P và N nhỏ, tải thêm không đáng kể.
4. **Khoảng thời gian tồn tại lỗi:** đến khi enumerator liệt kê lại partition (tối đa `scan.partition.discovery.interval`) với phía đọc. Với phía ghi, nếu bảng không bật historical partition thì không có điểm dừng rõ ràng (xem #4726).

Với bảng partition theo **giờ** hoặc ngắn hơn (partition xóa thường xuyên), sự kiện này lặp lại đều đặn. #3510 nêu đúng trường hợp "xoay vòng partition theo giờ".

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` có một phần giảm nhẹ (negative cache theo ID), chưa xử lý phần còn lại.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người chạy cluster Fluss có **bảng partition với auto-partition TTL**, kèm Flink source/sink song song cao. Ảnh hưởng lên **ZK và toàn cluster**, không chỉ lên job gây ra.

**Hậu quả (theo issue; mình chưa tái hiện):**
1. **Tải đột biến lên ZooKeeper** quanh thời điểm partition hết hạn, kéo dài tới một chu kỳ discovery.
2. Nếu ZK quá tải: các thao tác cần ZK của cả cluster chậm đi (metadata, bầu leader...), nhưng mức độ cụ thể mình chưa kiểm chứng.
3. **Không làm sai dữ liệu.** Đây là vấn đề tải và sẵn sàng, không phải sai lệch dữ liệu.
4. Phía ghi có thể kẹt batch (liên quan #4726), đó là hậu quả riêng, không thuộc ZK.

## 4. Đính chính so với `relate_issue.md`

Dòng #3466 ghi "1.0.0 có negative cache cho partition không tồn tại (#3511), giảm tải đáng kể". Đối chiếu:

- **Đúng một phần:** negative cache có trong `v1.0.0`, và nó giảm các lượt hỏi ZK cho **request theo partition ID**.
- **Phạm vi hẹp hơn:** nó **không áp dụng cho request theo partition tên**. Trong `v1.0.0`, `Sender`/`WriterClient` dùng `checkAndUpdatePartitionMetadata(PhysicalTablePath)` và `updatePhysicalTableMetadata`, gửi request theo **tên**. Reader (`LogFetcher`) thì gửi theo **ID** (`updateMetadata(tablePath, null, partitionIds)`). Nên:
  - **Phía đọc:** có thể được giảm tải bởi #3511 (suy luận từ mã, chưa đo).
  - **Phía ghi:** đường hỏi theo tên chưa có negative cache.
- **Cần lưu ý thêm:** PR #3511 liên kết issue #3510 chứ không phải #3466. Issue #3466 đề xuất cache theo `PhysicalTablePath` (tên) và tự dừng phía client; #3511 là cache theo ID. Không thể coi #3466 đã được giải quyết bởi #3511.
- **Chưa biết:** mình không đo được mức giảm tải thực tế của #3511 so với con số trong issue. Câu "giảm tải đáng kể" trong `relate_issue.md` mình không kiểm chứng được. Mình đề xuất đổi thành "giảm một phần (phía đọc theo ID), phía ghi theo tên chưa xử lý".

## 5. Mức ảnh hưởng với CDP

- Nếu bảng profile PK của bạn **không có partition** hoặc **không dùng auto-partition TTL**, bạn **không** gặp vấn đề này.
- Nếu bảng sự kiện/lịch sử có partition theo ngày/giờ và có TTL, rủi ro tăng theo `parallelism × số partition hết hạn cùng lúc`. Một cụm nhỏ với song song thấp thì khả năng thấp. Mình chưa có số đo.
- Phần **AI agent chỉ ghi** vào bảng partition: nếu bảng đó có TTL và agent ghi dữ liệu trễ vào partition đã hết hạn thì rơi vào #4726 (batch kẹt), chứ không chỉ là tải ZK.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Giữ retention dài hơn độ trễ tối đa** của dữ liệu, để client không ghi/đọc partition sắp bị xóa.
- **Tránh hết hạn nhiều partition cùng lúc** nếu có thể (ví dụ không dồn nhiều bảng cùng giờ hết hạn).
- **Không hạ `scan.partition.discovery.interval`** xuống dưới mặc định. Issue nói tài liệu cũng cảnh báo ZK chịu tải nếu hạ thấp.
- **Giám sát số request và độ trễ ZK** (Prometheus của ZK) quanh thời điểm partition hết hạn, trước go-live thử bằng cách cho hết hạn partition ở môi trường kiểm thử với `parallelism` giống thật.
- **Lọc dữ liệu trễ ở Flink** trước khi sink (cũng giúp #4726).
- Theo dõi issue #3466 và PR của fxbing/loserwang1024.

## 6. Những điều chưa kiểm chứng

- Con số "≥ 10.000 lượt đọc ZK" và thời gian "tới 1 phút" là ước tính của người báo, mình chưa tái hiện.
- Mình chưa kiểm tra mã `0.9.0` để biết reader khi đó gửi request theo ID hay theo tên.
- Mình chưa đo hiệu quả thực tế của #3511 (chưa chạy thử).
- Ảnh hưởng chính xác khi ZK quá tải lên các thao tác khác của cluster là suy luận, không có dữ liệu.
