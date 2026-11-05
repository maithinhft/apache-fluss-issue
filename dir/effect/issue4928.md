# Issue #4928 — Khởi tạo metadata "thành công" nhưng thiếu ánh xạ bảng, writer ghi partition lỗi lặp lại

Link: https://github.com/apache/fluss/issues/4928 — Phiên bản được báo: client hạ nguồn dựa trên `0.9` (người báo không giữ lại binary đúng thời điểm sự cố). Liên quan: #483 và #1458 (refactor metadata, timeout và retry cấu hình được). Chưa thấy PR sửa trong `relate_issue.md`.

> **Lưu ý về độ chắc chắn:** người báo cáo nói rõ họ **chưa tái hiện được** trên mã upstream; sự cố xảy ra ở một triển khai hạ nguồn và việc khởi tạo bị timeout chỉ là **nghi vấn**, log lịch sử không xác nhận trực tiếp. Mình đã đọc cùng các đoạn mã tại tag `v1.0.0` và xác nhận các điểm họ nêu có thật trong mã, nhưng chưa chạy tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Metadata (phía client)** | "Bản đồ" cluster mà client giữ trong bộ nhớ, lớp `Cluster`: danh sách server, bảng, partition, bucket nằm ở đâu. |
| **`tableId`** | Số do server cấp cho mỗi bảng. Khác với đường dẫn bảng dạng chữ (`database.table`). |
| **Ánh xạ bảng (table mapping)** | Hai bảng tra trong bản đồ: **đường dẫn bảng → `tableId`** và ngược lại **`tableId` → đường dẫn bảng**. |
| **Bảng partition** | Bảng chia thành nhiều partition (thường theo ngày). Mỗi partition có `partitionId` và thuộc về một bảng cha. |
| **Phản hồi metadata của partition** | Khi client chỉ hỏi về một partition, server trả thông tin partition kèm `tableId` của bảng cha, **không kèm tên bảng**. Muốn biết tên bảng, client phải tra `tableId` trong bản đồ của mình. |
| **`updateMetadata()`** | Hàm trong `MetadataUpdater` gửi RPC hỏi server rồi dựng lại bản đồ; dùng chung cho cả lúc khởi tạo và lúc làm mới nền. |
| **Nuốt lỗi** | Bắt exception, chỉ ghi log rồi chạy tiếp như không có gì xảy ra, thay vì báo lên cho nơi gọi. |
| **Accumulator của writer** | Bộ đệm trong writer gom bản ghi thành batch trước khi gửi (xem [issue2967.md](issue2967.md)). |

## Tóm tắt

Có hai khiếm khuyết nối thành một chuỗi lỗi:

1. **Khởi tạo nuốt lỗi.** Khi `getTable()` nạp metadata mà bị timeout hoặc lỗi có thể thử lại, lỗi bị bỏ qua. Hàm vẫn trả về một `Table` trông bình thường, dù bản đồ của client chưa có ánh xạ `tableId` → đường dẫn bảng.
2. **Làm mới sau đó không tự chữa.** Khi ghi vào bảng partition, client chỉ hỏi metadata của partition. Phản hồi chỉ chứa `tableId`. Client tra `tableId` mà bản đồ không có nên ném `IllegalArgumentException: table path not found for tableId <id> in cluster`, và lần làm mới nào sau đó cũng lặp lại y hệt vì không bao giờ hỏi bổ sung metadata của bảng cha.

Kết quả: writer **kẹt**, ghi lỗi liên tục, không báo lỗi lúc khởi động.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, phần quản lý metadata và khởi tạo bảng.
- **Các điểm trong mã** (đã đọc tại tag `v1.0.0`):
  1. `FlussConnection.getTable(tablePath)`:

     ```java
     metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
     Admin admin = getOrCreateAdmin();
     return new FlussTable(this, tablePath, admin.getTableInfo(tablePath).join());
     ```

     Hai bước độc lập: nạp metadata vào bản đồ, rồi lấy thông tin bảng qua Admin. Bước hai thành công **không chứng tỏ** bản đồ đã đầy đủ.
  2. `MetadataUpdater.updateMetadata(...)`: bắt `RetriableException` và `TimeoutException`, chỉ ghi `LOG.warn("Failed to update metadata, but the exception is re-triable.", t)` rồi **không ném lại**. Nó còn đánh dấu server là không khả dụng. Đây là chỗ "nuốt lỗi".
  3. `MetadataUtils.sendMetadataRequestAndRebuildCluster(...)`: đợi phản hồi bằng `.get(30, TimeUnit.SECONDS)`, nên timeout 30 giây là một trong những đường dẫn tới bước nuốt lỗi ở trên.
  4. `MetadataUtils.getTableMetadataToUpdate(...)`: khi xử lý phản hồi có thông tin partition, nó tìm đường dẫn bảng theo `tableId` trong chính phản hồi; nếu không có thì gọi `cluster.getTablePathOrElseThrow(tableId)` trên bản đồ hiện tại.
  5. `Cluster.getTablePathOrElseThrow(long tableId)`: nếu không tìm thấy thì ném `IllegalArgumentException("table path not found for tableId <id> in cluster")`. Đúng thông báo trong issue.
  6. `MetadataUpdater.checkAndUpdatePartitionMetadata(PhysicalTablePath)`: khi partition chưa có trong bản đồ, gọi `updateMetadata(null, singleton(partitionPath), null)`, tức **chỉ hỏi partition**, không hỏi bảng cha.
- **Vì sao không tự phục hồi:** các lần làm mới chỉ gửi yêu cầu partition-only; không có logic nào "thấy thiếu ánh xạ bảng cha thì hỏi thêm metadata của bảng". Nên kể cả khi mạng đã khỏe lại, mỗi lần làm mới vẫn thất bại ở cùng điểm.

## 2. Khi nào lỗi xảy ra

Cần đồng thời (theo mô tả trong issue, một phần là suy đoán của người báo):

1. **Bảng có partition.** Lỗi nằm ở đường làm mới metadata của partition. Với bảng không partition, đường này không được dùng.
2. **Lần nạp metadata ban đầu thất bại âm thầm**, ví dụ timeout hoặc lỗi có thể thử lại khi khởi tạo kết nối (server, ZooKeeper hoặc mạng đang chậm lúc ứng dụng khởi động). Người báo nghi đây là nguyên nhân nhưng log không xác nhận.
3. **`admin.getTableInfo()` vẫn thành công**, nên `getTable()` không ném lỗi.
4. **Sau đó ghi vào một partition mà client chưa có trong bản đồ**, ví dụ partition của ngày mới hoặc ngay lần ghi đầu tiên, khiến client phải gửi yêu cầu metadata partition-only.

**Dấu hiệu:** khởi động không có lỗi; sau đó mọi lần ghi lỗi với `IllegalArgumentException: table path not found for tableId <id> in cluster`, lỗi xảy ra **trước khi** bản ghi vào accumulator, nên không có batch nào được gửi.

**Mình chưa kiểm tra:**
- Có đường nào khác khiến bản đồ thiếu ánh xạ (ví dụ một metadata response bị cắt) hay không.
- Cách `FlinkSinkWriter` hoặc writer xử lý lỗi này lặp lại (retry, fail job hay chỉ log). Issue chỉ nói "write failed repeatedly".

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** ứng dụng dùng Fluss Java client để **ghi vào bảng có partition** (ví dụ job Flink sink ghi sự kiện theo ngày, hoặc ứng dụng tự viết), đặc biệt nếu client khởi động khi cluster hoặc ZooKeeper đang chậm.

**Hậu quả:**
1. **Writer kẹt và mất khả năng ghi** vào bảng đó. Lỗi lặp lại cho đến khi restart client.
2. **Lỗi im lặng lúc khởi động.** `getTable()` trả về bình thường, nên health check hoặc logic kiểm tra khi khởi động không phát hiện được.
3. **Không có dữ liệu sai âm thầm.** Lỗi xảy ra trước khi bản ghi vào accumulator. Rủi ro là dữ liệu ngừng chảy: nguồn upstream (Flink, queue) có thể tích lag, hoặc phải retry/replay.
4. **Khó chẩn đoán.** Nguyên nhân gốc (timeout lúc khởi tạo) chỉ nằm trong một dòng cảnh báo ở thời điểm khởi động, cách xa chỗ lỗi xuất hiện.

**Hướng sửa theo issue:**
1. **Khởi tạo phải thành công thật hoặc ném lỗi sau số lần thử có giới hạn.** `getTable()` phải lấy và kiểm tra metadata cần thiết trước khi báo thành công; thử lại các lỗi tạm thời trong ngân sách hữu hạn; hết hạn thì ném exception kèm nguyên nhân gốc. Việc `getTableInfo()` thành công không được che lấp việc nạp metadata chưa đủ.
2. **Khi làm mới metadata của partition, phát hiện thiếu ánh xạ bảng cha và hỏi bổ sung** (kèm metadata bảng trong request, hoặc lấy trước khi xử lý phản hồi). Áp dụng cho cả đường làm mới một partition và theo batch.
3. Tách yêu cầu: khởi tạo phải chờ hoặc thất bại, còn làm mới nền vẫn phải giữ hành vi thử lại cho lỗi tạm thời.

**Biện pháp giảm thiểu hiện tại (suy ra từ phân tích, không phải từ issue):**
- Alert khi throughput của sink về 0 hoặc khi log xuất hiện `table path not found for tableId` lặp lại.
- Restart job hoặc writer khi gặp lỗi này: client dựng lại bản đồ từ đầu.
- Với ứng dụng tự viết: sau `getTable()`, thực hiện một thao tác nhẹ trên bảng partition (ví dụ một lần ghi hoặc lookup thử) để phát hiện sớm lỗi này trước khi nhận tải, thay vì tin hoàn toàn vào việc `getTable()` trả về.
