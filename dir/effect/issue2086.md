# Issue #2086 — `FlussConnection.getTable` thất bại khi TabletServer được chọn đang không sẵn sàng

Link: https://github.com/apache/fluss/issues/2086 — Phiên bản được báo: `0.8.0` (ngày 03/12/2025, người báo swuferhong). Tiêu đề: *"FlussConnection cann't get table while the request server not ready"*. Issue gắn kèm hai stack trace, trong đó `getTable` thất bại với `DisconnectException: Cannot send request to server ... (id: ts-2) because it is disconnected`.

> **Lưu ý quan trọng (khác với `relate_issue.md`):**
> 1. Dòng #2086 trong `relate_issue.md` ghi trạng thái "⏭️ Fix liên quan #4873 chỉ có trên `main`". Mình đã đọc #4873: đó là **một issue khác** (về `Admin#getServerNodes()` giữ địa chỉ Coordinator cũ), **không phải** bản sửa của #2086.
> 2. Khi đọc mã `v1.0.0`, mình thấy `getTableInfo` (hàm mà `getTable` gọi) **đã đi qua cơ chế retry một lần kèm làm mới metadata**. Cơ chế này không có ở `0.9.0-incubating` và `0.9.1-incubating`. Nên lỗi trong issue (báo ở 0.8.0) **được giảm nhẹ trong 1.0.0**, dù chưa chắc đã hết hoàn toàn (mục 4).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`FlussConnection`** | Đối tượng kết nối của client Fluss. Từ đó lấy `Table`, `Admin`. |
| **`getTable(tablePath)`** | Lấy handle của bảng. Bên trong, nó hỏi server thông tin bảng (`GetTableInfoRequest`) rồi dựng `FlussTable`. Flink dùng ở lúc mở source reader và lookup function. |
| **`Admin` / `FlussAdmin`** | API quản trị của client (tạo/xóa/xem bảng...). `getTable` dùng `Admin.getTableInfo`. |
| **TabletServer (`ts-2`)** | Server lưu dữ liệu. Client giữ danh sách các TabletServer đang sống và chọn **một cái ngẫu nhiên** để hỏi metadata/thông tin bảng. |
| **`DisconnectException` / `NetworkException`** | Lỗi mạng "kết nối bị ngắt". Trong mã `v1.0.0`, `DisconnectException` và `NetworkException` đều thuộc nhóm **`RetriableException`** (lỗi đáng thử lại). |
| **Rolling upgrade** | Nâng cấp lần lượt từng server. Trong lúc đó có server tạm tắt hoặc đổi IP. |
| **Metadata cache của client** | Danh sách server và vị trí leader client đang nhớ. Có thể cũ (stale) sau khi server đổi. |
| **`RetryableGatewayClientProxy`** | Lớp bọc trong `fluss-rpc`: khi RPC lỗi `RetriableException`, nó **làm mới metadata rồi gọi lại đúng một lần**. |
| **Idempotent / read-only (đọc, lặp lại không đổi kết quả)** | Gọi lại an toàn. Khác với thao tác ghi (tạo bảng...), gọi lại có thể trùng lặp. |
| **Bootstrap servers** | Danh sách địa chỉ khởi động trong cấu hình client. Dùng để dựng lại danh sách server khi mọi địa chỉ đang nhớ đều hỏng. |

## Tóm tắt

Khi client mở bảng, nó hỏi một TabletServer **ngẫu nhiên** trong danh sách đang nhớ. Nếu server đó vừa tắt (ví dụ đang nâng cấp), `getTableInfo` nhận `DisconnectException` và `getTable` thất bại ngay, **không thử server khác**. Hậu quả với Flink: task mở source hoặc lookup function bị lỗi, job phải failover rồi khởi động lại. Ở `v1.0.0`, `getTableInfo` có thêm cơ chế "lỗi đáng thử lại thì làm mới metadata và thử lại một lần", nên kịch bản trong issue có khả năng không còn xảy ra khi chỉ có một server hỏng.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client` (`FlussConnection`, `FlussAdmin`, `MetadataUpdater`) và `fluss-rpc` (`RetryableGatewayClientProxy`).
- **Đường đi gây lỗi (đã đọc mã):**
  1. `FlussConnection.getTable` trước hết gọi `metadataUpdater.updateTableOrPartitionMetadata(...)`, rồi `admin.getTableInfo(tablePath).join()`.
  2. `FlussAdmin.getTableInfo` gửi `GetTableInfoRequest` qua `readOnlyGateway`, cổng này nhắm vào `metadataUpdater::getRandomTabletServer`, tức một TabletServer ngẫu nhiên trong cluster metadata của client.
  3. Nếu server được chọn đã ngắt kết nối, `ServerConnection` bọc lỗi thành `NetworkException`/`DisconnectException` (đều là `RetriableException`) và `join()` ném lỗi ra.
- **Cách sửa đã có trong `v1.0.0`:** `FlussAdmin` bọc `readOnlyGateway` bằng `RetryableGatewayClientProxy.createRetryableGatewayProxy(..., metadataUpdater::refreshClusterUntilAvailable, refreshExecutor, ...)`. Khi gặp `RetriableException`, proxy:
  1. Chạy `refreshClusterUntilAvailable`: lặp qua các TabletServer khả dụng, đánh dấu server lỗi là không khả dụng và thử server kế; nếu hết thì dựng lại cluster từ **bootstrap servers**.
  2. **Gọi lại RPC đúng một lần** với danh sách server đã làm mới.
  3. Nhiều lời gọi lỗi cùng lúc dùng chung một lần làm mới (gộp, tránh dồn tải).
- **Phiên bản có cơ chế này:** mình kiểm tra `RetryableGatewayClientProxy.java`: **không có** ở `v0.9.0-incubating` và `v0.9.1-incubating` (HTTP 404), **có** ở `v1.0.0`; `FlussAdmin` của 0.9.x cũng không dùng nó. Mình chưa xác định PR nào đưa nó vào. PR #2132 mà người bình luận trong issue nhắc (*"[RPC] Add a retry mechanism for idempotent interface requests"*, sd4324530) hiện ở trạng thái **closed** và mình không biết nó có được merge hay không; không nên coi nó là bản sửa.

## 2. Khi nào lỗi xảy ra

Trong issue (0.8.0), cần đồng thời:
1. **Một TabletServer tạm không sẵn sàng** (nâng cấp, restart, pod bị thay) tại thời điểm client gọi `getTable`.
2. **Client chọn đúng server đó** để hỏi (chọn ngẫu nhiên, nên xác suất tỉ lệ với số server hỏng).
3. **Client khởi tạo lại thường xuyên**: Flink mở source reader/lookup function lúc job khởi động hoặc failover (hai stack trace trong issue là `FlinkSourceSplitReader.<init>` và `FlinkAsyncLookupFunction.open`). Với dịch vụ chạy lâu thường chỉ gọi `getTable` lúc khởi động.

**Ở `v1.0.0`:** vì `getTableInfo` được retry một lần có làm mới, lỗi chỉ còn lọt nếu **cả lần thử lại cũng thất bại** (xem mục 4). Mình chưa tái hiện trên `1.0.0`.

**Một chi tiết nhỏ (đã đọc mã):** bước đầu `updateTableOrPartitionMetadata` của `getTable` nuốt lỗi `RetriableException`/`TimeoutException` (chỉ log WARN, đánh dấu server không khả dụng) nên bản thân nó không làm `getTable` thất bại.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng (theo issue, bản 0.8.0):** ai mở `FlussConnection.getTable` trong lúc có server tạm tắt: job Flink khởi động hoặc failover, dịch vụ khởi động lại trong lúc nâng cấp cluster.

**Hậu quả:**
1. **Lỗi khởi động/mở bảng**, không phải sai dữ liệu. Task Flink ném lỗi, job failover, thử lại cho đến khi chọn trúng server khỏe.
2. **Khó đoán:** do chọn ngẫu nhiên nên lúc lỗi lúc không, dễ gây "job khởi động chập chờn" trong rolling upgrade.
3. **Không mất hay sai dữ liệu** (mình không thấy cơ chế nào làm sai dữ liệu).

## 4. Đính chính và điều chưa chắc so với `relate_issue.md`

- **#4873 không phải bản sửa.** Issue #4873 (báo trên 1.0.0, người báo swuferhong) nói `Admin#getServerNodes()` có thể tiếp tục hỏi **địa chỉ Coordinator cũ** sau khi IP Coordinator đổi, vì lệnh `describeCluster` dùng cổng thường thay vì cổng có retry. Đây là lỗi khác, ở API khác. Nó có chung tinh thần (địa chỉ cũ khi rolling upgrade) và dùng chung `RetryableGatewayClientProxy`, nhưng không sửa #2086. Dòng trong `relate_issue.md` nên bỏ liên kết "fix liên quan" này hoặc ghi rõ "lỗi họ hàng".
- **Trạng thái đúng hơn:** có vẻ 🟡 (đã giảm nhẹ trong 1.0.0 bằng retry-một-lần), không phải ⏭️ "chỉ trên main". Mình chưa chạy thử, nên chưa nâng lên ✅.
- **Vì sao chưa chắc đã hết lỗi:**
  - Retry chỉ **một lần** (thiết kế có chủ đích, theo javadoc). Nếu lần làm mới vẫn trúng server hỏng hoặc server khác cũng đang lỗi thì lỗi vẫn được ném.
  - `refreshClusterUntilAvailable` lặp qua các server hỏng, mỗi lần thất bại phải chờ hết thời gian kết nối, nên thời gian chờ có thể dài (javadoc ghi rõ "each failed attempt already pays a connection timeout"). Mình chưa đo.
  - Cơ chế chỉ áp dụng cho `readOnlyGateway`. Các cổng ghi (`gateway` cho Coordinator) cố ý không retry (có TODO trong mã vì thao tác không idempotent).
- **Các hàm khác của client có an toàn không?** Mình chỉ xác nhận `getTableInfo` và các hàm dùng `readOnlyGateway`. Lookup/ghi/đọc log dùng đường khác (`MetadataUpdater` và các thread nền), mình chưa kiểm tra trong issue này.

## 5. Mức ảnh hưởng với CDP

- **Khi khởi động hoặc failover dịch vụ lookup cho AI agent / job Flink** trong lúc đang rolling upgrade cluster Fluss trên Kubernetes: với `1.0.0` rủi ro thấp hơn 0.8.0/0.9.x, nhưng vẫn có thể gặp lỗi mở bảng nếu nhiều server đang xoay vòng.
- Nếu dịch vụ của bạn giữ `FlussConnection` lâu dài và chỉ gọi `getTable` một lần lúc khởi động, rủi ro tập trung ở **lúc khởi động**.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Dùng `1.0.0` trở lên**, không dùng 0.8.x/0.9.x cho phần này.
- **Thêm retry ở tầng ứng dụng** quanh khởi tạo `FlussConnection`/`getTable` với backoff (như `relate_issue.md` đã khuyến nghị): khởi động lại dịch vụ hoặc job Flink tự động khi lỗi, readiness probe của Kubernetes chỉ báo sẵn sàng sau khi mở bảng thành công.
- **Rolling upgrade từng node**, chờ node trước vào lại hoàn toàn rồi mới sang node kế, để giảm số server hỏng cùng lúc.
- **Cấu hình `bootstrap.servers`** bằng tên ổn định (Service/DNS của StatefulSet) để việc dựng lại cluster từ bootstrap có đích đến đúng.
- **Diễn tập:** khởi động job/dịch vụ trong lúc xóa một pod TabletServer ở môi trường thử và xem `getTable` có ném lỗi hay không.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện lỗi trên cả `0.8.0` lẫn `1.0.0`.
- Mình chưa xác định PR nào đưa `RetryableGatewayClientProxy` vào `1.0.0`; chỉ biết nó vắng ở `0.9.0-incubating`/`0.9.1-incubating` và có ở `1.0.0`.
- Mình chưa biết issue #2086 vẫn mở vì còn khoảng hở nào, hay chỉ vì chưa ai đóng. Trang GitHub hiển thị lẫn lộn dữ liệu trạng thái nên mình không kết luận từ đó (`issue.md` ghi nó là issue đang mở).
- Thời gian chờ tối đa của `refreshClusterUntilAvailable` khi nhiều server hỏng: chưa đo.
