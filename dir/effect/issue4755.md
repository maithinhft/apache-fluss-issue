# Issue #4755 — Server trả sai lỗi cho bảng không tồn tại, và dùng metadata cũ khi bảng bị xóa rồi tạo lại cùng tên

Link: https://github.com/apache/fluss/issues/4755 — Phiên bản được báo: `main` (1.1-SNAPSHOT); người báo (dstolis) phát hiện khi làm issue #4706. Không có bình luận. PR sửa: [#4758](https://github.com/apache/fluss/pull/4758) *"[server] Fix stale metadata served for missing and re-created tables"*, người viết là dstolis, polyzos là người merge.

> **Lưu ý quan trọng (khác với `relate_issue.md`):** dòng #4755 ghi *"❌ PR #4758 chưa merge"*. Khi mình kiểm tra, **PR #4758 đã được merge vào `main`** (merge commit `185d1ff`, ngày commit 2026-11-04). Tuy nhiên, bản sửa **chưa có trong `v1.0.0`** và cũng **chưa có trong nhánh `release-1.0`** (mình so sánh trực tiếp mã `MetadataManager.getTables`). Nên kết luận thực tế với bạn vẫn là "1.0.0 còn lỗi", nhưng lý do "PR chưa merge" cần đổi thành "đã merge vào `main`, chưa vào bản release".

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Metadata request (`GET_METADATA`)** | Client hỏi server "bảng này có id gì, bucket nào, leader ở đâu". Client cần thông tin này để biết gửi ghi/đọc đi đâu. |
| **`TableNotExistException` / `TABLE_NOT_EXIST`** | Lỗi và mã lỗi chuẩn cho "bảng không tồn tại". Client hiểu mã này và có thể dừng thử lại, báo lỗi rõ ràng. |
| **`UNKNOWN_SERVER_ERROR`** | Mã lỗi chung chung khi server gặp lỗi không nằm trong danh sách mã đã biết. Client không biết nguyên nhân thật. |
| **Wrap (bọc) ngoại lệ** | Bắt một ngoại lệ rồi ném lại bằng ngoại lệ khác (ở đây là `FlussRuntimeException`), làm mất kiểu ngoại lệ gốc. |
| **Table id** | Mỗi bảng có một id số do Fluss cấp. **Xóa rồi tạo lại bảng cùng tên sẽ có id mới**, dù đường dẫn bảng giống hệt. |
| **Bucket / partition id** | Bucket là phần chia nhỏ của bảng; partition có id riêng. Các id này gắn với một table id cụ thể. |
| **`TabletServerMetadataCache`** | Bản sao metadata nằm trong bộ nhớ của TabletServer. Coordinator đẩy cập nhật xuống bằng `UpdateMetadata`. |
| **Stale (cũ) metadata** | Thông tin mà server còn nhớ nhưng đã lỗi thời (bảng đã bị xóa/tạo lại). |
| **`UpdateMetadata`** | Thông điệp Coordinator gửi cho TabletServer để cập nhật cache. Có độ trễ so với lúc ZooKeeper đã đổi. |
| **`InvalidBucketRoutingException`** | Lỗi khi client gửi ghi tới bucket không khớp (ví dụ số bucket client nghĩ là 10 nhưng server nói 1). |
| **`flush()`** | Lệnh của writer chờ mọi bản ghi đang đệm được gửi xong. Nếu batch không bao giờ được xác nhận, `flush()` treo. |

## Tóm tắt

Issue gồm **hai lỗi ở phía server**, cùng xoay quanh bảng bị xóa hoặc tạo lại:
1. **Sai mã lỗi.** Khi client hỏi metadata của một bảng đã bị xóa, server bắt `TableNotExistException` rồi bọc nó thành `FlussRuntimeException`, nên phản hồi là `UNKNOWN_SERVER_ERROR` thay vì `TABLE_NOT_EXIST`. Client không phân biệt được "bảng đã bị xóa". Người báo cho rằng đây là một lý do khiến lookup trên bảng bị xóa không bao giờ trả về (#4706).
2. **Metadata trộn lẫn.** Sau khi bảng bị xóa rồi tạo lại cùng tên, cho tới khi TabletServer nhận được `UpdateMetadata` cho bảng mới, cache của nó có thể **ghép thông tin của bảng mới (đọc từ ZK) với bucket/partition của bảng cũ (còn trong cache)**. Client sau đó định tuyến ghi tới bucket hoặc partition không tồn tại. Tùy thời điểm, ghi thất bại nhanh hoặc `flush()` treo mãi.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`: `MetadataManager`, `TabletServerMetadataCache`, `RpcServiceBase`.
- **Lỗi 1 (đã xác nhận ở `v1.0.0`):** `MetadataManager.getTables` ném `TableNotExistException` khi bảng thiếu, nhưng toàn bộ khối nằm trong `try { ... } catch (Exception e) { throw new FlussRuntimeException("Failed to get tables '...'", e); }`. `TableNotExistException` bị bắt bởi `catch (Exception e)` và bọc lại. Hàm `Errors.forException` chỉ tra theo lớp ngoại lệ ngoài cùng (và các lớp cha), không đi vào nguyên nhân bên trong, rồi mặc định `UNKNOWN_SERVER_ERROR`. (Hàm `getTable` cho một bảng đơn lẻ thì **không** bọc như vậy.)
- **Lỗi 2 (đã xác nhận ở `v1.0.0`):** `TabletServerMetadataCache.getTableMetadata` lấy `tableId` từ cache, rồi gọi `metadataManager.getTable(tablePath)` đọc thông tin bảng từ ZK, rồi ghép với `snapshot.getBucketMetadataForTable(tableId)` (bucket theo id **cũ** trong cache). Không có bước nào kiểm tra `tableInfo.getTableId()` có bằng `tableId` trong cache hay không. Đúng như issue mô tả. Với `getPartitionMetadata`, mã `v1.0.0` chỉ trả dữ liệu từ cache (id bảng và id partition lấy từ cache), không đối chiếu với ZK.
- **Bản sửa đã merge vào `main` (PR #4758), mình đã đọc mã `main`:**
  - `getTables` thêm `catch (TableNotExistException e) { throw e; }` trước `catch (Exception e)`, nên `TableNotExistException` không bị bọc.
  - `getTableMetadata` thêm `if (tableInfo.getTableId() != tableId) return Optional.empty();` và kiểm tra bucket đủ (`getCompleteBucketMetadata`); nếu không khớp thì trả rỗng, tức rơi về đọc ZK.
  - Phần partition: `getPartitionsMetadata` kiểm tra từng partition trong cache với đăng ký trên ZK (`PartitionRegistration`): chỉ dùng cache nếu `tableId` và `partitionId` đều khớp, dùng một lần đọc ZK theo lô cho mỗi bảng.
- **Phạm vi PR:** theo danh sách file của PR: `RpcServiceBase`, `MetadataManager`, `MetadataProvider`, `TabletServerMetadataCache`, `TabletServerMetadataProvider`, `ZooKeeperClient`.
- **Có trong bản release không:** mình so sánh `MetadataManager.getTables` ở `v1.0.0`, `release-1.0` và `main`: chỉ `main` có `catch (TableNotExistException e)`. Nhánh `release-1.0` đang hơn `v1.0.0` 13 commit (backport) nhưng vẫn **chưa có** bản sửa này. `TabletServerMetadataCache` ở `release-1.0` cũng chưa có kiểm tra table id.

## 2. Khi nào lỗi xảy ra

**Lỗi 1 (sai mã lỗi):**
1. Client gửi metadata request cho một **bảng không tồn tại** (đã bị xóa, hoặc sai tên).
2. Cache của server không có bảng đó nên đi qua nhánh đọc ZK (`getTablesMetadataFromZK`). Khi đó lỗi bị bọc và trả `UNKNOWN_SERVER_ERROR`.

**Lỗi 2 (metadata trộn lẫn):**
1. **Xóa rồi tạo lại bảng cùng đường dẫn** (hoặc xóa/tạo lại partition cùng tên trong bảng có partition).
2. Một TabletServer **chưa nhận được `UpdateMetadata`** cho bảng mới nhưng đã có ZK mới. Khoảng thời gian này ngắn bình thường, nhưng dài ra khi hệ thống **chịu tải** (người báo thấy khi chạy `mvn verify` đầy đủ).
3. Có client hỏi/ghi vào bảng trong khoảng đó.

**Phiên bản:** báo trên `main`, nhưng mã gây lỗi có trong `v1.0.0`, nên `1.0.0` bị ảnh hưởng.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành có quy trình **drop rồi create lại bảng cùng tên** (làm lại bảng sau khi đổi schema, môi trường test/CI chạy lặp), và ứng dụng ghi/lookup đúng lúc đó.

**Hậu quả (theo issue; mình chưa tái hiện):**
1. **Ghi bị định tuyến sai:** `InvalidBucketRoutingException: ... requested bucket count 10, expected 1`, hoặc `IllegalArgumentException: table path not found for tableId 3 in cluster`.
2. **Treo:** `flush()` không bao giờ xong, hoặc test hang (`FlussTableITCase#testAppendAndPoll`).
3. **Lỗi khó chẩn đoán:** `UNKNOWN_SERVER_ERROR` không cho biết bảng bị xóa; client không biết nên ngừng thử lại. Người báo viết đây là *"one reason"* khiến lookup trên bảng đã xóa không trả về (#4706). Nguyên nhân chính của #4706 theo người báo của issue đó lại là một chỗ khác: lookup được xếp lại hàng đợi mà không đếm số lần thử, nên `lookup.max-retries` không có tác dụng khi không tìm được leader. Nên #4755 không phải nguyên nhân duy nhất.
4. **Không thấy bằng chứng mất dữ liệu đã ghi thành công.** Lỗi là định tuyến sai hoặc treo, không phải sai giá trị. Mình chưa kiểm tra liệu có batch nào được ghi nhầm vào bucket của bảng mới khi id trùng ngẫu nhiên.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

- **"❌ PR #4758 chưa merge"**: **không còn đúng**, PR đã merge vào `main` (commit `185d1ff`, ngày 2026-11-04). Mình không biết nó đã được merge trước hay sau thời điểm `relate_issue.md` được phân tích (2026-11-04). Nhưng bản sửa **không có trong `v1.0.0` và không có trong nhánh `release-1.0`**. Trạng thái nên là ⏭️ ("đã fix trên `main`, chưa có trong 1.0.0"), cùng nhóm với #4256, #3084, #5038.
- **"Như #4741"**: #4741 là lỗi khác: *metadata của bảng mới có thể bị xóa nhầm khi drop rồi tạo lại cùng tên* (lỗi phía xóa/Coordinator). #4755 là lỗi phía **đọc/cache** của TabletServer và mã lỗi. Cùng kịch bản "drop rồi tạo lại cùng tên" nên cùng một biện pháp phòng ngừa, nhưng không phải cùng một lỗi. Mình chưa đọc #4741.
- **Lỗi 1 chỉ là "sai mã lỗi"**, mức hậu quả phụ thuộc cách client xử lý `UNKNOWN_SERVER_ERROR`. Mình chưa đọc phía client để biết nó xử lý ra sao (thử lại, hay ném lỗi).

## 5. Mức ảnh hưởng với CDP

- **Nếu CDP không bao giờ drop rồi tạo lại bảng cùng tên** trên cluster đang có client chạy (khuyến nghị sẵn có trong `relate_issue.md`), thì **lỗi 2 gần như không xảy ra**. Lỗi 1 chỉ xuất hiện khi ứng dụng hỏi một bảng thật sự không tồn tại.
- **Rủi ro cao hơn** trong các việc vận hành như: migrate schema bằng cách drop/create, môi trường staging được reset thường xuyên, job tạo bảng theo kỳ (ví dụ bảng theo tháng) tái sử dụng tên.
- Luồng lookup của AI agent trên bảng profile đang tồn tại ổn định thì không dính.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng):**
- **Không drop rồi tạo lại bảng cùng tên.** Dùng tên có phiên bản (`profile_v2`) rồi chuyển ứng dụng sang bảng mới, sau đó mới xóa bảng cũ.
- **Nếu bắt buộc phải tạo lại:** dừng job ghi/đọc bảng đó trước, đợi đủ lâu để mọi TabletServer nhận cập nhật metadata, rồi mới tạo lại và khởi động lại client.
- **Đặt timeout, retry có giới hạn và circuit breaker ở tầng ứng dụng** để không bị treo vô hạn khi gặp `flush()`/lookup kẹt.
- **Dùng bản build chứa PR #4758** (từ `main`) nếu cần ngay; nếu chờ bản chính thức, theo dõi bản phát hành kế sau `1.0.0`. Tự build từ `main` mang theo các thay đổi khác nên cần thử kỹ.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện cả hai lỗi, chỉ đối chiếu mã `v1.0.0` với mô tả trong issue và so sánh với mã `main`.
- Ngày merge chính xác của #4758 mình suy từ ngày commit (2026-11-04T17:23:27Z); mình chưa đọc dòng thời gian của PR.
- Mình chưa kiểm tra phía client xử lý `UNKNOWN_SERVER_ERROR` ra sao, nên chưa biết lỗi 1 gây hậu quả cụ thể nào ngoài thông báo kém rõ ràng.
- Mình chưa xác nhận lời của người báo rằng lỗi 2 chủ yếu xảy ra khi tải cao.
- Mình chưa đọc #4741 và chưa kiểm tra liệu #4758 có xử lý cả #4741 hay không.
- Mình chưa kiểm tra bản `1.0.x` nào sẽ có bản sửa này.
