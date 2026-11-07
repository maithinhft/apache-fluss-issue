# Issue #3223 — TabletServer vẫn recover log của các bucket thuộc partition đã bị xóa

Link: https://github.com/apache/fluss/issues/3223 — Phiên bản được báo: `0.9.0` (người báo: zuston, 28/04/2026). Issue chỉ có một câu: *"There is no need to recover logs when the partition not exist."* Issue được tách ra từ #3224 (cùng người báo). Ở #3224, sau khi TabletServer chết vì đầy đĩa, recovery thất bại, và *"sau khi drop các partition liên quan bằng admin, recovery partition vẫn không được bỏ qua"*. Không có bình luận. Trạng thái: còn mở. PR [#3225](https://github.com/apache/fluss/pull/3225) **đang mở**.

> **Mức chắc chắn:** cao, và kết luận là **đã được xử lý trong 1.0.0** bằng một PR khác. Mình đọc mã `v1.0.0`: `LogManager.loadLog` **kiểm tra partition trên ZooKeeper trước khi recover log**. Nếu partition không còn (hoặc ID không khớp), nó **không recover mà xóa thư mục rác**. Đoạn này đến từ commit [#3388](https://github.com/apache/fluss/pull/3388) (07/06/2026, sửa issue #3387, có trong `v1.0.0`). Mình cũng nêu một **rủi ro ngược** suy ra từ mã (mục 3), chưa kiểm chứng.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Load / recover log khi khởi động** | Khi TabletServer khởi động, `LogManager` quét **mọi thư mục tablet** (mỗi bucket một thư mục) trên các ổ dữ liệu, rồi nạp và phục hồi log của từng bucket. Nếu shutdown không sạch, nó đọc lại segment để kiểm tra. |
| **Partition đã xóa nhưng còn thư mục** | Partition bị drop (thủ công hoặc hết hạn) mà thư mục trên đĩa TabletServer **vẫn còn**, ví dụ vì lúc xóa server đang chết (#1629, #3357, #4516). |
| **`PartitionNotExistException` / `SchemaNotExistException`** | Lỗi "không tìm thấy partition" hoặc "không tìm thấy schema bảng" trên ZooKeeper. |
| **Partition ID** | Mỗi partition có ID duy nhất. Drop rồi tạo lại **cùng tên** thì ra ID **mới**. |
| **ZooKeeper follower lag** | ZooKeeper follower có thể chưa thấy dữ liệu vừa được ghi qua node khác (xem #4522). |

## Tóm tắt

Ở `0.9.0`: TabletServer recover mọi thư mục tablet trên đĩa, **kể cả** của partition đã xóa. Nếu thư mục đó bị hỏng (ví dụ sau khi đầy đĩa, #3224), recovery lỗi và **server không khởi động được**, dù partition đó không còn cần nữa.

Ở `1.0.0`: trước khi tạo hoặc recover `LogTablet`, `loadLog` đọc schema bảng (`getTableInfo`) và **đăng ký partition** trên ZooKeeper.
- Bảng không còn: `SchemaNotExistException`.
- Partition không còn, hoặc partition ID khác (bị tạo lại): `PartitionNotExistException`.

Job nạp log bắt hai lỗi này, log `"Table or partition for {} has already been dropped, the residual data will be removed."`, **xóa thư mục log**, xóa thư mục KV tương ứng nếu có, dọn thư mục cha rỗng, rồi tiếp tục với các bucket khác. Vậy partition đã xóa **không bị recover** và rác được dọn luôn.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `log/LogManager` (`loadLog`, `createLogLoadingJob`) và `TabletManagerBase.getTableInfo`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `LogManager.loadLog(...)`, **trước** `LogTablet.create(...)`:
    ```java
    TableInfo tableInfo = getTableInfo(zkClient, tablePath);           // ném SchemaNotExistException nếu bảng đã xóa
    // Table schema exists, but the partition may have been dropped (or dropped
    // and recreated with a new ID) while this TS was offline.
    // Validate both partition name and partition ID against ZK.
    if (partitionName != null) {
        Optional<PartitionRegistration> registration = zkClient.getPartition(tablePath, partitionName);
        if (!registration.isPresent() || registration.get().getPartitionId() != tableBucket.getPartitionId()) {
            throw new PartitionNotExistException("Failed to load partition '%s' (partitionId=%d) of table '%s': partition not found or partitionId mismatch in zookeeper metadata.");
        }
    }
    ```
  - `createLogLoadingJob(...)`: bắt `SchemaNotExistException` hoặc `PartitionNotExistException`, log ERROR, gọi `FileUtils.deleteDirectoryQuietly(tabletDir)`, xóa `kvTabletDir` nếu có, xóa thư mục partition và bảng nếu rỗng, rồi `return`. Lỗi khác vẫn `throw new FlussRuntimeException(e)`, làm server không khởi động được.
  - Lịch sử git: đoạn kiểm tra partition do commit `02e40ec17` *"fix: remote orphan files and directories when tablet starts up (#3388)"* (07/06/2026) thêm vào. Commit này có trong tag `v1.0.0`. PR #3388 liên kết với issue #3387, không phải #3223.

## 2. Khi nào lỗi xảy ra

- **Bản cũ:** TabletServer khởi động khi trên đĩa còn thư mục của partition hoặc bảng đã xóa, và thư mục đó hỏng.
- **Bản `1.0.0`:** theo mã, partition hoặc bảng đã xóa được bỏ qua và dọn. Recovery **vẫn** có thể lỗi với partition **còn tồn tại** mà dữ liệu hỏng (xem #3224, #2943).

## 3. Tác động đến ai, làm sai lệch gì

- **Bản cũ:** server không khởi động được vì thư mục rác hỏng. Khởi động chậm vì recover dữ liệu thừa.
- **Bản `1.0.0`:** không còn tác động đó.
- **Rủi ro ngược, suy ra từ mã và chưa kiểm chứng:** việc đọc ZooKeeper ở đây **không gọi `sync`** (cùng hàm `getTableInfo` của #4522). Nếu lúc TabletServer khởi động, session của nó nối vào một ZooKeeper follower **chưa thấy** bảng hoặc partition **vừa được tạo**, thì `loadLog` có thể coi bảng hoặc partition hợp lệ là "đã xóa" và **xóa thư mục dữ liệu local** của nó. Dữ liệu vẫn còn trên các replica khác và sẽ được sao chép lại, nhưng nếu đây là replica duy nhất còn dữ liệu thì có thể mất. Cửa sổ thời gian rất hẹp (bảng hoặc partition phải được tạo chỉ vài mili giây trước khi server đọc), nên khả năng thấp.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` gom #3223 vào ghi chú cuối mục 4.4 (*"Recover cả partition đã xóa"*, kèm *"Không thấy fix nào cho các issue này trong 1.0.0"*). Đối chiếu:
- **Sai về trạng thái:** 1.0.0 **đã xử lý** (commit #3388). Nên là **✅** (đã xác minh trong mã), dù issue và PR #3225 vẫn mở.
- Lỗi recovery do **đĩa đầy** ở #3224 với partition **còn tồn tại** thì vẫn là vấn đề riêng.

## 5. Mức ảnh hưởng với CDP

- **Không còn ảnh hưởng** như mô tả trên 1.0.0. Thậm chí có lợi: rác do #1629, #3357, #4516 để lại được dọn khi TabletServer restart.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi gặp rác trên đĩa (xem #4516, #1629), **restart TabletServer** là một cách dọn.
- Để tránh rủi ro ngược ở mục 3: không restart TabletServer **ngay khi** vừa tạo bảng hoặc partition mới. Giữ ZooKeeper khỏe để giảm độ trễ của follower.
- Theo dõi log `"has already been dropped, the residual data will be removed"` sau mỗi lần TabletServer khởi động. Nếu thấy dòng này cho một bảng hoặc partition **đang tồn tại**, đó là dấu hiệu rủi ro ngược đã xảy ra.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy thử TabletServer khởi động với thư mục của partition đã xóa.
- Mình chưa kiểm chứng rủi ro "ZooKeeper follower chậm khiến dữ liệu hợp lệ bị xóa" ở mục 3.
- Mình chưa kiểm tra `KvManager` có cùng kiểm tra khi nạp KV hay không (issue #4522 nói `KvManager` cũng gọi `getTableInfo`).
- Mình chưa đọc #3387 và PR #3225.
