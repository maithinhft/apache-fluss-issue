# Issue #2967 — Ghi KV bị lỗi `IndexOutOfBoundsException` khi một batch có quá nhiều dòng changelog

Link: https://github.com/apache/fluss/issues/2967 — Phiên bản được báo: `0.9.0`; một người dùng khác xác nhận gặp lại trên `1.0.0` (bình luận ngày 2026-10-27).

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Bảng khóa chính (PK/KV)** | Bảng Fluss lưu trạng thái mới nhất của mỗi khóa (RocksDB). Mỗi lần ghi, TabletServer còn sinh ra một **changelog** (WAL) mô tả "dòng nào đổi thành gì" để Flink đọc lại. |
| **Changelog / WAL** | Dãy các dòng thay đổi, mỗi dòng gắn một **loại thay đổi** (change type). |
| **Change type** | `+I` insert, `-U` update-before (giá trị cũ), `+U` update-after (giá trị mới), `-D` delete, `+A` append-only. Mỗi loại được lưu bằng **1 byte** (0..4). |
| **Change type vector** | Một mảng byte: byte thứ *i* là loại thay đổi của dòng thứ *i* trong batch changelog. Đây là thứ `ChangeTypeVectorWriter` ghi vào. |
| **`ChangeTypeVectorWriter`** | Lớp (trong `fluss-common`, package `org.apache.fluss.record`) ghi từng byte change type vào một vùng nhớ liên tục. |
| **Memory segment / page** | Một khối nhớ có kích thước cố định lấy từ memory pool của server (xem [issue4290.md](issue4290.md)). Mặc định `server.buffer.page-size = 128kb` = 131072 byte. |
| **`IndexOutOfBoundsException`** | Ngoại lệ Java khi ghi/đọc vào vị trí nằm ngoài vùng nhớ đã cấp. Ở đây là ghi byte thứ 131073 vào một khối chỉ có 131072 byte. |

## Tóm tắt

Khi TabletServer ghi một batch vào bảng PK, nó dựng một batch changelog định dạng Arrow. Phần "loại thay đổi của từng dòng" được lưu trong **một page duy nhất** (mặc định 128 KB), nên một batch changelog chứa nhiều hơn khoảng 131.000 dòng sẽ tràn page đó. Lớp `ChangeTypeVectorWriter` không biết xin thêm page, và kiểm tra biên của nó sai một đơn vị, nên lỗi hiện ra là `IndexOutOfBoundsException` từ tầng bộ nhớ thay vì thông báo "vector đầy" có chủ đích. Batch bị server từ chối, Flink sink báo lỗi.

Dòng lỗi trong issue:

```text
UnknownServerException: java.lang.IndexOutOfBoundsException: capacity: 131072, index: 131072, put length: 1
  at MemorySegment.put(MemorySegment.java:325)
  at ChangeTypeVectorWriter.writeChangeType(ChangeTypeVectorWriter.java:44)
  at MemoryLogRecordsArrowBuilder.append(MemoryLogRecordsArrowBuilder.java:185)
  at ArrowWalBuilder.append(ArrowWalBuilder.java:48)
  at KvTablet.applyUpdate(KvTablet.java:624)
```

Đọc dòng đầu: "capacity: 131072" là kích thước page, "index: 131072" là vị trí định ghi byte tiếp theo, tức đã chạm cuối page; "put length: 1" là ghi 1 byte.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common` (lớp ghi) được gọi từ `fluss-server` (đường ghi KV). Lỗi xảy ra **trên TabletServer**, không phải trên client; client chỉ nhận lại `UnknownServerException` bọc ngoại lệ gốc.
- **Chuỗi gọi** (theo stack trace trong issue): `TabletService.putKv` → `ReplicaManager.putRecordsToKv` → `Replica.putRecordsToLeader` → `KvTablet.putAsLeader` → `processKvRecords` → `applyUpdate` → `ArrowWalBuilder.append` → `MemoryLogRecordsArrowBuilder.append` → `ChangeTypeVectorWriter.writeChangeType`.
- **Hai khuyết điểm trong `ChangeTypeVectorWriter`** (đã đọc mã tại tag `v1.0.0`, vẫn còn nguyên):

```java
public ChangeTypeVectorWriter(MemorySegment segment, int startPosition) {
    ...
    this.capacity = segment.size() - startPosition;   // chỉ MỘT segment
}

public void writeChangeType(ChangeType changeType) {
    if (recordsCount > capacity) {
        // TODO: support AbstractPagedOutputView to have extendable capacity
        throw new IllegalStateException("The change type vector is full.");
    }
    segment.put(startPosition + recordsCount, changeType.toByteValue());
    recordsCount++;
}
```

  1. **Dung lượng cố định.** Writer chỉ giữ một `MemorySegment`. Dung lượng bằng kích thước page trừ phần header của batch, và có comment `TODO` ngay trong mã thừa nhận chưa hỗ trợ mở rộng.
  2. **Kiểm tra biên lệch một.** Điều kiện là `recordsCount > capacity`, trong khi vị trí hợp lệ cuối cùng là `capacity - 1`. Khi `recordsCount == capacity` kiểm tra vẫn qua, rồi `segment.put` ghi đúng vào vị trí `segment.size()` và ném `IndexOutOfBoundsException`. Vì vậy người vận hành không bao giờ thấy thông báo "The change type vector is full." mà tác giả định hiển thị.
- **Nơi writer này được tạo:** `MemoryLogRecordsArrowBuilder` lấy `firstSegment = pagedOutputView.getCurrentSegment()` rồi `new ChangeTypeVectorWriter(firstSegment, headerSize)`. Các dòng dữ liệu Arrow thì ghi vào các page sau và mở rộng được; chỉ vector change type bị khóa trong page đầu.
- **Hướng sửa** (PR [#3004](https://github.com/apache/fluss/pull/3004), chưa merge): cho `ChangeTypeVectorWriter` hỗ trợ nhiều page, và sửa kiểm tra biên thành `>=`. Người báo lỗi trên 1.0.0 cho biết PR này vẫn áp dụng sạch trên `main` và test tái hiện của họ qua được khi áp dụng.

## 2. Khi nào lỗi xảy ra

**Điều kiện cốt lõi:** số **dòng changelog** trong một batch ghi vượt khoảng `server.buffer.page-size − header` (mặc định khoảng 131.000 dòng). Cần chú ý đây là số dòng **changelog**, không phải số bản ghi client gửi.

Số dòng changelog có thể gấp đôi số bản ghi client, theo mô tả của người dùng trong issue:

| Thao tác của client | Dòng changelog sinh ra (changelog image mặc định `FULL`) |
|---|---|
| Insert khóa mới | 1 (`+I`) |
| Upsert/partial update lên dòng **đã có** | 2 (`-U` và `+U`) |
| Partial delete, giữ lại các cột khác | 2 (`-U` và `+U`) |

Người dùng cho biết: ~65.000 partial delete trên dòng đã tồn tại là đủ chạm ngưỡng 131.072 dòng changelog.

**Điều kiện thứ hai:** client gom đủ nhiều bản ghi vào **một** batch. `client.writer.batch-size` mặc định `2mb`. Bản ghi **delete chỉ mang khóa** nên rất nhỏ, một batch 2 MB chứa được rất nhiều delete, nhiều hơn mức page 128 KB chịu được. Ngược lại, upsert mang cả dòng nên một batch 2 MB chứa ít bản ghi hơn và hiếm khi chạm ngưỡng. Đây là lý do lỗi hay gặp nhất với **xóa/cập nhật từng phần hàng loạt** và với bản ghi rất nhỏ.

Ví dụ trong issue: Flink `FlussSink` với partial update, khoảng 1 triệu dòng, parallelism 1; lỗi xuất hiện sau khoảng 66.000 dòng.

**Phiên bản:** báo trên `0.9.0`, tái hiện trên `1.0.0` và `main` (commit `8a92b4f`). Mình đã đọc mã `v1.0.0` và xác nhận hai khuyết điểm trên còn nguyên.

**Mình chưa kiểm tra:**
- Cách tính chính xác `headerSize` (nên con số 131.000 là xấp xỉ; con số chính xác là `131072 − headerSize`).
- Chế độ `changelog image = WAL` có sinh số dòng khác không (issue chỉ nói về `FULL`).
- Log format `INDEXED` (`IndexWalBuilder`) có bị không: lỗi trong issue nằm ở nhánh Arrow.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người chạy Flink job ghi vào bảng PK có **update/delete từng phần** với số lượng lớn trong thời gian ngắn (backfill, dọn dữ liệu, xóa cột thuộc tính hàng loạt, job replay sau sự cố) hoặc bản ghi rất nhỏ.

**Hậu quả:**

1. **Ghi thất bại, job Flink lỗi.** Batch bị server từ chối bằng `UnknownServerException`; `FlussSink` báo "One or more Fluss Writer send requests have encountered exception" khi kiểm tra lỗi bất đồng bộ, làm task thất bại.
2. **Có khả năng lặp lại.** Sau khi Flink restart từ checkpoint, nó gửi lại cùng dữ liệu; nếu gom batch giống như cũ thì vẫn vượt ngưỡng và lỗi lại. (Đây là suy luận của mình, issue không mô tả vòng lặp này. Trong `relate_issue.md` mình ghi "failover lặp lại" dựa trên suy luận đó.)
3. **Không có sai lệch dữ liệu âm thầm được ghi nhận.** Lỗi là rõ ràng (ngoại lệ), không phải dữ liệu sai. Mình chưa kiểm tra liệu phần KV của batch đã được áp dụng một phần trước khi exception ném ra hay không; mã `KvTablet.java:128` có comment nói rằng việc kiểm tra "keep its KV mutations atomic", nhưng mình chưa đọc nó đủ để khẳng định cách batch lỗi được rollback.
4. **Thông báo lỗi gây hiểu nhầm.** `IndexOutOfBoundsException: capacity: 131072, index: 131072` trông giống lỗi bộ nhớ nội bộ, không chỉ ra rằng nguyên nhân là batch quá nhiều dòng.

**Với CDP:** bảng profile cập nhật từng phần và job xóa/sửa hàng loạt thuộc đúng nhóm dễ gặp. Luồng cập nhật thường ngày với batch nhỏ thường không chạm ngưỡng.

**Cách giảm thiểu (từ người dùng trong issue):** giảm `client.writer.batch-size` (họ dùng `256kb`, giữ batch dưới khoảng 44.000 delete ngay cả khi khóa chỉ 1 byte). Cách tính an toàn: số dòng changelog tối đa mỗi batch ≈ `(batch-size ÷ kích thước bản ghi nhỏ nhất) × 2` phải thấp hơn khoảng 131.000. Cần load test với dữ liệu thật vì kích thước bản ghi khác nhau. Giải pháp triệt để là merge [#3004](https://github.com/apache/fluss/pull/3004) (hoặc tự cherry-pick vào bản build).
