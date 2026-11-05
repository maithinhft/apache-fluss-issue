# Issue #2943 — TabletServer không khởi động được: `NoSuchElementException` khi khôi phục một segment log không có batch

Link: https://github.com/apache/fluss/issues/2943 — Phiên bản được báo: `0.9.0` (jar `0.9-SNAPSHOT`, log ngày 2026-03-27). Không có bình luận, không có PR sửa. Người báo nhận làm PR.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Log của bucket** | Mỗi bucket lưu dữ liệu thành một chuỗi **segment**, mỗi segment là một file trên đĩa (kèm file chỉ mục). Dữ liệu mới được ghi vào segment cuối (active segment). |
| **Batch** | Đơn vị ghi trong segment: một nhóm bản ghi liền nhau, có header, offset đầu/cuối và CRC. |
| **Offset** | Số thứ tự của bản ghi trong log. `baseOffset` của segment là offset của bản ghi đầu tiên segment đó chứa. |
| **Log recovery (khôi phục log)** | Khi TabletServer khởi động, nó duyệt lại các segment để kiểm tra tính hợp lệ, cắt phần dở dang ở cuối, và tính **offset tiếp theo** sẽ ghi. |
| **Tắt sạch / tắt không sạch** | Tắt sạch (clean shutdown) ghi một marker nên lần sau có thể bỏ qua kiểm tra; crash hoặc mất điện thì không có marker nên phải khôi phục. |
| **`readNextOffset()`** | Hàm của `LogSegment` tính offset tiếp theo của segment bằng cách đọc batch cuối cùng. |
| **`Iterables.getLast`** | Hàm Guava lấy phần tử cuối của một tập; **ném `NoSuchElementException` nếu tập rỗng**. |

## Tóm tắt

Khi TabletServer khôi phục log lúc khởi động, hàm `LogSegment.readNextOffset()` lấy batch cuối bằng `Iterables.getLast(batches)` mà không kiểm tra xem có batch nào không. Nếu `batches` rỗng, hàm ném `NoSuchElementException`. Ngoại lệ này không được xử lý, được bọc thành `FlussRuntimeException: Failed to recovery log`, và **TabletServer không khởi động được**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, phần log: `LogSegment`, `LogLoader`, `LogManager`.
- **Đoạn mã lỗi** (đã đọc tại `v1.0.0`, `LogSegment.java` khoảng dòng 418–436, và **cũng còn nguyên trên nhánh `main`**):

```java
public long readNextOffset() throws IOException {
    FetchDataInfo fetchData = read(offsetIndex().lastOffset(), ...);
    if (fetchData == null) {
        return baseOffset;
    } else {
        Iterable<LogRecordBatch> batches = fetchData.getRecords().batches();
        LogRecordBatch lastBatch = Iterables.getLast(batches);   // ném nếu batches rỗng
        if (lastBatch != null) {                                  // kiểm tra này vô tác dụng
            return lastBatch.nextLogOffset();
        } else {
            return baseOffset;
        }
    }
}
```

  Có một nhánh `else { return baseOffset; }` cho trường hợp "không có batch", nhưng nó **không bao giờ chạy được**: `Iterables.getLast` ném ngoại lệ chứ không trả `null` khi rỗng. Giải pháp trong issue: kiểm tra `Iterables.isEmpty(batches)` và trả `baseOffset`.
- **Chuỗi gọi trong stack trace:** `TabletServer.startServices` → `LogManager.startup/loadLogs` → `LogManager.loadLog` → `LogTablet.create` → `LogLoader.load` → `LogLoader.recoverLog` (dòng 198 của bản 0.9) → `LogSegment.readNextOffset` → `Iterables.getLast` → `NoSuchElementException`.
- **Hai chỗ `readNextOffset()` được gọi trong `LogLoader.recoverLog()` (đã đọc `v1.0.0`):**
  1. Trong log cảnh báo khi phát hiện hỏng dữ liệu (`truncatedBytes > 0`), sau khi `recoverSegment` đã cắt bớt.
  2. **Sau cùng, luôn luôn:** `long logEndOffset = logSegments.lastSegment().get().readNextOffset();` kể cả khi tắt sạch.
- **Vì sao ngoại lệ làm server không lên:** `LogManager.loadLog` chỉ bỏ qua và dọn dẹp khi gặp `SchemaNotExistException` hoặc `PartitionNotExistException`; mọi ngoại lệ khác bị bọc thành `FlussRuntimeException` và làm nạp log thất bại, dẫn tới `Failed to start the TabletServer`.

## 2. Khi nào lỗi xảy ra

- **Điều kiện kích hoạt trong mã:** `readNextOffset()` được gọi cho một segment mà phép đọc trả về dữ liệu **không có batch nào**.
- **Điều mình chưa xác định được:** trạng thái đĩa cụ thể tạo ra điều kiện đó. Issue không mô tả segment chứa gì hay vì sao rỗng. Mình cũng không nên giả định "segment rỗng nào cũng gây lỗi": nếu vậy, bất kỳ bucket nhàn rỗi nào có active segment rỗng cũng làm server lỗi khi restart, trong khi thực tế không thế. Nên có một trạng thái cụ thể hơn khiến `read(...)` trả về dữ liệu khác `null` nhưng không có batch. Mình chưa dò nhánh `translateOffset` để biết đó là trạng thái nào.
- **Khả năng thường gặp (suy luận, chưa xác nhận):** crash hoặc mất điện giữa lúc tạo/ghi segment, file có nhưng chưa có batch hợp lệ; hoặc đã bị `recover()` cắt hết phần không hợp lệ về 0 byte. Hàm `LogSegment.recover()` ở `v1.0.0` đúng là cắt phần hỏng ở cuối về `validBytes` (có thể bằng 0).
- **Phiên bản:** báo trên `0.9.0`. Mình đã đọc mã `v1.0.0` và `main`, hai nơi đều giữ nguyên đoạn lỗi trên, nên lỗi còn trong `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành TabletServer, ở lần khởi động lại sau sự cố làm hỏng hoặc để dở một segment (crash, mất điện, kill cứng, đầy đĩa giữa lúc ghi).

**Hậu quả:**
1. **TabletServer không khởi động được.** Lỗi lặp lại ở mỗi lần thử khởi động cho đến khi can thiệp thủ công (ví dụ xử lý/xóa segment có vấn đề trong thư mục dữ liệu của bucket). Cách can thiệp cụ thể mình **chưa kiểm chứng**; issue không nêu.
2. **Mất khả dụng các bucket trên node đó.** Nếu bucket còn replica khác thì leader chuyển đi, còn nếu node là bản duy nhất thì bucket không phục vụ được cho đến khi node lên.
3. **Có thể chồng chéo với sự cố khác:** ví dụ khi đĩa đầy (xem #3224), crash để lại segment dở rồi restart lỗi vì #2943.
4. **Không có dữ liệu sai âm thầm:** đây là lỗi dừng khởi động rõ ràng, không phải trả dữ liệu sai.

**Mức ảnh hưởng với CDP:** TabletServer không lên được sau sự cố là kịch bản sẵn sàng/phục hồi quan trọng; tần suất thấp nhưng khi xảy ra thì cần can thiệp tay.

**Biện pháp giảm thiểu:**
- Replication factor ≥ 3 để có replica khác tiếp quản khi một node không lên.
- Chuẩn bị runbook cho kịch bản "TabletServer không khởi động, log có `NoSuchElementException` ở `LogSegment.readNextOffset`": xác định bucket/segment gây lỗi từ log, cách xử lý cần được kiểm tra trên môi trường thử trước khi dùng thật. Khi có dữ liệu ở replica khác, mục tiêu thường là để bucket đó được khôi phục từ replica còn lại.
- Diễn tập kịch bản kill cứng node trên môi trường thử.
- Theo dõi PR sửa (đề xuất của issue: kiểm tra rỗng và trả `baseOffset`), hoặc tự áp dụng patch nhỏ này nếu build riêng, sau khi kiểm thử.
