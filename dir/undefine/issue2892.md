# Issue #2892 — Luồng gửi dữ liệu (Sender) của writer báo lỗi `Accounted size went negative` từ bộ nhớ Arrow

Link: https://github.com/apache/fluss/issues/2892 — Phiên bản được báo: `0.9.0` (log ngày 2026-03-16). **Không có bình luận, không có PR sửa, người báo không nhận làm PR, chưa có cách tái hiện.**

> **Giới hạn quan trọng:** issue chỉ gồm một stack trace và câu *"The error surfaces during server upgrade (though the exact correlation is unclear)"*. Không ai xác định nguyên nhân gốc. Phần "nguyên nhân" trong file này là **giả thuyết của mình** dựa trên việc đọc mã `v1.0.0`; mình đánh dấu rõ chỗ nào là bằng chứng trong mã, chỗ nào là suy luận. Cụm "Arrow double-release" trong `relate_issue.md` cũng là giả thuyết của mình, issue không nói vậy.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Writer / Sender thread** | Khi ứng dụng ghi vào Fluss, writer của client gom bản ghi vào các batch trong `RecordAccumulator`. Một thread riêng tên `fluss-write-sender-thread-N` (lớp `Sender`) lấy (drain) các batch ra, đóng gói rồi gửi lên server. |
| **Arrow** | Định dạng cột (Apache Arrow) mà Fluss dùng để đóng gói batch bản ghi. Bộ nhớ của nó được quản lý bằng một **allocator** (bộ cấp phát) có sổ sách đếm số byte đang cấp. |
| **Accountant / "accounted size"** | Phần sổ sách trong allocator của Arrow: cộng khi cấp bộ nhớ, trừ khi trả. |
| **Release (trả bộ nhớ)** | Giải phóng một vùng nhớ về allocator để trừ khỏi sổ sách. |
| **`Accounted size went negative`** | Allocator bị yêu cầu trả **nhiều byte hơn** số nó đã ghi nhận đang cấp, tức sổ sách âm. Dấu hiệu thường gặp là một vùng nhớ bị trả **hai lần**, hoặc bị trả qua allocator khác với allocator đã cấp. |
| **Double-release** | Một vùng nhớ bị trả hai lần. |
| **Rolling upgrade** | Nâng cấp cluster từng node (xem [issue4256.md](issue4256.md)). |
| **Idempotence (ghi idempotent)** | Cơ chế gán số thứ tự (writer id + batch sequence) cho mỗi batch để server loại bỏ batch gửi trùng. |

## Tóm tắt

Trong lúc Sender thread đóng một batch Arrow để gửi, việc trả bộ nhớ của Arrow ném `IllegalArgumentException: Accounted size went negative`. Ngoại lệ xuất hiện ở bước **đóng gói batch trước khi gửi**, tức phía **client** (không phải server), và người báo thấy nó khi đang nâng cấp server nhưng không chắc có liên quan.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client` (luồng ghi) cộng với `fluss-common` (đóng gói Arrow), nằm trên thư viện Arrow đã shade.
- **Chuỗi gọi trong stack trace:** `Sender.run` → `Sender.runOnce` → `Sender.sendWriteData` → `RecordAccumulator.drain` → `drainBatchesForOneNode` → `ArrowLogWriteBatch.close` → `MemoryLogRecordsArrowBuilder.close/build` → `ArrowWriter.serializeToOutputView` → `ArrowRecordBatch.close` → `BufferLedger.release` → `Accountant.releaseBytes` → ném lỗi.
- **Điều đã đọc trong mã `v1.0.0`:**
  1. `serializeToOutputView` tạo `ArrowRecordBatch` bằng `try-with-resources` từ `VectorUnloader`, nên `ArrowRecordBatch.close()` được gọi tự động ở cuối và trả các buffer mà batch giữ. Chính lệnh trả này ném lỗi.
  2. Trong `drainBatchesForOneNode`: batch được lấy ra khỏi hàng đợi bằng `deque.pollFirst()`, nếu bật idempotence thì batch được gán số thứ tự và đăng ký in-flight, **rồi sau đó** mới `batch.close()` bên ngoài khóa. Không có `try/catch` quanh `batch.close()`.
  3. `Sender.run` có hai vòng lặp bắt `Exception` và chỉ ghi log. `IllegalArgumentException` là `Exception` nên **không làm thread chết**; thread ghi log rồi chạy tiếp. (Issue dùng chữ "crashes", mình đọc mã `v1.0.0` thì thấy thread không dừng; chưa kiểm tra mã `0.9.0`.)
  4. Dòng log trong issue là *"Uncaught error in Fluss write sender I/O thread"*. Ở `v1.0.0`, chuỗi chính xác này nằm ở **vòng lặp chờ xả hết batch khi đóng** (`while (!forceClose && accumulator.hasUnDrained())`), còn vòng lặp chính dùng *"... sender thread"* không có chữ "I/O". Nếu mã `0.9.0` giống vậy thì lỗi xảy ra trong lúc client đang đóng (ví dụ Flink task bị dừng khi failover), nhưng mình chưa xác nhận mã `0.9.0`.

- **Nguyên nhân gốc (giả thuyết, chưa chứng minh):** một vùng nhớ Arrow bị trả hai lần hoặc trả khi writer/allocator đã bị đóng/tái chế bởi luồng khác (ví dụ lúc client đóng, hủy batch, hoặc tái chế `ArrowWriter` cùng lúc Sender đang serialize). Mình chưa tìm được đường gọi cụ thể nào trong mã chứng minh điều này.

## 2. Khi nào lỗi xảy ra

- **Bằng chứng duy nhất:** xuất hiện một lần trong log của người báo khi đang nâng cấp server (rolling upgrade), trên `0.9.0`. Người báo nói rõ mối liên hệ không chắc.
- **Điều kiện chưa biết:** không có bước tái hiện, không rõ tần suất, không rõ có liên quan đến đóng client, hủy batch hay lỗi gửi do server restart hay không.
- **Suy luận có cơ sở từ mã (cần kiểm chứng):** trong rolling upgrade, client gặp lỗi mạng/lỗi server, batch bị hủy hoặc thử lại, client có thể đang đóng; các đường hủy/tái chế này có thể giẫm lên nhau với đường serialize của Sender. Đây là giả thuyết.
- **Phiên bản:** báo trên `0.9.0`; không có báo cáo trên `1.0.0` và mình không biết lỗi có còn không. `ArrowWriter.serializeToOutputView` ở `v1.0.0` vẫn có cấu trúc `try-with-resources` như đã mô tả, nên đường mã vẫn tồn tại.

## 3. Tác động đến ai, làm sai lệch gì

**Ai có thể bị ảnh hưởng:** ứng dụng dùng Fluss Java client để ghi (Flink sink, ứng dụng tự viết), đặc biệt khi đang có rolling upgrade server hoặc khi client đóng lại.

**Hậu quả có căn cứ trong mã `v1.0.0`:**
1. **Batch bị lấy khỏi hàng đợi nhưng không được gửi, cũng không được hủy.** `deque.pollFirst()` xảy ra trước `batch.close()`. Nếu `close()` ném lỗi, ngoại lệ thoát khỏi `drainBatchesForOneNode`, batch này **không** được thêm vào danh sách `ready` và không có mã nào đưa nó trở lại hàng đợi hay hoàn tất future của nó. Danh sách `ready` cục bộ, kèm các batch khác của bucket khác đã gom trong cùng lần drain, cũng bị bỏ đi. Đây là suy luận từ mã, mình chưa chạy thử. Nếu đúng, các `CompletableFuture` của những batch đó không bao giờ hoàn tất, nên việc `flush()` hoặc checkpoint của Flink có thể chờ mãi.
2. **Nếu bật idempotence:** batch đã được gán số thứ tự và đăng ký in-flight **trước** khi `close()`. Batch rơi mất thì số thứ tự đó không bao giờ được gửi; các batch sau của cùng bucket có thể bị server từ chối với `OutOfOrderSequenceException` (cùng nhóm lỗi trong các issue #1163, #709). Cũng là suy luận.
3. **Thread vẫn chạy.** Vì vòng lặp bắt `Exception`, thread tiếp tục. Điều này khác với chữ "crash" trong issue và trong dòng #2892 của `relate_issue.md`.
4. **Chưa rõ có mất dữ liệu hay không:** nếu bản ghi trong batch rơi mất mà không có lỗi báo ra phía ứng dụng, đó là mất dữ liệu; nếu future được hoàn tất lỗi ở đâu đó mà mình chưa thấy thì ứng dụng sẽ biết. Mình chưa kiểm tra `abortBatches`/`maybeAbortBatches` có được gọi cho các batch này không.

**Mức nghiêm trọng thực tế:** thấp tần suất (chỉ một báo cáo, không tái hiện được) nhưng hậu quả tiềm năng không nhỏ nếu giả thuyết ở mục 1 đúng. `relate_issue.md` hiện xếp #2892 ở mức 🟠 Cao với mô tả "Sender I/O thread crash ... Flink job failover"; mô tả này cần chỉnh theo phát hiện trên (thread không chết, nguy cơ là batch bị bỏ rơi hoặc treo `flush`). Mình không tự sửa dòng đó.

**Biện pháp giảm thiểu:**
- Giám sát log client có chuỗi `Accounted size went negative` hoặc `Uncaught error in Fluss write sender`; coi đó là dấu hiệu cần restart job/writer.
- Theo dõi throughput và lag của sink trong lúc rolling upgrade; nâng cấp server ngoài giờ cao điểm.
- Nếu gặp lại, thu thập: bản Fluss client, cấu hình idempotence, và liệu client có đang đóng không, rồi bổ sung vào issue để maintainer có dữ liệu tái hiện.
