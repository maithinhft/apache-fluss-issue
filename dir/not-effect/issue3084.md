# Issue #3084 — Flink source đọc log table chạy bình thường cả tuần, một bucket kẹt âm thầm, restart thì `OutOfRange` --> không ảnh hưởng lớn do flink job cdp không phải đọc changelog hay log table

Link: https://github.com/apache/fluss/issues/3084 — Phiên bản được báo: `0.9.0`. Có 3 bình luận. PR liên quan: [#1752](https://github.com/apache/fluss/pull/1752) *"[client] Throw Exception when retry several times when downloading log"* (đóng issue #1751), **đã merge vào `main` ngày 2026-09-22**, sau khi tách nhánh `v1.0.0` (2026-09-15), nên **không có trong 1.0.0**. Issue chưa có kết luận chắc chắn về nguyên nhân gốc.

> **Mức chắc chắn:** người báo nói rõ họ **không xác định được nguyên nhân** vì thiếu log của ngày bị kẹt. Giả thuyết nguyên nhân là của một contributor (`loserwang1024`) và được người báo cho là "hợp lý", **chưa được tái hiện hay xác nhận**. Mình đã đọc mã `v1.0.0` của `RemoteLogDownloader` và `RemoteLogDownloadFuture` để kiểm tra phần liên quan, chưa chạy tái hiện.
>
> Khác với [issue3228.md](issue3228.md): ở #3228 job dừng lâu nên offset cũ; ở #3084 job **không dừng**, một bucket bị kẹt, và `OutOfRange` chỉ lộ ra khi restart.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Bucket / partition** | Bảng chia thành partition (ở đây theo ngày, ví dụ `2026-04-07`), mỗi partition chia thành nhiều bucket. Flink source đọc từng bucket như một **split**. |
| **Split & offset trong checkpoint** | Mỗi split nhớ "đã đọc tới offset nào". Khi job restore từ checkpoint, nó đọc tiếp từ offset đó (xem [issue3228.md](issue3228.md)). |
| **Subtask** | Một luồng song song của Flink source. Job trong issue có 240 subtask, mỗi subtask phụ trách vài bucket. |
| **`OutOfRange` / log start offset** | Offset yêu cầu nằm ngoài phạm vi log còn lưu (xem [issue3228.md](issue3228.md)). Trong issue: yêu cầu 1546148 nhưng server chỉ còn từ 2199904 đến 2799328. |
| **Remote log** | Segment log đã được đẩy lên object store (S3/HDFS) rồi xóa khỏi đĩa local TabletServer. Client đọc offset cũ bằng cách **tải file segment remote** về máy mình. |
| **`RemoteLogDownloader`** | Thành phần phía client tải các segment remote về đĩa local để đọc. Có hàng đợi yêu cầu tải và một **semaphore (prefetch permit)** giới hạn số segment được tải trước. |
| **Semaphore / permit** | Bộ đếm giới hạn đồng thời. Muốn tải một segment phải lấy một permit; permit trả lại khi tải lỗi hoặc khi segment đã được đọc xong. |
| **Fail fast** | Khi có lỗi thì báo lỗi ngay thay vì treo âm thầm. |
| **Backfill** | Chạy job để xử lý lại dữ liệu cũ; ở đây là job đọc dữ liệu lịch sử và đã bắt kịp mức mới nhất sau khoảng một tuần. |

## Tóm tắt

Người báo có một job Flink đọc log table, chạy ổn định khoảng một tuần và đã bắt kịp offset mới nhất ở các bucket. Sau khi restart từ checkpoint, một subtask lỗi `FetchException: The fetching offset 1546148 is out of range ... we only have log segments from offset 2199904 up to 2799328` cho bucket 50 của partition `2026-04-07`. So sánh offset trong checkpoint với `endOffset` của cluster cho thấy **chỉ bucket này đứng yên ở 1546148**, các bucket còn lại đã theo kịp (offset bằng `endOffset`). Nghĩa là bucket này đã **kẹt trong lúc job vẫn chạy**, không báo lỗi, và khi restart thì phần log ở offset cũ đã bị xóa.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, đường đọc remote log của scanner (`RemoteLogDownloader`, `RemoteLogDownloadFuture`, `LogFetchBuffer`), cộng với Flink source dùng scanner này.
- **Giả thuyết của contributor trong issue:** nếu việc tải file remote thất bại, lỗi **không được báo lên**, nên fetch treo vô hạn và offset của split không tiến lên. Cụ thể: `RemoteLogDownloadFuture` chỉ có `onComplete`, không có đường truyền lỗi về cho người chờ.
- **Điều mình xác nhận trong mã `v1.0.0`:**
  - `RemoteLogDownloadFuture` chỉ có `isDone`, `getFileLogRecords`, `discard`, `onComplete`; không có đường báo lỗi cho người chờ. Đúng như mô tả.
  - `RemoteLogDownloader` khi tải thất bại (cả nhánh `whenComplete` lẫn `catch Throwable`) chỉ `LOG.error(...)`, trả permit, tăng metric `remoteFetchErrorCount`, rồi **đưa yêu cầu trở lại hàng đợi để thử lại mãi**. Không có giới hạn số lần thử và không ném lỗi về scanner. Nghĩa là nếu một file luôn tải lỗi (hoặc lỗi lặp lại), split đó sẽ **thử lại vô hạn mà không fail** — khớp với hiện tượng "kẹt, không fail fast".
  - Không có timeout riêng cho một lần tải ở `RemoteLogDownloader` mà mình thấy trong đoạn đã đọc.
- **Điều mình chưa xác nhận:** liệu đây thực sự là nguyên nhân trong sự cố của người báo. Họ không có log ngày bị kẹt, nên không loại trừ nguyên nhân khác.
- **Cách PR #1752 sửa** (theo tên PR và danh sách file đã đổi, mình chưa đọc diff): giới hạn số lần thử lại khi tải log, rồi **ném exception** thay vì thử mãi; sửa `RemoteLogDownloader`, `RemoteLogDownloadFuture`, `LogFetchBuffer`, `LogFetcher`, thêm một `ConfigOption`.
- **Liên quan #3085 (đã đóng ngày 2026-04-17):** cùng một người báo ghi nhận một sự cố tương tự, throughput về 0 vì **semaphore permit không lấy được** (deadlock khi tải lỗi, kèm không có log lỗi tải). Có PR sửa (cho `main` và nhánh `release-0.9`, #3136). Mình chưa kiểm tra bản sửa đó có trong `v1.0.0` hay không; mã `v1.0.0` mình đọc đã có `prefetchSemaphore.release()` trong các nhánh lỗi, nhưng mình chưa đối chiếu từng commit.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Bảng bật remote log tiering** và job đọc offset **cũ** nằm trong phần đã đẩy sang remote (ví dụ backfill, hoặc job tụt lại sau).
2. **Việc tải segment remote gặp sự cố** (lỗi đọc S3/HDFS, lỗi mạng, hoặc deadlock semaphore như #3085). Với mã `v1.0.0`, lỗi tải bị thử lại mãi, không báo lỗi.
3. **Job không dừng ngay**: các bucket khác vẫn tiến lên nên job trông "đang chạy", còn bucket kẹt không được ai để ý.
4. Về sau job **restart từ checkpoint** khi log của offset kẹt đã hết hạn (`table.log.ttl`), nên restore thất bại với `OutOfRange` (và cứ thế lặp lại, như #3228).

**Dấu hiệu nhận biết:** offset của một số bucket đứng yên trong khi các bucket khác tăng; metric `remoteFetchErrorCount` tăng; log client có `Failed to download remote log segment file ...` lặp lại (nếu có log). Trong sự cố thật, người báo **không có** log của ngày kẹt nên không biết.

**Phiên bản:** báo trên `0.9.0`. Đoạn thử lại vô hạn còn trong `v1.0.0`; PR #1752 chỉ có trên `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job Flink **đọc log hoặc changelog của Fluss** (ví dụ job tính feature/segment từ bảng Fluss) khi bảng có **remote log**, đặc biệt các job **chạy dài** hoặc **backfill** đọc offset cũ.

**Hậu quả:**
1. **Một phần dữ liệu ngừng chảy mà không có lỗi.** Bucket kẹt không xử lý thêm gì, nhưng job vẫn báo chạy bình thường. Nếu downstream (profile/feature) phụ thuộc đủ dữ liệu của mọi bucket thì kết quả bị **thiếu hoặc lệch** trong thời gian kẹt.
2. **Lỗi lộ muộn:** chỉ đến khi restart mới thấy `OutOfRange`.
3. **Mất dữ liệu chưa đọc:** khi đã `OutOfRange`, đoạn log giữa offset kẹt (1546148) và log start offset (2199904) đã bị xóa, không đọc lại được. Fluss `v1.0.0` không có chính sách tự nhảy qua đoạn mất (xem [issue3228.md](issue3228.md) mục 4).
4. **Khó chẩn đoán:** nguyên nhân gốc nằm ở tầng tải remote log, nhưng triệu chứng thấy ở tầng Flink.

**Mức ảnh hưởng với CDP:** chỉ áp dụng nếu bạn có job Flink đọc log/changelog của Fluss và bật remote log tiering. Với luồng chính của bạn (ghi vào bảng PK, lookup) không ảnh hưởng. Nếu có job đọc changelog để tính toán tiếp, rủi ro "kẹt âm thầm" khá khó chịu vì cần phát hiện bằng giám sát chứ không bằng lỗi.

**Biện pháp giảm thiểu (suy ra từ phân tích, không phải từ issue):**
- **Cảnh báo bucket đứng yên:** so sánh offset đã đọc của từng bucket với `fluss_tabletserver_table_bucket_log_endOffset` (cách người báo tự dùng để phát hiện). Nếu offset không tăng trong N phút trong khi `endOffset` tăng thì báo.
- Theo dõi metric `remoteFetchErrorCount` của scanner và log `Failed to download remote log segment`.
- Đặt `table.log.ttl` đủ dài so với thời gian bạn có thể mất để phát hiện và xử lý một bucket kẹt (để restart vẫn còn dữ liệu).
- Với job đọc changelog quan trọng: hạn chế phụ thuộc vào đọc offset cũ nằm ở remote log; nếu có thể, giữ job luôn gần đầu log.
- Theo dõi bản có #1752 (trên `main`) hoặc cân nhắc cherry-pick; sau khi có bản sửa, tải lỗi sẽ làm job fail rõ ràng thay vì treo.
