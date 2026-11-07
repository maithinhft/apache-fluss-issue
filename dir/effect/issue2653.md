# Issue #2653 — Đặt `table.log.ttl = -1` (giữ log vô hạn) bị từ chối

Link: https://github.com/apache/fluss/issues/2653 — Phiên bản được báo: `main` thời khoảng 0.9 (người báo: bakjos, 09/02/2026). Issue ngắn: tài liệu thời đó nói có thể dùng `-1` cho `table.log.ttl`, nhưng đặt giá trị này thì nhận `InvalidConfigException: Invalid value for config 'table.log.ttl'...`. Issue trỏ tới PR [#2621](https://github.com/apache/fluss/pull/2621), hiện **đang mở**. Không có bình luận. Trạng thái: còn mở.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0`:
> - `-1` **vẫn bị từ chối**, vì trình phân tích thời lượng không nhận dấu `-`.
> - **`0ms`** là cách "giữ vô hạn" mà tài liệu cấu hình của 1.0.0 ghi rõ, và mã xóa log (cả local lẫn remote) đều coi `≤ 0` là **không xóa theo thời gian**.
>
> Mình chưa chạy thử tạo bảng.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`table.log.ttl`** | Thời gian giữ log của bảng. Segment log cũ hơn TTL sẽ bị xóa. **Mặc định 7 ngày.** Khi bật remote log tiering, giá trị này quy định thời gian giữ **segment trên remote**. |
| **`table.log.local-ttl`** | Thời gian giữ segment trên **đĩa local** khi đã có remote tiering. Nếu không đặt, Fluss dùng `table.log.ttl`. |
| **Segment** | Log chia thành nhiều file (segment). Việc xóa diễn ra theo từng segment, dựa trên timestamp lớn nhất trong segment. |
| **Duration** | Kiểu cấu hình thời lượng, ví dụ `7 d`, `24 h`, `0ms`. Fluss tự viết bộ phân tích (`TimeUtils.parseDuration`). |
| **TTL vô hạn** | Không bao giờ xóa log vì quá hạn. Log chỉ bị xóa theo các quy tắc khác (ví dụ đã tier lên lake, hoặc giữ số segment local). |

## Tóm tắt

Tài liệu thời 0.9 nói đặt `-1` để giữ log vô hạn, nhưng `-1` không phân tích được thành Duration, nên bị `InvalidConfigException`.

Trong `1.0.0`:
- `-1` **vẫn bị từ chối** với cùng lý do.
- Mô tả của `table.log.ttl` trong 1.0.0 nói rõ: *"Setting the value to '0ms' disables TTL-based deletion."* Mã xóa log local (`LogTablet.isSegmentExpired`) và remote (`RemoteLogTablet.expiredRemoteLogSegments`) đều có `if (ttlMs <= 0) → không coi là hết hạn`.

Nghĩa là "giữ vô hạn" **đã được hỗ trợ**, chỉ là bằng `0ms` chứ không phải `-1`. Issue và PR #2621 (cho phép `-1`) vẫn mở.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-common`: `utils/TimeUtils.parseDuration`, `config/ConfigOptions` (mô tả `TABLE_LOG_TTL`);
  - `fluss-server`: `log/LogTablet`, `log/remote/RemoteLogTablet`, `utils/TableDescriptorValidation`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `TimeUtils.parseDuration(text)` chỉ đọc **chữ số** ở đầu chuỗi (`while (... current >= '0' && current <= '9')`). Nếu không có chữ số đầu thì `throw new NumberFormatException("text does not start with a number")`. `-1` bắt đầu bằng `-` nên bị từ chối.
  - `ConfigOptions.TABLE_LOG_TTL`: `defaultValue(Duration.ofDays(7))`, mô tả: *"... When remote log tiering is enabled, this value controls the retention of remote log segments. Setting the value to '0ms' disables TTL-based deletion. The default value is 7 days."*
  - `LogTablet.isSegmentExpired(now, segment, ttlMs)`: `if (ttlMs <= 0L) return false;`. TTL local hiệu dụng là `table.log.local-ttl` nếu có remote log, ngược lại là `table.log.ttl`.
  - `RemoteLogTablet.expiredRemoteLogSegments(currentTimeMs, lakeLogEndOffset, ttlMs)`: `if (ttlMs <= 0) { ... }`, tức không xóa theo thời gian.
  - `TableDescriptorValidation.checkTieredLog`: so sánh `local-ttl ≤ ttl` **bỏ qua** trường hợp bằng 0 hoặc âm, nên kết hợp `table.log.ttl = 0ms` với `local-ttl` hữu hạn là hợp lệ.

## 2. Khi nào lỗi xảy ra

Khi đặt `'table.log.ttl' = '-1'` (theo tài liệu hoặc hướng dẫn cũ, hoặc thói quen từ Kafka `retention.ms = -1`).

**Phiên bản:** báo trên `main` thời 0.9. `-1` vẫn bị từ chối ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

- **Tạo hoặc sửa bảng thất bại** ngay lúc cấu hình. Lỗi lộ ra ngay, **không có rủi ro ngầm**.
- Không ảnh hưởng dữ liệu.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #2653 trong `relate_issue.md` ghi: *"`table.log.ttl = -1` bị từ chối | main | ❌ PR #2621 chưa merge | Đặt TTL rất lớn thay thế"*. Đối chiếu:
- **Nên là 🟡**: `-1` vẫn bị từ chối, nhưng 1.0.0 có cách chính thức là **`'0ms'`**, được ghi trong mô tả cấu hình và được mã xóa log xử lý đúng ở cả local lẫn remote.
- **"Đặt TTL rất lớn"** cũng được, nhưng `'0ms'` rõ ràng hơn.

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Chỉ liên quan nếu cần giữ log hoặc changelog vô hạn, ví dụ để có thể replay toàn bộ lịch sử thay đổi profile.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Muốn giữ vô hạn: đặt **`'table.log.ttl' = '0ms'`**.
- **Cẩn thận dung lượng:** giữ vô hạn thì log chỉ tăng.
  - Nên bật **remote log tiering**, và đặt `table.log.local-ttl` hữu hạn (ví dụ vài giờ đến 1 ngày), để đĩa local không đầy.
  - Alert dung lượng đĩa. Lưu ý 1.0.0 từ chối ghi khi đĩa đạt `server.data-disk.write-limit-ratio`, mặc định 85% (xem #3224).
- Khi lên 1.1: `log.retention.roll-active-segment.enabled` đổi mặc định thành `true` (theo upgrade notes trên `main`). Kiểm tra lại các bảng có TTL hữu hạn, vì log hết hạn có thể bị dọn sớm hơn.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy thử tạo bảng với `0ms` và `-1` trên 1.0.0.
- Mình chưa kiểm tra tài liệu website của 1.0.0 còn nhắc tới `-1` ở đâu hay không.
- Mình chưa kiểm tra tác động của `0ms` khi bảng bật lakehouse (các quy tắc dọn log theo tiến độ lake).
