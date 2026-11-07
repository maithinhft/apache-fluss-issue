# Issue #5021 — Ba lỗi nhỏ ở Java client: mô tả SASL bị đảo, metric `lastPollSecondsAgo` sai trước lần poll đầu, mã lỗi lạ bị mất

Link: https://github.com/apache/fluss/issues/5021 — Phiên bản được báo: `main` (người báo: ss666, 04/11/2026). Đây là một phần của issue tổng [#4980](https://github.com/apache/fluss/issues/4980) (cải thiện độ bền của Java client). Issue nêu ba lỗi, mỗi lỗi kèm cách sửa. Không có bình luận. Trạng thái: còn mở. PR [#5044](https://github.com/apache/fluss/pull/5044) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã xác nhận **cả ba** lỗi trong mã `v1.0.0`. Không lỗi nào ảnh hưởng tới dữ liệu.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **SASL** | Cơ chế xác thực username và password giữa client và server. Client cấu hình bằng `client.security.sasl.username` và `client.security.sasl.password` (hoặc bằng chuỗi JAAS đầy đủ trong `client.security.sasl.jaas.config`). |
| **`ConfigOptions` / mô tả option** | Danh sách cấu hình của Fluss, mỗi option kèm một đoạn mô tả. Tài liệu trên website được sinh từ đó. |
| **Scanner / `poll()`** | Đối tượng đọc log. Mỗi lần `poll()` lấy về một lô bản ghi. Flink source dùng scanner. |
| **`lastPollSecondsAgo`** | Metric (gauge) của scanner: số giây kể từ lần `poll()` gần nhất. Hay dùng để alert "consumer bị treo". |
| **Mã lỗi (error code)** | Số mà server trả về để chỉ loại lỗi. Client ánh xạ mã sang enum `Errors`. |
| **`UNKNOWN_SERVER_ERROR`** | Giá trị "lỗi server không rõ loại". |

## Tóm tắt

1. **Mô tả bị đảo:** mô tả của `client.security.sasl.username` nói *"The password to use..."*, còn mô tả của `client.security.sasl.password` nói *"The username to use..."*. Tài liệu website (`security/authentication.md`) chép lại phần sai cho `username`.
2. **`lastPollSecondsAgo` trước lần poll đầu:** giá trị tính bằng `(now - lastPollMs) / 1000`, mà `lastPollMs` ban đầu là `0`. Vì vậy trước khi scanner gọi `poll()` lần đầu, metric báo **số giây từ năm 1970** (khoảng 1,8 tỷ), và alert "consumer treo" bắn **mỗi lần scanner khởi động**. Cách sửa: trả `-1` trước lần poll đầu, giống Kafka.
3. **Mã lỗi lạ bị mất:** `Errors.forCode(code)` ánh xạ mã client không biết thành `UNKNOWN_SERVER_ERROR`, chỉ log WARN `"Unexpected error code: {}."`. `ApiError` giữ enum chứ không giữ mã gốc, nên **mã gốc không có trong exception** mà ứng dụng nhận được.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-common`: `config/ConfigOptions`;
  - `fluss-client`: `metrics/ScannerMetricGroup`;
  - `fluss-rpc`: `protocol/Errors`, `ApiError`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - ```java
    key("client.security.sasl.username") ... .withDescription("The password to use for client-side SASL JAAS authentication. ...")
    key("client.security.sasl.password") ... .withDescription("The username to use for client-side SASL JAAS authentication. ...")
    ```
    Riêng câu cuối của mỗi mô tả (*"If not provided, the username/password will be read from ..."*) thì **đúng** với tên option. Chỉ câu đầu bị đảo.
  - `ScannerMetricGroup`:
    - `private volatile long lastPollMs;` (mặc định 0);
    - gauge `lastPollSecondsAgo` gọi `(System.currentTimeMillis() - lastPollMs) / 1000`;
    - `recordPollStart` gán `lastPollMs = pollStartMs`.
  - `Errors.forCode(int code)`: không thấy trong bảng thì `LOG.warn("Unexpected error code: {}.", code); return UNKNOWN_SERVER_ERROR;`.

## 2. Khi nào lỗi xảy ra

1. Khi người cấu hình đọc **mô tả** (trong tài liệu) để biết option nào là username, option nào là password.
2. **Mỗi lần scanner khởi động**, trong khoảng từ lúc metric được đăng ký tới lần `poll()` đầu tiên: Flink source mới start, mới restore, hoặc split mới. Nếu reporter thu metric đúng khoảng này thì thấy giá trị khổng lồ.
3. Khi server trả **mã lỗi mới** mà client chưa biết. Điều này thường xảy ra khi **client cũ hơn server**, ví dụ server đã nâng cấp mà job Flink chưa.

**Phiên bản:** báo trên `main`. Cả ba có trong `v1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Không ảnh hưởng dữ liệu.**

1. **Cấu hình nhầm SASL:** người làm theo mô tả có thể đặt username vào ô password và ngược lại, nên xác thực thất bại. Lỗi lộ ra ngay khi kết nối, dễ phát hiện.
2. **Alert giả:** mỗi lần job đọc khởi động lại, alert "consumer treo" bắn. Lâu dần đội vận hành **quen bỏ qua** alert này, và có thể bỏ lỡ lần treo thật.
3. **Khó chẩn đoán khi lệch phiên bản:** ứng dụng chỉ thấy `UnknownServerException` mà không biết mã thật. Log WARN có ghi mã, nhưng chỉ ở log client.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #5021 trong `relate_issue.md` ghi: *"Metric `lastPollSecondsAgo` báo rất lớn trước lần poll đầu (cảnh báo giả) | main | ❌ | Bỏ qua alert lúc khởi động"*. Đối chiếu:
- **Đúng**, nhưng chỉ nêu một trong ba lỗi. Hai lỗi còn lại (mô tả SASL, mã lỗi) cũng có trong 1.0.0.

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Chủ yếu ảnh hưởng **chất lượng giám sát** cho job đọc.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Rule alert cho `lastPollSecondsAgo`:** bỏ qua giá trị lớn bất thường (ví dụ > 10^8 giây). Hoặc chỉ alert khi giá trị **vượt ngưỡng và đang tăng** trong nhiều lần thu liên tiếp. Hoặc chỉ alert khi job đã chạy quá N phút.
- **Cấu hình SASL:** dựa vào **tên option** (`...username`, `...password`), không dựa vào mô tả.
- **Giữ client và server cùng phiên bản**, hoặc nâng cấp client trước server, để tránh mã lỗi lạ. Khi gặp `UnknownServerException`, tìm log WARN `"Unexpected error code"` phía client để biết mã thật.

## 6. Những điều chưa kiểm chứng

- Mình chưa kiểm tra tài liệu website của 1.0.0 có chép sai mô tả hay không (chỉ dựa vào issue).
- Mình chưa kiểm tra các metric khác của scanner hoặc writer có cùng kiểu "giá trị ban đầu bằng 0" hay không.
