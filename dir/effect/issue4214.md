# Issue #4214 — Gateway REST không nhận giá trị DECIMAL viết dạng số mũ (`1e2`, `1234e-2`)

Link: https://github.com/apache/fluss/issues/4214 — Phiên bản được báo: `main` (người báo: naivedogger, 03/09/2026). Issue nêu:
- quy định của FIP-49: JSON cho `DECIMAL(p, s)` nhận **chuỗi hoặc số**;
- các ví dụ hợp lệ bị từ chối;
- yêu cầu: không đi qua `f64`, và vẫn từ chối khi vượt precision hoặc scale;
- cách sửa và danh sách test.

Không có bình luận. Trạng thái: còn mở. PR [#4215](https://github.com/apache/fluss/pull/4215) **đang mở**.
**Nhóm B:** chỉ khi **ghi** dữ liệu qua **Fluss Gateway (REST/JSON)**.

> **Mức chắc chắn:** cao. Mình đã đọc bộ giải mã của `fluss-gateway` (viết bằng Rust) trong `v1.0.0`: hàm giải mã DECIMAL **cố ý từ chối** ký hiệu số mũ. Trong khi đó hàm giải mã **số nguyên** ngay cạnh lại **chấp nhận** ký hiệu số mũ, tức hai kiểu số xử lý không nhất quán. Mình chưa chạy thử Gateway.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Fluss Gateway** | Dịch vụ REST của Fluss (thư mục `fluss-gateway`, viết bằng **Rust**, có `openapi.yaml`). Ứng dụng đọc ghi bằng JSON qua HTTP thay vì Java client. Có image Docker riêng (`docker/fluss-gateway`). |
| **FIP-49** | Đề xuất thiết kế Fluss quy định định dạng JSON của Gateway. Theo issue, `DECIMAL` nhận chuỗi hoặc số. |
| **Ký hiệu số mũ** | `1e2` = 100, `1234e-2` = 12,34, `1.234e1` = 12,34. Nhiều thư viện JSON (JavaScript, Python, Jackson với một số cấu hình) **tự sinh** dạng này cho số rất lớn hoặc rất nhỏ. |
| **`DECIMAL(p, s)`** | Số thập phân chính xác: `p` chữ số, trong đó `s` chữ số sau dấu phẩy. |
| **Unscaled value** | Số nguyên biểu diễn DECIMAL. Ví dụ `12.34` với `s = 2` thành `1234`. |
| **`f64`** | Số thực dấu phẩy động 64-bit. Đi qua `f64` sẽ **mất độ chính xác** với số tiền, nên không được dùng. |

## Tóm tắt

Bộ giải mã dòng của Gateway chỉ nhận DECIMAL dạng **thập phân thường** (`12.34`, `"100.00"`). Các giá trị **hợp lệ và biểu diễn được chính xác** như `1e2`, `"1e2"`, `1234e-2`, `"1.234e1"` cho cột `DECIMAL(5,2)` bị từ chối với lỗi invalid-argument. Đề xuất sửa: tự phân tích dấu, phần định trị và phần mũ từ chuỗi gốc, rồi tính unscaled value bằng số học có kiểm tra tràn, không qua `f64`.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-gateway` (Rust), file `src/protocol/rest/codec/decoder.rs`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `decode_decimal(path, value, precision, scale)`: nhận `ScalarValue::String(text)` hoặc `ScalarValue::ExactNumber(lexeme)` (chuỗi gốc của số JSON), rồi `decimal_to_unscaled(text, precision, scale)`.
  - `decimal_to_unscaled`:
    ```rust
    let (int_part, frac_part) = match unsigned.split_once('.') { ... };
    if !int_part.chars().all(|c| c.is_ascii_digit()) || !frac_part.chars().all(|c| c.is_ascii_digit()) {
        return Err("not a plain base-10 decimal (exponents are not accepted)".to_string());
    }
    ```
    Chữ `e` hoặc `E` không phải chữ số, nên bị từ chối. Thông báo lỗi nói rõ đây là **cố ý**.
  - Phần thập phân có số 0 thừa ở cuối vẫn được chấp nhận (`trim_end_matches('0')` trước khi so với scale). Ví dụ `12.3400` cho `DECIMAL(5,2)` vẫn hợp lệ.
  - Ngược lại, `integer_lexeme(lexeme)` (dùng cho kiểu số nguyên) có comment *"Zero fractional parts and exponent notation are accepted only when the exact result is integral"*, và tách `split_once(['e', 'E'])`. Tức kiểu số nguyên **có** hỗ trợ số mũ.

## 2. Khi nào lỗi xảy ra

- Ứng dụng **ghi** qua Gateway REST vào bảng có cột `DECIMAL`.
- Giá trị gửi lên ở **dạng số mũ**, có thể do thư viện JSON phía client tự sinh (ví dụ `JSON.stringify(1e21)` trong JavaScript ra `"1e+21"`, Python `json.dumps(1e-7)` ra `1e-07`).

**Phiên bản:** báo trên `main`. Gateway trong `v1.0.0` có cùng mã.

## 3. Tác động đến ai, làm sai lệch gì

- **Request ghi bị từ chối** (lỗi invalid-argument). Lỗi rõ ràng, **không ghi sai ngầm**.
- Lỗi thường **không đều**: chỉ một số giá trị (rất lớn hoặc rất nhỏ) bị serialize ra dạng số mũ, nên lỗi xuất hiện rải rác và khó đoán (suy luận).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4214 trong `relate_issue.md` ghi: *"REST Gateway (nếu AI agent đọc qua REST) | DECIMAL không nhận ký hiệu số mũ | 🟢 | ❌"*. Đối chiếu:
- **Đúng** về trạng thái.
- **Chính xác hơn:** lỗi ở đường **ghi** (giải mã JSON đầu vào), không phải đường **đọc**. AI agent chỉ **đọc** qua REST thì không gặp lỗi này. Mình chưa kiểm tra bộ **mã hóa** (encoder) khi trả DECIMAL ra JSON.

## 5. Mức ảnh hưởng với CDP

- Chỉ liên quan nếu có service **ghi** vào Fluss qua REST Gateway (ví dụ AI agent ghi ghi chú hoặc context, hoặc service không viết bằng Java).

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Luôn gửi DECIMAL dưới dạng chuỗi thập phân thường** đã format sẵn ở client. Ví dụ Python `format(Decimal(x), 'f')`, Java `BigDecimal.toPlainString()`, JavaScript dùng thư viện decimal thay vì `Number`.
- Không gửi DECIMAL dưới dạng số JSON (`12.34` không có ngoặc kép) nếu thư viện có thể tự đổi sang dạng số mũ.
- Viết test với các giá trị biên (rất lớn, rất nhỏ) cho đường ghi qua Gateway.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy Gateway để tái hiện.
- Mình chưa kiểm tra bộ encoder của Gateway khi **đọc** DECIMAL ra JSON (định dạng xuất ra).
- Mình chưa kiểm tra các kiểu số khác (FLOAT, DOUBLE) trên Gateway có nhận số mũ hay không.
