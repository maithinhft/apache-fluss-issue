# Issue #1819 — Cùng một giá trị `TIME`, lookup và scan trả kết quả khác nhau (scan mất phần mili giây)

Link: https://github.com/apache/fluss/issues/1819 — Phiên bản được báo: `main` thời khoảng 0.8 (người báo: LiebingYu, 17/10/2025). Issue có **code tái hiện đầy đủ** bằng Java client:
- ghi `10:10:10.123` vào cột `TIME()` (cột này là cả khóa chính lẫn cột partition);
- scan log trả `36610000` ms;
- lookup trả `36610123` ms.

Không có bình luận. Trạng thái: còn mở. PR [#1822](https://github.com/apache/fluss/pull/1822) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và tìm ra cơ chế: kiểu `TIME()` **mặc định có độ chính xác 0**. Khi ghi log định dạng Arrow, cột `TIME(0)` dùng vector **theo giây** và **chia 1000**, nên mất phần mili giây. Còn KV lưu nguyên số mili giây mà client gửi. Mình chưa tái hiện, và chưa kiểm tra đường ghi qua Flink SQL.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Kiểu `TIME(p)`** | Giờ trong ngày, không có ngày. `p` là **độ chính xác**: `TIME(0)` tới giây, `TIME(3)` tới mili giây, tối đa `TIME(9)`. **`DataTypes.TIME()` không truyền tham số thì p = 0** (`TimeType.DEFAULT_PRECISION = 0`). |
| **Biểu diễn nội bộ của TIME** | Trong dòng dữ liệu của Fluss, `TIME` là một **số int: số mili giây từ nửa đêm** (36610123 = 10:10:10.123). |
| **Lookup** | Đọc theo khóa chính từ **KV store**. KV lưu dòng ở định dạng compacted, ghi số int đó **nguyên vẹn**. |
| **Scan log / changelog** | Đọc từ **log**. Log của bảng (kể cả bảng PK) mặc định ở định dạng **Arrow** (`table.log.format = ARROW`). |
| **Arrow `TimeSecVector` / `TimeMilliVector` / `TimeMicroVector` / `TimeNanoVector`** | Các loại cột Arrow cho kiểu thời gian, lưu theo **giây**, **mili giây**, **micro giây** hoặc **nano giây**. |
| **Đầu vào vượt độ chính xác** | Ghi giá trị có mili giây vào cột chỉ khai báo tới giây. Fluss **không làm tròn hay từ chối** ở đầu vào. |

## Tóm tắt

`TIME()` = `TIME(0)` được ánh xạ sang Arrow `Time(SECOND)`. Khi ghi vào log, `ArrowTimeWriter` lấy số mili giây rồi **chia 1000** (bỏ phần lẻ) để lưu theo giây. Khi đọc ra, giá trị là số giây × 1000, nên **mất phần .123**. Trong khi đó, đường ghi KV lưu nguyên số int mili giây, nên lookup trả **36610123**.

Vấn đề gốc là client được phép ghi giá trị **chính xác hơn kiểu khai báo** mà không bị chuẩn hóa, và hai đường lưu trữ xử lý "phần thừa" khác nhau.

Với `TIME(1)`, `TIME(2)` hoặc `TIME(3)`, Arrow dùng `TimeMilliVector`, lưu đúng số mili giây, nên log và KV **khớp nhau**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, gồm `utils/ArrowUtils` (ánh xạ kiểu sang Arrow), `row/arrow/writers/ArrowTimeWriter`, `types/TimeType`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `TimeType`: `public static final int DEFAULT_PRECISION = 0;`. Constructor không tham số `TimeType()` gọi `this(DEFAULT_PRECISION)`.
  - `ArrowUtils` (visitor sang `ArrowType`):
    ```java
    if (timeType.getPrecision() == 0)                       return new ArrowType.Time(TimeUnit.SECOND, 32);
    else if (precision >= 1 && precision <= 3)              return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
    else if (precision >= 4 && precision <= 6)              return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
    else                                                    return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
    ```
  - `ArrowTimeWriter.doWrite`:
    - `TimeSecVector`: `int sec = readTime(row, ordinal) / 1000;`, **cắt phần mili giây**;
    - `TimeMilliVector`: lưu nguyên số mili giây;
    - `TimeMicroVector` hoặc `TimeNanoVector`: nhân 1000 hoặc 1.000.000. Nghĩa là `TIME(4..9)` cũng chỉ có độ chính xác thực tế tới **mili giây**, vì biểu diễn nội bộ là int mili giây.
  - Đường KV: `BinaryWriter` có nhánh `case TIME_WITHOUT_TIME_ZONE:` ghi số int. Mình đã thấy nhánh này, chưa đọc chi tiết.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. Bảng có cột **`TIME` với độ chính xác 0** (`TIME()` hoặc `TIME(0)`), mà đây là mặc định khi không ghi độ chính xác.
2. **Giá trị ghi vào có phần mili giây khác 0.**
3. Đọc cùng dữ liệu qua **hai đường**: lookup (KV) và scan log hoặc changelog (Arrow). Với bảng log thuần thì chỉ có một đường, nên chỉ thấy "mất mili giây" chứ không có chuyện "lệch nhau".

**Về đường ghi qua Flink SQL (suy luận, chưa kiểm chứng):** Flink thường đảm bảo giá trị `TIME(0)` là số giây tròn khi cast hoặc chèn, nên có thể không gặp lỗi. Ghi trực tiếp bằng Java client (như test trong issue) thì gặp.

**Phiên bản:** báo trên `main` thời 0.8. Mã `v1.0.0` vẫn như vậy.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** bảng có cột `TIME` (độ chính xác 0) được ghi bằng Java client với giá trị có mili giây.

**Hậu quả:**
1. **Hai hệ thống downstream thấy giá trị khác nhau cho cùng một bản ghi:** AI agent lookup thấy `10:10:10.123`, còn job Flink đọc changelog thấy `10:10:10.000`.
2. **Nếu cột TIME nằm trong khóa chính** (như test của issue): khóa trong KV có `.123`, còn khóa trong changelog có `.000`. Consumer đọc changelog rồi **lookup lại bằng khóa trong changelog** sẽ **không tìm thấy** dòng. Đây là suy luận từ cơ chế.
3. **Dữ liệu trong log bị mất độ chính xác vĩnh viễn**, kể cả khi dữ liệu được tier sang lake.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #1819 trong `relate_issue.md` ghi: *"Lookup và scan trả giá trị kiểu `TIME` khác nhau (mất phần ms) | main | ❌ PR #1822 chưa merge | Tránh dùng kiểu `TIME`, dùng `TIMESTAMP`"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** lỗi chỉ xảy ra với **`TIME(0)`** (mặc định). Chỉ cần khai báo `TIME(3)` là log và KV khớp nhau. Không nhất thiết phải bỏ hẳn kiểu `TIME`.

## 5. Mức ảnh hưởng với CDP

- **Thấp:** CDP thường dùng `TIMESTAMP` hoặc `DATE`, ít dùng `TIME`.
- Nếu có dùng `TIME` (ví dụ "giờ hoạt động ưa thích" của khách hàng), cần khai báo đúng độ chính xác.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Khai báo `TIME(3)`** nếu dữ liệu có mili giây. Hoặc dùng `TIMESTAMP(3)` hay số nguyên (giây hoặc mili giây từ nửa đêm).
- **Chuẩn hóa ở ứng dụng** trước khi ghi bằng Java client: cắt về đúng độ chính xác của cột.
- **Không dùng `TIME` trong khóa chính** của bảng mà có consumer đọc changelog rồi lookup lại.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện trên 1.0.0.
- Mình chưa kiểm tra đường ghi qua Flink SQL có chuẩn hóa độ chính xác trước khi gửi hay không.
- Mình chưa đọc chi tiết đường KV (`BinaryWriter`, compacted row) để chắc chắn không có làm tròn nào.
- Mình chưa kiểm tra tên partition được sinh từ giá trị `TIME` có mili giây (cột partition trong test) và có lệch theo cách tương tự hay không.
