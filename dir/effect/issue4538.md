# Issue #4538 — `Decimal#hashCode` không nhất quán với `Decimal#equals`

Link: https://github.com/apache/fluss/issues/4538 — Phiên bản được báo: `1.0.0` (người báo: firasbouzazi, 03/10/2026). Issue ngắn, kèm ví dụ 3 dòng chứng minh lỗi và cách sửa một dòng. Có một bình luận xin nhận làm. Trạng thái: còn mở. PR [#4539](https://github.com/apache/fluss/pull/4539) **đang mở**.

> **Mức chắc chắn:** cao về lỗi, vì mình đã đọc `Decimal` trong `v1.0.0`. Về **tác động bên trong Fluss**, mình kiểm tra thấy các bộ chuyển đổi chính thức (POJO, Flink) đều **chuẩn hóa scale theo kiểu cột** trước khi tạo `Decimal`. Vì vậy trong luồng dữ liệu bình thường, hai giá trị cùng cột luôn cùng scale, và lỗi **khó lộ ra**. Mình chưa rà hết mọi nơi dùng `hashCode` của dòng dữ liệu.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`org.apache.fluss.row.Decimal`** | Kiểu số thập phân chính xác của Fluss (giá trị của cột `DECIMAL(p, s)`). Có chú thích `@PublicStable`, tức là API công khai cho người dùng. |
| **Precision / scale** | `p` là tổng số chữ số, `s` là số chữ số sau dấu phẩy. `1.0` có scale 1, `1.00` có scale 2. |
| **Hợp đồng `equals` / `hashCode`** | Quy tắc của Java: nếu `a.equals(b)` thì **bắt buộc** `a.hashCode() == b.hashCode()`. `HashMap` và `HashSet` dựa vào quy tắc này: tìm theo hash trước, rồi so `equals`. |
| **`BigDecimal.hashCode()`** | Hash của `BigDecimal` trong Java **phụ thuộc scale**: `1.0` và `1.00` có hash khác nhau, dù `compareTo` bằng 0. |
| **`GenericRow`** | Một dạng dòng dữ liệu của Fluss. `hashCode()` của nó là `Arrays.deepHashCode(fields)`, tức gọi `hashCode()` của từng giá trị, kể cả `Decimal`. |

## Tóm tắt

`Decimal.equals` so sánh **theo giá trị số** (`compareTo == 0`), nên `1.0` bằng `1.00`. Nhưng `Decimal.hashCode` trả `toBigDecimal().hashCode()`, mà hàm này **phụ thuộc scale**, nên hai giá trị "bằng nhau" có hash khác nhau (311 và 3102 trong ví dụ của issue). Hệ quả: cho `1.0` vào `HashSet`, rồi tìm `1.00` sẽ **không thấy**. Cách sửa đề xuất: hash từ `toBigDecimal().stripTrailingZeros()`.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, gồm `row/Decimal`.
- **Đã xác nhận trong mã `v1.0.0`:**
  ```java
  @Override public int hashCode() { return toBigDecimal().hashCode(); }
  @Override public boolean equals(final Object o) {
      if (!(o instanceof Decimal)) return false;
      Decimal that = (Decimal) o;
      return this.compareTo(that) == 0;
  }
  ```
- **Lan tới `GenericRow`:** `GenericRow.hashCode()` là `Arrays.deepHashCode(fields)`. Hai dòng chứa `Decimal` khác scale nhưng bằng giá trị có thể "bằng nhau" theo `equals` mà khác hash (suy ra từ mã).
- **Vì sao trong luồng chuẩn hiếm khi lộ (đã đọc mã):** các nơi tạo `Decimal` từ dữ liệu người dùng đều đưa về scale của cột:
  - `fluss-client` `converter/PojoTypeToFlussTypeConverter` gọi `Decimal.fromBigDecimal(scaled, precision, scale)` (biến tên là `scaled`, tức đã đổi scale);
  - các converter của Flink (`FlinkRowToFlussRowConverter`, `PojoToRowConverter`, `FlinkAsFlussRow` qua `fromUnscaledLong`) nhận precision và scale của cột.

  Vì vậy hai giá trị cùng một cột thường **cùng scale**, và hash nhất quán.

## 2. Khi nào lỗi xảy ra

Khi có **hai `Decimal` bằng giá trị nhưng khác scale** được đặt chung trong cấu trúc dựa trên hash. Thường là:
- **code ứng dụng** tự tạo `Decimal` bằng `Decimal.fromBigDecimal(...)` với scale khác nhau rồi dùng làm khóa `HashMap` hoặc phần tử `HashSet` (đúng ví dụ của issue);
- so sánh hoặc khử trùng giá trị **giữa hai cột khác kiểu** (ví dụ `DECIMAL(10,2)` và `DECIMAL(10,4)`) trong code ứng dụng.

**Phiên bản:** báo trên `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** chủ yếu **code ứng dụng** dùng trực tiếp lớp `Decimal` của Fluss (service AI agent, công cụ xử lý dữ liệu đọc qua Java client).

**Hậu quả:**
1. `HashMap.get` hoặc `HashSet.contains` **không tìm thấy** giá trị bằng nhau, nên khử trùng, gộp nhóm hay tra cứu theo số tiền, số điểm có thể sai **âm thầm**.
2. **Bên trong Fluss:** mình không thấy kịch bản cụ thể gây sai dữ liệu trong luồng chuẩn (mục 1), nhưng chưa rà hết.
3. Không ảnh hưởng **lưu trữ** hay **mã hóa khóa** của Fluss: khóa được mã hóa thành byte theo kiểu cột, không dùng `hashCode` (suy luận từ cách mã hóa khóa ở #4295).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4538 trong `relate_issue.md` ghi: *"❌ | Tránh dùng `Decimal` làm key HashMap ở ứng dụng"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** luồng ghi chuẩn (Flink, POJO converter) đã đưa scale về theo kiểu cột, nên ít gặp. Rủi ro chủ yếu ở code ứng dụng.

## 5. Mức ảnh hưởng với CDP

- **Thấp.**
- Liên quan nếu service AI agent đọc dữ liệu bằng Java client rồi dùng `Decimal` (ví dụ tổng chi tiêu, điểm) làm khóa trong collection.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Trước khi dùng làm khóa: chuyển sang `BigDecimal` với `stripTrailingZeros()`, hoặc sang chuỗi chuẩn hóa (`toPlainString()` sau khi đưa về cùng scale), hoặc sang số nguyên đơn vị nhỏ nhất (ví dụ xu).
- Không trộn giá trị từ các cột `DECIMAL` khác scale trong cùng một collection mà không chuẩn hóa.

## 6. Những điều chưa kiểm chứng

- Mình chưa rà mọi nơi bên trong Fluss dùng `hashCode` của dòng hoặc `Decimal` (ví dụ merge engine aggregation, xử lý trong Flink connector).
- Mình chưa kiểm tra `compareTo` có xử lý đúng `Decimal` khác precision hay không.
