# Issue #4000 — Bộ chuyển đổi bộ lọc (`PredicateConverter`) xử lý sai `BETWEEN`, gây lỗi lúc chạy

Link: https://github.com/apache/fluss/issues/4000 — Phiên bản được báo: `0.9.0`. Tiêu đề trên GitHub: có nội dung về `PredicateConverter` và `BETWEEN` (người báo WenDing-Y, đồng thời là người viết PR). Không có bình luận. PR sửa: [#3947](https://github.com/apache/fluss/pull/3947) *"[flink] Fix BETWEEN literal extraction in PredicateConverter"*, đang **mở (chưa merge)**. Thay đổi rất nhỏ: gọi `extractLiteral(...)` cho hai cận của `BETWEEN` trước khi tạo predicate, kèm test mới.

> **Lưu ý quan trọng:** issue này là lỗi **có thật trong mã `v1.0.0`** (mình đã đọc). Nhưng dòng trong `relate_issue.md` ghi *"Query Flink SQL trên bảng Fluss dùng `BETWEEN` sẽ lỗi"* là **chưa được chứng minh**: bằng chứng trong issue và test của PR đều dùng biểu thức Flink dựng tay, không phải câu SQL chạy thật. Có khả năng bộ lập kế hoạch của Flink đã đổi `BETWEEN` trong SQL thành `>= AND <=` trước khi tới connector (xem mục 4). Mình chưa chạy thử để xác nhận.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Predicate (vị từ/bộ lọc)** | Điều kiện lọc dòng, ví dụ `score >= 10 AND score <= 20`. |
| **Filter pushdown (đẩy bộ lọc xuống)** | Flink đưa điều kiện `WHERE` xuống connector để Fluss lọc sớm (bỏ qua partition/lô dữ liệu không khớp) thay vì đọc hết rồi lọc ở Flink. |
| **`PredicateConverter`** | Lớp trong connector Flink của Fluss, đổi biểu thức `WHERE` của Flink (`ResolvedExpression`) sang `Predicate` của Fluss. |
| **`BETWEEN`** | `x BETWEEN a AND b` nghĩa là `x >= a AND x <= b`. Trong Fluss được dựng bằng `PredicateBuilder.between`, thực chất là AND của hai so sánh. |
| **`ValueLiteralExpression`** | Nút trong cây biểu thức Flink bọc một hằng số (ví dụ số 10). Nó **không phải** số 10 của Java mà là một đối tượng bọc quanh nó. |
| **`extractLiteral(...)`** | Hàm của `PredicateConverter` lấy giá trị Java thật (đúng kiểu, ví dụ `Long`, `BinaryString`) ra khỏi `ValueLiteralExpression`. Các nhánh `EQUALS`, `IN`, `LIKE`, so sánh đều dùng hàm này; nhánh `BETWEEN` thì không. |
| **`CompareUtils.compareLiteral`** | Hàm so sánh hai giá trị của predicate. Chỉ so sánh được giá trị `Comparable` hoặc `byte[]`, còn lại ném `RuntimeException("Unsupported type: ...")`. |
| **Partition filter (lọc partition)** | Bộ lọc áp dụng trên giá trị các cột partition, để enumerator của Flink source chỉ lấy các partition khớp. |
| **Record batch filter** | Bộ lọc dựa trên thống kê (min/max) của từng lô bản ghi ở bảng log, gửi xuống TabletServer để bỏ qua cả lô không khớp. |
| **Safety net (lưới an toàn)** | Fluss luôn trả lại **toàn bộ** bộ lọc cho Flink (`Result.of(acceptedFilters, filters)`), nên Flink vẫn lọc lại ở phía mình dù connector đã đẩy xuống. |

## Tóm tắt

Trong `PredicateConverter` ở `v1.0.0`, nhánh `BETWEEN` truyền thẳng hai cận dưới dạng `ValueLiteralExpression` (đối tượng bọc của Flink) vào `builder.between(...)`, thay vì rút ra giá trị Java như các nhánh khác. Khi in ra bằng `toString()` thì trông vẫn đúng (`And([GreaterOrEqual(long1, 10), LessOrEqual(long1, 20)])`), nên test chỉ so sánh chuỗi không phát hiện. Nhưng khi predicate được **đánh giá thật**, `compareLiteral` nhận một đối tượng không phải `Comparable` và ném `RuntimeException("Unsupported type: BIGINT")`. Hậu quả: câu truy vấn có điều kiện `BETWEEN` được đẩy xuống Fluss có thể lỗi lúc chạy.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink-common`, lớp `org.apache.fluss.flink.utils.PredicateConverter`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - Nhánh `BETWEEN` (dòng 130–134) là: `builder.between(builder.indexOf(fieldRefExpr.getName()), children.get(1), children.get(2))` — `children.get(1)`, `children.get(2)` là nút biểu thức thô. Các nhánh khác (ví dụ dòng 109) gọi `extractLiteral(fieldRefExpr.getOutputDataType(), children.get(i))`.
  - `PredicateBuilder.between(idx, lower, upper)` dựng `And(greaterOrEqual(idx, lower), lessOrEqual(idx, upper))`.
  - `GreaterOrEqual.test(type, field, literal)` gọi `compareLiteral(type, literal, field)`. Tức giá trị đầu tiên là **hằng số của predicate**; nếu nó là `ValueLiteralExpression` (không `Comparable`, không `byte[]`) thì rơi vào nhánh ném `RuntimeException("Unsupported type: " + type)`. Khớp với thông báo trong issue.
  - Test hiện có trong `PredicateConverterTest` cho `BETWEEN` chỉ so sánh chuỗi (`BUILDER.between(0, 10, 20)`), đúng như issue nói.
- **Hướng sửa (PR #3947):** gọi `extractLiteral(...)` cho cả hai cận, thêm test `testBetweenEvaluatesAgainstRow` kiểm tra `predicate.test(GenericRow.of(15L))`, 10L, 20L. Chưa merge.
- **Nơi predicate được dùng lúc chạy (đã đọc mã):**
  1. **Lọc partition** (`FlinkSourceEnumerator.applyPartitionFilter`, dòng ~870): `partitionFilters.test(partitionRow)` với từng partition. Áp dụng cho bảng có partition.
  2. **Lọc theo lô ở bảng log** (`FlinkTableSource.pushdownRecordBatchFilter`, chỉ khi `!hasPrimaryKey()`): predicate được đổi sang dạng protobuf (`PredicateMessageUtils.toPbPredicate`) trong `LogFetcher.TableReadContext` rồi gửi cho TabletServer. Hàm `toPbLiteralValue` ép kiểu hằng số theo kiểu cột (ví dụ `(Boolean) literal`), nên với hằng số là `ValueLiteralExpression` thì có thể lỗi ép kiểu ngay phía client. Mình chưa đọc hết các nhánh kiểu nên chưa khẳng định từng kiểu.
  3. **Lọc dữ liệu lake** (`pushdownLakeFilters`) khi có lakehouse. Với CDP của bạn (không dùng lakehouse theo `relate_issue.md`) bỏ qua.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. **Một biểu thức `BETWEEN` thật sự tới được `PredicateConverter` dưới dạng hàm `BETWEEN` của Flink** (`BuiltInFunctionDefinitions.BETWEEN`). Đây là điều kiện **chưa chắc xảy ra với SQL** (xem mục 4). Chắc chắn xảy ra nếu người dùng gọi `Expression.between(...)` của Table API, hoặc nếu Flink không đổi `BETWEEN` thành so sánh.
2. **Biểu thức được đẩy xuống** (`applyFilters`) và lọt vào một trong các nhánh: bảng có partition và `BETWEEN` đặt trên cột partition (lọc partition), hoặc bảng log (không khóa chính) có thống kê cột và `BETWEEN` đặt trên cột có thống kê (lọc theo lô).
3. **Bảng khóa chính không partition:** `BETWEEN` có thể không được đẩy xuống ở đâu cả (nhánh lọc theo lô chỉ cho bảng không có khóa chính, nhánh partition chỉ cho bảng có partition). Với bảng PK lookup (đường chính của CDP) thì không dính.

**Phiên bản:** báo trên `0.9.0`; mã `v1.0.0` còn nguyên lỗi (đã đọc dòng 130–134).

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người chạy truy vấn/lập job Flink **đọc (scan)** bảng Fluss với điều kiện `BETWEEN` rơi vào hai trường hợp đẩy xuống ở mục 2.2.

**Hậu quả có thể (chưa tái hiện):**
1. **Lỗi lúc chạy** (`RuntimeException: Unsupported type: ...` hoặc `ClassCastException`). Ở nhánh lọc partition, lỗi xảy ra trong enumerator lúc khám phá partition, nên có thể làm job không khởi động được. Mình chưa lần theo cách Flink xử lý ngoại lệ ở bước này.
2. **Không thấy bằng chứng sai kết quả âm thầm**, vì lỗi dạng ném ngoại lệ chứ không trả dữ liệu sai. Tuy nhiên có một khả năng mình **không loại trừ**: với kiểu chuỗi, `toPbLiteralValue` dùng `literal.toString()`; nếu `toString()` của `ValueLiteralExpression` có thêm dấu nháy (mình tin là có với chuỗi, nhưng chưa kiểm chứng), server có thể so sánh sai và bỏ qua nhầm một số lô. Lưới an toàn của Flink chỉ lọc lại kết quả đã trả về, không khôi phục lô bị bỏ nhầm. Đây là suy đoán, cần kiểm chứng trước khi coi là rủi ro thật.
3. **Phạm vi hẹp:** không ảnh hưởng ghi dữ liệu, lookup theo khóa, hay độ bền dữ liệu.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4000 ghi: *"Query Flink SQL trên bảng Fluss dùng `BETWEEN` (khoảng thời gian, điểm số) sẽ lỗi. Viết thành `>= AND <=`"*. Đối chiếu:
- **Lỗi trong mã là có thật** (mục 1), PR chưa merge là đúng.
- **Câu "SQL `BETWEEN` sẽ lỗi" chưa được chứng minh.** Bằng chứng duy nhất là test đơn vị tự dựng `CallExpression` (trong issue và trong PR). Fluss không có test tích hợp SQL nào dùng `BETWEEN` (mình đã tìm trong `PredicateConverterTest`, `FlinkTableSourceITCase`, `FlinkTableSourceFilterPushdownITCase`; chỉ `PredicateConverterTest` nhắc `BETWEEN`).
- **Giả thuyết cần kiểm chứng:** theo hiểu biết của mình (chưa kiểm chứng trong môi trường này), Flink SQL (dựa trên Calcite) thường đổi `x BETWEEN a AND b` thành `x >= a AND x <= b` ngay khi lập kế hoạch, nên connector có thể không bao giờ nhận hàm `BETWEEN`. Nếu đúng, lỗi chỉ lộ ra với Table API (`.between()`) hoặc trường hợp đặc biệt. Mình không có công cụ chạy Flink SQL trong phiên này nên **chưa xác minh** được; đây là việc nên thử.
- Khuyến nghị "viết thành `>= AND <=`" vẫn an toàn và không phụ thuộc giả thuyết trên.

## 5. Mức ảnh hưởng với CDP

- **Lookup/KV (đường chính của CDP):** không bị ảnh hưởng. Lookup dùng khóa chính, không đi qua `BETWEEN` của bộ lọc scan.
- **Ghi context cho AI agent:** không bị ảnh hưởng.
- **Chỉ liên quan nếu** bạn có job Flink hoặc truy vấn **scan bảng Fluss có điều kiện khoảng** (ví dụ lấy sự kiện trong khoảng thời gian từ bảng log hay bảng có partition theo ngày). Khi đó rủi ro có thật nếu bạn dùng Table API `.between()`; với SQL thì chưa rõ.

**Biện pháp giảm thiểu:**
- **Viết `col >= a AND col <= b`** thay vì `BETWEEN` / `.between()`. An toàn và đơn giản.
- **Thử một câu truy vấn mẫu có `BETWEEN`** trên bảng log hoặc bảng partition ở môi trường thử, với đúng phiên bản Flink và Fluss sẽ dùng, để biết nó có lỗi hay không (đây là cách nhanh nhất để giải quyết điểm chưa chắc ở mục 4).
- Theo dõi PR #3947.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy Flink SQL nên không biết `BETWEEN` trong SQL có tới được connector dưới dạng hàm `BETWEEN` hay không.
- Mình chưa tái hiện lỗi `Unsupported type` hay `ClassCastException`; chỉ đọc mã và đối chiếu với thông báo trong issue.
- Mình chưa kiểm tra kiểu chuỗi (có dấu nháy trong `toString()`) và các kiểu khác trong `toPbLiteralValue`.
- Mình chưa lần theo cách Flink xử lý ngoại lệ ném ra từ `applyPartitionFilter` trong enumerator (job fail hay chỉ log).
- Mình chưa kiểm tra bảng log cần bật thống kê cột thế nào để nhánh lọc theo lô hoạt động (`availableStatsColumns`).
