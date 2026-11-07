# Issue #4548 — Kiểm tra thời gian của auto-partition từ chối mọi partition QUARTER, nhưng lại chấp nhận ngày không tồn tại

Link: https://github.com/apache/fluss/issues/4548 — Phiên bản được báo: `1.0.0` (người báo: firasbouzazi, 05/10/2026). Issue có **code tái hiện**: sinh tên partition QUARTER bằng chính Fluss, rồi kiểm tra lại bằng Fluss thì bị từ chối. Issue cũng liệt kê các hệ quả và đề xuất sửa (parser strict). Không có bình luận. Trạng thái: còn mở. PR [#4549](https://github.com/apache/fluss/pull/4549) (cùng tác giả) **đang mở**.

> **Mức chắc chắn:** cao. Mình đã đọc `PartitionUtils` trong `v1.0.0` và các chỗ gọi hàm kiểm tra. Mô tả trong issue khớp với mã. Mình chưa tự chạy lại đoạn tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Auto-partition** | Coordinator tự tạo partition theo thời gian (`table.auto-partition.enabled`) với đơn vị `table.auto-partition.time-unit`: `HOUR`, `DAY`, `MONTH`, `QUARTER`, `YEAR`. |
| **Định dạng tên partition** | `yyyy` (năm), `yyyyQ` (quý, ví dụ `20264` là quý 4/2026), `yyyyMM`, `yyyyMMdd`, `yyyyMMddHH`. |
| **`PartitionUtils.isValidPartitionTime`** | Hàm kiểm tra chuỗi tên partition có đúng định dạng thời gian không. |
| **`DateTimeFormatter`** | Lớp Java phân tích chuỗi ngày giờ theo mẫu. |
| **Resolver SMART / STRICT** | Cách `DateTimeFormatter` xử lý giá trị "lạ". **SMART** (mặc định) điều chỉnh mềm dẻo, nên `20240230` (30/02) vẫn được chấp nhận. **STRICT** từ chối ngày không tồn tại. |
| **Dynamic partition creation** | Client tự tạo partition khi ghi vào partition chưa có (`client.writer.dynamic-create-partition.enabled`, mặc định bật, xem #311). |
| **Historical partition / `isPastAutoPartition`** | Từ 1.0.0, lookup có thể đọc partition **đã qua**, có thể từ lake. `isPastAutoPartition` quyết định một partition có phải "quá khứ" hay không. |

## Tóm tắt

`isValidPartitionTime` trong `1.0.0` làm đúng một việc: `DateTimeFormatter.ofPattern(format).parse(time)`, bắt `DateTimeParseException`. Có hai lỗi:
1. **QUARTER không bao giờ hợp lệ.** Mẫu `yyyyQ` không phân tích được chuỗi `20264`, vì trường năm có độ dài thay đổi "ăn" hết các chữ số, nên trường quý không còn gì. Mọi giá trị QUARTER đều bị từ chối, kể cả quý hiện tại do chính Fluss sinh ra.
2. **Ngày không tồn tại được chấp nhận.** Với resolver SMART mặc định, `20240230` (30/02), `20230229` (29/02 năm không nhuận), `20240431` (31/04) hay giờ `2024010124` đều qua được kiểm tra, nên có thể tạo partition sai tên.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, gồm `utils/PartitionUtils` (`isValidPartitionTime`, `validateAutoPartitionTime`, `isPastAutoPartition`).
- **Đã xác nhận trong mã `v1.0.0`:**
  - ```java
    private static boolean isValidPartitionTime(String time, AutoPartitionTimeUnit timeUnit, AutoPartitionStrategy s) {
        try {
            DateTimeFormatter.ofPattern(getPartitionTimeFormat(timeUnit, s)).parse(time);   // SMART, không strict
            return true;
        } catch (DateTimeParseException e) { return false; }
    }
    ```
  - `private static final String QUARTER_FORMAT = "yyyyQ";`
  - `validateAutoPartitionTime(...)`: nếu không hợp lệ thì ném `InvalidPartitionException("Partition value '%s' does not match the expected format '%s' for auto-partition time unit '%s'.")`. Nếu hợp lệ mà **cũ hơn mốc retention** (tính bằng `Instant.now()` cộng `numToRetain`) thì ném `InvalidPartitionException("Partition value '%s' is out-of-date. The earliest retained partition is '%s'.")`.
  - `isPastAutoPartition(...)`: nếu không hợp lệ thì trả **`false`**.
- **Các chỗ gọi (đã tìm trong mã chính của `v1.0.0`):**
  - `fluss-server` `CoordinatorService.java:916`, trong đường `Admin#createPartition`;
  - `fluss-client` `DynamicPartitionCreator.java:97`, trong đường writer tự tạo partition (xem #311);
  - `fluss-client` `PrimaryKeyLookuper.java:174`, trong đường lookup quyết định partition lịch sử.
- **Không đi qua hàm này:** `AutoPartitionManager` trên Coordinator (tự tạo partition theo lịch). Vì vậy bảng QUARTER **vẫn có partition tự động**, và điều đó **che giấu** lỗi (đúng như issue nói).

## 2. Khi nào lỗi xảy ra

- **Lỗi 1:** chỉ với bảng `table.auto-partition.time-unit = QUARTER`. Lỗi xảy ra khi:
  - tạo partition bằng tay (`Admin#createPartition`);
  - writer ghi vào partition **chưa được tạo sẵn**, nên dynamic partition creation chạy kiểm tra và **lỗi**;
  - lookup partition quý đã qua, nên `isPastAutoPartition` trả `false` và lookup **không** đi đường partition lịch sử.
- **Lỗi 2:** khi tạo partition bằng tay, hoặc ghi dữ liệu có giá trị partition **sai lịch** (ví dụ `20240230`) vào bảng có auto-partition.

**Phiên bản:** báo trên `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** bảng auto-partition theo QUARTER (lỗi 1), hoặc nguồn dữ liệu có giá trị ngày sai (lỗi 2).

**Hậu quả:**
1. **Bảng QUARTER:**
   - không tạo partition bằng tay được;
   - writer ghi vào quý chưa có partition sẽ lỗi `InvalidPartitionException`, nên job Flink failover;
   - lookup partition quý cũ không dùng được tính năng partition lịch sử.
2. **Ngày sai:** tạo ra partition với tên không phải ngày thật (`20240230`). Partition này nằm ngoài lịch tự động: không chắc được dọn theo retention đúng cách, có thể tồn tại mãi (suy luận). Dữ liệu sai lịch bị ghi vào đó thay vì bị từ chối.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4548 trong `relate_issue.md` ghi: *"Auto-partition đơn vị QUARTER luôn bị từ chối | 1.0.0 | ❌ | Không dùng QUARTER"*. Đối chiếu:
- **Đúng** cho lỗi 1.
- **Bổ sung lỗi 2** (chấp nhận ngày không tồn tại). Lỗi này áp dụng cho **mọi** đơn vị, kể cả DAY và HOUR mà CDP hay dùng.
- **Bổ sung:** hàm này cũng từ chối partition **cũ hơn retention** ("out-of-date"), và so theo đồng hồ của **máy chạy client**. Đây là thứ quyết định số phận của sự kiện đến trễ (liên quan #4726).

## 5. Mức ảnh hưởng với CDP

- CDP thường dùng partition theo **DAY** hoặc **HOUR**, nên lỗi 1 không ảnh hưởng.
- Lỗi 2 ảnh hưởng nếu dữ liệu nguồn có thể mang giá trị ngày sai (lỗi parse ở upstream, dữ liệu bẩn) và giá trị partition được lấy trực tiếp từ dữ liệu.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Không dùng `QUARTER`.** Nếu cần nhóm theo quý, dùng `MONTH` rồi gộp ở tầng truy vấn.
- **Validate giá trị cột partition ở Flink** trước khi sink (ví dụ parse lại bằng `LocalDate` strict, hoặc lấy partition từ timestamp chuẩn thay vì chuỗi thô).
- **Đồng bộ đồng hồ (NTP)** trên máy chạy writer, vì kiểm tra "out-of-date" dùng `Instant.now()` phía client.
- Định kỳ liệt kê partition của bảng (`Admin#listPartitionInfos`) để phát hiện tên partition bất thường.

## 6. Những điều chưa kiểm chứng

- Mình chưa tự chạy đoạn tái hiện của issue.
- Mình chưa kiểm tra partition có tên sai lịch có bị `AutoPartitionManager` dọn theo retention hay không.
- Mình chưa đọc diff của PR #4549.
