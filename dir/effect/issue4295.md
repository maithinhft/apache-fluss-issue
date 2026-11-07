# Issue #4295 — Khóa chính kiểu TIMESTAMP trước năm 1970 không giải mã được ở bộ giải mã khóa Iceberg

Link: https://github.com/apache/fluss/issues/4295 — Phiên bản được báo: `main` (người báo: fxbing, 11/09/2026). Issue ngắn, nêu chính xác giá trị lỗi, nguyên nhân và cách sửa. Trạng thái: **đã đóng** nhờ PR [#4296](https://github.com/apache/fluss/pull/4296), merge vào `main` ngày **06/11/2026** (commit `a79175e3`, sửa 1 file mã và 1 file test). Fix **không có trong 1.0.0** và chưa backport sang `release-1.0`.

> **Mức chắc chắn:** cao. Mình đã đọc `IcebergKeyDecoder` trong `v1.0.0` và xác nhận phép tách gây lỗi. Mình cũng lần ra **decoder này được dùng ở đâu**: chỉ ở đường **lookup kèm "chèn nếu chưa có"** (`KeyRecordBatch`), cho bảng có **định dạng lake là Iceberg**. Phạm vi hẹp hơn nhiều so với cách hiểu "timestamp trước 1970 nói chung". Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Epoch** | Mốc 1970-01-01 00:00:00. Thời điểm trước mốc này biểu diễn bằng **số âm** (micro giây âm). |
| **`TIMESTAMP(6)` / micro giây** | Timestamp có độ chính xác tới micro giây (1/1.000.000 giây). |
| **Mã hóa khóa (key encoding)** | Bảng PK lưu khóa trong KV dưới dạng byte. Cách mã hóa **phụ thuộc định dạng lake** của bảng: bảng có `table.datalake.format = iceberg` (với `kv format version 1`, hoặc version 2 dùng bucket key mặc định) mã hóa khóa **theo kiểu Iceberg** để khớp với lake. |
| **`IcebergKeyDecoder`** | Giải mã byte khóa kiểu Iceberg về lại giá trị cột. |
| **`TimestampNtz`** | Kiểu timestamp không múi giờ của Fluss. Gồm số mili giây và **phần nano trong mili giây phải nằm trong khoảng 0–999.999**. |
| **Lookup kèm "chèn nếu chưa có" (insert-if-not-exists)** | Một chế độ lookup: nếu khóa chưa có thì server **tự chèn** một dòng mới với khóa đó (thường dùng cho cột tự tăng). Server phải **giải mã khóa** thành dòng để chèn. |

## Tóm tắt

Giải mã khóa Iceberg kiểu `TIMESTAMP` có giá trị **trước 1970** và phần micro giây **không chia hết cho 1000** thì bị lỗi `IllegalArgumentException`. Ví dụ: `1969-12-31 23:59:59.999999` (= −1 micro giây) lỗi, còn −1.000 micro giây thì không. Code tách micro giây bằng `micros / 1000` và `micros % 1000`. Với số âm, `%` cho phần dư **âm**, nên phần nano nằm ngoài khoảng 0–999.999 mà `TimestampNtz` yêu cầu. Fix trên `main` dùng `TimestampNtz.fromMicros()`, hàm này chuẩn hóa đúng giá trị âm.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, gồm `row/decode/iceberg/IcebergKeyDecoder`, `row/decode/KeyDecoder`, `record/KeyRecordBatch`, và `fluss-server` `replica/ReplicaManager` (lookup kèm chèn).
- **Đã xác nhận trong mã `v1.0.0`:** trong `IcebergKeyDecoder`, nhánh `TIMESTAMP_WITHOUT_TIME_ZONE`:
  ```java
  long millis = micros / 1000L;
  int nanoOfMillis = (int) ((micros % 1000L) * 1000L);   // âm khi micros âm và không chia hết
  return TimestampNtz.fromMillis(millis, nanoOfMillis);
  ```
  Nhánh kiểu `TIME` ngay trên đó dùng `(int) (micros / 1000L)`.
- **Decoder này được dùng ở đâu (đã đọc mã):**
  - `KeyDecoder.ofPrimaryKeyDecoder(rowType, keyFields, kvFormatVersion, lakeFormat, isDefaultBucketKey)` trả `IcebergKeyDecoder` khi `lakeFormat == ICEBERG` và (`kvFormatVersion == 1` hoặc (`kvFormatVersion == 2` và dùng bucket key mặc định)).
  - Nơi gọi duy nhất mình tìm thấy trong mã chính là `KeyRecordBatch.LazyKvRecordIterator`.
  - `KeyRecordBatch` được tạo trong `ReplicaManager` ở đường **lookup kèm insert-if-not-exists**: các khóa không tìm thấy được bọc vào `KeyRecordBatch.create(missingKeys, tableInfo)`, rồi được `putRecordsToKv` giải mã thành dòng để chèn. Comment: *"the original key bytes are wrapped in KeyRecordBatch, then during putRecordsToKv they are decoded to rows"*.
  - Lookup thường **không** giải mã khóa, vì nó trả giá trị từ KV.

## 2. Khi nào lỗi xảy ra

Cần **đủ** các điều kiện:
1. Bảng PK có **`table.datalake.format = iceberg`**, và cách mã hóa khóa là kiểu Iceberg (theo điều kiện về kv format version ở mục 1).
2. **Khóa chính có cột `TIMESTAMP`** độ chính xác micro giây.
3. Giá trị **trước 1970** và có **phần micro giây lẻ** (không chia hết cho 1000).
4. Dùng **lookup kèm insert-if-not-exists** với khóa chưa tồn tại.

**Phiên bản:** báo trên `main`. Mã `v1.0.0` như vậy. Fix chỉ có trên `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** rất hẹp, chỉ bảng thỏa mọi điều kiện ở mục 2.

**Hậu quả:** request lookup kèm chèn cho khóa đó **thất bại** (`IllegalArgumentException`). Dòng không được chèn. Có báo lỗi rõ ràng, không sai dữ liệu ngầm.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4295 trong `relate_issue.md` (mục 4.4) ghi: *"Timestamp trước 1970 có phần dưới ms không decode được | main | ❌ PR #4296 chưa merge | Ngày sinh khách hàng trước 1970. Dùng `DATE` hoặc `TIMESTAMP(3)`"*. Đối chiếu:
- **Trạng thái đã thay đổi:** PR #4296 **đã merge** vào `main` (06/11/2026) và issue đã đóng. Với 1.0.0 nên ghi **⏭️**.
- **Phạm vi chưa đúng:** lỗi **không** ảnh hưởng mọi timestamp trước 1970, mà chỉ ở bộ giải mã khóa **Iceberg**, trong đường **lookup kèm insert-if-not-exists**. Issue nên thuộc **nhóm B (Iceberg)** chứ không phải nhóm A.
- Ví dụ "ngày sinh khách hàng" chỉ đúng nếu ngày sinh nằm trong **khóa chính**, kiểu `TIMESTAMP(6)`, có micro giây lẻ, trên bảng Iceberg, và dùng lookup kèm chèn. Trong thực tế CDP gần như không gặp.

## 5. Mức ảnh hưởng với CDP

- **Gần như không ảnh hưởng.**
- Nếu CDP dùng lakehouse Iceberg **và** lookup kèm chèn: tránh đặt timestamp micro giây trong khóa chính. Ngày sinh nên dùng `DATE`.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa kiểm tra chắc chắn rằng không có nơi nào khác (ví dụ trong module lake Iceberg) dùng `IcebergKeyDecoder`. Mình chỉ tìm trong mã chính của `fluss-common` và `fluss-server`.
- Mình chưa kiểm tra chiều **mã hóa** (`IcebergKeyEncoder`) với giá trị âm có đúng không.
