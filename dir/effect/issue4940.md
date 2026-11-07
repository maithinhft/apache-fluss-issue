# Issue #4940 — File position-delete của Iceberg chỉ lưu 16 ký tự đầu của đường dẫn, làm hỏng compaction

Link: https://github.com/apache/fluss/issues/4940 — Phiên bản được báo: `1.0.0` (người báo: polyzos, 29/10/2026). Issue phân tích rõ nguyên nhân, tác động và cách sửa. Có hai bình luận:
- *"This only affect the 1.10 Iceberg version - the `1.12 plugin` writes full `file_path` bounds and is coming in the next release"* (tham chiếu #4592);
- maintainer: *"Then I believe we can live with this for now. Let's treat it as an incremental fix that will be available in the next release with the Iceberg 1.12 plugin."*

Không có PR riêng. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bảng **PK** được tier sang **Iceberg**.

> **Mức chắc chắn:** cao. Mình đã đọc `TaskWriterFactory` của module Iceberg trong `v1.0.0` (Iceberg **1.10.1**): delta writer của bảng PK tạo `GenericAppenderFactory` **không kèm `Table`**, đúng như issue nói. Trên `main` có module mới `fluss-lake-iceberg-1.12` (#4671, 16/10/2026), **không có trong 1.0.0**. Mình chưa đọc mã module 1.12 để xác nhận nó ghi đủ đường dẫn, và chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Tier bảng PK sang Iceberg** | Fluss ghi trạng thái bảng PK vào Iceberg: dòng mới vào **data file**, còn update và delete được biểu diễn bằng **delete file**. |
| **Position-delete file** | Delete file của Iceberg ghi "dòng thứ N trong data file X đã bị xóa". Mỗi dòng có cột `file_path` trỏ tới data file. |
| **Metrics (lower/upper bounds)** | Iceberg lưu giá trị nhỏ nhất và lớn nhất của từng cột trong mỗi file, để biết nhanh file nào liên quan. Với position-delete, bound của `file_path` cho Iceberg biết delete file **áp cho data file nào**. |
| **Metrics mode `truncate(16)`** | Chế độ mặc định của Iceberg: chỉ lưu **16 ký tự đầu** của giá trị chuỗi. Riêng position-delete, Iceberg có cấu hình giữ **đủ** `file_path`, nhưng cấu hình đó chỉ áp dụng khi appender factory **biết `Table`**. |
| **`referenced_data_file`** | Trường mà Iceberg suy ra khi một delete file chỉ trỏ tới **một** data file. |
| **Compaction / rewrite** | Gộp hoặc viết lại file. Commit rewrite phải chắc rằng không có delete mới áp lên file vừa bị thay. |

## Tóm tắt

Khi tier bảng PK sang Iceberg (plugin 1.10), Fluss tạo position-delete file mà bound của `file_path` **chỉ có 16 ký tự đầu**, ví dụ `s3://data-lakeho`. Iceberg không biết delete đó thuộc data file nào, nên coi nó **có thể áp cho mọi data file** trong partition (`referenced_data_file = null`). Nguyên nhân: `GenericAppenderFactory` của delta writer được tạo **không truyền `Table`**, nên Iceberg dùng metrics mode mặc định, và các thuộc tính của bảng cũng **không có tác dụng**. Cách sửa: truyền `Table` vào factory.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-lake/fluss-lake-iceberg`, `tiering/writer/TaskWriterFactory`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `pom.xml`: `<iceberg.version>1.10.1</iceberg.version>`.
  - `TaskWriterFactory`:
    ```java
    if (equalityFieldIds.length == 0) {                          // bảng log (append-only)
        FileAppenderFactory<Record> fileAppenderFactory = new GenericAppenderFactory(schema, table.spec());
        ...
    } else {                                                      // bảng PK
        FileAppenderFactory<Record> appenderFactory =
            new GenericAppenderFactory(schema, table.spec(), equalityFieldIds, schema, null);   // không có Table
        ...
        return new GenericRecordDeltaWriter(table, deleteSchema, format, appenderFactory, ...);
    }
    ```
- **Trên `main`:**
  - `fluss-lake/` có thêm `fluss-lake-iceberg-common` và **`fluss-lake-iceberg-1.12`** (#4646 tách module, #4671 thêm plugin 1.12);
  - `fluss-lake/pom.xml` ghi chú *"The Iceberg 1.12 plugin needs a JDK 17 toolchain"*.

  Theo bình luận, plugin 1.12 ghi đầy đủ bound. Mình chưa đọc mã để xác nhận.

## 2. Khi nào lỗi xảy ra

- Bảng **PK** có `table.datalake.format = iceberg` (plugin 1.10 trong 1.0.0).
- Có update hoặc delete, nên sinh position-delete file.
- Hậu quả lộ ra khi chạy **compaction** Iceberg song song với tiering, hoặc khi đọc.

**Phiên bản:** báo trên `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Theo issue:**
1. **Compaction lỗi khi tiering đang chạy:** mỗi vòng tiering thêm position-delete vào mọi partition, nên rewrite nào commit sau vòng kế tiếp sẽ lỗi `Cannot commit, found new position delete for replaced data file`. Nếu bật partial progress, các nhóm đó bị **bỏ qua im lặng**.
2. **Compaction viết lại cả partition mỗi lần**, vì mọi data file đều có vẻ có delete.
3. **Đọc chậm hơn**, vì mỗi position-delete file phải so với **mọi** data file trong partition.

**Không sai dữ liệu:** Iceberg vẫn áp delete đúng khi đọc, chỉ là tốn công hơn (theo cơ chế mô tả).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #4940 trong nhóm Iceberg (❌). Đối chiếu:
- **Nên là ⏭️ (có điều kiện):** lỗi còn trong 1.0.0. Bản sửa đến dưới dạng **plugin Iceberg 1.12** trên `main` (sẽ có ở bản tiếp theo), chứ không phải patch cho plugin 1.10. Maintainer chấp nhận để nguyên lỗi ở 1.10.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu tier **bảng profile (PK)** sang **Iceberg**.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Với bảng PK cần tier: ưu tiên **Paimon** trên 1.0.0. Iceberg trên 1.0.0 cũng chưa hỗ trợ union read bảng PK (#3048).
- Nếu bắt buộc dùng Iceberg:
  - **không chạy compaction Iceberg song song** với tiering, hoặc chạy compaction khi tiering tạm dừng;
  - chấp nhận đọc chậm hơn;
  - lên plugin Iceberg 1.12 khi có bản phát hành (cần JDK 17).
- Theo dõi log compaction có lỗi `Cannot commit, found new position delete for replaced data file`.

## 6. Những điều chưa kiểm chứng

- Mình chưa đọc mã `fluss-lake-iceberg-1.12` trên `main`.
- Mình chưa kiểm tra bảng **log** (append-only, dùng `GenericAppenderFactory(schema, spec)`) có chịu ảnh hưởng nào không. Bảng log không có position-delete nên có lẽ không.
- Mình chưa tái hiện.
