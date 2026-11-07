# Issue #4303 — `TypedUpsertWriter.delete(pojo)` làm mất giá trị khóa partition trên bảng PK có partition, và ném NPE

Link: https://github.com/apache/fluss/issues/4303 — Phiên bản được báo: `main` ngay trước khi phát hành 1.0.0 (người báo: fxbing, 11/09/2026). Issue có:
- ví dụ: bảng `id INT, dt STRING, payload STRING`, `PRIMARY KEY (id, dt)`, `PARTITION BY dt`; xóa bằng POJO `{id: 42, dt: "2026-09-11", payload: null}`;
- **mong đợi:** xóa khóa `id=42` trong partition `2026-09-11`;
- **thực tế:** POJO được chuyển thành `[42, null, null]`, và bước xác định partition ném `NullPointerException: Partition value shouldn't be null.` trước khi gửi;
- nguyên nhân: `TypedUpsertWriterImpl` dùng **khóa chính vật lý** (không gồm cột partition) cho cả phép chiếu lẫn bước mở rộng dòng;
- hướng sửa: dùng khóa chính **logic** (gồm cả cột partition).

Không có bình luận. Trạng thái: còn mở. PR [#4304](https://github.com/apache/fluss/pull/4304) **đang mở**.
**Nhóm B:** chỉ khi ứng dụng Java dùng **Typed API** (`TypedUpsertWriter`, ghi bằng POJO) để **xóa** trên **bảng PK có partition**.

> **Mức chắc chắn:** cao. Mình đã đọc `TypedUpsertWriterImpl` trong `v1.0.0`: mã khớp đúng mô tả. Issue mở 11/09/2026, trước khi tag `v1.0.0` được tạo (15/09/2026), và PR chưa merge, nên 1.0.0 còn lỗi. Mình chưa chạy thử.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Typed API (`TypedUpsertWriter<T>`)** | API Java ghi bằng **POJO** (lớp Java thường có các trường trùng tên cột) thay vì `InternalRow`. Có `upsert(pojo)` và `delete(pojo)`. |
| **Khóa chính logic** | Khóa chính như khai báo: `(id, dt)`. |
| **Khóa chính vật lý** (`getPhysicalPrimaryKeys`) | Khóa chính **trừ** cột partition: `(id)`. Dùng để định vị dòng **bên trong** một partition. |
| **Định tuyến partition** | Client đọc giá trị cột partition (`dt`) trong dòng để biết gửi tới partition nào (`RowPartitionGetter`). Giá trị null thì ném NPE. |
| **`PojoToRowConverter`** | Bộ chuyển POJO thành `GenericRow` theo một phép chiếu cột. |

## Tóm tắt

Khi gọi `delete(pojo)`, `TypedUpsertWriterImpl`:
1. chuyển POJO thành dòng **chỉ gồm khóa chính vật lý** (`id`);
2. dựng dòng đầy đủ, chỉ điền các cột của khóa chính vật lý, còn lại để **null**, kể cả **`dt`**;
3. đưa xuống `UpsertWriter.delete(row)`. Bước định tuyến cần `dt`, gặp null nên ném **NPE**.

Kết quả: **không thể xóa** bằng Typed API trên bảng PK có partition. Lỗi rõ ràng (exception), không xóa nhầm.

`upsert(pojo)` không bị: nó dùng toàn bộ schema hoặc `targetColumns`.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, gồm `table/writer/TypedUpsertWriterImpl`; lỗi lộ ra ở `fluss-common` `row/RowPartitionGetter`.
- **Đã xác nhận trong mã `v1.0.0`:**
  ```java
  this.pkProjection = this.tableSchema.project(tableInfo.getPhysicalPrimaryKeys());   // chỉ (id)
  this.pkConverter  = PojoToRowConverter.of(pojoClass, tableSchema, pkProjection);
  ...
  public CompletableFuture<DeleteResult> delete(T record) {
      if (record instanceof InternalRow) return delegate.delete((InternalRow) record);  // InternalRow: đi thẳng
      InternalRow pkOnly = convertPojo(record, /*forDelete=*/ true);
      return delegate.delete(pkOnly);
  }
  ...
  if (forDelete) {
      // set PK fields, others null
      for (String pk : tableInfo.getPhysicalPrimaryKeys()) {     // bỏ sót cột partition
          full.setField(tableSchema.getFieldIndex(pk), projected.getField(projection.getFieldIndex(pk)));
      }
  }
  ```
  - `RowPartitionGetter`: `checkNotNull(partitionValue, "Partition value shouldn't be null.")`.

## 2. Khi nào lỗi xảy ra

- Bảng **có khóa chính** và **có partition**.
- Xóa bằng **`TypedUpsertWriter.delete(pojo)`** với `pojo` là POJO, không phải `InternalRow`.
- Luôn xảy ra (không phụ thuộc thời điểm hay tải).

**Phiên bản:** báo trên `main` trước 1.0.0. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

- **Không xóa được** dòng qua Typed API trên bảng PK có partition: lời gọi `delete` ném exception.
- Nếu ứng dụng **nuốt exception** hoặc chỉ log, các bản ghi cần xóa (ví dụ do yêu cầu "quyền được lãng quên" hay hủy đơn) sẽ **vẫn còn** trong Fluss. Đây là rủi ro dữ liệu ở tầng ứng dụng (suy luận).
- **Không xóa nhầm** dòng khác.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Rebalance / Typed API / JMX | ..., #4303, ... | 🟢 | ❌"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** chỉ ảnh hưởng **delete** trên bảng **có partition**. Có cách tránh đơn giản (mục 5).

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu service Java của CDP dùng Typed API để xóa bản ghi trong bảng PK có partition (ví dụ bảng profile hoặc sự kiện partition theo ngày).

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Dùng `UpsertWriter` thường** (không typed) cho thao tác xóa: tự dựng `GenericRow` với đầy đủ cột khóa chính **và** cột partition, rồi `delete(row)`.
- Hoặc vẫn dùng `TypedUpsertWriter` nhưng truyền vào một **`InternalRow`** (ví dụ `GenericRow`). Theo mã, `delete` chuyển thẳng `InternalRow` xuống writer, bỏ qua bước chuyển đổi lỗi. Dòng phải có đủ cột theo schema.
- **Không nuốt exception** của `delete(...).get()`. Có test tích hợp cho đường xóa trên bảng partition.
- Với bảng có partition theo thời gian, cân nhắc dùng TTL hoặc xóa partition thay cho xóa từng dòng, nếu nghiệp vụ cho phép.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy thử trên 1.0.0.
- Mình chưa kiểm tra **Typed lookup** (`TypedLookuper` hoặc tương tự) có cùng vấn đề với khóa partition hay không.
- Mình chưa đọc diff của PR #4304.
