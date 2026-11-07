# Issue #3975 — Flink catalog (Flink 2.2+) từ chối bảng khi có hai index trùng danh sách cột

Link: https://github.com/apache/fluss/issues/3975 — Phiên bản được báo: không ghi rõ, thuộc `main` khoảng tháng 8/2026 (người báo: platinumhamburg, 13/08/2026). Đây là issue dạng "Motivation/Solution":
- **vấn đề:** khi nhiều index của Fluss có **cùng danh sách cột theo đúng thứ tự**, connector Flink thêm **tất cả** vào schema Flink, và Flink từ chối bảng với lỗi `Invalid index. There is a duplicated index composed of the same columns: [...]`;
- **hướng sửa:** `SchemaAdapter` của Flink 2.2 chỉ giữ index **đầu tiên** cho mỗi danh sách cột. Thứ tự cột vẫn quan trọng, nên `[a, b]` và `[b, a]` là hai index khác nhau.

Không có bình luận. Trạng thái: còn mở. PR [#3976](https://github.com/apache/fluss/pull/3976) **đang mở**, chỉ sửa `fluss-flink-2.2/.../SchemaAdapter.java` kèm test.
**Nhóm B:** chỉ khi dùng **Flink 2.2 trở lên** với **Fluss catalog** (đọc bảng PK qua catalog).

> **Mức chắc chắn:** trung bình. Mình đã đọc mã `v1.0.0`:
> - `SchemaAdapter` của Flink 2.2 và 2.3 thêm index **không loại trùng**, nên đúng như issue mô tả;
> - nhưng với **hai** index mà `FlinkCatalog.wrapWithIndexes` tạo ra trong 1.0.0 (khóa chính và "khóa bucket + khóa partition"), mình **không tìm ra** cách nào để chúng trùng nhau.
>
> Vì vậy trong 1.0.0, lỗi có lẽ chỉ xảy ra khi schema gốc **đã mang sẵn index**, hoặc trong tình huống mình chưa nghĩ tới. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Index trong schema Flink** | Từ Flink 2.2, `Schema` có thể khai báo **index**: danh sách cột mà bảng hỗ trợ tra nhanh. Planner dùng nó để quyết định **lookup join** hoặc **delta join** có khả thi hay không. |
| **`SchemaAdapter`** | Lớp chuyển tiếp theo phiên bản Flink:<br>• bản `fluss-flink-common` (Flink 1.x): `supportIndex() = false`, có index thì ném lỗi;<br>• bản `fluss-flink-2.2` và `fluss-flink-2.3`: `supportIndex() = true`, thêm index vào schema. |
| **`wrapWithIndexes`** | Hàm trong `FlinkCatalog.getTable` sinh danh sách index cho bảng PK khi Flink hỗ trợ index. |
| **Prefix lookup** | Tra theo **một phần đầu** của khóa chính, bằng đúng **bucket key** (khi bucket key là tiền tố thực sự của khóa chính vật lý). |
| **Khóa chính vật lý** | Khóa chính trừ các cột partition (bảng partition). |

## Tóm tắt

Trong Flink 2.2+, khi Flink đọc bảng PK qua Fluss catalog, connector gắn thêm **index** vào schema:
1. **khóa chính** (luôn có);
2. **bucket key + partition key**, nếu bucket key là tiền tố thực sự của khóa chính vật lý.

`SchemaAdapter` (2.2 và 2.3) gọi `Schema.newBuilder().fromSchema(unresolvedSchema)` rồi `.index(...)` cho từng index, **không kiểm tra trùng**. Nếu hai index có cùng danh sách cột, hoặc index mới trùng index **đã có sẵn** trong `unresolvedSchema`, Flink ném `Invalid index ... duplicated index`. Khi đó **`getTable` thất bại**, và **mọi truy vấn** vào bảng đó qua catalog đều lỗi.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-flink/fluss-flink-2.2` và `fluss-flink/fluss-flink-2.3`: `adapter/SchemaAdapter`;
  - `fluss-flink/fluss-flink-common`: `catalog/FlinkCatalog.getTable` và `wrapWithIndexes`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `SchemaAdapter` (2.2, và **giống hệt** ở 2.3):
    ```java
    public static Schema withIndex(Schema unresolvedSchema, List<List<String>> indexes) {
        Schema.Builder newSchemaBuilder = Schema.newBuilder().fromSchema(unresolvedSchema);
        for (List<String> index : indexes) {
            newSchemaBuilder.index(index);            // không loại trùng
        }
        return newSchemaBuilder.build();
    }
    ```
  - `FlinkCatalog.getTable`: với `TableKind.TABLE`, `if (supportIndex()) table = wrapWithIndexes(table, tableInfo);`.
  - `wrapWithIndexes`:
    ```java
    indexes.add(pkOp.get().getColumnNames());                    // index 1: khóa chính
    List<String> physicalPrimaryKeys = tableInfo.getPhysicalPrimaryKeys();
    if (isPrefixList(physicalPrimaryKeys, bucketKeys)) {         // bucket key là tiền tố THỰC SỰ
        indexKeys.addAll(bucketKeys);
        if (tableInfo.isPartitioned()) indexKeys.addAll(tableInfo.getPartitionKeys());
    }
    if (!indexKeys.isEmpty()) indexes.add(indexKeys);            // index 2
    ```
    `isPrefixList` trả `false` khi `fullList.size() <= prefixList.size()`. Tức index 2 luôn có **ít cột vật lý hơn** khóa chính. Mình thử các cách sắp thứ tự khóa chính, khóa partition và bucket key, và không tìm được trường hợp index 2 **trùng** index 1.
- **PR #3976** chỉ sửa module 2.2. Mình chưa kiểm tra vì sao không sửa 2.3, có thể 2.3 được thêm sau hoặc sửa riêng.

## 2. Khi nào lỗi xảy ra

- Flink **2.2 hoặc 2.3** với Fluss catalog, bảng **có khóa chính**.
- Danh sách index đưa cho Flink có **hai mục trùng cột và thứ tự**. Trong 1.0.0, theo phân tích của mình, chỉ có thể xảy ra nếu:
  - `unresolvedSchema` **đã có sẵn** index trùng với khóa chính, ví dụ một đường dựng schema khác đã thêm index (mình chưa xác định có đường nào như vậy không);
  - hoặc ở `main` có thêm nguồn index mới (issue nói chung "nhiều index của Fluss").
- Flink 1.x không bị (không hỗ trợ index).

## 3. Tác động đến ai, làm sai lệch gì

- Khi xảy ra: **không đọc được bảng** qua Flink catalog (lỗi khi `getTable`), nên mọi job hoặc truy vấn dùng bảng đó **không khởi động** được. Lỗi rõ ràng.
- **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Flink SQL Gateway / Flink 2.2 catalog | #2198, #3975 | 🟢 | ❌"*. Đối chiếu:
- **Trạng thái mã:** ❌ đúng, vì `SchemaAdapter` 1.0.0 không loại trùng.
- **Khả năng gặp trong 1.0.0:** theo phân tích mã, **thấp**, vì hai index do 1.0.0 tự sinh không thể trùng nhau. Có thể ghi là **🟡 (cần xác minh)**.
- **Bổ sung:** module **Flink 2.3** có cùng mã.

## 5. Mức ảnh hưởng với CDP

- Chỉ liên quan nếu CDP dùng **Flink 2.2 hoặc 2.3**. Với Flink 1.20, không ảnh hưởng.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Khi dùng Flink 2.2+, **kiểm thử** `DESCRIBE`/`SELECT` mọi bảng PK quan trọng qua Fluss catalog trước khi đưa vào production. Đặc biệt bảng partition có bucket key tùy chỉnh.
- Nếu gặp lỗi: tạm dùng **bảng tạm** (`CREATE TEMPORARY TABLE ... WITH ('connector' = 'fluss', ...)`) thay cho catalog. Cách này không đi qua `FlinkCatalog.getTable`. Mình chưa kiểm chứng.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện, và chưa tìm được kịch bản cụ thể gây trùng index trong 1.0.0.
- Mình chưa đọc mô tả đầy đủ và test của PR #3976. Test đó có thể cho thấy kịch bản thật.
- Mình chưa kiểm tra `main` có thêm loại index mới (ví dụ secondary index) sau 1.0.0 hay không.
