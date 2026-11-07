# Issue #3812 — `TableInfo` hiển thị warehouse Iceberg của cluster như thể đó là thuộc tính của bảng

Link: https://github.com/apache/fluss/issues/3812 — Phiên bản được báo: `main` (người báo: litiliu, 30/07/2026). Issue có:
- cách tái hiện với Glue catalog;
- nguyên nhân trong mã: `LakeStorageUtils#generateDefaultTableLakeOptions` và `TableRegistration#toTableInfo`;
- lịch sử thiết kế: #411 yêu cầu lộ thông tin lake qua table properties, #503 làm overlay động, #3601 cho phép gắn vào bảng Iceberg có sẵn;
- hai hướng sửa.

Không có bình luận, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi dùng lakehouse (issue nói về Iceberg, nhưng cơ chế áp dụng cho mọi định dạng lake).

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và xác nhận cơ chế "trộn option cluster vào thuộc tính bảng" áp dụng cho **mọi** option `datalake.<format>.*`, không chỉ `warehouse`. Mình cũng kiểm tra một rủi ro liên quan, là **lộ thông tin bí mật**: 1.0.0 đã **lọc** các khóa nhạy cảm trước khi trộn. Mình chưa chạy thử.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`Admin#getTableInfo` / `TableInfo`** | API lấy thông tin bảng: schema, bucket, **properties**... Catalog (Flink catalog) và UI hiển thị từ đây. |
| **Option lake của cluster** | Cấu hình server dạng `datalake.format: iceberg`, `datalake.iceberg.type: glue`, `datalake.iceberg.warehouse: s3://...`. |
| **Thuộc tính kế thừa (overlay)** | Fluss lấy option lake của cluster, đổi tiền tố thành `table.datalake.iceberg.*`, rồi **trộn** vào properties của bảng **lúc trả về**. Chúng không thực sự được lưu cùng bảng. |
| **Bảng Iceberg có location riêng** | Bảng Iceberg có sẵn nằm ở đường dẫn khác warehouse mặc định. Từ #3601, Fluss chấp nhận gắn vào loại bảng này. |
| **Khóa nhạy cảm** | Option chứa mật khẩu, secret key... Fluss có danh sách mẫu để nhận diện (`ConfigurationUtils.isSensitiveKey`). |

## Tóm tắt

Với bảng bật lake, `getTableInfo` trả các option lake **của cluster** dưới dạng thuộc tính **của bảng**. Ví dụ `table.datalake.iceberg.warehouse = s3://<bucket>/fluss/iceberg`, kể cả khi bảng Iceberg thật nằm ở `s3://<bucket>/fluss/native_implicit_day_pk_demo_v2`. Người dùng hoặc công cụ đọc properties sẽ hiểu **sai vị trí dữ liệu**. "Thuộc tính bảng" còn **thay đổi theo** cấu hình cluster mà không cần `ALTER TABLE`.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `utils/LakeStorageUtils`, `zk/data/TableRegistration`, `coordinator/MetadataManager`, `coordinator/LakeCatalogDynamicLoader`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `LakeStorageUtils.generateDefaultTableLakeOptions(clusterConf)`:
    ```java
    String dataLakePrefix = "datalake." + optDataLakeFormat.get() + ".";
    return asPrefixedMap(extractPrefix(clusterConf.toMap(), dataLakePrefix), "table.");
    ```
    Lấy **tất cả** option `datalake.<format>.*` (`type`, `warehouse`, `uri`, `s3.*`, ...), rồi đổi tiền tố thành `table.datalake.<format>.*`.
  - `TableRegistration.toTableInfo(tablePath, schemaInfo, defaultTableLakeOptions)`:
    ```java
    if (properties.get(ConfigOptions.TABLE_DATALAKE_ENABLED)) {
        // only make the lake options visible when the datalake is enabled on the table
        defaultTableLakeOptions.forEach(properties::setString);
    }
    ```
    Option cluster **ghi đè** lên properties đã lưu của bảng.
  - `MetadataManager.getTable(tablePath)`: lấy `defaultTableLakeOptions` từ `lakeCatalogDynamicLoader`, tạo bản sao, gọi **`removeSensitiveTableOptions(tableLakeOptions)`**, rồi mới `toTableInfo`.
  - `ConfigurationUtils.isSensitiveKey`: các khóa chứa `password`, `secret`, `fs.azure.account.key`, `apikey`... bị coi là nhạy cảm.
  - `RpcServiceBase.getTableInfo` trả `tableInfo.toTableDescriptor().toJsonBytes()` cho client.

## 2. Khi nào lỗi xảy ra

- Cluster cấu hình `datalake.format` cùng các option `datalake.<format>.*`.
- Bảng có `table.datalake.enabled = true`.
- Rõ nhất khi gắn vào **bảng lake có sẵn với location riêng** (#3601), hoặc khi **đổi** cấu hình lake của cluster.

## 3. Tác động đến ai, làm sai lệch gì

1. **Thông tin sai lệch** cho người vận hành và công cụ (catalog, UI, script) đọc properties: vị trí warehouse không phải vị trí thật của bảng.
2. **"Thuộc tính bảng" đổi theo cấu hình cluster**, gây nhầm khi đối chiếu cấu hình hoặc audit.
3. **Bí mật không bị lộ** qua đường này trong 1.0.0, nhờ `removeSensitiveTableOptions`. Các option **không nhạy cảm** như `uri` của catalog, endpoint, region, type thì **vẫn lộ** cho mọi client có quyền `DESCRIBE` bảng (suy ra từ mã). Thường không phải bí mật, nhưng nên biết.
4. **Không ảnh hưởng đọc ghi dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #3812 trong nhóm Iceberg (🟢, ❌). Đối chiếu:
- **Đúng.**
- **Bổ sung:** cơ chế áp dụng cho **mọi** định dạng lake (Paimon cũng vậy), và cho **mọi** option `datalake.<format>.*` chứ không riêng `warehouse`. Khóa nhạy cảm đã được lọc.

## 5. Mức ảnh hưởng với CDP

- **Thấp.**

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Không dùng** `table.datalake.<format>.warehouse` hay các option tương tự trong `TableInfo` làm nguồn sự thật về **vị trí bảng lake**. Tra **trực tiếp catalog lake** (Glue, Nessie, Hive, Paimon catalog).
- Đặt tên khóa chứa bí mật (nếu tự thêm option vào `datalake.<format>.*`) sao cho khớp mẫu nhạy cảm (`password`, `secret`...), để chúng được lọc. Kiểm tra bằng `getTableInfo` sau khi cấu hình.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy `getTableInfo` thật để xem danh sách option bị lộ.
- Đã kiểm tra `SENSITIVE_KEY_PARTS` trong 1.0.0: gồm `password`, `secret`, `fs.azure.account.key`, `apikey`, `api-key`, `auth-params`, `service-key`, `token`, `basic-auth`, `jaas.config`, `http-headers`, `private.key`, `private-key`, `access.key`, `access-key`, `access_key`, `accesskey`. Nghĩa là `s3.access-key` cũng bị lọc. Mình chưa kiểm tra danh sách ngoại lệ `NON_SENSITIVE_KEYS`.
- Mình chưa kiểm tra Flink catalog có hiển thị các option này trong `SHOW CREATE TABLE` hay không.
