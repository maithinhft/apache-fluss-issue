# Issue #2422 — Bật lakehouse bằng `ALTER TABLE` thì thiếu metric tiering, và không đổi được `table.datalake.freshness`

Link: https://github.com/apache/fluss/issues/2422 — Phiên bản được báo: `0.8.0` (người báo: beryllw, 21/01/2026). Issue có câu lệnh tái hiện và nêu hai lỗi:
1. sau `ALTER TABLE ... SET ('table.datalake.enabled'='true')`, bảng tier bình thường nhưng **không có metric** `table_bucket_lakeTiering`;
2. `ALTER TABLE ... SET ('table.datalake.freshness'='30s')` báo `InvalidAlterTableException: The option 'table.datalake.freshness' is not supported to alter yet`.

Issue có tham chiếu #2328. Không có bình luận, không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bật lakehouse.

> **Mức chắc chắn:** cao, và kết luận là **cả hai lỗi đã được sửa trong 1.0.0** (đã xác minh trong mã, chưa chạy thử):
> - Lỗi 2 do [#2365](https://github.com/apache/fluss/pull/2365) *"[lake] Support alter table.datalake.freshness"* (20/01/2026, một ngày trước khi issue được báo, có trong `v0.9.0` và `v1.0.0`).
> - Lỗi 1 do commit `fccf047e` *"[server] Propagate table config updates through TableInfo"* (20/08/2026, có trong 1.0.0): replica leader **đăng ký metric lake** khi `table.datalake.enabled` đổi từ tắt sang bật.
>
> Issue vẫn mở, có lẽ vì các commit này không liên kết tới nó.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`ALTER TABLE ... SET (...)`** | Đổi thuộc tính bảng đã tồn tại. Chỉ các option trong `FlussConfigUtils.ALTERABLE_TABLE_OPTIONS` được phép đổi. |
| **`table.datalake.enabled`** | Bật hoặc tắt việc tier bảng sang lake. |
| **`table.datalake.freshness`** | Độ tươi mục tiêu của lake, tức tier định kỳ bao lâu một lần. |
| **Metric `lakeTiering` (theo bucket)** | Nhóm metric trên **replica leader** của mỗi bucket: số bản ghi chưa tier (`LAKE_PENDING_RECORDS`), độ trễ thời gian (`LOG_LAKE_TIMESTAMP_LAG`), độ trễ bản ghi đang chờ (`LAKE_PENDING_RECORDS_LAG`). |
| **`Replica.updateTableInfo`** | Hàm cập nhật cấu hình bảng trên replica khi bảng bị `ALTER`. |

## Tóm tắt

- **Ở 0.8:**
  - metric `lakeTiering` chỉ được đăng ký **khi replica trở thành leader** và bảng **đã bật** lake. Bật lake **sau đó** bằng `ALTER` thì không ai đăng ký metric;
  - `freshness` không nằm trong danh sách option alter được.
- **Ở `1.0.0`:**
  - `Replica.updateTableInfo` so trạng thái lake cũ và mới. Nếu replica đang là leader và lake vừa **bật** thì `registerLakeTieringMetrics()`; nếu vừa **tắt** thì đóng group metric;
  - `table.datalake.freshness` nằm trong `ALTERABLE_TABLE_OPTIONS`.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server` `replica/Replica` (`onBecomeNewLeader`, `registerLakeTieringMetrics`, `updateTableInfo`), cùng `fluss-common` `config/FlussConfigUtils`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `onBecomeNewLeader()`: `if (isDataLakeEnabled()) { registerLakeTieringMetrics(); }`. Đây là đường duy nhất thời 0.8.
  - `updateTableInfo(TableInfo tableInfo)`:
    ```java
    boolean wasDataLakeEnabled = isDataLakeEnabled();
    this.tableInfo = newTableInfo;
    logTablet.applyTableConfig(newTableInfo.getTableConfig());
    boolean isDataLakeEnabled = isDataLakeEnabled();
    if (isLeader() && wasDataLakeEnabled != isDataLakeEnabled) {
        if (isDataLakeEnabled) { registerLakeTieringMetrics(); }
        else if (lakeTieringMetricGroup != null) { lakeTieringMetricGroup.close(); lakeTieringMetricGroup = null; }
    }
    ```
  - `registerLakeTieringMetrics()` đăng ký `bucketMetricGroup.addGroup("lakeTiering")` với ba gauge `LAKE_PENDING_RECORDS`, `LOG_LAKE_TIMESTAMP_LAG`, `LAKE_PENDING_RECORDS_LAG`.
  - `FlussConfigUtils.ALTERABLE_TABLE_OPTIONS` gồm `TABLE_DATALAKE_ENABLED`, `TABLE_DATALAKE_FRESHNESS`, `TABLE_DATALAKE_AUTO_COMPACTION`, `TABLE_LOG_TTL`, `TABLE_LOG_LOCAL_TTL`, `TABLE_AUTO_PARTITION_NUM_RETENTION`, `TABLE_AUTO_PARTITION_NUM_PRECREATE`...
  - Lịch sử git:
    - `fccf047e` (20/08/2026) là commit đầu tiên có điều kiện `wasDataLakeEnabled != isDataLakeEnabled` trong `Replica.java` (tìm bằng `git log -S`);
    - #2365 có trong tag `v0.9.0-incubating`.

## 2. Khi nào lỗi xảy ra

- **Bản cũ (0.8):** bật lake sau khi tạo bảng, hoặc đổi freshness.
- **Bản `1.0.0`:** theo mã, không còn xảy ra. Một trường hợp mình chưa kiểm tra: follower chưa phải leader lúc `ALTER` mà sau đó **trở thành leader**. Theo `onBecomeNewLeader`, nó sẽ đăng ký metric vì lake đã bật, tức có lẽ vẫn đúng.

## 3. Tác động đến ai, làm sai lệch gì

- **Bản cũ:** không giám sát được tiến độ tier của bảng bật lake bằng `ALTER`, và không chỉnh được freshness.
- **Bản `1.0.0`:** không còn tác động theo mã.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #2422 trong `relate_issue.md` (gộp với #4702) ghi: *"thiếu metric lake | 🟡 | ❌"*. Đối chiếu:
- **Sai về trạng thái:** cả hai lỗi đã được sửa trong 1.0.0. Nên là **✅** (đã xác minh trong mã).

## 5. Mức ảnh hưởng với CDP

- **Không còn ảnh hưởng** trên 1.0.0.
- Có thể dùng `ALTER TABLE` để bật lake hoặc đổi freshness cho bảng đang chạy. Sau khi `ALTER`, kiểm tra metric `lakeTiering` có xuất hiện trên leader của các bucket.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy thử `ALTER TABLE` trên 1.0.0 để xác nhận metric xuất hiện.
- Mình chưa kiểm tra tên metric khi xuất ra Prometheus có đúng là `table_bucket_lakeTiering_*` như issue gọi hay không.
- Mình chưa đọc #2328 (được tham chiếu trong issue).
