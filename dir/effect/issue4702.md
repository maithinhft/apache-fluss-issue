# Issue #4702 — Job tiering bị dừng có thể làm mất báo cáo "bảng đã tier xong", khiến vòng tiering kế tiếp bị trễ

Link: https://github.com/apache/fluss/issues/4702 — Phiên bản được báo: `1.0.0` (người báo: yangshangqing95, 18/10/2026). Issue ngắn, nêu cơ chế, hậu quả và cách tái hiện xác định (xử lý `FinishedTieringEvent` rồi đóng enumerator trước heartbeat kế tiếp), kèm link một lần CI lỗi. Không có bình luận. Trạng thái: còn mở. PR gắn kèm [#4703](https://github.com/apache/fluss/pull/4703) đã **bị đóng**. Tuy vậy, commit [#4663](https://github.com/apache/fluss/pull/4663) *"[flink] Flush finished tiering tables when closing enumerator"* đã **merge vào `main` ngày 23/10/2026** (commit `b620397e`, liên kết với issue #4233) và làm đúng việc này. Commit đó **không có trong 1.0.0**.
**Nhóm B:** chỉ khi bật lakehouse tiering.

> **Mức chắc chắn:** cao. Mình đã đọc `TieringSourceEnumerator.close()` trong `v1.0.0` và đọc diff của #4663 trên `main`. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **`TieringSourceEnumerator`** | Thành phần điều phối của job tiering (trên JobManager Flink), gửi heartbeat và báo cáo tiến độ cho Coordinator (xem #3921). |
| **`FinishedTieringEvent`** | Sự kiện từ committer: "bảng X đã tier xong vòng này". Enumerator ghi bảng vào `finishedTables` và **chờ heartbeat kế tiếp** để báo Coordinator. |
| **`tieringTableEpochs` / `failedTableEpochs` / `finishedTables`** | Ba danh sách trong enumerator: bảng đang tier, bảng thất bại, bảng đã xong (chưa báo). |
| **Tiering timeout** | Coordinator không nhận báo cáo trong `TIERING_SERVICE_TIMEOUT_MS` (2 phút) thì coi là thất bại và đưa bảng về PENDING (xem #2648). |
| **Bảng không sinh split** | Vòng tiering mà bảng không có dữ liệu mới. Bảng được đánh dấu xong gần như ngay lập tức. |

## Tóm tắt

Khi một bảng tier xong, enumerator **gỡ bảng khỏi `tieringTableEpochs`**, đưa nó vào `finishedTables`, và đợi heartbeat kế tiếp mới báo Coordinator. Nếu job tiering bị **cancel hoặc dừng trước** heartbeat đó, `close()` chỉ báo các bảng **thất bại** (gồm các bảng đang tier, được coi là thất bại), **bỏ qua `finishedTables`**. Coordinator tiếp tục coi bảng đó "đang tier" cho tới khi hết timeout 2 phút, nên vòng tiering kế tiếp của bảng **bị trễ**.

Thêm một điểm trong mã 1.0.0: nếu báo cáo thất bại ném lỗi, các bước đóng tài nguyên phía sau (`rpcClient.close()`) **không chạy**.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink` (tiering), `tiering/source/enumerator/TieringSourceEnumerator.close()`.
- **Đã xác nhận trong mã `v1.0.0`:**
  ```java
  public void close() throws IOException {
      closed = true;
      timerService.shutdownNow();
      if (rpcClient != null) {
          failedTableEpochs.putAll(tieringTableEpochs);    // bảng đang tier -> coi là failed
          tieringTableEpochs.clear();
          if (!failedTableEpochs.isEmpty()) {
              reportFailedTable(basicHeartBeat(), failedTableEpochs);   // ném FlinkRuntimeException nếu lỗi
          }
          try { rpcClient.close(); } catch (Exception e) { ... }
      }
      ... flussAdmin.close(); connection.close(); ...
  }
  ```
  - Không có dòng nào báo `finishedTables`.
  - `reportFailedTable` ném `FlinkRuntimeException` khi heartbeat lỗi. Lỗi ném ra **trước** `rpcClient.close()`.
- **Diff của #4663 trên `main`** (đã đọc):
  - Thêm kiểm tra `if (closed) return;`, để lặp lại thì không làm gì.
  - Gửi cả `finishedTables` hiện có trong heartbeat cuối, với comment: *"The finished tables have to be flushed here: a table that produced no splits is removed from the in-flight epochs as soon as it is marked as finished ... Without flushing it explicitly, its completion would never reach the coordinator and the table remains in the Tiering state until the tiering service timeout expires."*
  - Dùng `IOUtils` để đảm bảo tài nguyên vẫn được đóng khi heartbeat cuối lỗi (theo mô tả commit: *"Ensure resources are closed even if the final heartbeat fails."*).

## 2. Khi nào lỗi xảy ra

- Job tiering bị **cancel, stop, failover hoặc nâng cấp** ngay sau khi một hoặc nhiều bảng tier xong, trước heartbeat kế tiếp.
- Rõ nhất với bảng **không có dữ liệu mới** trong vòng đó, vì chúng "xong" gần như ngay khi được giao.

**Phiên bản:** báo trên `1.0.0`. Fix chỉ có trên `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người dùng dữ liệu lake, khi job tiering hay bị restart.

**Hậu quả:**
1. **Lake trễ thêm** khoảng 2 phút (một tiering timeout) cho mỗi bảng bị mất báo cáo.
2. **Rò kết nối hoặc tài nguyên** trên JobManager nếu heartbeat cuối lỗi (trường hợp phụ, suy ra từ mã).
3. **Không mất hay trùng dữ liệu:** snapshot lake đã commit. Chỉ Coordinator chưa biết. Khi bảng hết timeout, Coordinator đưa về PENDING rồi giao lại, và missing-snapshot check sẽ phát hiện snapshot đã có (xem #2648).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4702 trong `relate_issue.md` (gộp với #2422) ghi: *"mất báo cáo tiering, thiếu metric lake | 🟡 | ❌"*. Đối chiếu:
- **Nên là ⏭️**: fix đã có trên `main` (#4663, 23/10/2026). 1.0.0 vẫn còn lỗi.

## 5. Mức ảnh hưởng với CDP

- **Thấp.** Chỉ làm lake trễ thêm vài phút khi job tiering restart.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- Tránh restart job tiering thường xuyên. Chấp nhận lake trễ thêm một chu kỳ sau mỗi lần restart.
- Nếu tự build: cherry-pick #4663. Thay đổi nằm gọn trong `TieringSourceEnumerator` và có kèm test.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa kiểm tra Coordinator xử lý thế nào khi bảng đã có snapshot lake mới mà vẫn ở trạng thái "đang tier" tới timeout.
