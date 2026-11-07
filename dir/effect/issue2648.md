# Issue #2648 — Hai job tiering có thể cùng commit vào lake (trùng dữ liệu) khi heartbeat tới Coordinator bị timeout

Link: https://github.com/apache/fluss/issues/2648 — Phiên bản được báo: `0.8.0`, môi trường thử là 0.10-SNAPSHOT (người báo: litiliu, 11/02/2026). Issue có sơ đồ trình tự (mermaid), kịch bản và tác động. Có hai bình luận của maintainer:
- thừa nhận đây là *"corner case"*, và muốn chặn triệt để thì cần lake hỗ trợ **commit có điều kiện** (Iceberg issue 6514);
- đề xuất giảm thiểu: enumerator phát hiện epoch bị fence qua heartbeat thì **hủy ngay** việc đồng bộ đang chạy. Cách này đã bàn lúc thiết kế nhưng chưa làm.

Không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ liên quan khi bật **lakehouse tiering** (Paimon, Iceberg, Lance, Hudi).

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` ở cả Coordinator (`LakeTableTieringManager`) và job tiering (`TieringCommitOperator`). Thứ tự trong mã khớp với kịch bản của issue: bước commit vào lake **không mang** tiering epoch, và epoch chỉ được kiểm tra **sau** khi đã ghi vào lake. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Lakehouse tiering** | Fluss định kỳ đẩy (tier) dữ liệu sang data lake (Paimon, Iceberg...) để lưu lâu dài và cho Spark, Trino... truy vấn. Việc này do một **job Flink riêng** (tiering service, `fluss-flink-tiering`) làm. |
| **Một vòng tiering (round)** | Coordinator giao bảng T cho một job. Job đọc log Fluss từ offset đã tier tới offset mới, ghi file vào lake, rồi **commit một snapshot** vào lake. Sau đó báo Coordinator. |
| **Tiering epoch** | Số "nhiệm kỳ" mỗi lần Coordinator giao bảng cho một job. Giao lại thì epoch tăng, và yêu cầu mang epoch cũ bị từ chối (`FencedTieringEpochException`). |
| **Heartbeat tiering** | Job định kỳ báo Coordinator "tôi vẫn đang tier bảng T". Quá `TIERING_SERVICE_TIMEOUT_MS` (**2 phút**) không có heartbeat thì Coordinator coi job đã chết và đưa bảng về PENDING để giao cho job khác. |
| **Zombie job** | Job bị coi là chết (mất heartbeat) nhưng thật ra vẫn chạy và vẫn ghi được vào lake. |
| **Missing-snapshot check** | Trước khi commit, job hỏi Fluss "snapshot lake mới nhất Fluss biết là gì", rồi so với lake, để không commit trùng khi Fluss **bỏ lỡ** một snapshot đã có trong lake. |
| **Commit có điều kiện** | Lake chỉ chấp nhận commit nếu snapshot hiện tại **vẫn là** X. Iceberg chưa hỗ trợ (theo bình luận). |

## Tóm tắt

1. Job A tier bảng T với epoch 1, rồi mất kết nối heartbeat tới Coordinator. Kết nối tới lake vẫn bình thường.
2. Sau 2 phút, Coordinator coi A đã chết, đưa T về PENDING, và giao cho job B với epoch 2.
3. A và B **cùng** chạy missing-snapshot check. Lúc đó chưa ai commit, nên cả hai đều thấy "không thiếu gì".
4. **Cả hai cùng commit** vào lake. Lake có hai snapshot chứa **cùng một khoảng dữ liệu**, tức trùng.
5. Khi A báo Coordinator, nó bị từ chối vì epoch cũ. Nhưng lúc đó dữ liệu **đã nằm trong lake**.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-server`: `coordinator/LakeTableTieringManager`;
  - `fluss-flink` (tiering): `tiering/committer/TieringCommitOperator`, `FlussTableLakeSnapshotCommitter`, `tiering/source/enumerator/TieringSourceEnumerator`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `LakeTableTieringManager`:
    - `protected static final long TIERING_SERVICE_TIMEOUT_MS = 2 * 60 * 1000; // 2 minutes`, không cấu hình được;
    - luồng `fluss-lake-tiering-timeout-checker` kiểm tra `currentTime - lastHeartbeat >= TIERING_SERVICE_TIMEOUT_MS`, rồi log *"The lake tiering service for table {}({}) is timeout, change it to PENDING."*;
    - khi giao lại, *"increase tiering epoch"*. `validateTieringServiceRequest(tableId, tieringEpoch)` ném `FencedTieringEpochException` nếu epoch không khớp. Hàm này được gọi ở các yêu cầu từ tiering service (khoảng dòng 449, 495, 507: heartbeat, finish, fail).
  - `TieringCommitOperator.commitWriteResults(...)`, theo thứ tự:
    ```java
    LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
    checkFlussNotMissingLakeSnapshot(...);                        // so lake với Fluss
    String file = flussTableLakeSnapshotCommitter.prepareLakeSnapshot(...);
    LakeCommitResult r = lakeCommitter.commit(committable, snapshotProperties);   // GHI VÀO LAKE
    flussTableLakeSnapshotCommitter.commit(tableId, tablePath, r, ...);           // rồi mới báo Fluss
    ```
    Bước `lakeCommitter.commit` không mang tiering epoch nào, và không có bước nào hỏi Coordinator "tôi còn giữ bảng này không" ngay trước khi commit.
- **Trên `main`**, mình thấy vài thay đổi liên quan tới tiering, nhưng **không** có fence epoch trước khi commit vào lake:
  - #4445 *"Handle duplicate tiering completion reports idempotently"*: xử lý báo cáo trùng phía Coordinator, không ngăn ghi trùng vào lake (suy ra từ tên commit);
  - #5031 *"Commit tiering tables concurrently"*;
  - #5065 *"Surface repeated tiering commit failures"*.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. Bật lakehouse tiering.
2. Kết nối heartbeat giữa job tiering và Coordinator **chập chờn hơn 2 phút**, trong khi job **vẫn truy cập được lake**. Ví dụ: mạng phân vùng, Coordinator failover (xem #3921 về epoch Coordinator), GC dài của JobManager.
3. Job thứ hai được giao bảng và **commit gần như cùng lúc** với job cũ.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người dùng dữ liệu **trong lake** (Spark, Trino, BI, ML) của bảng bị tier trùng.

**Hậu quả:**
1. **Dữ liệu trùng trong lake.** Với **bảng log (append-only)**, cùng bản ghi xuất hiện hai lần, và exactly-once bị vi phạm (đúng như issue nói). Với **bảng PK**, merge theo khóa trong lake (Paimon) thường che bớt hậu quả, nhưng Iceberg (position delete) thì mình chưa đánh giá (suy luận).
2. **Không ảnh hưởng dữ liệu trong Fluss:** lookup của AI agent và changelog vẫn đúng.
3. **Union read** (đọc lake cộng log) có thể thấy bản ghi trùng của bảng log (suy luận).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #2648 trong `relate_issue.md` ghi: *"Lakehouse tiering (chung) | commit trùng khi heartbeat timeout | 🔴 | ❌"*. Đối chiếu:
- **Đúng** về trạng thái và cơ chế.
- **Mức 🔴 nên đọc là "nếu dùng lake cho bảng log mà downstream cần chính xác".** Nó không ảnh hưởng dữ liệu phục vụ trực tiếp trong Fluss.

## 5. Mức ảnh hưởng với CDP

- Chỉ liên quan nếu CDP bật tiering sang lake. Ví dụ để analytics hoặc BI đọc lịch sử sự kiện và profile từ Paimon hay Iceberg.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Chạy tiering service ổn định**, đặt gần Coordinator về mạng, theo dõi GC của JobManager.
- **Alert theo log Coordinator** `"The lake tiering service for table ... is timeout, change it to PENDING"`. Mỗi dòng là một lần có nguy cơ hai job cùng tier. Nếu thấy, kiểm tra snapshot lake của bảng đó quanh thời điểm đó.
- **Chạy một tiering job cho mỗi cluster** nếu tải cho phép, để hạn chế việc giao lại cho job khác. Mình chưa kiểm tra job cũ có thể nhận lại bảng hay không.
- Với bảng **log** cần chính xác ở lake: khử trùng ở tầng truy vấn hoặc ETL, theo (bucket, offset) nếu có lưu, hoặc theo khóa nghiệp vụ.
- Ưu tiên **bảng PK** cho dữ liệu cần tier chính xác.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện.
- Mình chưa kiểm tra job tiering cũ (zombie) có dừng ghi khi nhận `FencedTieringEpochException` từ heartbeat hay không. Bình luận của maintainer nói việc này **chưa làm**.
- Mình chưa kiểm tra hậu quả cụ thể với bảng PK trên Iceberg và Paimon khi commit trùng.
- Mình chưa xác định chính xác ba lời gọi `validateTieringServiceRequest` tương ứng với yêu cầu nào.
