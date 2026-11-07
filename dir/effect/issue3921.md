# Issue #3921 — Tiering kẹt sau khi Coordinator failover, vì job không tự restart khi epoch của Coordinator thay đổi

Link: https://github.com/apache/fluss/issues/3921 — Phiên bản được báo: `0.9.0` (người báo: Shawn-Hx, 10/08/2026). Issue ngắn, nêu rõ cơ chế và hướng sửa. Không có bình luận. Trạng thái: còn mở. PR [#3923](https://github.com/apache/fluss/pull/3923) **đang mở**.
**Nhóm B:** chỉ khi bật lakehouse tiering.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` ở cả enumerator của job tiering và `CoordinatorService.lakeTieringHeartbeat`:
> - epoch Coordinator chỉ được ghi **một lần** lúc đăng ký;
> - Coordinator từ chối **từng bảng** khi epoch không khớp, nhưng chỉ ghi lỗi vào phản hồi, và enumerator không phản ứng.
>
> Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Coordinator epoch** | Số tăng mỗi lần có **Coordinator leader mới** (sau failover hoặc nâng cấp). Dùng để từ chối yêu cầu gửi theo "nhiệm kỳ" cũ. |
| **Tiering epoch** | Khác với coordinator epoch: số nhiệm kỳ của **mỗi lần giao bảng** cho job tiering (xem #2648). |
| **`TieringSourceEnumerator`** | Thành phần của job tiering (chạy trên JobManager Flink). Nó đăng ký với Coordinator, xin bảng để tier, giao split cho task, và gửi heartbeat định kỳ. |
| **Heartbeat tiering** | Yêu cầu `lakeTieringHeartbeat` gửi các bảng đang tier, đã xong hoặc thất bại, mỗi bảng kèm `coordinatorEpoch` và `tieringEpoch`. |
| **`InvalidCoordinatorException`** | Lỗi Coordinator trả khi epoch trong yêu cầu không khớp epoch hiện tại. |
| **`failuresTotal`** | Metric số lần tiering thất bại. |

## Tóm tắt

Khi job tiering khởi động, enumerator gửi heartbeat đăng ký và **lưu `coordinatorEpoch`** từ phản hồi. Sau đó nó **không cập nhật nữa**.

Khi **Coordinator failover hoặc nâng cấp**, epoch mới lớn hơn. Mọi heartbeat sau đó mang epoch cũ cho từng bảng. Coordinator mới kiểm tra **từng bảng**, ném `InvalidCoordinatorException`, rồi **ghi lỗi vào phản hồi của bảng đó** (không làm hỏng cả yêu cầu). Enumerator không xử lý lỗi này, nên:
- Coordinator không nhận được heartbeat **gia hạn** cho bảng đang tier, và sau 2 phút coi là timeout, đưa về PENDING;
- báo cáo "xong" hoặc "thất bại" bị từ chối;
- `failuresTotal` tăng mãi.

**Tiering dừng** cho tới khi job tiering restart. Đề xuất sửa: kiểm tra epoch trong mỗi phản hồi, và nếu khác thì ném lỗi để **Flink failover job**, rồi đăng ký lại với epoch mới.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink` (tiering): `tiering/source/enumerator/TieringSourceEnumerator`. Phía server: `fluss-server` `coordinator/CoordinatorService.lakeTieringHeartbeat`.
- **Đã xác nhận trong mã `v1.0.0` (enumerator):**
  ```java
  LakeTieringHeartbeatResponse heartbeatResponse = waitHeartbeatResponse(coordinatorGateway.lakeTieringHeartbeat(basicHeartBeat()));
  this.flussCoordinatorEpoch = heartbeatResponse.getCoordinatorEpoch();
  LOG.info("Register Tiering Service to Fluss Coordinator(epoch={}) success.", flussCoordinatorEpoch);
  ```
  - Đây là chỗ **duy nhất** gán `flussCoordinatorEpoch`.
  - Các heartbeat định kỳ (`requestTieringTableSplitsViaHeartBeat`) và báo cáo bảng thất bại (`reportFailedTable(..., flussCoordinatorEpoch)`) đều dùng giá trị này.
  - Kết quả của `waitHeartbeatResponse(...)` trong vòng định kỳ chỉ được dùng để lấy bảng mới (`hasTieringTable()`). Mình không thấy chỗ nào kiểm tra lỗi theo bảng hay so epoch.
- **Đã xác nhận trong mã `v1.0.0` (Coordinator):**
  - `lakeTieringHeartbeat`: `heartbeatResponse.setCoordinatorEpoch(currentCoordinatorEpoch)`. Với mỗi bảng (failed, tiering, finished), gọi `validateHeartbeatRequest(table, currentCoordinatorEpoch)` trong `try` và đưa lỗi vào `pbHeartbeatRespForTable.setError(...)`.
  - `validateHeartbeatRequest`: `if (req.getCoordinatorEpoch() != currentEpoch) throw new InvalidCoordinatorException("The coordinator epoch %s in request is not match current coordinator epoch %d for table %d.")`.
  - Vì lỗi bị bắt theo từng bảng, `renewTieringHeartbeat` và `finishTableTiering` **không** được gọi. Bảng sẽ timeout sau `TIERING_SERVICE_TIMEOUT_MS` (2 phút, xem #2648).

## 2. Khi nào lỗi xảy ra

- Bật lakehouse tiering, job tiering đang chạy.
- **Coordinator leader đổi**: failover do sự cố, rolling upgrade Coordinator, hoặc mất ZooKeeper session rồi bầu lại (#4362, #5038).

**Phiên bản:** báo trên `0.9.0`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người dùng dữ liệu lake (analytics, BI) và tính năng phụ thuộc tiến độ lake.

**Hậu quả:**
1. **Tiering dừng:** lake ngày càng cũ, độ tươi (`table.datalake.freshness`) vượt xa mục tiêu.
2. **Lỗi lặp lại:** `failuresTotal` tăng, log lỗi trên Coordinator và job.
3. **Gián tiếp:** từ 1.0.0, việc dọn log hoặc giữ WAL lịch sử có thể dựa vào tiến độ lake (ví dụ `main` có #4487 *"Advance historical WAL retention with lake progress"*). Tiering kẹt thì log có thể bị **giữ lâu hơn**. Mình chưa kiểm tra cơ chế này trong 1.0.0.
4. **Không ảnh hưởng dữ liệu trong Fluss.** Lookup và ghi bình thường.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #3921 trong `relate_issue.md` ghi: *"tiering kẹt sau khi Coordinator failover | 🟠 | ❌ PR #3923 chưa merge"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** Coordinator **có** gửi epoch mới trong mọi phản hồi heartbeat, chỉ là enumerator không đọc. Restart job tiering là đủ để phục hồi.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu CDP bật tiering sang lake. Mỗi lần nâng cấp hoặc failover Coordinator, tiering sẽ dừng.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Đưa "restart job tiering" vào runbook** sau mỗi lần nâng cấp hoặc failover Coordinator.
- **Alert:**
  - độ tươi của lake (thời điểm snapshot lake mới nhất của từng bảng) vượt N lần `freshness`;
  - `failuresTotal` tăng liên tục;
  - log Coordinator `"The coordinator epoch ... is not match current coordinator epoch"`.
- Cấu hình restart strategy của job tiering sao cho khi được restart thủ công hoặc tự động thì nó đăng ký lại đúng (đăng ký diễn ra lúc enumerator khởi động).
- Có thể viết một watchdog: so epoch Coordinator hiện tại (ví dụ đọc ZooKeeper) với epoch mà job tiering đã log khi đăng ký. Nếu khác thì restart job.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện việc tiering kẹt sau failover Coordinator.
- Mình chưa kiểm tra job có thể vô tình **xin được bảng mới** sau failover hay không. Yêu cầu xin bảng (`basicHeartBeat` cộng `setRequestTable`) không mang epoch theo bảng, nên có thể vẫn nhận được bảng, nhưng báo cáo "xong" sau đó sẽ bị từ chối. Đây là suy luận.
- Mình chưa đọc diff của PR #3923.
