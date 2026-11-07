# Issue #5012 — Client gia hạn security token của remote storage liên tục (vòng lặp chặt) khi token đã hết hạn hoặc đồng hồ lệch

Link: https://github.com/apache/fluss/issues/5012 — Phiên bản được báo: `main` (người báo: pbanakar, 02/11/2026). Đây là một phần của issue tổng #4980. Issue nêu công thức tính delay, điều kiện gây vòng lặp và hành vi mong đợi (dùng retry backoff, log cảnh báo lệch đồng hồ, giữ nguyên hành vi với token không có hạn). Không có bình luận. Trạng thái: còn mở. PR [#5013](https://github.com/apache/fluss/pull/5013) **đang mở**.
**Nhóm B (đính chính phạm vi):** token ở đây là **token để client đọc remote storage** (S3, OSS...), **không phải SASL**. Liên quan tới mọi cluster có remote storage cấp token có thời hạn mà client cần đọc trực tiếp.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` ở cả client (`DefaultSecurityTokenManager`) và server (`RpcServiceBase.getFileSystemSecurityToken`, `S3DelegationTokenProvider`). Vòng lặp đúng như issue mô tả. Mình chưa tái hiện, và chưa đo tốc độ request trong vòng lặp.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Security token (filesystem)** | Thông tin xác thực **tạm thời** mà server cấp cho client để client **tự đọc file trên remote storage** (log đã tier, KV snapshot) mà không cần giữ khóa bí mật cố định. Với S3, server dùng STS (`GetSessionToken` hoặc `AssumeRole`) để tạo. |
| **`getFileSystemSecurityToken`** | RPC client gọi server để lấy token. |
| **Gia hạn token (renewal)** | Client lên lịch lấy token mới **trước** khi token cũ hết hạn: delay = `time-ratio × (validUntil − now)`. |
| **`client.filesystem.security.token.renewal.time-ratio`** | Tỷ lệ trên, mặc định **0,75**. |
| **`client.filesystem.security.token.renewal.backoff`** | Thời gian chờ khi lấy token **thất bại**, mặc định **1 giờ**. |
| **Clock skew** | Đồng hồ máy client lệch so với nơi cấp token (STS hoặc server). |
| **Vòng lặp chặt** | Lặp liên tục không có thời gian chờ đáng kể. |

## Tóm tắt

Client tính `renewalDelay = Math.round(0,75 × (validUntil − now))`. Nếu **token đã hết hạn** khi tới tay, hoặc **đồng hồ client chạy nhanh** hơn thời hạn token, thì `validUntil − now ≤ 0`, nên delay ≤ 0. Client **lên lịch lấy token lại ngay lập tức**, và nhận một token mà theo đồng hồ của nó **cũng đã hết hạn**. Cứ thế lặp, nên client **liên tục gọi RPC lấy token** và **ghi log INFO** mỗi vòng.

Trường hợp **lấy token thất bại** thì có backoff (1 giờ). Chỉ trường hợp **lấy thành công nhưng delay ≤ 0** là không có backoff.

## 1. Lỗi nằm ở thành phần nào

- **Module:**
  - `fluss-client`: `token/DefaultSecurityTokenManager`;
  - `fluss-server`: `RpcServiceBase.getFileSystemSecurityToken`;
  - `fluss-filesystems/fluss-fs-s3`: `token/S3DelegationTokenProvider`.
- **Đã xác nhận trong mã `v1.0.0` (client):**
  ```java
  void startTokensUpdate() {
      try {
          Optional<Long> nextRenewal = obtainSecurityTokensAndGetNextRenewal(tokenContainer);
          ...
          if (nextRenewal.isPresent()) {
              long renewalDelay = calculateRenewalDelay(Clock.systemDefaultZone(), nextRenewal.get());
              tokensUpdateFuture = scheduledExecutor.schedule(this::startTokensUpdate, renewalDelay, TimeUnit.MILLISECONDS);
              LOG.info("Tokens update task started with {} ms delay", renewalDelay);
          } ...
      } catch (Exception e) {
          tokensUpdateFuture = scheduledExecutor.schedule(this::startTokensUpdate, renewalRetryBackoffPeriod, TimeUnit.MILLISECONDS);
          ...
      }
  }
  long calculateRenewalDelay(Clock clock, long nextRenewal) {
      long now = clock.millis();
      return Math.round(tokensRenewalTimeRatio * (nextRenewal - now));     // có thể ≤ 0
  }
  ```
  Delay ≤ 0 với `ScheduledExecutorService.schedule`, nên chạy **ngay**. Không có giới hạn dưới.
- **Đã xác nhận trong mã `v1.0.0` (server):**
  - `RpcServiceBase.getFileSystemSecurityToken`: cache token trong `TOKEN_EXPIRATION_TIME_MS = 60 * 1000` (**60 giây**). Hết 60 giây mới `remoteFileSystem.obtainSecurityToken()` lại.
  - Có ghi chú `TODO: add ACL for per-table in #752`.
- **S3:** `S3DelegationTokenProvider` log `"Session credentials obtained successfully with access key: {} expiration: {}"` và dùng `credentials.expiration().toEpochMilli()` làm thời hạn. Thời hạn do **STS** quyết định.
- **Client nào dùng token (xem #4116):** `FlussConnection` tạo `DefaultSecurityTokenManager` cùng `RemoteFileDownloader` khi cần **tải file từ remote storage**. Tài liệu S3 của 1.0.0 xác nhận server dùng `AssumeRole` *"to generate temporary credentials for clients (Flink/Spark connectors) that read tiered data from S3"*.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:
1. Remote storage cấp **token có thời hạn**. Ví dụ S3 với STS. Local filesystem và HDFS cấp token không có hạn, nên không bị.
2. Client cần **đọc remote storage**: job Flink đọc log cũ đã tier, batch scan bảng PK (KV snapshot), union read...
3. **Đồng hồ client chạy nhanh hơn** so với thời hạn của token, đủ để `validUntil − now ≤ 0`. Hoặc token trả về **đã hết hạn**, chẳng hạn khi thời hạn STS rất ngắn.

Với lệch đồng hồ, vòng lặp **không tự dừng**, vì mỗi token mới cũng bị coi là hết hạn.

**Phiên bản:** báo trên `main`. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** client (job Flink, ứng dụng Java) đọc remote storage S3 hoặc tương tự.

**Hậu quả:**
1. **Bão RPC** `getFileSystemSecurityToken` từ client tới server. Server trả token từ cache (rẻ) trong phần lớn thời gian, và gọi STS mỗi 60 giây. Một luồng của client bị chiếm hoàn toàn.
2. **Log INFO tràn ngập** phía client (`"Tokens update task started with {} ms delay"`).
3. Nếu nhiều client cùng lệch đồng hồ (ví dụ cả một node Kubernetes), server và Coordinator có thể bị **quá tải**, ảnh hưởng tới các client khác (suy luận).
4. **Đọc remote có thể lỗi** nếu S3 từ chối token "đã hết hạn" (khi token thật sự hết hạn).

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng nhóm "SASL / security token" trong `relate_issue.md` gom #5012 với #2698 (SASL). Đối chiếu:
- **Sai phạm vi:** #5012 là **token của remote storage**, không liên quan SASL. Nên tách riêng thành nhóm "remote storage S3".
- **❌ đúng** về trạng thái.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu remote storage là **S3 hoặc MinIO** và có job đọc remote. Khả năng cao nếu bật remote log tiering.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Đồng bộ đồng hồ (NTP hoặc chrony)** cho mọi máy chạy client (TaskManager Flink, service) và server. Đây là biện pháp chính.
- **Thời hạn token đủ dài** ở phía S3 hoặc STS. Đừng đặt thời hạn ngắn bất thường.
- **Alert** theo log client `"Tokens update task started with"` xuất hiện dày đặc, hoặc theo số request `getFileSystemSecurityToken` trên server (nếu có metric request theo loại, xem #4139).
- Nếu gặp: sửa đồng hồ rồi restart client.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện, và chưa đo tốc độ vòng lặp.
- Mình chưa kiểm tra MinIO trả thời hạn token thế nào (STS của MinIO).
- Mình chưa kiểm tra OSS, GS và Azure có cùng hành vi không.
- Mình chưa đọc diff của PR #5013.
