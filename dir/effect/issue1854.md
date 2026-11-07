# Issue #1854 — Container Docker của Fluss chạy bằng user root

Link: https://github.com/apache/fluss/issues/1854 — Phiên bản được báo: `0.7.0` (người báo: loserwang1024, 23/10/2025). Issue có 8 bình luận thảo luận cách sửa:
- dùng `gosu` trong `docker-entrypoint.sh` để chuyển sang user `fluss`;
- gặp vấn đề: thư mục do volume mount tạo ra thuộc root, nên user `fluss` không ghi được;
- đề xuất: `chown -R fluss:fluss .` trong entrypoint rồi `exec gosu fluss:fluss ...`, hoặc mount `data.dir` và `remote.data.dir` vào thư mục của Fluss;
- maintainer đồng ý (*"sounds good to me"*), nhưng **chưa có PR**.

Trạng thái: còn mở.

> **Mức chắc chắn:** cao. Mình đã đọc `docker/fluss/Dockerfile`, `docker-entrypoint.sh` và Helm chart của `v1.0.0`. Khi đọc chart, mình thấy thêm **hai mặc định nguy hiểm hơn cả việc chạy root**, dù không thuộc issue này: dữ liệu TabletServer nằm trên `emptyDir`, và `remote.data.dir` trỏ vào đĩa local của pod (mục 3). Mình chưa thử chạy với user không phải root.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **root** | User có toàn quyền trong container (UID 0). Process chạy root mà bị khai thác thì kẻ tấn công có quyền cao nhất trong container, và dễ thoát ra máy chủ hơn. |
| **Chỉ thị `USER` (Dockerfile)** | Chọn user để chạy process chính. Không có `USER` thì mặc định là root. |
| **`gosu`** | Công cụ nhỏ cho script entrypoint (chạy root) **chuyển sang user thường** rồi `exec` process chính. |
| **`chown`** | Đổi chủ sở hữu file hoặc thư mục. |
| **`securityContext` (Kubernetes)** | Cấu hình bảo mật của pod hoặc container: `runAsUser`, `runAsNonRoot`, `fsGroup`... **Pod Security Standards** mức `restricted` yêu cầu `runAsNonRoot: true`. |
| **`fsGroup`** | Kubernetes đổi group của volume mount sang group này, để user không phải root ghi được. |
| **`emptyDir`** | Volume tạm của Kubernetes, **bị xóa khi pod bị xóa hoặc tạo lại**. |
| **PV / PVC (`volumeClaimTemplates`)** | Ổ đĩa bền vững gắn với pod StatefulSet. Pod tạo lại trên node khác vẫn giữ dữ liệu. |

## Tóm tắt

Image `fluss` của 1.0.0 tạo user `fluss` (UID và GID **9999**) và `chown` thư mục `/opt/fluss` cho user đó, nhưng **không có `USER`**, và entrypoint **chạy thẳng** `coordinator-server.sh` hoặc `tablet-server.sh` bằng `exec`, không `gosu`. Vì vậy Coordinator và TabletServer **chạy bằng root**. Thư mục `/tmp/fluss...` cũng thuộc root. Helm chart 1.0.0 **không đặt `securityContext`** nào. Hướng sửa đã được thống nhất trong bình luận nhưng chưa ai làm.

## 1. Lỗi nằm ở thành phần nào

- **Thành phần:** `docker/fluss/Dockerfile`, `docker/fluss/docker-entrypoint.sh`, `helm/templates/sts-*.yaml`.
- **Đã xác nhận trong `v1.0.0`:**
  - `Dockerfile`:
    ```
    FROM eclipse-temurin:17-jre-noble AS builder
    RUN groupadd --system --gid=9999 fluss && useradd --system --home-dir $FLUSS_HOME --uid=9999 --gid=fluss fluss
    COPY --chown=fluss:fluss build-target/ /opt/fluss/
    RUN ["chown", "-R", "fluss:fluss", "."]
    COPY docker-entrypoint.sh /
    ENTRYPOINT ["/docker-entrypoint.sh"]
    ```
    Không có `USER`.
  - `docker-entrypoint.sh`: sửa `server.yaml` (thêm `FLUSS_PROPERTIES`, `envsubst`), rồi `exec "$FLUSS_HOME/bin/tablet-server.sh" start-foreground ...`. Không `gosu`, không `su`.
  - So sánh: image `docker/fluss-gateway/Dockerfile` có `USER 9999:9999`.
  - Helm chart: không có `securityContext`, `runAsUser` hay `fsGroup` trong `values.yaml` và các template StatefulSet.
  - `main`: không có commit nào sửa `docker/fluss` sau 1.0.0.

## 2. Khi nào lỗi xảy ra

Mọi lần chạy image `fluss` chính thức (Docker, docker-compose, Helm chart).

**Phiên bản:** báo trên `0.7.0`. Còn ở `1.0.0` và `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Hậu quả của issue:**
1. **Bảo mật:** vi phạm nguyên tắc quyền tối thiểu. Lỗ hổng trong Fluss hoặc thư viện (Netty, RocksDB JNI...) bị khai thác thì kẻ tấn công có root trong container.
2. **Không triển khai được** trên cluster Kubernetes áp Pod Security Standards mức `restricted`, hoặc có chính sách kiểu OPA/Kyverno "cấm chạy root". Pod bị từ chối.
3. Không ảnh hưởng chức năng.

**Phát hiện thêm khi đọc Helm chart 1.0.0 (ngoài issue, nhưng quan trọng hơn cho production):**
- `values.yaml`: `tablet.storage.enabled: false` (`size: 1Gi`). Khi tắt, `sts-tablet.yaml` gắn `data` bằng **`emptyDir: {}`** vào `/tmp/fluss/data`, cũng là `data.dir: /tmp/fluss/data` trong `values.yaml`. Vì vậy **mặc định, mỗi lần pod TabletServer bị xóa hoặc tạo lại, toàn bộ dữ liệu local (log, KV) mất**. Đó đúng là kịch bản của #4566 (KV không recover được khi mất toàn bộ đĩa local) và tình huống (B) của #596.
- `values.yaml`: `remote.data.dir: /tmp/fluss/remote-data`, một đường dẫn **local trong pod**, không phải S3 hay HDFS dùng chung. KV snapshot và remote log nằm trên đĩa tạm của từng pod, nên **không dùng được để phục hồi** khi mất pod. Mình suy ra từ cấu hình, chưa kiểm tra chart có ghi đè giá trị này ở chỗ khác không.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #1854 trong `relate_issue.md` ghi: *"Container chạy bằng root | 0.7.0 | ❔ | Đặt `securityContext.runAsUser`"*. Đối chiếu:
- **Nên là ❌** (đã xác nhận trong mã 1.0.0), không phải ❔.
- **Biện pháp `runAsUser` cần thêm `fsGroup`** và kiểm tra quyền ghi thư mục, vì đây chính là khó khăn mà bình luận trong issue đã gặp (mục 5).
- **Nên bổ sung vào `relate_issue.md`:** cảnh báo về `tablet.storage.enabled: false` và `remote.data.dir` local trong Helm chart mặc định.

## 5. Mức ảnh hưởng với CDP

- CDP chứa **dữ liệu cá nhân khách hàng**, nên yêu cầu bảo mật, audit và cấu hình cluster doanh nghiệp thường **bắt buộc** container không chạy root.
- Quan trọng hơn: nếu triển khai bằng Helm chart với giá trị mặc định thì **mất dữ liệu khi pod restart**.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Bắt buộc khi dùng Helm:**
  - `tablet.storage.enabled: true` với `size` và `storageClass` phù hợp (PV bền), và tương tự cho coordinator nếu cần (`values.yaml` dòng 125 có khối `storage` thứ hai);
  - `remote.data.dir` trỏ tới **S3 hoặc HDFS dùng chung** (cần plugin filesystem tương ứng, xem #4116 và #4072).
- **Chạy không root:**
  ```yaml
  securityContext:          # cấp pod
    runAsUser: 9999
    runAsGroup: 9999
    fsGroup: 9999           # để PV gắn vào ghi được bởi user fluss
    runAsNonRoot: true
  ```
  UID và GID 9999 lấy từ Dockerfile. Chart 1.0.0 không có chỗ khai báo sẵn, nên cần sửa template hoặc dùng post-renderer hay kustomize.
- Kiểm tra mọi thư mục ghi được: `data.dir`, thư mục log, `/tmp` dùng cho RocksDB và tải remote log. Nếu thiếu quyền, có thể dùng `initContainer` chạy root để `chown`, hoặc tự build image thêm `USER fluss`.
- **Test khởi động và restart pod** với cấu hình không root trước khi go-live.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy Fluss bằng UID 9999 để xem thư mục nào thiếu quyền.
- Mình chưa kiểm tra các thư mục tạm mà RocksDB, Arrow hoặc tải remote log dùng (có ghi vào `/tmp` ngoài `data.dir` không).
- Mình chưa kiểm tra chart 1.0.0 có cảnh báo hay tài liệu nào về `tablet.storage.enabled` và `remote.data.dir` cho production hay không.
