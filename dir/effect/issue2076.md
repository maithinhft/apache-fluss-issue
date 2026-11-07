# Issue #2076 — Helm chart chỉ quảng bá địa chỉ CLIENT, còn địa chỉ INTERNAL thì dùng Pod IP thô

Link: https://github.com/apache/fluss/issues/2076 — Phiên bản được báo: `0.8.0` (người báo: litiliu, 02/12/2025). Issue chỉ trích hai dòng trong template Helm và nói `advertised.listeners` *"chỉ có địa chỉ CLIENT"*. Có hai bình luận: một bình luận tag người khác, và một maintainer hỏi lại *"chính xác thì vấn đề là gì? Bạn vẫn truy cập Fluss qua client được mà"*. Người báo **chưa trả lời**. Không có PR. Trạng thái: còn mở.

> **Mức chắc chắn:** cao về **hiện trạng cấu hình**. Mình đã đọc Helm chart của `v1.0.0` và `main`, và đọc mã `Endpoint.loadAdvertisedEndpoints` để biết địa chỉ nào thực sự được đăng ký. Người báo **không nêu hậu quả**. Hậu quả mình nêu ở mục 3 dựa trên mã và **một quan sát của người dùng 1.0.0 ở issue #4256**. Mình **chưa tái hiện**, và chưa chứng minh được việc quảng bá Pod IP là nguyên nhân của quan sát đó.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Helm chart** | Bộ mẫu để cài Fluss lên Kubernetes (thư mục `helm/` trong mã nguồn). |
| **Listener** | Một cổng mạng có tên mà server lắng nghe. Chart dùng hai listener: **INTERNAL** (server nói chuyện với nhau, ví dụ follower fetch từ leader, TabletServer gọi Coordinator) và **CLIENT** (Flink, ứng dụng, AI agent kết nối vào). `internal.listener.name: INTERNAL` trong `values.yaml` chọn listener dùng cho giao tiếp nội bộ. |
| **`bind.listeners`** | Địa chỉ mà server **thực sự mở cổng** lắng nghe. |
| **`advertised.listeners`** | Địa chỉ mà server **đăng ký vào ZooKeeper** để người khác dùng khi kết nối tới nó. |
| **Pod IP** | IP của pod Kubernetes. **Đổi mỗi khi pod bị tạo lại** (restart, nâng cấp, chuyển node). |
| **Headless service DNS** | Tên DNS ổn định cho từng pod của StatefulSet, ví dụ `tablet-server-0.tablet-server-hs.<ns>.svc.cluster.local`. **Không đổi** khi pod restart. Tên luôn trỏ tới IP hiện tại của pod. |
| **Metadata cache (server)** | Bản sao danh sách server và địa chỉ mà mỗi TabletServer giữ trong bộ nhớ, được Coordinator cập nhật. |
| **`ServerConnection` / `NettyClient`** | Lớp quản lý kết nối RPC. Kết nối được cache **theo server ID** (xem #4256). |

## Tóm tắt

Trong Helm chart, mỗi pod khởi động với:
```
bind.listeners:       INTERNAL://${POD_IP}:9123, CLIENT://${POD_IP}:9124
advertised.listeners: CLIENT://${POD_NAME}.tablet-server-hs.${POD_NAMESPACE}.svc.cluster.local:9124
```
Mã Fluss lấy **địa chỉ bind** cho mọi listener không có trong `advertised.listeners`. Vì vậy địa chỉ **INTERNAL** mà các server dùng để gọi nhau là **Pod IP thô**, đổi mỗi khi pod restart. Còn địa chỉ CLIENT dùng tên DNS ổn định.

Hệ quả là sau mỗi lần restart pod, các server khác phải nhận được metadata mới mới biết IP mới. Nếu chúng còn giữ IP cũ thì kết nối nội bộ thất bại. Một người dùng 1.0.0 (bình luận ở #4256) quan sát đúng hiện tượng này: replica fetcher gọi IP cũ khoảng **10 phút** sau khi TabletServer restart, và trong thời gian đó server vừa restart nằm ngoài ISR.

## 1. Lỗi nằm ở thành phần nào

- **Thành phần:** `helm/templates/sts-tablet.yaml`, `helm/templates/sts-coordinator.yaml`, và trong mã: `fluss-common` `cluster/Endpoint.loadAdvertisedEndpoints`.
- **Đã xác nhận trong `v1.0.0` (Helm):**
  - `sts-tablet.yaml`:
    ```
    BIND_LISTENERS="INTERNAL://${POD_IP}:{{ .Values.listeners.internal.port }}, CLIENT://${POD_IP}:{{ .Values.listeners.client.port }}"
    ADVERTISED_LISTENERS="CLIENT://${POD_NAME}.tablet-server-hs.${POD_NAMESPACE}.svc.cluster.local:{{ .Values.listeners.client.port }}"
    ```
  - `sts-coordinator.yaml` cũng bind INTERNAL bằng `${POD_IP}` theo cùng cách.
  - `values.yaml` có `internal.listener.name: INTERNAL`.
- **Đã xác nhận trong mã `v1.0.0`:** `Endpoint.loadAdvertisedEndpoints(bindEndpoints, conf)` duyệt mọi **bind** endpoint và trả `advertisedEndpointMap.getOrDefault(endpoint.getListenerName(), endpoint)`. Listener không có trong `advertised.listeners` sẽ **dùng chính địa chỉ bind**. Mô tả cấu hình `advertised.listeners`: *"Defaults to the value of `bind.listeners` if not explicitly configured"*. Vậy INTERNAL được đăng ký là `POD_IP:9123`.
- **Trên `main`:**
  - #4108 (04/11/2026) thêm listener **EXTERNAL** tùy chọn cho client ngoài cluster.
  - #4424 hỗ trợ nhiều cluster trong một namespace.
  - Helper `fluss.listeners.advertised` (trong `_listeners.tpl`) chỉ sinh `CLIENT://...` (và `EXTERNAL://...` nếu bật). **Vẫn không có INTERNAL.**
- **Đường đi của địa chỉ INTERNAL khi server gọi nhau (đã đọc mã):**
  - Replica fetcher dùng `GatewayClientProxy` lấy `ServerNode` của leader từ metadata cache (`serverNodeMetadataCache.apply(leaderId)`) **ở mỗi lần gửi**.
  - `NettyClient.getOrCreateConnection(node)` cache kết nối theo `node.uid()`. Kết nối hỏng thì bị gỡ khỏi map (`closeCallback`), và lần sau tạo kết nối mới với `ServerNode` hiện có trong cache.
  - Vì vậy nếu metadata cache còn IP cũ, server sẽ **tạo kết nối mới tới IP cũ** liên tục. Điều này khớp với mô tả "fresh dials" trong #4256.
  - Mình **chưa xác định** vì sao metadata cache có thể giữ IP cũ tới 10 phút. Còn một khả năng khác: kết nối cũ được cache theo server ID mà không kiểm tra địa chỉ (#4256, đã sửa trên `main` bằng #4263).

## 2. Khi nào lỗi xảy ra

- Triển khai Fluss bằng **Helm chart chính thức** (hoặc cấu hình tương tự) trên Kubernetes.
- **Mỗi lần pod TabletServer hoặc Coordinator bị tạo lại** và nhận Pod IP mới: rolling upgrade, restart, pod bị đẩy sang node khác, node hỏng.
- Không xảy ra khi Pod IP được giữ cố định, hoặc khi triển khai không dùng Kubernetes.

**Phiên bản:** báo trên `0.8.0`. Chart của `1.0.0` và `main` vẫn như vậy.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** cluster chạy trên Kubernetes bằng chart chính thức. Client (Flink, ứng dụng) **không bị trực tiếp**, vì chúng dùng địa chỉ CLIENT là tên DNS ổn định.

**Hậu quả có thể (suy luận, kèm một quan sát thực tế):**
1. **Replication bị chậm sau restart** (quan sát ở #4256 trên 1.0.0):
   - các TabletServer khác gọi IP cũ khoảng 10 phút, với lỗi `Connection refused` và `connection timed out after 15000 ms`;
   - server vừa restart nằm ngoài ISR trong thời gian đó;
   - readiness gate "cluster healthy" của chart bị giữ lại, làm rolling upgrade chậm hẳn.
2. **TabletServer gọi Coordinator** cũng đi qua INTERNAL, ví dụ báo thay đổi ISR (`AdjustIsr`), commit remote log manifest (#692), báo KV snapshot. Khi pod Coordinator restart với IP mới, các lời gọi này có thể lỗi tới khi TabletServer biết địa chỉ mới. Mình chưa kiểm tra cách TabletServer tìm địa chỉ Coordinator (có thể qua ZooKeeper).
3. Khi ISR co lại trong lúc rolling upgrade, nếu thêm một node nữa chết thì bucket có thể chỉ còn 1 replica hoặc mất leader.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #2076 trong `relate_issue.md` ghi: *"⏭️ Liên quan #4108 (listener cho client ngoài cluster) mới merge vào `main` | Nếu triển khai bằng Helm và AI agent chạy **ngoài** K8s cluster. Tự cấu hình listeners"*. Đối chiếu:
- **⏭️ không đúng.** #4108 giải bài toán **client ngoài cluster** (listener EXTERNAL), **không** thêm INTERNAL vào `advertised.listeners`. Trên `main` vẫn còn nguyên vấn đề, nên trạng thái nên là **❌ (cả `main`)**.
- **"AI agent chạy ngoài K8s" là chuyện khác.** Issue #2076 nói về địa chỉ **INTERNAL**, tức server với server. Nếu AI agent chạy ngoài cluster thì vấn đề là địa chỉ CLIENT dùng DNS nội bộ cluster, và #4108 mới là thứ liên quan.

## 5. Mức ảnh hưởng với CDP

- Nếu CDP triển khai Fluss bằng Helm chart trên Kubernetes thì **có dính**. Hệ quả chính là **rolling upgrade và restart chậm, kèm mất dự phòng tạm thời**, đúng vào lúc rủi ro cao nhất (xem #4256, #4269, #709).

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Tự sửa chart** để quảng bá cả INTERNAL bằng tên DNS headless:
  ```
  ADVERTISED_LISTENERS="INTERNAL://${POD_NAME}.tablet-server-hs.${POD_NAMESPACE}.svc.cluster.local:9123, CLIENT://${POD_NAME}.tablet-server-hs.${POD_NAMESPACE}.svc.cluster.local:9124"
  ```
  Làm tương tự cho Coordinator với service headless của nó.
  - `loadAdvertisedEndpoints` chấp nhận listener có trong `bind.listeners`, mà INTERNAL có. Vì vậy cấu hình này hợp lệ theo mã.
  - **Lưu ý:** DNS của pod StatefulSet chỉ có khi pod đã chạy. Có thể cần `publishNotReadyAddresses: true` trên headless service để server khác resolve được trong lúc khởi động. Cần thử.
- **Rolling upgrade từng node, chờ ISR đầy đủ** (alert under-replicated bằng 0) rồi mới sang node kế.
- **Theo dõi log** `Error in response from leader server` và lỗi `Connection refused` tới IP không còn tồn tại sau mỗi lần restart.
- Kết hợp với **#4263** (trên `main`, cache kết nối theo cả host và port) nếu tự build.

## 6. Những điều chưa kiểm chứng

- Mình chưa thử thêm INTERNAL vào `advertised.listeners` để xem hiện tượng 10 phút ở #4256 có hết không.
- Mình chưa xác định vì sao metadata cache của TabletServer có thể giữ IP cũ lâu như vậy.
- Mình chưa kiểm tra TabletServer tìm địa chỉ Coordinator bằng cách nào, nên chưa biết pod Coordinator restart ảnh hưởng ra sao.
- Mình chưa kiểm tra việc dùng DNS cho INTERNAL có làm chậm khởi động hay không (DNS chưa sẵn sàng).
