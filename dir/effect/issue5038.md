# Issue #5038 — Metric của Coordinator bị "đóng băng" sau khi Coordinator mất rồi lấy lại vai trò leader

Link: https://github.com/apache/fluss/issues/5038 — Phiên bản được báo: `1.0.0` (người báo: loserwang1024, 04/11/2026). Issue ngắn, tự nêu nguyên nhân. Trạng thái: **đã đóng** (04/11/2026) nhờ PR [#5039](https://github.com/apache/fluss/pull/5039), merge vào `main` ngày 04/11/2026 (commit `a2e0c769`). Fix **không có trong 1.0.0** và **chưa được backport** sang nhánh `release-1.0`.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0` và xác nhận cơ chế. Ngoài hiện tượng issue nêu, mình thấy thêm một hệ quả **quan trọng cho vận hành**, suy ra từ mã và chưa tái hiện: metric `activeCoordinatorCount` là gauge trả **hằng số 1**, nên một Coordinator **đã mất leadership** (kể cả khi kẹt như #4362) vẫn có thể tiếp tục báo `activeCoordinatorCount = 1`.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Metric / gauge** | Metric là con số Fluss xuất ra để giám sát. **Gauge** là loại metric mà mỗi lần reporter đọc, nó gọi một hàm để lấy giá trị hiện tại. Hàm đó "dính" vào đối tượng tạo ra nó. |
| **Metric group** | Nhóm metric có chung tiền tố hoặc scope. Trong một group, **tên metric là duy nhất**. |
| **Reporter** | Thành phần đẩy hoặc xuất metric ra ngoài (Prometheus, JMX...). |
| **Coordinator leadership / nhiệm kỳ (term)** | Có thể chạy nhiều Coordinator. Một cái được bầu làm **leader (active)** qua ZooKeeper, các cái còn lại là standby. Một process có thể **mất** leadership (ví dụ mất ZK session) rồi **lấy lại** mà không restart. |
| **`CoordinatorEventProcessor` / `CoordinatorEventManager`** | Bộ xử lý sự kiện của leader. **Mỗi nhiệm kỳ tạo một đối tượng mới**, và khi mất leadership thì `shutdown()`. |
| **`CoordinatorMetricGroup`** | Group metric của Coordinator. **Tạo một lần lúc process khởi động** và **dùng lại** qua mọi nhiệm kỳ. |
| **`activeTabletServerCount`, `offlineBucketCount`, `activeCoordinatorCount`, ...** | Các metric của Coordinator: số TabletServer sống, số bucket không có leader, số Coordinator active... |

## Tóm tắt

`CoordinatorEventManager` đăng ký các gauge (`activeCoordinatorCount`, `activeTabletServerCount`, `offlineBucketCount`, `tableCount`, `bucketCount`, ...) vào `CoordinatorMetricGroup` **dùng chung**. Khi nhiệm kỳ kết thúc, manager bị đóng nhưng **gauge không được gỡ**. Nhiệm kỳ sau tạo manager mới và đăng ký gauge **cùng tên**, nhưng group **từ chối tên trùng và giữ gauge cũ**. Kết quả: reporter tiếp tục đọc gauge của **manager đã chết**, nên giá trị **đứng yên** ở lúc nhiệm kỳ cũ kết thúc. Ví dụ trong issue: tăng TabletServer từ 9 lên 12, Coordinator làm việc đúng với 12 server nhưng `activeTabletServerCount` vẫn báo 9. Chỉ restart process mới hết.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, gồm `coordinator/event/CoordinatorEventManager`, `metrics/group/CoordinatorMetricGroup`, `coordinator/CoordinatorServer`, cùng `fluss-common`: `metrics/groups/AbstractMetricGroup`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `CoordinatorServer` tạo `serverMetricGroup` (một `CoordinatorMetricGroup`) **một lần** khi khởi động. Mỗi lần trở thành leader, nó tạo **mới** `CoordinatorEventProcessor(..., serverMetricGroup, ...)`, và processor này tạo `new CoordinatorEventManager(this, coordinatorMetricGroup)`.
  - Constructor của `CoordinatorEventManager` gọi `registerMetrics()`:
    ```java
    coordinatorMetricGroup.gauge(MetricNames.ACTIVE_COORDINATOR_COUNT, () -> 1);
    coordinatorMetricGroup.gauge(MetricNames.ALIVE_COORDINATOR_COUNT, () -> aliveCoordinatorServerCount);
    coordinatorMetricGroup.gauge(MetricNames.ACTIVE_TABLET_SERVER_COUNT, () -> tabletServerCount);
    coordinatorMetricGroup.gauge(MetricNames.OFFLINE_BUCKET_COUNT, () -> offlineBucketCount);
    ... BUCKET_COUNT, TABLE_COUNT, LAKE_TABLE_COUNT, PARTITION_COUNT, REPLICAS_TO_DELETE_COUNT, PENDING_LEADER_ACTIVATION_COUNT
    ```
    Cộng thêm histogram `EVENT_QUEUE_TIME_MS`. Các lambda đọc **trường của chính manager đó**.
  - `CoordinatorEventManager.close()` chỉ dừng luồng sự kiện, **không gỡ metric**.
  - `CoordinatorServer.cleanupCoordinatorLeader()` (khi mất leadership) gọi `coordinatorEventProcessor.shutdown()` rồi đặt `null`, nhưng **không** đóng hay dọn `serverMetricGroup`. `serverMetricGroup.close()` chỉ được gọi khi tắt hẳn server.
  - `AbstractMetricGroup.addMetric`: `metrics.put(name, metric)`. Nếu đã có metric cùng tên (`prior != null`), nó *"put back the original value"*, tức **giữ metric cũ** và bỏ metric mới.
- **Fix trên `main`** (#5039, mình đọc mô tả commit, không đọc diff): *"Unregister event-manager metrics when a leadership term ends so the next term reports current values."* Thay đổi nằm ở `CoordinatorEventManager` và `CoordinatorMetricGroup`.

## 2. Khi nào lỗi xảy ra

- Một Coordinator process **đã từng là leader**, mất leadership, rồi **lấy lại** leadership mà **không restart**. Ví dụ: mất ZooKeeper session do GC hoặc mạng rồi kết nối lại; leader election xoay vòng giữa các Coordinator; hoặc #4362 sau khi được khắc phục bằng cách nào đó.
- Coordinator standby **chưa từng** làm leader thì chưa đăng ký gauge nào, nên không có giá trị cũ. Khi nó lên leader lần đầu, metric đúng.

**Phiên bản:** báo trên `1.0.0`. Fix chỉ có trên `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành dùng metric Coordinator để giám sát và alert, đặc biệt khi chạy **nhiều Coordinator (HA)**.

**Hậu quả:**
1. **Metric nhiệm kỳ mới đứng yên** ở giá trị cũ: số TabletServer, số bucket offline, số bảng, số partition... Không ảnh hưởng dữ liệu hay hoạt động. Coordinator vẫn làm việc đúng.
2. **Alert sai hoặc không bắn:**
   - `offlineBucketCount` đứng ở 0 thì alert "bucket mất leader" **không bắn** khi có sự cố thật (xem #3011, #4522).
   - `activeTabletServerCount` sai thì không phát hiện TabletServer chết.
3. **Hệ quả thêm, suy ra từ mã:** `activeCoordinatorCount` là `() -> 1`, một hằng số. Gauge này nằm trong group dùng chung và **không bị gỡ khi mất leadership**, nên một Coordinator **đã mất leader** vẫn có thể tiếp tục xuất `activeCoordinatorCount = 1`. Hậu quả:
   - Với #4362 (Coordinator mất ZK session và **không lấy lại** được leadership), process kẹt đó **vẫn báo mình là active**. Alert kiểu "tổng `activeCoordinatorCount` = 0" sẽ **không bắn** đúng lúc cluster không có leader.
   - Nếu một Coordinator khác lên làm leader, tổng `activeCoordinatorCount` có thể là **2**.

   Mình chưa tái hiện điểm này. Chỉ suy ra từ việc reporter còn giữ gauge sau khi processor bị shutdown.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #5038 trong `relate_issue.md` ghi: *"⏭️ Fix #5039 trên `main` (2026-11-04) | Cảnh báo sai. Không chỉ dựa vào metric Coordinator"*. Đối chiếu:
- **Đúng.** Issue đã đóng trên GitHub nhưng **1.0.0 vẫn còn lỗi**.
- **Bổ sung quan trọng:** mục 7 của `relate_issue.md` khuyến nghị *"alert: có active Coordinator hay không"* (cho #4362). Với lỗi này, **`activeCoordinatorCount` không đáng tin** cho mục đích đó trên 1.0.0. Cần cách khác (mục 5).

## 5. Mức ảnh hưởng với CDP

- Không ảnh hưởng dữ liệu, ghi hay lookup của AI agent.
- Ảnh hưởng tới **khả năng phát hiện sự cố**. Đây là điều quan trọng vì nhiều rủi ro khác (#4362, #3011, #4522) cần dựa vào metric Coordinator để phát hiện.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Đừng dùng `activeCoordinatorCount` làm tín hiệu duy nhất** cho "cluster có Coordinator leader". Thay bằng:
  - kiểm tra node leader trong ZooKeeper;
  - hoặc kiểm tra chủ động bằng Admin API (ví dụ một thao tác metadata hoặc DDL nhẹ định kỳ);
  - hoặc theo dõi metric nội dung **thay đổi theo thời gian**, ví dụ `eventQueueTime` có `_count` tăng (giá trị đứng yên là dấu hiệu bất thường).
- **Đối chiếu metric Coordinator với nguồn khác:** số TabletServer theo metric của chính TabletServer hoặc theo Kubernetes, số bucket không có leader theo metadata do client hoặc Admin trả về.
- **Theo dõi log** `"Cleaning up coordinator leader services."` rồi tiếp theo là log trở thành leader trong **cùng một process**. Khi thấy, **restart Coordinator đó** vào thời điểm thích hợp để metric chính xác trở lại.
- **Cân nhắc cherry-pick #5039** nếu tự build. Thay đổi nhỏ, chỉ nằm trong phần metric (3 file, chủ yếu là test).

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện việc metric đứng yên.
- Mình chưa tái hiện việc Coordinator đã mất leadership vẫn xuất `activeCoordinatorCount = 1`. Điểm này phụ thuộc vào reporter (Prometheus, JMX) có tiếp tục đọc gauge của group chưa đóng hay không. Mình suy ra là có, nhưng chưa thử.
- Mình chưa đọc diff của #5039 để biết nó có xử lý riêng `activeCoordinatorCount` khi mất leadership hay không.
- Mình chưa kiểm tra các metric khác của Coordinator ngoài `CoordinatorEventManager` (ví dụ metric bảng hoặc bucket, metric tiering) có cùng vấn đề không.
