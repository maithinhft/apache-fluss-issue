# Issue #2712 — Bật JMX metrics reporter thì server báo lỗi khi khởi động trên JDK 17 (`IllegalAccessError` với `sun.rmi.registry.RegistryImpl`)

Link: https://github.com/apache/fluss/issues/2712 — Phiên bản được báo: `main` thời 0.9 (người báo: morazow, 24/02/2026). Issue có log:

```
ERROR org.apache.fluss.metrics.reporter.ReporterSetup - Could not instantiate MetricReporter with name jmx. Metrics might not be exposed/reported.
java.lang.IllegalAccessError: superclass access check failed: class org.apache.fluss.metrics.jmx.JMXServer$JmxRegistry (in unnamed module ...) cannot access class sun.rmi.registry.RegistryImpl (in module java.rmi) because module java.rmi does not export sun.rmi.registry to unnamed module
```

Stack trace đi từ `CoordinatorServer.startServices`, qua `MetricRegistry.create` và `ReporterSetup`, tới `JMXReporter` và `JMXServer.internalStart`. Hướng sửa đề xuất: dùng `LocateRegistry.createRegistry` (API công khai) thay vì kế thừa lớp nội bộ JDK. Có 1 bình luận xin sửa theo hướng đó. Trạng thái: còn mở. Có **hai PR đang mở**: [#2713](https://github.com/apache/fluss/pull/2713) và [#4534](https://github.com/apache/fluss/pull/4534).
**Nhóm B:** chỉ khi **bật JMX reporter** (`metrics.reporters` chứa `jmx`) và chạy server trên **JDK 17 trở lên**.

> **Mức chắc chắn:** cao. Mình đã xác nhận trong mã `v1.0.0`:
> - `JMXServer.JmxRegistry` vẫn `extends sun.rmi.registry.RegistryImpl`;
> - image Docker chính thức dùng `eclipse-temurin:17-jre-noble`;
> - script khởi động chỉ thêm `--add-opens=java.base/java.nio=...` (cho Arrow), **không** có `--add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED`;
> - `ReporterSetup` bắt `Throwable`, nên server **vẫn chạy**, chỉ là **không có JMX**.
>
> Cách khắc phục bằng `--add-exports` là kiến thức về JDK (module system). Mình chưa chạy thử.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **JMX** | Chuẩn Java để xem metric hoặc quản lý tiến trình từ xa (JConsole, VisualVM, `jmx_exporter`...). |
| **JMX reporter của Fluss** | Bật bằng `metrics.reporters: jmx`. Có thể mở cổng riêng `metrics.reporter.jmx.port` (ví dụ `9250-9260`) để công cụ bên ngoài kết nối qua RMI. |
| **RMI registry** | Dịch vụ tra cứu đối tượng từ xa. JMX qua mạng cần nó. Fluss tự viết `JmxRegistry` **kế thừa** lớp nội bộ `sun.rmi.registry.RegistryImpl`. |
| **Module system (JDK 9+)** | Từ JDK 16 hoặc 17, các package nội bộ (`sun.*`) **không còn mở** cho mã ngoài theo mặc định. Muốn dùng phải thêm cờ JVM `--add-exports <module>/<package>=ALL-UNNAMED`. |
| **`env.java.opts.all`** | Option trong `server.yaml` để thêm cờ JVM cho mọi tiến trình Fluss (đọc bởi `bin/config.sh`). Ngoài ra có `env.java.opts.coordinator-server` và `env.java.opts.tablet-server`. |

## Tóm tắt

Khi bật JMX reporter, `JMXServer` tạo `new JmxRegistry(port, ...)`, một lớp con của `sun.rmi.registry.RegistryImpl`. Trên **JDK 17+**, package `sun.rmi.registry` của module `java.rmi` không được export, nên việc **nạp lớp** `JmxRegistry` thất bại với `IllegalAccessError`. `ReporterSetup` bắt lỗi, ghi ERROR, và **bỏ qua** reporter JMX. Server **vẫn khởi động bình thường**, nhưng **không có metric JMX**. Các reporter khác (Prometheus) không bị ảnh hưởng.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-metrics/fluss-metrics-jmx` (`JMXServer`, `JMXService`, `JMXReporter`), cùng script khởi động trong `fluss-dist` và `docker/fluss`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `JMXServer.internalStart(port)`: `rmiRegistry = new JmxRegistry(port, "jmxrmi", rmiServerReference);`
  - ```java
    /** A registry that only exposes a single remote object. */
    @SuppressWarnings("restriction")
    private static class JmxRegistry extends sun.rmi.registry.RegistryImpl { ... }
    ```
  - `ReporterSetup`: `catch (Throwable t)` rồi log *"Could not instantiate {} with name {}. Metrics might not be exposed/reported."*. Lỗi **không làm server dừng**.
  - `fluss-daemon.sh`: với JDK ≥ 17 chỉ thêm `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED`. Không có cờ cho `java.rmi`.
  - `docker/fluss/Dockerfile`: `FROM eclipse-temurin:17-jre-noble`.
  - `bin/config.sh`: đọc `env.java.opts.all`, `env.java.opts.coordinator-server`, `env.java.opts.tablet-server`.
  - Helm `values.yaml`: `metrics.reporters: ""` (mặc định **tắt** mọi reporter), `metrics.jmx.port: 9250`. Template `_metrics.tpl` có điều kiện bật JMX.

## 2. Khi nào lỗi xảy ra

- `metrics.reporters` có `jmx`.
- Server chạy trên **JDK 17 trở lên**, mà **image Docker chính thức** chính là JDK 17. Trên JDK 11 thì không bị (theo hiểu biết về JDK: JDK 11 chỉ cảnh báo truy cập nội bộ).

**Phiên bản:** báo trên `main` thời 0.9. Mã `v1.0.0` như vậy.

## 3. Tác động đến ai, làm sai lệch gì

- **Mất metric JMX** của CoordinatorServer và TabletServer. Ai thu metric qua JMX (`jmx_exporter`, công cụ APM dùng JMX) sẽ **không thấy dữ liệu**.
- Lỗi chỉ hiện **một dòng ERROR lúc khởi động**, dễ bỏ sót. Dashboard trống mà không ai biết vì sao.
- **Không ảnh hưởng dữ liệu** hay hoạt động của cluster. Reporter **Prometheus** không bị ảnh hưởng.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng *"Rebalance / Typed API / JMX | ..., #2712, ... | 🟢 | ❌"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** server **không** bị dừng, chỉ thiếu JMX. Có cách tránh bằng cờ JVM hoặc dùng Prometheus (mục 5).

## 5. Mức ảnh hưởng với CDP

- **Thấp** nếu thu metric bằng **Prometheus reporter**, cách phổ biến trên Kubernetes.
- Liên quan nếu hạ tầng giám sát dựa trên JMX.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Ưu tiên `metrics.reporters: prometheus`.** Trong Helm: `metrics.reporters: prometheus`, cổng 9249.
- Nếu bắt buộc dùng JMX trên JDK 17: thêm cờ JVM
  ```yaml
  env.java.opts.all: --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED
  ```
  hoặc biến môi trường `FLUSS_ENV_JAVA_OPTS` trong container. Sau khi khởi động, kiểm tra log **không còn** dòng `Could not instantiate MetricReporter with name jmx`. Với Helm, mình chưa kiểm tra cách đưa `env.java.opts.all` vào `server.yaml` (có thể qua `configurationOverrides`).
- **Alert** khi log khởi động có `Could not instantiate MetricReporter`.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy thử trên JDK 17 để xác nhận cờ `--add-exports` đủ để khắc phục.
- Mình chưa đọc diff của PR #2713 và #4534.
- Mình chưa kiểm tra metric JMX "mặc định cục bộ" (tài liệu nói *"Metrics are always available on the default local JMX interface"*) có còn hoạt động khi việc tạo reporter thất bại hay không. Theo mã, đối tượng `JMXReporter` không được tạo, nên có lẽ cũng **không có MBean nào được đăng ký**.
