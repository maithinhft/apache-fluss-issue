# Issue #4139 — Histogram không bao giờ loại mẫu cũ, làm percentile (P99) "đứng" ở giá trị lỗi thời trên metric ít mẫu

Link: https://github.com/apache/fluss/issues/4139 — Phiên bản được báo: `main` (người báo: fxbing, 31/08/2026). Người báo nói bản `0.9.1-incubating` cũng có lỗi. Issue có tái hiện xác định (1024 mẫu) và đề xuất sửa chi tiết. Trạng thái: còn mở. PR gắn kèm [#4167](https://github.com/apache/fluss/pull/4167) **đang mở**. Tuy vậy, trên `main` đã có commit [#4829](https://github.com/apache/fluss/pull/4829) *"[metrics] Expire idle histogram samples across reporters"* (29/10/2026), với nội dung: *"Bound built-in histogram samples by count and age while preserving cumulative counts. Omit empty-window quantiles in Prometheus and expose window counts in InfluxDB and JMX."* Đây đúng là hướng sửa issue đề xuất. Commit này **không có trong 1.0.0**.

> **Mức chắc chắn:** cao. Mình đã đọc `DescriptiveStatisticsHistogram` trong `v1.0.0`: nó không có khái niệm thời gian. Mình cũng đã liệt kê các histogram dùng nó (mục 1). Mình chưa đọc diff của #4829, chỉ đọc mô tả commit.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Histogram** | Loại metric lưu một **tập mẫu** (ví dụ thời gian xử lý của từng request) để tính trung bình, min/max và các **phân vị**. |
| **Percentile / P99** | P99 = 99% số mẫu nhỏ hơn giá trị này. Thường dùng để alert về độ trễ. |
| **Circular buffer (bộ đệm vòng)** | Mảng kích thước cố định (ví dụ 1024 ô). Mẫu mới ghi đè mẫu cũ nhất. Mẫu cũ chỉ bị đẩy ra khi có **mẫu mới** đến. |
| **Window size** | Số mẫu tối đa giữ lại: 1024, 100 hoặc 64 tùy metric. |
| **`_count`** | Tổng số mẫu **từ trước tới nay** (cộng dồn). Tăng chậm nghĩa là metric ít mẫu. |
| **Metric ít mẫu** | Metric ít khi có mẫu mới. Ví dụ `logFlushLatencyMs` chỉ khoảng 1,8 mẫu/phút trên cluster ít tải (theo issue), hoặc request lookup trên bảng ít được truy cập. |
| **Scrape** | Prometheus định kỳ đọc metric. Ở 1.0.0, việc đọc **không** làm mẫu cũ hết hạn. |

## Tóm tắt

`DescriptiveStatisticsHistogram` là cài đặt `Histogram` mà các metric group của Fluss dùng. Nó giữ một bộ đệm vòng kích thước cố định và **không quan tâm thời gian**: mẫu cũ chỉ bị thay khi có mẫu mới. Hai hệ quả:
1. Một đợt chậm ngắn trên metric ít mẫu giữ **P99 cao hàng giờ**. Theo tái hiện trong issue: P99 = 2.000 ms, cần thêm 1.015 mẫu bình thường mới xuống dưới 1 giây, tức khoảng **9,4 giờ** ở tốc độ 1,8 mẫu/phút.
2. Metric **ngừng nhận mẫu** thì giữ giá trị cũ **mãi** cho tới khi restart.

Trong lúc đó `_count` vẫn tăng, và alert P99 bắn dù server khỏe.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, `metrics/DescriptiveStatisticsHistogram`, cùng mọi metric group dùng nó.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `DescriptiveStatisticsHistogram(int windowSize)` bọc `CircularDoubleArray`. `update(value)` ghi vào `backingArray[nextPos++]` và quay vòng khi đầy. `getStatistics()` tính trên **toàn bộ mảng hiện có**. Không có timestamp, không có hết hạn.
  - Các nơi tạo histogram này trong mã chính (`main/java`) của 1.0.0:

    | Nơi | Metric | Window |
    |---|---|---|
    | `fluss-rpc` `RequestsMetrics` (mỗi loại request, ví dụ lookup, putKv, produceLog, fetchLog...) | `requestBytes`, `requestQueueTimeMs`, `requestProcessTimeMs`, `responseSendTimeMs`, `requestTotalTimeMs` | 1024 |
    | `TabletServerMetricGroup` | `logFlushLatencyMs`, `kvFlushLatencyMs` | 1024 |
    | `TableMetricGroup` | (một histogram theo bảng) | 64 |
    | `CoordinatorEventManager` | `eventQueueTimeMs` | 1024 |
    | `CoordinatorEventMetricGroup` | (theo loại sự kiện) | 100 |
    | `ControlRequestSendThread` | (thời gian gửi request điều khiển) | `HISTOGRAM_WINDOW_SIZE` |
    | `fluss-client` `WriterMetricGroup` | 2 histogram của writer | 1024 |
    | `fluss-client` `ScannerMetricGroup` | 1 histogram của scanner | 1024 |

  - Đáng chú ý cho CDP: **`requestTotalTimeMs` theo từng loại request** là metric tự nhiên để alert độ trễ lookup và ghi phía server. Với request ít (ví dụ lookup của AI agent vào giờ thấp điểm), P99 có thể là giá trị **của nhiều giờ trước**.
- **Hướng sửa trong issue:** mẫu hiện hành = *1024 mẫu gần nhất* ∩ *mẫu trong 5 phút gần nhất*; loại mẫu hết hạn cả khi `update()` và khi `getStatistics()` (scrape); `getCount()` vẫn cộng dồn. Mô tả #4829 trên `main` khớp với hướng này.

## 2. Khi nào lỗi xảy ra

- **Luôn có** với mọi histogram trên, nhưng **rõ rệt** khi:
  1. **Metric ít mẫu:** flush log hoặc KV trên cluster ít tải, request loại hiếm, bảng ít được truy cập.
  2. **Sau một đợt chậm ngắn:** GC dài, đĩa chậm, sự cố mạng. Mẫu chậm nằm trong bộ đệm cho tới khi bị đẩy ra.
  3. **Metric ngừng nhận mẫu:** ví dụ bảng không còn được ghi.
- Với metric nhiều mẫu (hàng nghìn request/giây), 1024 mẫu bị thay trong chưa tới một giây, nên lỗi gần như không lộ.

**Phiên bản:** báo trên `main`, có từ ít nhất `0.9.1`. Vẫn còn ở `1.0.0`. Fix (#4829) chỉ có trên `main`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành alert theo P99/P95 của histogram Fluss.

**Hậu quả:**
1. **Alert giả kéo dài:** alert P99 bắn hàng giờ sau khi sự cố đã qua. Người trực có thể nghĩ hệ thống còn chậm.
2. **Che giấu thay đổi thật:** sau một đợt chậm, P99 đã cao sẵn, nên một đợt chậm mới không làm thay đổi gì trên biểu đồ (suy luận).
3. **Dashboard sai:** metric của bảng, partition hay loại request không còn hoạt động vẫn hiện giá trị cũ, trông như vẫn đang chạy.
4. **Không ảnh hưởng dữ liệu** hay hoạt động của Fluss.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #4139 trong `relate_issue.md` ghi: *"⏭️ Fix liên quan #4829 trên `main` (2026-10-29) | Alert độ trễ lookup/ghi P99 có thể sai. Dùng thêm metric phía client"*. Đối chiếu:
- **Đúng.** #4829 không đóng issue (PR gắn kèm là #4167 vẫn mở), nhưng theo mô tả commit, nó làm đúng việc issue đề xuất.
- **Lưu ý về "metric phía client":** histogram của **Fluss client** (`WriterMetricGroup`, `ScannerMetricGroup`) **cũng dùng cùng lớp này**, nên cũng bị lỗi tương tự. "Metric phía client" nên hiểu là **metric do ứng dụng (AI agent, job) tự đo**, ví dụ Micrometer với cửa sổ thời gian, chứ không phải histogram của Fluss client.

## 5. Mức ảnh hưởng với CDP

- Không ảnh hưởng ghi hay lookup. Ảnh hưởng **chất lượng giám sát độ trễ**, mà đây là yêu cầu chính của CDP near realtime và AI agent.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Đo độ trễ lookup và ghi ngay trong ứng dụng** (service AI agent, job Flink) bằng thư viện có cửa sổ thời gian, và alert theo số đo đó.
- **Khi dùng histogram Fluss:** alert kèm điều kiện `_count` có tăng trong cửa sổ (ví dụ `increase(..._count[5m]) > 0`). Nếu không có mẫu mới thì bỏ qua giá trị P99.
- **Dùng thêm metric dạng counter hoặc rate** (số request, số lỗi, `errorsRate`), vì chúng không bị lỗi này.
- Sau sự cố, nếu cần P99 "sạch" ngay, restart process. Không khuyến nghị làm thường xuyên.
- Khi lên 1.1 (hoặc nếu tự build có #4829): đọc lại cách Prometheus xuất quantile khi cửa sổ rỗng (*"Omit empty-window quantiles in Prometheus"*, sau đổi thành NaN) để chỉnh rule alert.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện trên 1.0.0 và chưa đo thời gian P99 bị "đứng" với tải thực tế của CDP.
- Mình chưa đọc diff của #4829 (thời hạn mặc định, áp dụng cho những metric nào).
- Mình chưa xác định tên chính xác của histogram trong `TableMetricGroup` và `ControlRequestSendThread`.
