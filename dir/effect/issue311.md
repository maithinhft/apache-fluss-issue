# Issue #311 — Timeout khi cập nhật metadata quá ngắn (3 giây), khiến job ghi Flink lỗi "partition không tồn tại" dù partition có thật

Link: https://github.com/apache/fluss/issues/311 — Phiên bản được báo: `main` thời 0.5 (người báo: luoyuxia, 06/01/2025, repo cũ `alibaba/fluss`). Issue mô tả rõ kịch bản: bảng partition có 512 bucket, sink Flink parallelism 512, và cách tạm thời là tăng timeout lên 60 giây. Có một bình luận: *"refactor metadata updater in this issue"*. Không có PR riêng. Trạng thái: còn mở.

> **Mức chắc chắn:** cao về hành vi trong `1.0.0` (đã đọc mã). Hai yêu cầu của issue thì **một đã xong, một chưa**:
> - Timeout đã tăng từ 3 lên **30 giây**, nhưng **không cấu hình được**.
> - Lỗi timeout **vẫn bị nuốt** (chỉ log WARN, không báo cho bên gọi).
>
> Đường ghi của 1.0.0 có thêm **dynamic partition creation** (bật mặc định), nên kịch bản "báo nhầm partition không tồn tại thì job lỗi" phần lớn **không còn** làm job thất bại. Mình chưa tái hiện.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Metadata (client)** | Thông tin client cần để gửi request: bảng có ID gì, partition có ID gì, bucket nào có leader ở server nào. Client giữ trong `Cluster` và cập nhật qua `MetadataUpdater`. |
| **`updateMetadata` / `sendMetadataRequestAndRebuildCluster`** | Gửi request metadata tới một TabletServer rồi dựng lại `Cluster`. |
| **Timeout** | Thời gian tối đa chờ phản hồi. |
| **Nuốt lỗi (swallow)** | Bắt lỗi rồi chỉ ghi log, không ném tiếp. Bên gọi tưởng việc đã xong. |
| **`PartitionNotExistException`** | Lỗi "partition không tồn tại". |
| **Dynamic partition creation** | Khi writer ghi vào partition chưa có, client **tự gọi Admin để tạo partition** (`client.writer.dynamic-create-partition.enabled`, mặc định **true**). |
| **`ignoreIfExists`** | Cờ khi tạo partition: nếu partition đã có thì coi như thành công. |

## Tóm tắt

Ở bản 0.5: khi writer ghi vào một partition, client cập nhật metadata của partition đó với **timeout 3 giây**. Job lớn (512 bucket, 512 sink song song cùng hỏi metadata) dễ quá 3 giây. Khi đó metadata **không được cập nhật**, lỗi chỉ được log, và bước sau kết luận partition **không tồn tại**, nên job failover.

Ở bản `1.0.0`:
- Timeout là **30 giây** (hằng số trong mã).
- Lỗi timeout hoặc lỗi có thể thử lại vẫn **bị nuốt** trong `MetadataUpdater.updateMetadata`.
- Nhưng đường ghi vào partition giờ đi qua `DynamicPartitionCreator`. Nếu kiểm tra nói partition chưa có (kể cả do timeout), client **gọi tạo partition với `ignoreIfExists = true`**. Partition có thật thì việc tạo thành công ngay, và batch tiếp tục chờ metadata ở các vòng sau. Kết quả là việc **báo nhầm** thường **không còn làm job lỗi**.
- Nếu **tắt** dynamic partition creation, client vẫn ném `PartitionNotExistException` như cũ.

Việc nuốt lỗi timeout còn liên quan tới một lỗi khác, #4928 (writer kẹt vì thiếu mapping bảng).

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-client`, gồm `utils/MetadataUtils`, `metadata/MetadataUpdater`, `write/DynamicPartitionCreator`, `write/WriterClient`.
- **Đã xác nhận trong mã `v1.0.0`: timeout 30 giây, cố định.** `MetadataUtils.sendMetadataRequestAndRebuildCluster` kết thúc bằng:
  ```java
  .get(30, TimeUnit.SECONDS); // TODO currently, we don't have timeout logic in
  // RpcClient, it will let the get() block forever. So we time out here
  ```
  Không có option cấu hình. Comment cũng xác nhận RPC client không có timeout cho từng request (giống #692).
- **Đã xác nhận trong mã `v1.0.0`: lỗi timeout bị nuốt.** `MetadataUpdater.updateMetadata(...)` bắt lỗi:
  ```java
  if (t instanceof RetriableException || t instanceof TimeoutException) {
      if (serverNode != null) { unavailableTabletServerIds.add(serverNode.id()); LOG.warn(...); }
      LOG.warn("Failed to update metadata, but the exception is re-triable.", t);
  } else if (t instanceof PartitionNotExistException) { ... throw ...; }
  else { throw new FlussRuntimeException("Failed to update metadata", t); }
  ```
  Với timeout: server bị đánh dấu "không khả dụng" (lần sau thử server khác), **không ném lỗi**, và `Cluster` giữ nguyên.
- **Đã xác nhận trong mã `v1.0.0`: đường ghi vào partition.**
  - `WriterClient` gọi `dynamicPartitionCreator.checkAndCreatePartitionAsync(physicalTablePath, tableInfo)`.
  - Nếu cache chưa có partition ID, hàm gọi `forceCheckPartitionExist` → `metadataUpdater.checkAndUpdatePartitionMetadata(path)`, hàm này trả `cluster.getPartitionId(path).isPresent()` sau khi cập nhật. Khi timeout, kết quả là `false`.
  - Kết quả `false` thì `validateAutoPartitionTime(...)` rồi `admin.createPartition(tablePath, spec, true)`, tức **`ignoreIfExists = true`**, chạy bất đồng bộ. Thành công thì chỉ log `"Successfully created partition"`, kèm TODO *"trigger to update metadata here when metadataUpdater supports async update"*.
  - Nếu `dynamicPartitionEnabled = false`, `forceCheckPartitionExist` ném `PartitionNotExistException("Table partition '%s' does not exist.")` khi gặp lỗi partition không tồn tại.
- **Điểm phụ:** bước `validateAutoPartitionTime` trên đường này là nơi #4548 (partition QUARTER luôn bị từ chối) gây lỗi.

## 2. Khi nào lỗi xảy ra

- **Timeout cập nhật metadata** (> 30 giây) khi TabletServer phục vụ metadata quá tải, ZooKeeper chậm (phía server đọc ZooKeeper cho request metadata, xem #3466), hoặc rất nhiều client cùng hỏi (job parallelism lớn khởi động cùng lúc).
- **Hậu quả còn lại trên 1.0.0** chỉ lộ ra khi:
  1. tắt dynamic partition creation, hoặc
  2. không có server nào trả lời được, nên client rơi vào nhánh `initializeCluster` từ bootstrap, hoặc
  3. đường gọi khác không đi qua `DynamicPartitionCreator`, ví dụ lookup bảng partition qua `AbstractLookuper` (#2209) hoặc khởi tạo bảng trong `getTable` (#4928).

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** job hoặc ứng dụng dùng Java client với bảng partition, khi metadata server chậm.

**Hậu quả trên 1.0.0:**
1. **Ghi (mặc định):** thường **không lỗi**, chỉ có thêm một lời gọi `createPartition` thừa tới Coordinator, kèm log WARN. Batch chờ trong accumulator cho tới khi metadata được cập nhật ở vòng sau (suy luận).
2. **Ghi khi tắt dynamic partition:** `PartitionNotExistException`, job failover, giống bản 0.5.
3. **Đường khác:** nhận dữ liệu metadata **thiếu** mà không biết. Hệ quả tùy đường: lookup ra "partition không tồn tại" hoặc treo; writer kẹt kiểu `table path not found` (#4928).
4. **Không thấy lỗi rõ ràng:** do lỗi bị nuốt, người vận hành chỉ thấy log WARN.

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng #311 trong `relate_issue.md` ghi: *"✅ Mã `v1.0.0` dùng 30s (`MetadataUtils`) | Đã khắc phục trên thực tế. Không cấu hình được"*. Đối chiếu:
- **✅ hơi lạc quan.** Nên là **🟡**:
  - Timeout đã đủ dài cho đa số trường hợp, và đường ghi mặc định có dynamic partition giảm hậu quả.
  - Nhưng **yêu cầu thứ hai** của issue (*"should throw Timeout exception instead of just log it"*) **chưa làm**. Chính việc nuốt lỗi này là nguyên nhân gốc của #4928.

## 5. Mức ảnh hưởng với CDP

- **Thấp** với luồng ghi mặc định (dynamic partition bật).
- Đáng chú ý ở chỗ **lỗi bị nuốt gây ra trạng thái metadata thiếu một cách âm thầm**, ảnh hưởng tới lookup và khởi tạo bảng của AI agent (xem #4928, #2209).

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Giữ `client.writer.dynamic-create-partition.enabled = true`** (mặc định) cho job ghi vào bảng partition.
- **Theo dõi log client** `"Failed to update metadata, but the exception is re-triable."` và `"tabletServer {} is unavailable for updating metadata"`. Nếu xuất hiện thường xuyên, kiểm tra tải và độ trễ ZooKeeper của TabletServer.
- **Tránh khởi động hàng trăm sink cùng lúc** vào cùng một bảng partition lớn nếu cluster nhỏ. Có thể khởi động lệch thời gian, hoặc giảm parallelism ban đầu.
- Ứng dụng (AI agent) gọi lookup trên bảng partition nên **tự retry có backoff** khi nhận "partition không tồn tại" cho partition chắc chắn có.

## 6. Những điều chưa kiểm chứng

- Mình chưa tái hiện timeout metadata.
- Mình chưa kiểm tra batch ghi vào partition mà metadata thiếu sẽ chờ bao lâu trong accumulator, và có hết hạn (`delivery timeout`) hay không.
- Mình chưa đo thời gian phản hồi metadata thực tế dưới tải lớn.
