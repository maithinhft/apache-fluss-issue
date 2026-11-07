# Issue #4310 — Flink sink bảng PK: sau khi đổi số bucket của partition, job không còn phục hồi được từ checkpoint

Link: https://github.com/apache/fluss/issues/4310 — Phiên bản được báo: `main` (development). Tiêu đề: *"[flink] Bucket shuffle uses a fixed table-level numBuckets, breaking sink recovery after partition rescale"*. Không có bình luận. PR sửa: [#4311](https://github.com/apache/fluss/pull/4311) *"[flink] Resolve per-partition bucket count in sink bucket shuffle"*, **chưa merge** (thêm `PartitionBucketCountResolver`, sửa `FlinkRowDataChannelComputer`, `FlinkSink`, kèm nhiều test). Người báo nhận làm PR.

> **Lưu ý quan trọng:** khi đối chiếu mã `v1.0.0`, mình thấy hậu quả nghiêm trọng mà issue mô tả (không phục hồi được từ checkpoint) **chỉ xảy ra với bảng PK dùng merge engine `AGGREGATION`**. Với bảng PK thường thì mình không thấy hậu quả đó. Điều này khác với cách `relate_issue.md` mô tả ("rủi ro sai thứ tự cập nhật"); xem mục 4.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Bucket** | Mỗi bảng (hoặc mỗi partition của bảng partition) chia thành nhiều bucket. Một dòng thuộc đúng một bucket, tính bằng `hash(khóa bucket) % số bucket`. |
| **Partition rescale / `bucket.num`** | Từ PR #3908 (đã merge 2026-09-12, **có trong 1.0.0**), có thể chạy `ALTER TABLE ... SET ('bucket.num' = N)` trên bảng partition: các partition đã có **giữ số bucket cũ**, các partition tạo sau dùng số bucket mới. Nên cùng một bảng có thể có partition với số bucket khác nhau. |
| **Sink pre-write topology** | Các bước Flink chèn **trước** khi ghi vào Fluss. Với bảng PK, mặc định có bước **bucket shuffle**: phân phối bản ghi tới các subtask sao cho các bản ghi cùng bucket đi cùng một subtask. |
| **Bucket shuffle** | Dùng `hash(khóa) % numBuckets` để tính bucket rồi chọn subtask. Mục đích: **mỗi bucket chỉ do đúng một writer (subtask) ghi**. |
| **`FlinkRowDataChannelComputer`** | Lớp thực hiện bucket shuffle. Nhận `numBucket` làm tham số hàm dựng, tức là **một giá trị cố định cấp bảng chốt lúc submit job** (đã đọc ở `v1.0.0`). |
| **Subtask** | Một luồng song song của Flink operator. |
| **Checkpoint / failover** | Flink định kỳ lưu trạng thái; khi lỗi, restore từ checkpoint để chạy tiếp. |
| **Merge engine `AGGREGATION`** | Kiểu bảng PK trong đó các lần ghi cùng khóa được **gộp** (cộng dồn, v.v.) thay vì ghi đè. Ghi lại cùng một bản ghi hai lần sẽ cho kết quả sai, nên cần cơ chế khôi phục chính xác. |
| **Undo recovery** | Cơ chế của Flink sink: khi failover, **hoàn tác** các ghi xảy ra sau checkpoint cuối nhưng trước lúc lỗi, để ghi lại không bị cộng đôi. Mã `v1.0.0`: `UndoRecoveryManager` đọc changelog từ offset checkpoint tới offset mới nhất rồi tính thao tác ngược. **Chỉ bật cho bảng `AGGREGATION`** (`FlinkTableSink`: `enableUndoRecovery = mergeEngineType == AGGREGATION`). |
| **`UndoRecoveryOperator` / `bucketOffsets` / `WriterState`** | Toán tử đứng trước mỗi writer, ghi nhớ **offset cuối (LEO) của mỗi bucket** mà writer của nó đã ghi, và lưu vào checkpoint dưới dạng `WriterState`. |
| **LEO** | Log End Offset, offset kế tiếp sẽ ghi của một bucket. |
| **Union List State** | Kiểu state của Flink: khi restore, **mỗi subtask nhận bản sao toàn bộ state của mọi subtask cũ**. |
| **`RecoveryOffsetManager`** | Gộp các offset từ mọi mảnh `WriterState` khi restore để quyết định hoàn tác từ đâu. |

## Tóm tắt

Bucket shuffle của sink dùng `numBucket` **cố định cấp bảng** (chốt lúc submit job), không theo số bucket thật của từng partition. Sau khi một số partition có số bucket `C ≠ N` (do rescale), `hash % N` (chọn subtask) lệch với `hash % C` (bucket thật), nên **một bucket có thể được nhiều subtask ghi đồng thời**. Với bảng dùng undo recovery, mỗi `UndoRecoveryOperator` ghi offset của bucket đó với giá trị khác nhau; khi restore, `RecoveryOffsetManager.putMergedOffset` thấy offset xung đột và ném `IllegalStateException("Conflicting checkpoint offsets ...")`. Hệ quả: job **không thể phục hồi từ checkpoint/savepoint nữa**, chỉ còn cách bỏ state và chạy lại.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-flink-common`, phần Flink sink: `FlinkSink`, `FlinkRowDataChannelComputer`, `undo/RecoveryOffsetManager`, `undo/UndoRecoveryOperator`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `FlinkRowDataChannelComputer` nhận `int numBucket` trong hàm dựng và dùng `bucketingFunction.bucketing(bucketKey, numBucket)` để tính bucket cho mọi bản ghi. Không tra số bucket theo từng partition.
  - `ChannelComputer.shouldCombinePartitionInSharding(isPartitioned, numBuckets, numChannels)` cũng dựa vào `numBuckets` cố định đó (trả `true` khi bảng partition và `numBuckets % numChannels != 0`).
  - `RecoveryOffsetManager.putMergedOffset` ném `IllegalStateException("Conflicting checkpoint offsets for %s: %d and %d.")` khi cùng một bucket có hai offset khác nhau trong state đã gộp. Đúng thông báo trong issue.
  - Undo recovery chỉ bật cho `MergeEngineType.AGGREGATION` (`FlinkTableSink.getFlinkSink`).
  - Tính năng số bucket theo partition (#3908) đã có trong `1.0.0` (merge 2026-09-12, trước khi tách nhánh 2026-09-15).
- **Hướng sửa (issue và PR #4311):** `FlinkRowDataChannelComputer` không dùng `numBucket` cố định nữa mà **tra số bucket thật của từng partition lúc chạy** rồi cache (số bucket của một partition không đổi sau khi tạo). Ba trường hợp: partition tồn tại → lấy từ metadata; partition chưa tạo (tạo động) → tạm dùng số bucket cấp bảng hiện tại, sau đó sửa khi metadata sẵn sàng; lỗi truy vấn metadata → thử lại có giới hạn rồi ném lỗi (fail fast). Issue nhấn mạnh: bản sửa **chỉ bảo vệ checkpoint tương lai**; `WriterState` đã bị phân mảnh trong checkpoint cũ **không sửa được**, restore từ checkpoint đó vẫn lỗi và phải bỏ.

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Bảng PK có partition** (ví dụ partition theo ngày).
2. **Đã chạy `ALTER TABLE ... SET ('bucket.num' = ...)`** để đổi số bucket, nên một số partition (đã có) và partition mới có số bucket khác nhau.
3. **Có Flink sink đang chạy** dùng bucket shuffle với `numBucket` chốt lúc submit job cũ. (Job submit lại sau rescale có thể chốt số bucket mới, nhưng vẫn lệch với các partition cũ có số bucket khác.)
4. **Bảng dùng merge engine `AGGREGATION`** (để rơi vào lỗi phục hồi trong issue). Với bảng PK thường, mình chưa thấy cơ chế gây `Conflicting checkpoint offsets` (undo recovery không bật).
5. **Job failover** và cần restore từ checkpoint/savepoint.

**Phiên bản:** báo trên `main`, nhưng cả `FlinkRowDataChannelComputer` (số bucket cố định) lẫn tính năng rescale theo partition đều có trong `v1.0.0`, nên lỗi này tồn tại ở `1.0.0`.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người chạy Flink sink ghi bảng PK **partition + merge engine AGGREGATION** và dùng tính năng đổi số bucket cho partition. Nếu không dùng một trong hai, không rơi vào chuỗi lỗi trên.

**Hậu quả:**
1. **Job không phục hồi được** từ checkpoint/savepoint ở mọi lần failover sau rescale. Cách duy nhất theo issue: bỏ state và chạy lại.
2. **Bỏ state có thể kéo theo lệch dữ liệu** với bảng AGGREGATION, vì cơ chế hoàn tác chính xác bị bỏ qua (dữ liệu ghi sau checkpoint cuối có thể bị cộng lại khi chạy lại). Đây là suy luận của mình từ mục đích của undo recovery; issue không nêu trực tiếp.
3. **Một bucket bị nhiều writer ghi đồng thời** (trước khi phát hiện). Mỗi **khóa** vẫn đi về cùng một subtask (cùng khóa thì cùng hash, nên cùng `hash % N`), nên thứ tự cập nhật của từng khóa không bị đảo vì lý do này. Mình suy ra điều này từ cách tính kênh trong `FlinkRowDataChannelComputer`, chưa tái hiện.
4. **Lỗi lộ muộn:** việc chia sai không báo lỗi lúc chạy; chỉ lộ ra ở lần failover đầu tiên sau rescale.

## 4. Đính chính so với `relate_issue.md`

Dòng #4310 trong `relate_issue.md` ghi: *"sau khi rescale bucket của partition, cùng một key bị phân tán qua nhiều subtask, rủi ro sai thứ tự cập nhật"*. Sau khi đối chiếu:
- **Cùng một key không bị phân tán qua nhiều subtask**: kênh được chọn theo `hash(khóa) % N`, cùng khóa thì cùng kênh. Cái bị phân tán là các khóa **cùng bucket thật** (cùng `hash % C`).
- **Hậu quả được issue mô tả là mất khả năng phục hồi từ checkpoint** (`Conflicting checkpoint offsets`), không phải sai thứ tự cập nhật.
- Hậu quả đó chỉ rõ ràng với bảng **AGGREGATION** (undo recovery chỉ bật cho loại này).

Phần "không rescale bucket khi có Flink sink đang ghi" vẫn là khuyến nghị đúng.

## 5. Mức ảnh hưởng với CDP

- Nếu bảng profile PK của bạn là PK thường (ghi đè theo khóa, không dùng merge engine `AGGREGATION`) hoặc không partition, hoặc bạn **không** chạy `ALTER TABLE SET 'bucket.num'` trên bảng partition thì **không rơi vào lỗi này**.
- Nếu bạn dùng bảng aggregation (ví dụ cộng dồn chỉ số theo khách hàng) và chia partition theo ngày, rủi ro có thật nhưng chỉ xuất hiện sau khi đổi số bucket.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Không chạy `ALTER TABLE SET 'bucket.num'`** trên bảng PK partition đang có Flink sink, nhất là bảng aggregation. Chọn số bucket đủ lớn ngay từ đầu.
- Nếu buộc phải rescale: dừng job bằng savepoint **trước khi** rescale, rồi chạy lại với plan mới và kiểm thử khả năng phục hồi từ checkpoint trên môi trường thử trước khi dùng thật.
- Trước go-live, **diễn tập failover và restore từ checkpoint** trên bảng có partition với số bucket khác nhau, nếu bạn định dùng tính năng này.
- Theo dõi PR #4311.
