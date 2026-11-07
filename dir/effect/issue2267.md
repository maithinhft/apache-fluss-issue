# Issue #2267 — Tiering sang Paimon không tạo snapshot (hoặc rất lâu mới tạo) khi warehouse dùng Garage S3

Link: https://github.com/apache/fluss/issues/2267 — Phiên bản được báo: `0.8.0` (người báo: francocalvo, 28/12/2025). Đây là một PoC chạy local:
- PostgreSQL CDC → Fluss (bảng PK, `freshness = 30s`) → tiering sang Paimon;
- Paimon có catalog JDBC (PostgreSQL), warehouse trên **Garage** (object storage tương thích S3).

Có 10 bình luận, trao đổi giữa người báo và một maintainer:
- file dữ liệu xuất hiện trong bucket, nhưng **không có snapshot** trong nhiều giờ;
- batch query báo `Currently, Batch mode can only be supported if one lake snapshot exists for the table.`;
- maintainer hỏi log TaskManager;
- người báo thử máy mạnh hơn, vẫn vậy;
- **bình luận cuối của người báo:** *"I tried with S3 and this problem went away, so it's probably Garage S3 implementation? ... Sorry I added noise."*

Không có PR. Trạng thái: còn mở.
**Nhóm B:** chỉ khi bật lakehouse Paimon.

> **Mức chắc chắn:** cao rằng đây **không phải lỗi của Fluss**, theo kết luận của chính người báo sau khi đổi sang S3 thật. Mình đọc thêm mã `v1.0.0` để giải thích các triệu chứng (vì sao có file mà không có snapshot, và thông báo batch mode), nhưng **không** điều tra Garage.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **Paimon snapshot** | Phiên bản dữ liệu **đã commit** của bảng Paimon. File dữ liệu có trong bucket nhưng **chưa có snapshot trỏ tới** thì chưa "nhìn thấy" được. |
| **Commit của tiering** | Cuối mỗi vòng, job tiering **commit một snapshot** vào Paimon, ghi file metadata và manifest lên object storage, rồi báo Coordinator (xem #2648). |
| **Garage** | Phần mềm object storage mã nguồn mở có API **tương thích S3**. "Tương thích" không có nghĩa giống hệt S3 ở mọi thao tác: ghi đè, rename, list nhất quán... |
| **`table.datalake.freshness`** | Chu kỳ tier mục tiêu. Mặc định **3 phút** (`ConfigOptions.TABLE_DATALAKE_FRESHNESS`). Issue đặt 30 giây. |
| **Batch mode trên bảng lake** | Ở 0.8, Flink batch đọc bảng có lake **yêu cầu** đã có ít nhất một snapshot lake. |

## Tóm tắt

Người báo thấy file `data-*` và `changelog-*` trong bucket Garage, nhưng **không có snapshot Paimon** nào được tạo trong hàng giờ, nên batch query lỗi. Sau khi chuyển warehouse sang **AWS S3** thì mọi thứ chạy bình thường.

Kết luận hợp lý nhất: Garage xử lý **một thao tác cần cho việc commit snapshot** của Paimon khác với S3. Ví dụ: ghi file metadata hoặc hint, ghi có điều kiện, rename, list nhất quán. Vì vậy bước commit không thành công. File dữ liệu (ghi đơn giản) thì vẫn ghi được. Đây là suy luận, chưa kiểm chứng.

## 1. Lỗi nằm ở thành phần nào

- **Không nằm ở Fluss** (theo kết luận của người báo). Có thể nằm ở tương tác giữa Paimon (commit), Hadoop S3A hoặc S3 FileIO, và Garage.
- **Đối chiếu mã `v1.0.0` để hiểu triệu chứng:**
  - `TieringCommitOperator.commitWriteResults` (xem #2648): **file được ghi trước** (bởi các task writer), rồi `lakeCommitter.commit(...)` mới tạo snapshot. Commit lỗi thì file đã ghi vẫn nằm lại, khớp với quan sát "có file mà không có snapshot".
  - Commit lỗi thì `TieringCommitOperator` bắt lỗi và log *"Fail to commit tiering write result, will try to tier again in next round."*. Bảng sẽ được tier lại ở vòng sau, nên nếu lỗi ổn định (do Garage) thì **mãi không có snapshot**.
  - Thông báo `Currently, Batch mode can only be supported if one lake snapshot exists` **không còn** trong mã Flink của 1.0.0 (mình tìm trong `fluss-flink-common/src/main`). `LakeSplitGenerator` của 1.0.0 trả `null` khi chưa có snapshot lake, nên batch đọc sẽ không có phần lake. Mình chưa kiểm tra chi tiết batch read bảng lake khi chưa có snapshot trên 1.0.0.

## 2. Khi nào lỗi xảy ra

Khi warehouse Paimon nằm trên object storage **"tương thích S3" nhưng không đầy đủ** (ở đây là Garage). Không xảy ra với AWS S3 (theo người báo).

## 3. Tác động đến ai, làm sai lệch gì

- **Lake không có dữ liệu đã commit:** Spark, Trino và Paimon không thấy gì. Union read của Fluss không có phần lake.
- **File rác tích tụ** trong bucket, vì file đã ghi mà không bao giờ được commit.
- **Không ảnh hưởng dữ liệu trong Fluss.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

`relate_issue.md` liệt kê #2267 trong nhóm Paimon (❌). Đối chiếu:
- **Nên coi là "không phải lỗi Fluss"**, vì người báo đã tự kết luận. Có thể chuyển sang nhóm C, hoặc giữ như một **lưu ý về hạ tầng object storage**.

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu CDP dùng **object storage on-premise** (MinIO, Ceph RGW, Garage, SeaweedFS...) cho lake hoặc remote storage.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Test đầy đủ vòng tiering** (ghi file, commit snapshot, đọc lại bằng Paimon) trên **đúng hệ object storage** sẽ dùng, trước khi go-live.
- **Alert** theo log `"Fail to commit tiering write result"` của job tiering, và theo độ tươi của lake (thời điểm snapshot mới nhất).
- Ưu tiên object storage có độ tương thích S3 được kiểm chứng rộng rãi (AWS S3, MinIO).

## 6. Những điều chưa kiểm chứng

- Mình không điều tra thao tác cụ thể nào của Garage gây lỗi.
- Mình chưa kiểm tra batch read bảng lake **chưa có snapshot** trên 1.0.0 có chạy được không (thông báo lỗi cũ đã không còn).
- Mình chưa đọc log mà người báo chia sẻ (link ngoài).
