# Issue #4269 — Đổi leader "sạch" có thể để các replica trong ISR giữ dữ liệu khác nhau tại cùng một offset

Link: https://github.com/apache/fluss/issues/4269 — Phiên bản được báo: `0.9.0`. PR sửa được ghi trong `relate_issue.md`: [#4273](https://github.com/apache/fluss/pull/4273), chưa merge. Issue được theo dõi chung dưới [#673](https://github.com/apache/fluss/issues/673) (thiết kế leader epoch cho replication).

> **Giới hạn khi viết file này:** GitHub API đang bị giới hạn lượt gọi nên mình chỉ đọc được nội dung issue (không đọc được bình luận và nội dung PR #4273). Phần mã nguồn mình đọc trực tiếp tại tag `v1.0.0`.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa |
|---|---|
| **Bucket / replica** | Mỗi bucket của bảng được lưu thành nhiều bản sao gọi là replica, mỗi bản nằm trên một TabletServer khác nhau (ở ví dụ là A, B, C). |
| **Leader / follower** | Mỗi bucket có đúng một replica là **leader**: nhận ghi từ client. Các replica còn lại là **follower**: liên tục kéo (fetch) dữ liệu mới từ leader về. |
| **Offset** | Số thứ tự của bản ghi trong log của bucket (0, 1, 2, …). Hai replica "giống nhau" nghĩa là cùng offset thì có cùng nội dung. |
| **LEO (Log End Offset)** | Offset kế tiếp sẽ được ghi, tức "độ dài" log của replica đó. |
| **HW (High Watermark)** | Mốc offset mà mọi replica trong ISR đã có; các bản ghi dưới HW được coi là đã **commit**, client đọc được. |
| **ISR (In-Sync Replicas)** | Tập các replica được coi là đang theo kịp leader. Khi leader chết, leader mới được chọn từ ISR. |
| **`acks=1` / `acks=-1`** | `acks=1`: leader ghi xong là trả OK, chưa chờ follower. `acks=-1` (all): chờ tất cả replica trong ISR có bản ghi rồi mới trả OK. |
| **Khoảng nửa mở `[10,15)`** | Gồm offset 10, 11, 12, 13, 14 (không gồm 15): 5 bản ghi. |
| **Đổi leader sạch (clean leader reassignment)** | Chuyển leader sang một replica khác **vẫn còn trong ISR** mà không có replica nào chết, ví dụ khi rebalance, rolling restart hay controlled shutdown. Trong mã là `ReassignmentLeaderElection`. |
| **Phân kỳ (divergence)** | Hai replica có **nội dung khác nhau tại cùng offset**. |
| **Leader epoch** | Số thế hệ của leader, tăng mỗi lần đổi leader. Giao thức như Kafka dùng nó để biết follower phải cắt (truncate) log về đâu khi đổi leader. |
| **KIP-101** | Đề xuất của Kafka nêu rõ vì sao chỉ dựa vào HW để cắt log là không đủ và cần leader epoch. Mã Fluss có comment tham chiếu tới đề xuất này. |

## Tóm tắt

Trong giao thức nhân bản của Fluss, khi đổi leader, các replica chỉ dựa vào **offset** để biết mình có "khớp" với leader hay không. Nếu một follower vừa nhận thêm dữ liệu từ leader cũ mà replica khác (leader mới) thì chưa nhận, hai bên có thể có nội dung khác nhau tại cùng offset. Leader mới ghi dữ liệu mới đè lên đúng các offset đó, trả OK cho client với `acks=-1`, trong khi một replica vẫn nằm trong ISR giữ dữ liệu cũ. Kết quả là các replica trong ISR **không giống nhau** dù không có replica nào chết.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-server`, giao thức nhân bản log (replication) giữa leader và follower, cộng với bước bầu leader mới của Coordinator.
- **Các lớp liên quan** (đã đọc tại tag `v1.0.0`):
  - `ReplicaLeaderElection.ReassignmentLeaderElection`: chọn leader mới là replica đầu tiên còn sống và nằm trong ISR theo kế hoạch rebalance. Nó không xét xem replica đó có nội dung khớp với replica khác hay không.
  - `ReplicaFetcherThread`: luồng follower dùng để kéo dữ liệu từ leader. Hai hàm đáng chú ý:
    - `fetchOffsetAndTruncate(...)`: chỉ cắt log follower khi **LEO của leader nhỏ hơn LEO của follower** (khi offset nằm ngoài phạm vi). Chính comment trong hàm viết: *"There is a potential for a mismatch between the logs of the two replicas here. We don't fix this mismatch as of now."* và: *"In the second case, there will be some inconsistency of data between old and new leader. We are not solving it here."*
    - `truncateToLeaderEndOffsetSnapshot(...)`: cắt log follower về LEO mà leader mới ghi nhận lúc lên làm leader. Hàm này chỉ được gọi trên **đường xử lý lỗi** (khi gặp `DuplicateSequenceException`, `OutOfOrderSequenceException` hoặc `InvalidOffsetException`), kèm comment `TODO this part of logic need to be removed after we introduce leader epoch cache. Trace by .../issues/673`.
  - Đoạn cập nhật HW của follower: có comment `TODO, to avoid lose data in case of leader change ... KIP-101 ... if we want to have a strong consistency guarantee, we should do as KIP-101 do, trace by: .../issues/673`.
  - `Replica`: có `leaderEpoch` nhưng nó dùng để từ chối request của epoch cũ; mình chưa thấy nó được dùng để xác định điểm cắt log của follower.
- **Nguyên nhân gốc theo issue:** giao thức coi "cùng offset" là bằng chứng của "cùng lịch sử", nên không có bước thống nhất một tiền tố chung khi đổi leader. Chỉ kiểm tra tính liên tục của offset không phát hiện được vì dữ liệu khác nhau nằm đúng trên các offset trùng nhau.
- **Hướng sửa theo issue:** cần cơ chế hòa giải dựa trên **leader epoch**, xét cùng với quan hệ giữa HW của follower và LEO cục bộ.

## 2. Khi nào lỗi xảy ra

Kịch bản tái hiện trong issue (log table thường, không có secondary index; 3 replica A, B, C; A là leader; ISR = [A, B, C]):

| Bước | Việc xảy ra | A (leader) | B | C |
|---|---|---|---|---|
| 1 | Ghi `[0,10)` và commit trên cả ba | LEO 10 | LEO 10 | LEO 10 |
| 2 | Ghi thêm `[10,15)` vào A với `acks=1`. Phản hồi nhân bản chỉ tới được C; B vẫn ở 10 và **vẫn nằm trong ISR**; HW của A vẫn là 10 | LEO 15 (HW 10) | LEO 10 | LEO 15 (có dữ liệu cũ của A) |
| 3 | Đổi leader sạch sang B (`ReassignmentLeaderElection`) | follower | **leader** | follower |
| 4 | Client ghi bản ghi **khác** vào B; B có LEO 10 nên chúng chiếm `[10,15)`. Với `acks=-1`, ghi này **trả OK** | | `[10,15)` = bản ghi mới | |
| 5 | Tiếp tục nhân bản từ B sang A và C | `[10,15)` = bản ghi mới của B | | `[10,15)` = **bản ghi cũ của A** |

Kết quả cuối (theo issue): HW của cả ba là 15, nhưng C vẫn nằm trong ISR với bản ghi cũ tại offset 10–14, còn A và B có bản ghi mới. Phần đã commit `[0,10)` vẫn nguyên vẹn; sai lệch nằm ở các bản ghi **thay thế** mà leader mới đã xác nhận.

**Điều kiện cần:**
1. Ghi bằng `acks=1` hoặc leader có dữ liệu chưa được cả ISR nhận (trong ví dụ, B chưa nhận `[10,15)`).
2. Một follower còn trong ISR nhưng đang tụt lại sau leader.
3. Xảy ra đổi leader **sạch** (không cần crash) sang đúng follower đang tụt lại đó.
4. Có ghi mới vào leader mới trước khi các follower hòa giải xong.

**Những tình huống thực tế dễ dẫn tới:** rolling restart/rebalance/controlled shutdown (đều gây đổi leader sạch), kết hợp ghi nặng liên tục. Lưu ý các tình huống này đúng là điều `relate_issue.md` khuyến nghị tránh làm khi đang ghi nặng.

**Mình chưa tự dựng lại cơ chế chi tiết ở bước 5:** theo mã, follower chỉ cắt log khi LEO của leader nhỏ hơn LEO của nó hoặc khi gặp ba loại lỗi nêu trên. Mình chưa lần theo từng nhánh để giải thích vì sao trong bảng của issue A kết thúc với bản ghi của B còn C thì không; nên mình chỉ trích lại kết quả mà issue báo cáo.

**Phạm vi:** issue tái hiện trên log table thường. Mình chưa kiểm tra riêng với bảng khóa chính (KV), nơi mỗi replica còn dựng KV từ log.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành cluster có replication factor ≥ 2, đặc biệt khi ghi nặng liên tục trong lúc rolling restart, rebalance hoặc controlled shutdown; mọi đường đọc phụ thuộc vào việc "các replica nhất quán với nhau".

**Sai lệch:**
1. **Các replica trong ISR không đồng nhất.** Cùng offset nhưng nội dung khác nhau, trong khi không có replica nào bị đánh dấu ra khỏi ISR và HW đã tiến lên.
2. **Dữ liệu đã được `acks=-1` xác nhận có thể không còn hoặc khác ở một replica.** Nếu sau đó leader hiện tại hỏng và C (giữ bản ghi cũ) trở thành leader, client có thể đọc thấy bản ghi **khác** với bản ghi đã được xác nhận trước đó.
3. **Lỗi im lặng.** Không có exception hay cảnh báo ở thời điểm xảy ra; chỉ lộ ra khi đọc từ replica khác hoặc sau failover. Mình suy ra điều này từ mô tả của issue; chưa chạy thực tế.
4. **Với bảng khóa chính (CDP):** log này được dùng để dựng KV và changelog. Nếu replica phân kỳ trở thành leader hoặc được dùng để khôi phục KV, trạng thái có thể khác nhau giữa các replica. Đây là suy luận của mình, issue không nói trực tiếp về bảng PK.

**Điều issue xác nhận là an toàn:** phần tiền tố đã commit trước đó (`[0,10)`) không bị ảnh hưởng.

**Biện pháp giảm thiểu:**
- Tránh rebalance và chuyển leader thủ công khi đang ghi nặng.
- Rolling restart từng node và **chờ ISR đầy đủ, replica lag về 0** trước khi chuyển sang node kế tiếp.
- Dùng `acks=-1` (giảm nhưng không loại bỏ rủi ro, vì chính issue chỉ ra lỗi xảy ra dù `acks=-1` ở bước 4).
- Giám sát replica lag và kích thước ISR; theo dõi tiến độ PR #4273 và việc triển khai leader epoch ở #673.
