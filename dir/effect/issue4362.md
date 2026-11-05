# Issue #4362 — Coordinator không giành lại được leadership sau khi ZooKeeper session hết hạn

Link: https://github.com/apache/fluss/issues/4362 — Phiên bản được báo: `0.9.0`

## Tóm tắt

Trong một cluster chỉ có **một Coordinator**, Coordinator bị GC nặng nên mất kết nối tới ZooKeeper, session hết hạn. Sau đó kết nối ZooKeeper được khôi phục và Coordinator dọn dẹp các dịch vụ leader thành công, nhưng **không bao giờ được bầu lại làm leader**. Tiến trình Java vẫn sống, vẫn xuất metrics, nhưng các bộ đếm xử lý sự kiện đứng yên. Chỉ khi restart tiến trình thì leadership mới trở lại.

Diễn biến trong log của issue:

```text
00:07:51  Client session timed out, have not heard from server in 162997ms
01:01:06  State change: SUSPENDED
01:19:16  Coordinator server <server-id> has lost the leadership, cleaning up leader services.
01:19:16  Session expired event received
01:19:16  State change: LOST
01:19:16  State change: RECONNECTED
01:19:41  Coordinator leader services cleaned up successfully.
(không có dòng "has become the leader" nào sau đó cho tới khi restart thủ công)
```

## 1. Lỗi nằm ở những thành phần nào

- **Module:** `fluss-server`, cơ chế bầu leader của Coordinator.
- **Lớp liên quan** (đã đọc tại tag `v1.0.0`): `CoordinatorLeaderElection`. Nó dùng `LeaderLatch` của **Apache Curator** trên znode bầu cử (`ZkData.CoordinatorElectionZNode.path()`). Khi được bầu, `LeaderLatchListener.isLeader()` gọi khởi tạo dịch vụ leader; khi mất, `notLeader()` chuyển về `STANDBY` rồi dọn dẹp. Thiết kế mong muốn là sau khi mất leader, tiến trình vẫn tiếp tục tham gia bầu và có thể được bầu lại nhiều lần.
- **Nguyên nhân gốc theo issue (chưa được xác nhận trong issue):** có thể đường dẫn znode cha dùng để bầu cử bị mất (ZooKeeper trả `NONODE`) sau khi session hết hạn, và `LeaderLatch` không tạo lại nó để tham gia bầu lại. Issue viết điều này dưới dạng giả thuyết ("if the suspected NONODE path is confirmed").
- **Trạng thái xử lý (từ bình luận trong issue):** người bảo trì (`platinumhamburg`) trả lời ngày 2026-10-10 rằng lỗi này "đã được sửa trong **fluss-shaded**", hiện chờ phát hành phiên bản mới của fluss-shaded. Tại `v1.0.0` và nhánh `main`, `pom.xml` vẫn dùng `fluss.shaded.version = 1.0-incubating` và `curator.version = 5.4.0`, nên bản sửa chưa nằm trong hai mốc này. Mình chưa xác minh bản sửa cụ thể là gì, hay nó nằm ở thư viện nào trong fluss-shaded.
- **Yếu tố kích hoạt:** GC kéo dài khiến Coordinator không gửi heartbeat tới ZooKeeper trong khoảng 163 giây (đến khi session hết hạn).

## 2. Khi nào lỗi xảy ra

Cần đồng thời:

1. **Coordinator mất ZooKeeper session** (hết hạn): thường do GC dừng ứng dụng quá lâu, nghẽn mạng, hoặc ZooKeeper quá tải.
2. Sau khi mất session, kết nối ZooKeeper phục hồi (`RECONNECTED`), nhưng bước bầu lại thất bại âm thầm, không có exception nào làm tiến trình thoát.
3. **Cluster chỉ có một Coordinator**, nên không có Coordinator nào khác thay thế. Nếu có Coordinator thứ hai thì nhiều khả năng nó sẽ được bầu thay (đây là suy luận, issue không nói). Tuy vậy, Coordinator bị kẹt vẫn không quay lại làm standby khỏe mạnh.

Không ảnh hưởng: các sự cố không làm mất session ZooKeeper, hoặc Coordinator mất leader tạm thời mà bầu lại thành công.

## 3. Tác động đến ai, làm sai lệch gì

**Ai bị ảnh hưởng:** người vận hành production chạy một Coordinator (hoặc tất cả Coordinator cùng bị kẹt), đặc biệt khi Coordinator có thể bị GC pressure hoặc kết nối ZooKeeper không ổn định.

**Hậu quả được ghi trong issue:**

1. **Cluster không có leader hoạt động.** Mọi tác vụ chỉ Coordinator làm được đều dừng: các bộ đếm xử lý sự kiện đứng yên.
2. **Tự động hết hạn partition ngừng.** Partition cũ không bị xóa, dữ liệu và dung lượng đĩa tăng dần.
3. **Replica hết hạn không được gỡ** như mong đợi.
4. **Xóa partition thủ công bị từ chối** vì Coordinator không phải leader.
5. **Lỗi im lặng.** Tiến trình vẫn chạy và vẫn xuất metrics, nên liveness probe hoặc cảnh báo "process up" không phát hiện được. Muốn khôi phục phải restart tay.

Những gì issue **không** nói: tác động lên đường đọc/ghi dữ liệu của TabletServer khi không có Coordinator leader. Mình cũng chưa kiểm tra việc này, nên không khẳng định được.

**Phương án sửa được đề xuất trong issue:**
1. Tự phục hồi bầu cử không cần restart: tạo lại znode cha khi thiếu và tiếp tục bầu, giữ nguyên thứ tự khởi tạo leader và cơ chế epoch của Coordinator (qua nâng Curator hoặc sửa riêng).
2. Phương án dự phòng "tự sát" có kiểm soát: khi không thể phục hồi leadership một cách an toàn thì thoát tiến trình để hệ thống triển khai (Kubernetes) khởi động lại, nhưng không được restart chỉ vì đang làm standby bình thường hay mất kết nối tạm thời.

**Gợi ý cho production:**
- Chạy từ 2 Coordinator trở lên để có dự phòng.
- Đặt cảnh báo trên metric thể hiện Coordinator có đang là leader hay không (ví dụ bộ đếm xử lý sự kiện đứng yên, số lượng bảng/partition không đổi dù lẽ ra phải đổi), không chỉ cảnh báo "process alive".
- Giảm nguy cơ GC pause dài trên Coordinator (kích thước heap, bộ thu gom rác), và theo dõi bản phát hành mới của fluss-shaded có chứa bản sửa.
