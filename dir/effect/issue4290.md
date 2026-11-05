# Tóm tắt: 
Một batch KV tạo WAL lớn hơn shared memory pool thì ghi treo vô thời hạn, giữ KV lock và RPC worker, kéo theo các request khác.
# Phiên bản: 0.9.0