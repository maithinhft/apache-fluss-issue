# Issue #2698 — Client SASL hỏng khi mật khẩu (hoặc username) chứa ký tự đặc biệt như `"` hoặc `\`

Link: https://github.com/apache/fluss/issues/2698 — Phiên bản được báo: `main` thời 0.9 (người báo: morazow, 20/02/2026). Issue kèm một test lỗi (mật khẩu `pa$$wo\rd!@#%&"`), chỉ ra dòng ghép chuỗi trong `SaslClientAuthenticator`, và đề nghị kiểm tra cả phía server. Không có bình luận. Trạng thái: còn mở. PR [#2701](https://github.com/apache/fluss/pull/2701) **đang mở**.
**Nhóm B:** chỉ khi bật xác thực **SASL/PLAIN** bằng `client.security.sasl.username` và `client.security.sasl.password`.

> **Mức chắc chắn:** cao. Mình đã đọc mã `v1.0.0`:
> - chuỗi JAAS được **ghép bằng `String.format` mà không escape**;
> - sau đó được **phân tích bằng `java.io.StreamTokenizer`**, và lớp này **xử lý escape kiểu Java** trong chuỗi có ngoặc kép.
>
> Vì vậy không chỉ `"` làm hỏng cú pháp, mà **`\` cũng làm mật khẩu bị đổi âm thầm**. Ví dụ `\r` thành ký tự xuống dòng. Mình chưa chạy test của issue.

## 0. Giải thích các thuật ngữ trước

| Thuật ngữ | Nghĩa trong ngữ cảnh này |
|---|---|
| **SASL / PLAIN** | Xác thực username và password giữa client và server Fluss (`client.security.sasl.mechanism = PLAIN`). |
| **JAAS config** | Chuỗi cấu hình đăng nhập kiểu Java: `org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username="..." password="...";`. Giá trị nằm trong **ngoặc kép**. |
| **`client.security.sasl.jaas.config`** | Option cho phép đưa **nguyên chuỗi JAAS** (thay cho cặp username và password riêng). Fluss 1.0.0 chỉ chấp nhận `PlainLoginModule` trong chuỗi này. |
| **`StreamTokenizer`** | Bộ tách token của Java. Với chuỗi trong ngoặc kép, nó hiểu các escape `\"`, `\\`, `\n`, `\r`, `\t`, `\b`, `\f` và số bát phân. |
| **Escape** | Thêm `\` trước ký tự đặc biệt để nó được hiểu là ký tự thường. |

## Tóm tắt

Khi cấu hình `client.security.sasl.username` và `client.security.sasl.password`, client ghép chuỗi JAAS bằng mẫu `... username="%s" password="%s";`. Sau đó chuỗi được phân tích bằng `StreamTokenizer`:
- **`"`** trong mật khẩu **kết thúc chuỗi sớm**, nên cú pháp JAAS vỡ và xác thực lỗi;
- **`\`** trong mật khẩu được hiểu là **bắt đầu escape**: `\r` thành ký tự CR, `\n` thành xuống dòng, `\\` thành `\`... Mật khẩu **bị biến đổi âm thầm**, gửi sai lên server, và xác thực thất bại với thông báo kiểu "sai mật khẩu".

Mật khẩu trong test của issue (`pa$$wo\rd!@#%&"`) chứa cả hai loại.

## 1. Lỗi nằm ở thành phần nào

- **Module:** `fluss-common`, gồm `security/auth/sasl/authenticator/SaslClientAuthenticator` và `security/auth/sasl/jaas/JaasConfig`.
- **Đã xác nhận trong mã `v1.0.0`:**
  - `SaslClientAuthenticator`:
    ```java
    private static final String JAAS_CONF_FORMAT =
        "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
    ...
    String jaasConfigStr = configuration.getString(CLIENT_SASL_JAAS_CONFIG);
    if (jaasConfigStr != null) {
        validatePlainLoginModule(jaasConfigStr);          // dùng nguyên chuỗi người dùng đưa
    } else if (mechanism.equals(PlainSaslServer.PLAIN_MECHANISM)) {
        String username = configuration.get(CLIENT_SASL_JAAS_USERNAME);
        String password = configuration.get(CLIENT_SASL_JAAS_PASSWORD);
        ...
        jaasConfigStr = String.format(JAAS_CONF_FORMAT, username, password);   // không escape
    }
    ```
  - `JaasConfig`: `StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(jaasConfigParams));`, với `wordChars` cho `-`, `_`, `$`. Không đổi `quoteChar` mặc định (`"`), nên escape trong chuỗi ngoặc kép được xử lý theo hành vi chuẩn của `StreamTokenizer`. Hành vi escape này là kiến thức về `java.io.StreamTokenizer`, mình không đọc mã JDK.
  - **Phía server:** `PlainServerCallbackHandler` đọc user và mật khẩu từ JAAS server dạng `user_${username} = ${password}`. Nếu cấu hình bằng file hoặc chuỗi JAAS thì người cấu hình tự escape. Mình chưa kiểm tra có đường nào phía server tự ghép chuỗi từ option riêng hay không.
- **Trên `main`:** #3865 (*"Make SASL mechanisms extensible"*, 04/11/2026) tái cấu trúc SASL nhưng **vẫn giữ** mẫu `String.format` không escape (mình đã thấy cùng mẫu trong `main`).

## 2. Khi nào lỗi xảy ra

- Client dùng **`client.security.sasl.username` và `client.security.sasl.password`** (không dùng `jaas.config`).
- Username hoặc password chứa **`"`** hoặc **`\`**. Mật khẩu sinh tự động bởi trình quản lý bí mật (Vault, AWS Secrets Manager) hay chứa ký tự đặc biệt.

**Phiên bản:** báo trên `main` thời 0.9. Còn ở `1.0.0` và `main`.

## 3. Tác động đến ai, làm sai lệch gì

- **Client không kết nối được** cluster (xác thực thất bại). Lỗi lộ ra **ngay khi khởi động**, nhưng **khó chẩn đoán**: trường hợp `\` cho lỗi "sai mật khẩu" trong khi mật khẩu cấu hình "đúng".
- **Không ảnh hưởng dữ liệu.**

## 4. Đính chính / điều chưa chắc so với `relate_issue.md`

Dòng nhóm "SASL / security token" trong `relate_issue.md` ghi *"#2698 (mật khẩu có ký tự đặc biệt) | 🟡 | ❌"*. Đối chiếu:
- **Đúng.**
- **Bổ sung:** không chỉ `"` mà cả **`\`** cũng gây lỗi (biến đổi âm thầm). Có cách thay thế hợp lệ qua `client.security.sasl.jaas.config` (mục 5).

## 5. Mức ảnh hưởng với CDP

- Liên quan nếu bật SASL cho cluster Fluss, điều nên làm vì CDP chứa dữ liệu khách hàng, và mật khẩu được sinh tự động.

**Biện pháp giảm thiểu (suy ra từ phân tích, chưa kiểm chứng trên Fluss):**
- **Cách 1:** sinh mật khẩu **không chứa `"` và `\`**. Các ký tự đặc biệt khác (`$`, `!`, `@`, `#`, `%`, `&`) không bị `StreamTokenizer` xử lý đặc biệt trong chuỗi ngoặc kép.
- **Cách 2:** không dùng `username` và `password` riêng. Tự viết **`client.security.sasl.jaas.config`** với giá trị **đã escape** (`\"` cho `"`, `\\` cho `\`):
  ```
  client.security.sasl.jaas.config: org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required username="app" password="pa$$wo\\rd!@#%&\"";
  ```
  Lưu ý thêm một lớp escape nữa nếu giá trị đi qua YAML, properties hoặc biến môi trường.
- **Test kết nối** với đúng mật khẩu production trong môi trường thử.

## 6. Những điều chưa kiểm chứng

- Mình chưa chạy test của issue trên 1.0.0.
- **Phía server khi triển khai bằng Helm (đã đọc template `v1.0.0`):** `helm/templates/configmap-jaas-template.yaml` sinh `jaas.conf` bằng cách chèn thẳng biến môi trường vào chuỗi trong ngoặc kép:
  ```
  internal.FlussServer { ... PlainLoginModule required user_${FLUSS_JAAS_INTERNAL_USERNAME}="${FLUSS_JAAS_INTERNAL_PASSWORD}"; };
  FlussClient { ... username="${FLUSS_JAAS_INTERNAL_USERNAME}" password="${FLUSS_JAAS_INTERNAL_PASSWORD}"; };
  ```
  Vậy mật khẩu **nội bộ** (server với server) có `"` hoặc `\` cũng sẽ hỏng **ở phía server**. Mình chưa kiểm tra phần user của listener CLIENT (`security.client.sasl.plain.users`) được chèn thế nào, và chưa chạy thử.
- Mình chưa kiểm tra Rust client hay Gateway xử lý mật khẩu đặc biệt thế nào.
