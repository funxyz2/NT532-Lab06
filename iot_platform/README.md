# IoT Platform điểm danh

Service độc lập để lưu trữ và xem kết quả điểm danh từ Edge Server.

## IoT Platform hoạt động như thế nào?

Hệ thống hoàn chỉnh có luồng dữ liệu như sau:

```text
Raspberry Pi + Camera
        |
        | Ảnh JPEG qua Kafka topic `send_image`
        v
Kafka Broker
        |
        v
Edge Server
  - nhận ảnh
  - MTCNN tìm khuôn mặt
  - FaceNet tạo embedding
  - SVM nhận diện sinh viên
        |
        | JSON qua Kafka topic `recognition-result`
        v
IoT Platform
  - app.py đọc JSON qua thread consumer ngầm
  - app.py kiểm tra dữ liệu
  - SQLite lưu attendance
        |
        v
Dashboard / REST API
```

### 1. IoT Device

Thiết bị IoT chụp ảnh từ camera và gửi ảnh dạng bytes/JPEG lên Kafka. Mỗi thiết
bị cần có `device_id` để phân biệt nguồn gửi và `room_id` để xác định vị trí.

### 2. Kafka Broker

Kafka làm lớp trung gian truyền message:

- `send_image`: ảnh từ IoT Device đến Edge Server.
- `recognition-result`: kết quả nhận diện từ Edge Server đến IoT Platform.
- Có thể dùng thêm topic riêng để gửi kết quả ngược về thiết bị.

### 3. Edge Server

Edge Server nhận ảnh, chạy mô hình FaceNet và tạo kết quả nhận diện. Message
gửi sang platform phải là JSON, không phải JPEG thô:

```json
{
  "timestamp": "2026-08-17T10:20:30+07:00",
  "room_id": "LAB-06",
  "student_name": "Nguyen Van A",
  "device_id": "pi-01",
  "check_type": "check-in"
}
```

`iot_platform/app.py` subscribe topic `recognition-result` (topic mà
Edge Server `source 1/src/face_rec_flask.py` publish) qua một daemon thread
trong cùng tiến trình và map kết quả nhận diện thành bản ghi điểm danh:

- `recognized_name` → `student_name` (kết quả `"Unknown"` bị bỏ qua).
- `person_label` → `device_id`.
- `timestamp` → `timestamp` (hiển thị dạng `h:m:s d/m/y` trên dashboard).
- `room_id` lấy từ message, nếu không có dùng biến môi trường `ROOM_ID`
  (mặc định `LAB-06`).
- `check_type` mặc định `check-in`.

Chỉ xác nhận check-in sau khi nhận diện **3 lần liên tiếp** cùng một sinh viên
(`ATTENDANCE_CONFIRM_COUNT`); kết quả `Unknown` hoặc sinh viên khác làm đứt
chuỗi. Sau khi check-in, sinh viên được giữ trạng thái đã điểm danh trong
`ATTENDANCE_TTL_SECONDS` (mặc định 300 giây = 5 phút) — không tạo bản ghi trùng;
hết TTL mới cần một chuỗi 3 lần nhận diện liên tiếp khác để điểm danh lại.
Trạng thái xác nhận nằm trong bộ nhớ tiến trình (mất khi restart app).

Message đã có sẵn schema đầy đủ (`room_id`, `student_name`, `device_id`,
`check_type`) sẽ được lưu nguyên như cũ. Không cần chạy consumer riêng;
đặt `KAFKA_CONSUMER_ENABLED=false` nếu chỉ muốn API. `kafka_consumer.py`
chỉ là wrapper standalone dùng khi cần debug.

### 4. Database

SQLite được lưu tại `data/attendance.db`. Mỗi bản ghi gồm:

- `timestamp`: thời điểm điểm danh.
- `room_id`: phòng/vị trí thiết bị.
- `student_name`: tên sinh viên.
- `device_id`: ID thiết bị gửi dữ liệu.
- `check_type`: `check-in` hoặc `check-out`.
- `created_at`: thời điểm platform ghi dữ liệu.

### 5. Dashboard và API

Dashboard gọi REST API để hiển thị dữ liệu. API cũng cho phép Edge Server hoặc
client khác gửi bản ghi trực tiếp mà không cần Kafka.

```text
POST /api/attendance          Tạo bản ghi điểm danh
GET  /api/attendance          Xem danh sách, có filter
GET  /api/attendance/summary   Thống kê theo sinh viên/phòng/tháng
GET  /health                   Kiểm tra service
```

## Chức năng

- REST API nhận `timestamp`, `room_id`, `student_name`, `device_id` và `check_type`.
- SQLite lưu dữ liệu bền vững trong `data/attendance.db`.
- Dashboard tại `/`.
- Thống kê theo sinh viên, phòng và tháng.
- Kafka consumer chạy ngầm trong app đọc JSON từ topic `recognition-result`.

## Chạy local trên Windows

Mở PowerShell tại thư mục `iot_platform`:

```bash
cd iot_platform
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe app.py
```

Mở <http://127.0.0.1:5000> trên trình duyệt. Database sẽ tự động được tạo tại
`data/attendance.db`.

Kiểm tra service:

```powershell
Invoke-RestMethod http://127.0.0.1:5000/health
```

Gửi một bản ghi thử:

```powershell
$body = @{
    room_id = "LAB-06"
    student_name = "Nguyen Van A"
    device_id = "pi-01"
    check_type = "check-in"
} | ConvertTo-Json

Invoke-RestMethod `
    -Uri http://127.0.0.1:5000/api/attendance `
    -Method Post `
    -ContentType "application/json" `
    -Body $body
```

Xem danh sách điểm danh:

```powershell
Invoke-RestMethod http://127.0.0.1:5000/api/attendance
```

Nếu đã có môi trường Python và muốn chạy nhanh:

```powershell
.\.venv\Scripts\python.exe app.py
```

Nhấn `Ctrl+C` để dừng service.

## Chạy bằng Docker

Từ thư mục `iot_platform`:

```bash
docker compose up --build
```

Mặc định container kết nối Kafka tại `host.docker.internal:9092`. Có thể thay bằng
`KAFKA_BOOTSTRAP_SERVERS` nếu Kafka chạy ở hostname khác.

Dashboard vẫn truy cập tại <http://127.0.0.1:5000>.

## Kafka consumer

Consumer đọc topic `recognition-result` và chấp nhận JSON dạng Edge Server
`source 1` publish (xem mục "IoT Platform hoạt động như thế nào" ở trên), hoặc
bản ghi điểm danh đầy đủ:

```json
{
  "timestamp": "2026-08-17T10:20:30+07:00",
  "room_id": "LAB-06",
  "student_name": "Nguyen Van A",
  "device_id": "pi-01",
  "check_type": "check-in"
}
```

Chạy API + consumer (consumer chạy ngầm trong app):

```bash
python app.py
```

## Checklist hoàn thiện Lab06

### Đã có trong repo

- [x] Flask REST API.
- [x] SQLite schema lưu điểm danh.
- [x] Dashboard xem danh sách và thống kê cơ bản.
- [x] Kafka consumer cho topic `recognition_result`.
- [x] Dockerfile và Docker Compose cho IoT Platform.
- [x] Validation các trường `room_id`, `student_name`, `device_id`, `check_type`.

### Còn thiếu cần làm

1. **Tích hợp Edge Server với FaceNet và Kafka**
   - Nhận ảnh từ topic `send_image`.
   - Chạy MTCNN + FaceNet + SVM.
   - Publish JSON nhận diện vào `recognition_result`.
   - Hiện `source 2/recognize.py` mới chỉ đóng dấu `PROCESSED`, chưa nhận diện.

2. **Đồng bộ format message**
   - Bổ sung `device_id`, `room_id`, `timestamp` vào message ảnh hoặc cấu hình
     cố định theo từng thiết bị.
   - Dùng ISO-8601 cho timestamp.
   - Thống nhất tên topic giữa Edge Server và Platform.

3. **Consumer nhận kết quả ở IoT Device**
   - Viết chương trình subscribe topic kết quả.
   - Hiển thị tên sinh viên, trạng thái điểm danh và lỗi trên Raspberry Pi.

4. **Hỗ trợ tối thiểu hai thiết bị đồng thời**
   - Chạy hai producer với `device_id` khác nhau.
   - Kiểm tra Kafka không mất message và platform lưu đúng `room_id`/`device_id`.

5. **Hoàn thiện nghiệp vụ điểm danh**
   - Chống ghi trùng trong khoảng thời gian ngắn.
   - Xác định quy tắc check-in/check-out.
   - Thêm lọc theo ngày, tháng, phòng và sinh viên trên dashboard.

6. **Xác nhận mô hình AI**
   - Thu thập dataset nhóm.
   - Train classifier.
   - Chạy đánh giá độc lập và ghi lại accuracy trên 60%.
   - Không commit dataset/model nặng vào GitHub; dùng hướng dẫn tải/cấu hình.

7. **Kiểm thử và minh chứng nộp bài**
   - Test API và Kafka end-to-end.
   - Chụp màn hình broker, Edge Server, database và dashboard.
   - Quay video hai thiết bị gửi đồng thời.
   - Viết báo cáo và đóng gói theo định dạng đề yêu cầu.

## Thứ tự nên làm tiếp

1. Sửa Edge Server để publish JSON `recognition_result`.
2. Chạy thử một thiết bị end-to-end.
3. Thêm consumer hiển thị kết quả ở thiết bị.
4. Chạy hai thiết bị đồng thời và kiểm tra dữ liệu.
5. Bổ sung chống trùng, thống kê và tài liệu minh chứng.
