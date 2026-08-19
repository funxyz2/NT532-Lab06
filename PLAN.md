# PLAN — Webcam stream + Kafka-based recognition overlay

Decisions (from user): live preview window, boxes drawn ONLY from `recognition-result` consumed via Kafka, send 1 FPS by default.

## Batch 1 — Mở rộng kết quả nhận diện Kafka

> Source context: Tái sử dụng cấu trúc `bbox`, `probability` và cách vẽ màu từ `source 1/app.py:168-205`; không chạy nhận diện cục bộ ở edge client.

- [ ] #1 Refactor nhận diện Kafka để trả về mọi khuôn mặt gồm tên, xác suất và tọa độ box
  DoD: Hàm nhận diện trả danh sách `{recognized_name, probability, bbox: [x1, y1, x2, y2]}`; box được ép kiểu số nguyên và giới hạn trong kích thước frame; không có khuôn mặt trả danh sách rỗng.
  Tag: [judgment]
  File: source 1/src/face_rec_flask.py:116-157

- [ ] #2 Giữ tương thích cho Flask `/recog` trong khi sử dụng kết quả nhiều khuôn mặt
  DoD: `/recog` vẫn trả chuỗi tên khuôn mặt đầu tiên hoặc `"Unknown"` như trước; thay đổi cấu trúc nội bộ không phá `source 1/client.py`.
  Tag: [judgment]
  File: source 1/src/face_rec_flask.py:267-282

- [ ] #3 Mở rộng message `recognition-result` với danh sách khuôn mặt và kích thước frame nguồn
  DoD: Mỗi message giữ các field cũ và thêm `faces` cùng `frame_size`; mỗi phần tử `faces` có `recognized_name`, `probability`, `bbox`; `recognized_name` cấp cao vẫn là khuôn mặt đầu tiên hoặc `"Unknown"`.
  Tag: [judgment]
  File: source 1/src/face_rec_flask.py:90-114

- [ ] #4 Gửi kết quả mở rộng sau khi ráp đủ chunk và nhận diện frame
  DoD: Kết quả publish có cùng `file_id` với frame đầu vào, chứa box tính trực tiếp trên frame JPEG đã ráp, và không publish box giả khi decode hoặc nhận diện thất bại.
  Tag: [mechanical]
  File: source 1/src/face_rec_flask.py:200-253

## Batch 2 — Webcam producer trên MacBook

- [ ] #5 Thay dataset producer bằng vòng lặp webcam AVFoundation
  DoD: Client mở camera index `0` trên macOS, đọc frame liên tục, hiện lỗi rõ ràng nếu không mở được camera, và không còn phụ thuộc `source 1/Dataset/raw` hay danh sách `people`.
  Tag: [judgment]
  File: edge-stream-client.py:1-25,84-134

- [ ] #6 Encode và gửi một frame JPEG mỗi giây theo chunk contract hiện tại
  DoD: Mặc định tối đa 1 frame/giây; mỗi frame có `file_id` riêng; JPEG tiếp tục được chia thành chunk 512 KiB và gửi vào `device-subscribe` với `file_id`, `person`, `filename`, `chunk_index`, `total_chunks`; không `flush()` sau từng frame.
  Tag: [judgment]
  File: edge-stream-client.py:84-126

- [ ] #7 Thêm điều khiển vòng đời webcam và Kafka an toàn
  DoD: Nhấn `q`, Esc hoặc `Ctrl+C` dừng chương trình; camera được release, cửa sổ OpenCV được đóng, producer được flush, consumer được đóng và thread kết thúc qua shared stop event.
  Tag: [judgment]
  File: edge-stream-client.py:36-134

## Batch 3 — Overlay box từ Kafka lên live preview

- [ ] #8 Đồng bộ kết quả Kafka sang luồng giao diện bằng trạng thái thread-safe
  DoD: Consumer parse `faces`, `frame_size` và `file_id`, chỉ cập nhật overlay từ message `recognition-result`, bỏ qua payload lỗi mà không làm chết thread, và ngăn kết quả cũ ghi đè kết quả mới hơn.
  Tag: [judgment]
  File: edge-stream-client.py:36-82

- [ ] #9 Vẽ box Kafka mới nhất lên cửa sổ webcam live
  DoD: Main thread gọi `cv2.imshow()` cho mọi frame; mỗi box được scale từ `frame_size` của frame đã gửi sang kích thước live hiện tại; hiển thị tên và phần trăm xác suất; xanh cho người nhận diện được, đỏ cho `"Unknown"`.
  Tag: [judgment]
  File: edge-stream-client.py:84-134

- [ ] #10 Giới hạn thời gian tồn tại của overlay để tránh box cũ treo trên màn hình
  DoD: Box biến mất sau một khoảng TTL ngắn nếu không có kết quả Kafka mới; không có bất kỳ MTCNN/FaceNet hoặc logic tạo box nào chạy trong edge client.
  Tag: [judgment]
  File: edge-stream-client.py:36-134

## Batch 4 — Kiểm chứng và cập nhật contract

- [ ] #11 Kiểm tra cú pháp hai chương trình đã sửa
  DoD: `python -m py_compile edge-stream-client.py "source 1/src/face_rec_flask.py"` hoàn tất không lỗi.
  Tag: [mechanical]
  File: edge-stream-client.py; source 1/src/face_rec_flask.py

- [ ] #12 Kiểm tra thủ công luồng webcam–Kafka–cloud–overlay
  DoD: Server nhận chunk, trả message có đúng `file_id`, `faces`, `bbox`, `probability`; client gửi khoảng 1 FPS; cửa sổ live chỉ vẽ box sau khi consume kết quả Kafka; `q` đóng chương trình sạch sẽ.
  Tag: [judgment]
  File: edge-stream-client.py; source 1/src/face_rec_flask.py

- [ ] #13 Cập nhật hướng dẫn kiến trúc và message contract
  DoD: Tài liệu ghi rõ edge client lấy webcam 1 FPS, result có `faces`/`frame_size`, và overlay live có độ trễ vì dùng tọa độ từ frame Kafka trước đó.
  Tag: [mechanical]
  File: AGENTS.md:6-70; source 1/AGENTS.md:28-55

## Global Definition of Done

- Batch 2–3 chỉ bắt đầu sau khi payload Batch 1 được xác định và giữ tương thích với `/recog`.
- Overlay phải dùng duy nhất `bbox` nhận từ Kafka; edge client không được tự detect hoặc suy đoán box.
- Box có thể lệch khi người di chuyển trong thời gian round-trip Kafka; scaling và TTL phải hoạt động nhưng face tracking không thuộc phạm vi này.

## Out of scope

- Không sửa pipeline RTSP/ZeroMQ trong `source 1/app.py`.
- Không tích hợp payload này với topic `recognition_result` của `iot_platform`.
- Không sửa legacy relay trong `source 2`.
- Không thêm face tracking để dự đoán chuyển động giữa frame đã gửi và frame live hiện tại.
