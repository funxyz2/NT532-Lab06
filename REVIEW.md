# REVIEW — Webcam stream + Kafka-based recognition overlay

Scope: only findings that affect the expected behavior (Kafka pub/sub image flow:
Device produces frames → Edge Server FaceNet recognition → result back via pub/sub →
Device displays result). Security/hardening findings intentionally excluded per user.

## Batch 1 — Edge Server nhận diện và trả kết quả

- [x] #1 [MEDIUM] Frame webcam được đưa vào FaceNet dưới dạng BGR, không nhất quán với RGB của dữ liệu huấn luyện
  Fix DoD: Cùng một ảnh thử cho kết quả nhận diện và xác suất tương đương giữa Kafka pipeline và `source 1/app.py`; màu đầu vào MTCNN/FaceNet nhất quán với dữ liệu huấn luyện.
  Tag: [judgment]
  File: source 1/src/face_rec_flask.py:125-150; source 1/app.py:88-125
  Resolved: `recognize_faces()` now converts BGR→RGB once before MTCNN/FaceNet and crops from the RGB frame (mirrors `app.py`); `None` frames return `[]`. Compile-checked; visual confirmation pending the end-to-end run in #3.

## Batch 3 — Device nhận và hiển thị kết quả

- [x] #2 [HIGH] Client không xác minh `file_id` thuộc frame do chính nó gửi, nên có thể hiển thị kết quả cũ hoặc kết quả của device khác
  Fix DoD: Mỗi device chỉ hiển thị kết quả có `file_id` thuộc frame mà chính device đó đã gửi trong phiên hiện tại; chạy đồng thời hai client không làm client A vẽ kết quả của client B, và backlog Kafka cũ không xuất hiện trên preview.
  Tag: [judgment]
  File: edge-stream-client.py:46-98,116-143
  Resolved: per-session pending set (`add_pending` on send, `take_pending` before applying overlay, bounded at 1000). Unit smoke test passed: stale backlog ignored, other-device results ignored, own results applied exactly once, cap eviction and 4-thread concurrency clean.

## Coverage gaps

- [ ] #3 [MEDIUM] Chưa có bằng chứng chạy end-to-end với Kafka, webcam và model FaceNet thật
  Fix DoD: Một lần chạy thực tế xác nhận webcam gửi khoảng 1 FPS vào `device-subscribe`, Edge Server nhận và nhận diện bằng FaceNet, publish `recognition-result`, đúng device consume kết quả và hiển thị tên/bbox trên cửa sổ; `q` kết thúc client sạch sẽ.
  Tag: [mechanical]
  File: edge-stream-client.py:46-294; source 1/src/face_rec_flask.py:90-291
  Note: requires user environment (broker `172.20.66.139:9092`, TF1 venv, webcam permission). Runbook below.

## End-to-end runbook (cho #3)

1. `cd "source 1" && python src/face_rec_flask.py` (cần broker Kafka chạy tại `172.20.66.139:9092`).
2. Trong terminal khác: `python edge-stream-client.py` (mặc định 1 FPS; cấp quyền Camera cho terminal nếu lần đầu).
3. Kỳ vọng: client log `Queued: webcam frame N` mỗi giây; server log nhận chunk + tên nhận diện; client log `[Results] ✓` rồi box xanh/đỏ + tên hiện trên cửa sổ, tự biến mất sau 3 s nếu không có kết quả mới.
4. Thử 2 client đồng thời (chạy lệnh 2 lần): mỗi cửa sổ chỉ vẽ kết quả của chính nó.
5. `q`/Esc/Ctrl+C: cửa sổ đóng, log `Released camera and flushed producer`.

## Batch status
- Batch 1: complete — #1 fixed (verify visually in #3)
- Batch 2: complete
- Batch 3: complete — #2 fixed
- Batch 4: compile passed; #3 open (needs manual end-to-end run)
