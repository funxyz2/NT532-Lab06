# AGENTS.md

## Overview
This repository contains three Python services/demos plus one root integration client. Keep their dependency and message contracts separate unless an integration change explicitly spans them.

- `source 1/` — TensorFlow 1.x FaceNet/MTCNN recognition, SVM training, Flask `/recog`, Kafka image consumption, and an RTSP/ZeroMQ pipeline. Read `source 1/AGENTS.md` first.
- `source 2/` — legacy Kafka/OpenCV image relay demo. Read `source 2/AGENTS.md` first.
- `iot_platform/` — Flask + SQLite attendance API/dashboard with an optional Kafka JSON consumer.
- `edge-stream-client.py` — webcam Kafka producer: sends 1 FPS JPEG frames as 512 KiB chunks, consumes `recognition-result`, and draws boxes on the live preview.

## Commands
### Face recognition (`source 1`)
```bash
cd "source 1"
python src/align_dataset_mtcnn.py Dataset/raw Dataset/processed \
  --image_size 160 --margin 32 --random_order --gpu_memory_fraction 0.25
python src/classifier.py TRAIN Dataset/processed \
  Models/20180402-114759.pb Models/facemodel.pkl --batch_size 1000
python src/face_rec_flask.py
python client.py
python app.py --source rtsp://127.0.0.1:8554/stream --detect-every 10 \
  --max-dimension 960 --reconnect-delay 2.0 \
  --publish tcp://127.0.0.1:5556 --camera-id camera-01
```

### Legacy Kafka demo (`source 2`)
```bash
cd "source 2"
docker-compose -f docker-compose.yml up -d
# arm64 alternative
docker-compose -f docker-compose2.yml up -d
python test_send.py && python test_receive.py
python recognize.py
python send_image.py
```

### Attendance platform (`iot_platform`)
```bash
cd iot_platform
python -m venv .venv
python -m pip install -r requirements.txt
python app.py
python kafka_consumer.py
# Docker alternative

docker compose up --build
```

### Chunked Kafka integration
```bash
python edge-stream-client.py
```
Run `source 1/src/face_rec_flask.py` first; both scripts currently require the same hardcoded Kafka broker.

## Architecture and contracts
- `source 1` must remain TF1-style (`tf.compat.v1`, graph/session APIs). Its training flow is `Dataset/raw/<person>` → MTCNN-aligned `Dataset/processed/<person>` → linear SVM in `Models/facemodel.pkl`; class names replace `_` with spaces.
- `source 1/src/face_rec_flask.py` loads models at import time, serves port 8000, and also consumes 512 KiB image chunks from `device-subscribe`. Chunk metadata is carried in Kafka headers; results go to `recognition-result` as `{file_id, filename, person_label, recognized_name, faces, frame_size, timestamp}` — each `faces` entry has `recognized_name`, `probability`, `bbox: [x1,y1,x2,y2]` in the sent frame's coordinates.
- `edge-stream-client.py` reads the webcam (backend picked per OS: AVFoundation on macOS, DSHOW on Windows; camera index default 0 on Windows, 1 on macOS) and sends ~1 FPS JPEG frames to `device-subscribe` (person header = `DEVICE_ID`, default derived from hostname), waits for `recognition-result`, and draws only the Kafka-provided boxes on the live preview, scaled from `frame_size` to the current frame; boxes expire after a 3 s TTL. Only results whose `file_id` matches a frame this client sent in the current session are drawn, so stale backlog or other devices' results are ignored. Each client uses its own consumer group `edge-client-results-<DEVICE_ID>` with `auto.offset.reset=latest`, so multiple clients never compete for results; broker is `KAFKA_BOOTSTRAP_SERVERS` env (default `127.0.0.1:9092`). Run `source 1/src/face_rec_flask.py` first; it currently requires the same broker address hardcoded.
- `source 1/app.py` is a separate RTSP path: MTCNN → FaceNet → SVM → ZeroMQ multipart topic `recognition`; it requires `pyzmq`, which is not declared in `requirements.txt`.
- `source 2/config.py` is the broker-address source of truth for the legacy scripts. `send_image` carries raw JPEG bytes; `recognize.py` stamps and republishes JPEG to `receive_result` rather than performing face recognition.
- `iot_platform/app.py` initializes `data/attendance.db` at import, validates attendance records, exposes `/`, `/health`, `/api/attendance`, and `/api/attendance/summary` on port 5000, and by default starts the attendance Kafka consumer as a daemon thread in the same process (`attendance_kafka.py`), so no separate consumer process is needed; set `KAFKA_CONSUMER_ENABLED=false` for API-only mode.
- `iot_platform/attendance_kafka.py` consumes `recognition-result` (the `source 1` Edge Server topic) and maps it to attendance: `recognized_name` → `student_name`, `person_label` → `device_id`, `timestamp` → `timestamp`; `room_id` comes from the message or the `ROOM_ID` env (default `LAB-06`); `check_type` defaults to `check-in`. Results with no student name or `"Unknown"` are skipped. Messages that already carry the full attendance schema pass through unchanged. A check-in is only stored after `ATTENDANCE_CONFIRM_COUNT` (default 3) *consecutive* recognitions of the same student (Unknown or another student breaks the chain); once confirmed the student stays checked in for `ATTENDANCE_TTL_SECONDS` (default 300) and needs a fresh confirmation cycle afterward. The loop retries every 5 s when the broker is down so the Flask API stays alive. `iot_platform/kafka_consumer.py` is an optional standalone wrapper for debugging. Configuration is environment-based: `DATABASE_PATH`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `KAFKA_CONSUMER_ENABLED`, `ATTENDANCE_CONFIRM_COUNT`, `ATTENDANCE_TTL_SECONDS`, `ROOM_ID`, and `PORT`; keep `.env.example` readable. The dashboard formats timestamps as `h:m:s d/m/y`.

## Testing and conventions
- There is no automated test suite, lockfile, lint/typecheck configuration, or CI. Verify the changed flow manually; use `source 1/client.py` or classifier `CLASSIFY`, `source 2/test_send.py`/`test_receive.py`, and platform `/health` plus API requests.
- Preserve the style of the project being edited; do not cross-import the independent services merely to deduplicate code.
- Known `source 1` quirks: the tuple-form assert in `classifier.py` is an upstream no-op; recognition returns only the first detected face; model paths and Kafka broker settings are relative/hardcoded.
- Long-running commands: both Kafka consumers and `source 2/recognize.py` run until interrupted; `edge-stream-client.py` runs until `q`/Esc/`Ctrl+C`; `source 2/send_image.py` sleeps 60 seconds per frame.
- Heavy runtime data is excluded by root `.ignore`: FaceNet models/datasets, MTCNN weights, virtualenvs, bytecode, generated JPEG/SQLite artifacts, secrets, and macOS metadata.
