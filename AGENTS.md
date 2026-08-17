# AGENTS.md

## Overview
Monorepo root containing two **independent** projects — not one codebase; never cross-import between them. Each subproject has its own `AGENTS.md` + `.ignore`; read those first, they are authoritative and more detailed.

- `source 1/` — FaceNet (TensorFlow 1.x) face recognition: MTCNN align → linear-SVM classify, Flask API (`/recog`), RTSP live pipeline publishing labels over ZeroMQ PUB/SUB, plus DFC-VAE generative face-attribute editing. See `source 1/AGENTS.md`.
- `source 2/` — Python + Docker demo of image streaming over Apache Kafka (OpenCV frames in, annotated JPEG out). See `source 2/AGENTS.md`.

Root is a fresh git repo: no commits yet, empty `.gitignore`.

## Commands
### source 1 — face recognition
```bash
cd "source 1"
# 1. Align raw images with MTCNN into 160px crops
python src/align_dataset_mtcnn.py Dataset/raw Dataset/processed \
  --image_size 160 --margin 32 --random_order --gpu_memory_fraction 0.25
# 2. Train linear-SVM over FaceNet embeddings
python src/classifier.py TRAIN Dataset/processed \
  Models/20180402-114759.pb Models/facemodel.pkl --batch_size 1000
# 3. Flask server (0.0.0.0:8000) / test client
python src/face_rec_flask.py
python client.py
# 4. RTSP live recognition → ZeroMQ PUB labels
python app.py --source rtsp://127.0.0.1:8554/stream --detect-every 10 \
  --max-dimension 960 --reconnect-delay 2.0 \
  --publish tcp://127.0.0.1:5556 --camera-id camera-01
```

### source 2 — Kafka
```bash
cd "source 2"
docker-compose -f docker-compose.yml up -d      # wurstmeister stack (legacy)
docker-compose -f docker-compose2.yml up -d     # Confluent arm64 alt
python test_send.py && python test_receive.py   # sanity check, topic TEST
python recognize.py; python send_image.py       # image demo: send_image → receive_result
```

## Key facts
- **source 1** is TF1-era code (Sandberg FaceNet fork) — never "modernize" to TF2/Keras APIs. Pipeline: `Dataset/raw/<person>/` → MTCNN align → `Dataset/processed/<person>/` → SVM over FaceNet embeddings → `facemodel.pkl`. Class names = person dir names with `_` → space.
- **source 1 quirks**: `app.py` needs `pyzmq` (missing from requirements.txt); `classifier.py:59` assert is a no-op tuple bug (keep as-is, upstream); `face_rec_flask.py` uses deprecated `np.fromstring`; server returns only the first recognized face.
- **source 2**: all scripts import `config.py` for broker address (single source of truth — don't hardcode IPs); topics auto-create on first produce; `send_image` payloads are raw JPEG bytes, broker tuned to 9MB max message; `recognize.py` blocks forever, `send_image.py` sleeps 60s/frame.
- No test suites, lockfiles, or CI anywhere. Both READMEs are minimal; source 2's is Vietnamese (code comments are English).

## Ignore behavior
Heavy/binary data blocked via root `.ignore`: `source 1/Models/**` (~225M), `source 1/Dataset/**` (~85M), `src/align/det*.npy`, `.pyc`, `.env`, `.DS_Store`, `source 2/PROCESSED.png`. Note: the ignore plugin enforces only the **root** `.ignore` — the sub-project `.ignore` files exist but are not enforced from this root; keep entries mirrored.