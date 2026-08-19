# AGENTS.md

## Overview
Fork of David Sandberg's FaceNet (TF1) for face recognition with a Flask API, plus DFC-VAE generative face-attribute editing. TensorFlow 1.x code — do not "modernize" to TF2/Keras APIs.

## Commands
```bash
# 1. Align raw images with MTCNN into 160px crops
python src/align_dataset_mtcnn.py Dataset/raw Dataset/processed \
  --image_size 160 --margin 32 --random_order --gpu_memory_fraction 0.25

# 2. Train a linear-SVM classifier over FaceNet embeddings
python src/classifier.py TRAIN Dataset/processed \
  Models/20180402-114759.pb Models/facemodel.pkl --batch_size 1000

# 3. Run recognition server (flask, port 8000)
python src/face_rec_flask.py

# 4. Test client — POSTs every Dataset/raw image to /recog, prints accuracy summary
python client.py

# 5. RTSP live recognition → ZeroMQ PUB labels (root-level, newer than the flask server)
python app.py --source rtsp://127.0.0.1:8554/stream \
  --detect-every 10 --max-dimension 960 --reconnect-delay 2.0 \
  --publish tcp://127.0.0.1:5556 --camera-id camera-01
```

## Architecture
- `src/facenet.py` — core: `load_model()` (.pb), `prewhiten()`, `get_dataset()`, `load_data()`, embedding helpers
- `src/align/detect_face.py` + `det1/2/3.npy` — MTCNN face detection (numpy weights, `create_mtcnn(sess, "src/align")`)
- `src/align_dataset_mtcnn.py` — pipeline: `Dataset/raw/<person>/images` → aligned crops in `Dataset/processed/<person>/`
- `src/classifier.py` — modes `TRAIN` (fit `SVC(kernel='linear', probability=True)` on embeddings, pickle `(model, class_names)` to .pkl) / `CLASSIFY` (eval)
- `src/face_rec_flask.py` — loads SVM + FaceNet graph **at import time**; `POST /recog` (form fields: `image` base64, `w`, `h`) → returns person name or `"Unknown"` if top prob ≤ 0.5. CORS enabled, 100MB form limits. Also runs a Kafka consumer on `device-subscribe`: reassembles 512 KiB chunks, converts the frame BGR→RGB once (consistent with `app.py`; `None` frames yield an empty result), recognizes **all** faces via `recognize_faces()` (returns `{recognized_name, probability, bbox: [x1,y1,x2,y2]}` per face, box clipped to frame), and publishes `recognition-result` with `file_id`, `filename`, `person_label`, `recognized_name` (first face), `faces`, `frame_size`, `timestamp`. `recognize_face()` wrapper keeps `/recog` returning only the first face's name.
- `src/models/` — network defs: `inception_resnet_v1/v2`, `squeezenet`, `dummy`
- `src/generative/` — DFC-VAE: `train_vae.py`, `calculate_attribute_vectors.py`, `modify_attribute.py`; VAE defs in `src/generative/models/` (`dfc_vae`, `dfc_vae_resnet`, `dfc_vae_large`, `vae_base`) loaded via `importlib.import_module(args.vae_def)`
- `app.py` (root) — live pipeline: RTSP in → MTCNN detect (every N frames) → FaceNet embed → SVM classify → draw + publish. Same model files as the flask server. Details:
  - Downscales frames above `--max-dimension` (960) for detection, then scales bboxes back to original coords; `crop_face` casts coords to `int` (float bbox breaks slicing)
  - Auto-reconnects to RTSP with `--reconnect-delay`; exits on `q`/Esc
  - ZeroMQ PUB (`--publish`, default `tcp://127.0.0.1:5556`), multipart topic `recognition` + JSON payload: `{camera_id, timestamp, frame_id, faces: [{name, probability, bbox: [x1,y1,x2,y2]}]}`
  - Requires `pyzmq` — **not listed in requirements.txt**
- `Models/` — pretrained FaceNet `20180402-114759` (.pb/.meta/.ckpt, ~225M total) + `facemodel.pkl` SVM
- `client.py` — globs `Dataset/raw/**` images, posts each to `http://127.0.0.1:8000/recog` (w/h=100), expects parent dir name with `_` → space, prints per-class + TOTAL accuracy summary

## Testing quirks
- No test suite. "Testing" = `classifier.py CLASSIFY` eval accuracy printout, or `client.py`'s summary against the flask server.
- Known bug (upstream): `classifier.py:59` `assert(len(cls.image_paths)>0, '...')` is a tuple, never fails — keep as-is, matches upstream.
- `face_rec_flask.py:85` uses deprecated `np.frombuffer`/`np.fromstring`; Flask `/recog` returns the first recognized face's name only (via `recognize_face()`), while the Kafka path returns every face with bbox in `faces`.
- Upstream junk: 2-byte placeholder files named `a` in `src/align/`, `src/models/`, `src/generative/`, `src/generative/models/` — ignore them.

## Conventions
- Python 2/3 compat imports (`from __future__ import ...`) and TF1 `tf.compat.v1.*` — preserve style (app.py uses modern typing only, keep as-is).
- Class names = person directory names with `_` → space (`classifier.py:99`).
- Server: debug on `0.0.0.0:8000`; GPU fraction 0.6 (flask server + app.py), 0.25 in alignment.
- `requirements.txt` unpinned; heavy TF1 stack (tensorflow, keras, sklearn, opencv, h5py) + pyzmq (undeclared). No lockfiles, no CI, no .gitignore.
- Dataset dirs (`raw`, `processed`) and `Models/` are excluded via `.ignore` — don't read them.
