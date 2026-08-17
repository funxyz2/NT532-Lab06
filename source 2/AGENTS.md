# AGENTS.md

## Overview
Python + Docker demo of image streaming over Apache Kafka (OpenCV frames in, annotated JPEG out). All files live flat in the repo root. No package manifest, lockfile, or CI.

## Commands
- Start Kafka/Zookeeper: `docker-compose -f docker-compose.yml up -d`
- Alt stack (Confluent, arm64): `docker-compose -f docker-compose2.yml up -d`
- Consumer/producer demo: `python test_send.py` then `python test_receive.py` (topic `TEST`, auto-created)
- Image demo: `python recognize.py` then `python send_image.py` (topics `send_image` → `receive_result`)
- Kafka shell: `docker exec -it kafka bin/bash`, tools under `/opt/kafka/bin`

## Architecture
- `config.py` — single source of truth for broker address (`kafka_ip`); all scripts import it. Do not hardcode IPs elsewhere.
- `send_image.py` — camera producer: captures webcam frame, JPEG-encodes, sends to `send_image` every 60s.
- `recognize.py` — consumer of `send_image` + producer of `receive_result`; decodes bytes → np.uint8 → cv2 image, writes `PROCESSED.png`, re-encodes and publishes.
- `test_*.py` — plain JSON send/receive sanity checks on topic `TEST`.
- `docker-compose.yml` — legacy `wurstmeister` images, advertised host `192.168.1.6`.
- `docker-compose2.yml` — `confluentinc/cp-*:7.7.1` arm64 images, advertised listener `127.0.0.1:9092`.

## Conventions & quirks
- Vietnamese README; code comments/prints are English.
- Topics are auto-created on first produce (no manual admin step needed).
- `docker-compose.yml` requires editing `KAFKA_ADVERTISED_HOST_NAME` + `config.py` IP to match the host network.
- Payloads are raw image bytes (not JSON) on `send_image`; `max_request_size`/`fetch_max_bytes` are set to 9 MB to accommodate JPEG frames.
- `recognize.py` blocks forever consuming; runs until killed. `send_image.py` sleeps 60s per frame.
- Deps (implicit, no requirements.txt): `kafka-python`, `opencv-python` (cv2), `numpy`.