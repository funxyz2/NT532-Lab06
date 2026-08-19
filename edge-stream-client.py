from confluent_kafka import Producer, Consumer, KafkaError
from collections import deque
import argparse
import cv2
import json
import math
import os
import platform
import tempfile
import threading
import time
import uuid

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
DEVICE_ID = os.getenv(
    "DEVICE_ID",
    (platform.node() or "edge-client").replace(".local", "").lower(),
)

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "security.protocol": "PLAINTEXT",
})

TOPIC = "device-subscribe"
RESULT_TOPIC = "recognition-result"

CHUNK_SIZE = 512 * 1024  # 512 KB
OVERLAY_TTL = 3.0  # seconds before a stale overlay disappears
PENDING_MAX = 1000  # max frames awaiting a result (1 FPS -> ~16 min of backlog)

stop_event = threading.Event()

# Latest recognition result shared between consumer thread and main loop.
overlay_lock = threading.Lock()
overlay = {"timestamp": 0.0, "faces": [], "frame_size": None}

# file_ids of frames sent by THIS client in the current session, awaiting their
# result. Only results matching these ids may update the preview, so stale
# Kafka backlog or results from other devices are never drawn.
pending_lock = threading.Lock()
pending_ids = set()
pending_order = deque()


def add_pending(file_id):
    with pending_lock:
        pending_ids.add(file_id)
        pending_order.append(file_id)
        while len(pending_order) > PENDING_MAX:
            pending_ids.discard(pending_order.popleft())


def take_pending(file_id):
    with pending_lock:
        if file_id in pending_ids:
            pending_ids.discard(file_id)
            return True
        return False


def delivery_report(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(
            f"SENT partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def consume_results():
    """
    Background thread to consume recognition results from Kafka
    """
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        # One consumer group per device, so every client receives every result
        # and filters by its own file_ids instead of competing for messages.
        'group.id': f'edge-client-results-{DEVICE_ID}',
        'auto.offset.reset': 'latest',
        'security.protocol': 'PLAINTEXT',
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([RESULT_TOPIC])

    print(f"[Results Consumer] Listening to topic '{RESULT_TOPIC}'...")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[Results Consumer] Error: {msg.error()}")
                continue

            # Decode result
            try:
                result = json.loads(msg.value().decode('utf-8'))

                file_id = result.get('file_id')
                if not file_id:
                    continue

                frame_size = result.get('frame_size')
                if not isinstance(frame_size, (list, tuple)) or len(frame_size) != 2:
                    continue

                if not take_pending(file_id):
                    # Not one of our frames, already applied, or stale backlog.
                    continue

                print(f"\n[Results] ✓ {result['filename']}")
                print(f"           Label: {result['person_label']}")
                print(f"           Recognized: {result['recognized_name']}")
                print(f"           Time: {result['timestamp']}\n")

                with overlay_lock:
                    overlay['timestamp'] = time.time()
                    overlay['faces'] = result.get('faces') or []
                    overlay['frame_size'] = [int(frame_size[0]), int(frame_size[1])]
            except Exception as e:
                print(f"[Results Consumer] Error decoding result: {e}")

    except KeyboardInterrupt:
        print("[Results Consumer] Interrupted")
    finally:
        consumer.close()
        print("[Results Consumer] Closed")


def encode_frame(frame, quality):
    success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    if not success:
        return None
    return buffer.tobytes()


def send_frame(frame, frame_number, quality):
    image_bytes = encode_frame(frame, quality)
    if image_bytes is None:
        print(f"[Producer] Failed to encode frame {frame_number}")
        return

    file_id = str(uuid.uuid4())
    total_chunks = math.ceil(len(image_bytes) / CHUNK_SIZE)

    for chunk_index in range(total_chunks):
        start = chunk_index * CHUNK_SIZE
        end = start + CHUNK_SIZE

        chunk = image_bytes[start:end]

        producer.produce(
            TOPIC,
            key=file_id,
            value=chunk,
            headers={
                "file_id": file_id,
                "person": DEVICE_ID,
                "filename": f"{DEVICE_ID}-webcam-{frame_number}.jpg",
                "chunk_index": str(chunk_index),
                "total_chunks": str(total_chunks),
            },
            callback=delivery_report,
        )

        producer.poll(0)

    add_pending(file_id)

    print(
        f"Queued: webcam frame {frame_number} "
        f"({total_chunks} chunks, {len(image_bytes)} bytes)"
    )


def draw_overlay(frame):
    """Draw the latest Kafka recognition boxes onto the live frame."""
    with overlay_lock:
        if not overlay['faces']:
            return
        if time.time() - overlay['timestamp'] > OVERLAY_TTL:
            return
        faces = list(overlay['faces'])
        frame_size = list(overlay['frame_size'])

    height, width = frame.shape[:2]
    scale_x = width / float(frame_size[0])
    scale_y = height / float(frame_size[1])

    for face in faces:
        try:
            bbox = face['bbox']
            x1, y1, x2, y2 = [int(coord) for coord in bbox]
            x1, y1 = int(x1 * scale_x), int(y1 * scale_y)
            x2, y2 = int(x2 * scale_x), int(y2 * scale_y)

            name = face['recognized_name']
            probability = float(face.get('probability', 0.0))

            color = (0, 255, 0) if name != "Unknown" else (0, 0, 255)
            label = "{} {:.1f}%".format(name, probability * 100)

            cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)

            text_y = max(25, y1 - 10)
            cv2.putText(
                frame,
                label,
                (x1, text_y),
                cv2.FONT_HERSHEY_SIMPLEX,
                0.65,
                color,
                2,
                cv2.LINE_AA,
            )
        except (KeyError, TypeError, ValueError) as e:
            print(f"[Overlay] Skipping malformed face: {e}")
            continue


def camera_backend():
    """Pick the OpenCV capture backend for the current OS."""
    system = platform.system()
    if system == "Darwin":
        return cv2.CAP_AVFOUNDATION
    if system == "Windows":
        return cv2.CAP_DSHOW
    return cv2.CAP_ANY


def create_capture(index):
    """Open a camera index, trying the OS backend first, then the default one."""
    cap = cv2.VideoCapture(index, camera_backend())
    if not cap.isOpened():
        cap.release()
        cap = cv2.VideoCapture(index)
    return cap


def list_cameras():
    """Probe camera indices and save a snapshot of each working one."""
    print("Probing camera indices 0-5...")
    found = []
    for i in range(6):
        cap = create_capture(i)
        if not cap.isOpened():
            print(f"index {i}: FAILED to open")
            continue
        ok, frame = cap.read()
        if not ok or frame is None:
            print(f"index {i}: opened but no frame")
            cap.release()
            continue
        height, width = frame.shape[:2]
        path = os.path.join(tempfile.gettempdir(), f"cam-probe-{i}.jpg")
        cv2.imwrite(path, frame)
        print(f"index {i}: OK, {width}x{height}, snapshot saved to {path}")
        found.append(i)
        cap.release()
    print("done")


def open_camera(preferred_index):
    """Open the preferred camera index, falling back to any working one."""
    candidates = [preferred_index] + [i for i in range(6) if i != preferred_index]
    for index in candidates:
        cap = create_capture(index)
        if cap.isOpened():
            if index != preferred_index:
                print(
                    f"[Main] Camera index {preferred_index} unavailable; "
                    f"falling back to index {index}."
                )
            return cap, index
    return None, preferred_index


def main():
    parser = argparse.ArgumentParser(
        description="Stream webcam frames to Kafka and draw recognition results on the live preview."
    )
    parser.add_argument(
        "--list-cameras",
        action="store_true",
        help="Probe available webcam indices and exit.",
    )
    parser.add_argument(
        "--fps",
        type=float,
        default=1.0,
        help="Frames per second sent to Kafka (default: 1.0).",
    )
    parser.add_argument(
        "--quality",
        type=int,
        default=80,
        help="JPEG quality 0-100 (default: 80).",
    )
    parser.add_argument(
        "--max-dimension",
        type=int,
        default=960,
        help="Resize frames above this size before sending (default: 960).",
    )
    parser.add_argument(
        "--camera-index",
        type=int,
        default=0 if platform.system() == "Windows" else 1,
        help="Webcam index (default: 0 on Windows, 1 on macOS).",
    )
    args = parser.parse_args()

    if args.list_cameras:
        list_cameras()
        return

    if args.fps <= 0:
        parser.error("--fps must be positive.")
    if not (0 <= args.quality <= 100):
        parser.error("--quality must be between 0 and 100.")
    if args.max_dimension < 100:
        parser.error("--max-dimension must be at least 100.")

    # Start result consumer in background thread
    result_thread = threading.Thread(target=consume_results, daemon=True)
    result_thread.start()

    camera, camera_index = open_camera(args.camera_index)
    if camera is None:
        if platform.system() == "Darwin":
            hint = "Check camera permission in System Settings -> Privacy & Security -> Camera."
        elif platform.system() == "Windows":
            hint = "Check camera permission in Settings -> Privacy & security -> Camera."
        else:
            hint = "Check that a webcam is connected and has permission."
        print(
            f"[Main] Could not open any webcam (preferred index {args.camera_index}). "
            f"{hint}"
        )
        stop_event.set()
        return

    print(f"[Main] Streaming webcam {camera_index} at {args.fps} FPS. Press q or Esc to exit.")

    frame_number = 0
    last_send_time = 0.0

    try:
        while not stop_event.is_set():
            success, frame = camera.read()
            if not success:
                print("[Main] Failed to read webcam frame")
                time.sleep(0.5)
                continue

            now = time.time()
            if now - last_send_time >= 1.0 / args.fps:
                height, width = frame.shape[:2]
                max_dim = max(height, width)
                if max_dim > args.max_dimension:
                    scale = args.max_dimension / float(max_dim)
                    send_resized = cv2.resize(
                        frame,
                        (int(width * scale), int(height * scale)),
                        interpolation=cv2.INTER_AREA,
                    )
                else:
                    send_resized = frame
                send_frame(send_resized, frame_number, args.quality)
                last_send_time = now

            draw_overlay(frame)
            cv2.imshow("Webcam - Face Recognition (Kafka results)", frame)

            key = cv2.waitKey(1) & 0xFF
            if key in (27, ord("q")):
                break

    except KeyboardInterrupt:
        print("\n[Main] Exiting...")
    finally:
        stop_event.set()
        result_thread.join(timeout=2.0)
        camera.release()
        cv2.destroyAllWindows()
        producer.flush()
        print("[Main] Released camera and flushed producer")


if __name__ == "__main__":
    main()