"""Recognize faces from an RTSP stream with MTCNN + FaceNet, using only local code."""

from __future__ import annotations

import argparse
import json
import pickle
import sys
import time
from pathlib import Path

import cv2
import numpy as np
import tensorflow as tf
import zmq

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import align.detect_face
import facenet

WINDOW_TITLE = "Face Recognition"

INPUT_IMAGE_SIZE = 160
RECOGNITION_THRESHOLD = 0.5

# MTCNN detection settings
MINSIZE = 20
MTCNN_THRESHOLD = [0.6, 0.7, 0.7]
MTCNN_FACTOR = 0.709

CLASSIFIER_PATH = PROJECT_ROOT / "Models" / "facemodel.pkl"
FACENET_MODEL_PATH = PROJECT_ROOT / "Models" / "20180402-114759.pb"


class FaceRecognizer:
    """Detect faces with MTCNN, embed them with FaceNet, classify with SVM."""

    def __init__(self) -> None:
        with open(CLASSIFIER_PATH, "rb") as classifier_file:
            self.classifier, self.class_names = pickle.load(classifier_file)

        self.graph = tf.Graph()

        with self.graph.as_default():
            config = tf.compat.v1.ConfigProto()
            config.gpu_options.per_process_gpu_memory_fraction = 0.6

            self.session = tf.compat.v1.Session(graph=self.graph, config=config)

            with self.session.as_default():
                facenet.load_model(str(FACENET_MODEL_PATH))
                self.pnet, self.rnet, self.onet = align.detect_face.create_mtcnn(
                    self.session, str(SRC_DIR / "align")
                )

            self.images_placeholder = self.graph.get_tensor_by_name("input:0")
            self.embeddings = self.graph.get_tensor_by_name("embeddings:0")
            self.phase_train_placeholder = self.graph.get_tensor_by_name(
                "phase_train:0"
            )

    @staticmethod
    def crop_face(frame, box, margin_ratio=0.15):
        """Crop a face with extra context around the detector box."""
        frame_height, frame_width = frame.shape[:2]
        x1, y1, x2, y2 = [int(coord) for coord in box[:4]]

        box_width = x2 - x1
        box_height = y2 - y1
        margin_x = int(box_width * margin_ratio)
        margin_y = int(box_height * margin_ratio)

        x1 = max(0, x1 - margin_x)
        y1 = max(0, y1 - margin_y)
        x2 = min(frame_width, x2 + margin_x)
        y2 = min(frame_height, y2 + margin_y)

        if x1 >= x2 or y1 >= y2:
            return None

        return frame[y1:y2, x1:x2]

    def detect(self, frame):
        """Return a list of (x1, y1, x2, y2, score) boxes found by MTCNN."""
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

        bounding_boxes, _ = align.detect_face.detect_face(
            rgb_frame,
            MINSIZE,
            self.pnet,
            self.rnet,
            self.onet,
            MTCNN_THRESHOLD,
            MTCNN_FACTOR,
        )

        boxes = []
        for row in bounding_boxes:
            x1, y1, x2, y2 = row[:4].astype(int)
            boxes.append((x1, y1, x2, y2, float(row[4])))
        return boxes

    def recognize(self, frame, boxes):
        """Return (box, name, probability) for every valid face."""
        prepared_faces = []
        valid_boxes = []

        for box in boxes:
            cropped = self.crop_face(frame, box[:4])

            if cropped is None or cropped.size == 0:
                continue

            # OpenCV frames are BGR, whereas training images were loaded as RGB.
            cropped = cv2.cvtColor(cropped, cv2.COLOR_BGR2RGB)
            cropped = cv2.resize(
                cropped,
                (INPUT_IMAGE_SIZE, INPUT_IMAGE_SIZE),
                interpolation=cv2.INTER_CUBIC,
            )
            cropped = facenet.prewhiten(cropped)

            prepared_faces.append(cropped)
            valid_boxes.append(box)

        if not prepared_faces:
            return []

        face_batch = np.stack(prepared_faces)

        feed_dict = {
            self.images_placeholder: face_batch,
            self.phase_train_placeholder: False,
        }

        embeddings = self.session.run(self.embeddings, feed_dict=feed_dict)

        probabilities = self.classifier.predict_proba(embeddings)
        class_indices = np.argmax(probabilities, axis=1)

        results = []

        for box, row, class_index in zip(
            valid_boxes,
            probabilities,
            class_indices,
        ):
            probability = float(row[class_index])

            if probability > RECOGNITION_THRESHOLD:
                name = self.class_names[class_index]
            else:
                name = "Unknown"

            results.append((box, name, probability))

        return results

    def close(self) -> None:
        self.session.close()


def draw_recognitions(frame, recognitions) -> None:
    """Draw a box and a label for every recognized face."""
    for box, name, probability in recognitions:
        x1, y1, x2, y2 = [int(coord) for coord in box[:4]]

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


def publish_recognition(publisher, camera_id, frame_id, recognitions) -> None:
    """Publish recognition results as a JSON label over ZeroMQ PUB."""
    payload = {
        "camera_id": camera_id,
        "timestamp": time.time(),
        "frame_id": frame_id,
        "faces": [
            {
                "name": name,
                "probability": round(probability, 4),
                "bbox": [int(value) for value in box[:4]],
            }
            for box, name, probability in recognitions
        ],
    }
    publisher.send_multipart(
        [b"recognition", json.dumps(payload).encode("utf-8")]
    )


def should_exit(wait_seconds: float) -> bool:
    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline:
        if cv2.waitKey(100) & 0xFF in (27, ord("q")):
            return True
    return False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Recognize faces from an RTSP stream with MTCNN + FaceNet."
    )
    parser.add_argument(
        "--source",
        default="rtsp://127.0.0.1:8554/stream",
        help="RTSP URL to read (default: rtsp://127.0.0.1:8554/stream).",
    )
    parser.add_argument(
        "--detect-every",
        type=int,
        default=10,
        help="Run MTCNN detection once every N frames (default: 10).",
    )
    parser.add_argument(
        "--max-dimension",
        type=int,
        default=960,
        help="Downscale frames above this size for detection (default: 960).",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=2.0,
        help="Seconds between RTSP reconnect attempts (default: 2.0).",
    )
    parser.add_argument(
        "--publish",
        default="tcp://127.0.0.1:5556",
        help="ZeroMQ PUB endpoint to send labels to (default: tcp://127.0.0.1:5556).",
    )
    parser.add_argument(
        "--camera-id",
        default="camera-01",
        help="Identifier included in published labels (default: camera-01).",
    )
    return parser


def parse_args() -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args()
    if args.detect_every < 1:
        parser.error("--detect-every must be at least 1.")
    if args.max_dimension < 100:
        parser.error("--max-dimension must be at least 100.")
    if args.reconnect_delay <= 0:
        parser.error("--reconnect-delay must be positive.")
    return args


def open_camera(source: str):
    camera = cv2.VideoCapture(source)
    if camera.isOpened():
        return camera
    camera.release()
    return None


def wait_for_reconnect(source: str, reconnecting: bool, message: str, delay: float):
    if not reconnecting:
        print(f"{message}: {source}. Retrying.", file=sys.stderr)
    return True, should_exit(delay)


def downscale_for_detection(frame, max_dimension):
    """Resize the frame for detection and return (resized_frame, scale)."""
    height, width = frame.shape[:2]
    max_dim = max(height, width)
    if max_dim <= max_dimension:
        return frame, 1.0
    scale = max_dimension / float(max_dim)
    new_size = (int(width * scale), int(height * scale))
    resized = cv2.resize(frame, new_size, interpolation=cv2.INTER_AREA)
    return resized, scale


def run_preview(
    source: str,
    reconnect_delay: float,
    recognizer: FaceRecognizer,
    detect_every: int,
    max_dimension: int,
    publish_endpoint: str,
    camera_id: str,
) -> None:
    camera = None
    reconnecting = False
    frame_index = 0
    last_recognitions = []

    context = zmq.Context()
    publisher = context.socket(zmq.PUB)
    publisher.connect(publish_endpoint)

    try:
        while True:
            if camera is None:
                camera = open_camera(source)
                if camera is None:
                    reconnecting, should_stop = wait_for_reconnect(
                        source,
                        reconnecting,
                        "Could not open RTSP stream",
                        reconnect_delay,
                    )
                    if should_stop:
                        return
                    continue
                reconnecting = False

            success, frame = camera.read()
            if not success:
                camera.release()
                camera = None
                reconnecting, should_stop = wait_for_reconnect(
                    source,
                    reconnecting,
                    "Lost RTSP stream",
                    reconnect_delay,
                )
                if should_stop:
                    return
                continue

            if frame_index % detect_every == 0:
                detection_frame, scale = downscale_for_detection(
                    frame, max_dimension
                )
                boxes = recognizer.detect(detection_frame)
                if scale != 1.0:
                    boxes = [
                        (
                            x1 / scale,
                            y1 / scale,
                            x2 / scale,
                            y2 / scale,
                            score,
                        )
                        for x1, y1, x2, y2, score in boxes
                    ]
                last_recognitions = recognizer.recognize(frame, boxes)
                publish_recognition(
                    publisher, camera_id, frame_index, last_recognitions
                )

            draw_recognitions(frame, last_recognitions)
            cv2.imshow(WINDOW_TITLE, frame)

            if cv2.waitKey(1) & 0xFF in (27, ord("q")):
                return

            frame_index += 1
    except KeyboardInterrupt:
        return
    finally:
        publisher.close()
        context.term()

        if camera is not None:
            camera.release()


def main() -> int:
    args = parse_args()
    recognizer = None

    try:
        recognizer = FaceRecognizer()
        run_preview(
            args.source,
            args.reconnect_delay,
            recognizer,
            args.detect_every,
            args.max_dimension,
            args.publish,
            args.camera_id,
        )
    except (FileNotFoundError, ValueError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    finally:
        if recognizer is not None:
            recognizer.close()

        cv2.destroyAllWindows()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
