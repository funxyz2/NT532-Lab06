import json
import logging
import os
import threading
import time

from kafka import KafkaConsumer

from app import insert_record, validate_record

logger = logging.getLogger("attendance.kafka")

RETRY_DELAY_SECONDS = 5


class AttendanceTracker:
    """
    Confirms a check-in only after `confirm_count` *consecutive* recognitions
    of the same student, then suppresses further check-ins for `ttl_seconds`
    (the student is considered already checked in). After the TTL expires the
    cycle restarts: another `confirm_count` consecutive recognitions are needed
    to check in again.

    State is in-memory per process; Unknown/empty results break the chain and
    any recognition of a different student resets everyone else's counters.
    """

    def __init__(self, confirm_count=3, ttl_seconds=300):
        self.confirm_count = confirm_count
        self.ttl_seconds = ttl_seconds
        self._counters = {}
        self._checked_until = {}

    def on_recognition(self, student_name, now=None):
        """
        Feed one recognition result.

        Returns (decision, count):
          decision: "pending" (needs more confirmations), "confirmed"
                    (check-in now), "already" (TTL still active), or None when
                    the chain is broken (Unknown/empty student).
          count:    current consecutive count for the student (0 otherwise).
        """
        now = time.monotonic() if now is None else now

        if not student_name or student_name == "Unknown":
            self._counters.clear()
            return None, 0

        if self._checked_until.get(student_name, 0) > now:
            return "already", 0

        # New cycle (first recognition, or TTL expired): only this student
        # may keep counting, everyone else starts from zero.
        for name in list(self._counters):
            if name != student_name:
                del self._counters[name]
        count = self._counters.get(student_name, 0) + 1
        self._counters[student_name] = count

        if count >= self.confirm_count:
            self._checked_until[student_name] = now + self.ttl_seconds
            self._counters[student_name] = 0
            return "confirmed", count

        return "pending", count


def to_attendance_record(value):
    """
    Convert a `recognition-result` message (source 1 Edge Server schema) into an
    attendance record:
      recognized_name -> student_name
      person_label    -> device_id
      timestamp       -> timestamp (ISO-8601)
      room_id         -> message room_id, else ROOM_ID env (default LAB-06)
      check_type      -> message check_type, else "check-in"

    Messages that already carry the full attendance schema
    (room_id, student_name, device_id, check_type) pass through unchanged.
    """
    if not isinstance(value, dict):
        raise ValueError("message must be a JSON object")

    return {
        "timestamp": value.get("timestamp"),
        "room_id": value.get("room_id") or os.getenv("ROOM_ID", "LAB-06"),
        "student_name": value.get("student_name") or value.get("recognized_name"),
        "device_id": value.get("device_id") or value.get("person_label"),
        "check_type": value.get("check_type", "check-in"),
    }


def consume_forever(stop_event):
    """
    Subscribe to the recognition topic and store confirmed attendance records
    until stop_event is set. Reconnects with a delay if the broker is
    unavailable, so a Kafka outage never kills the Flask process.
    """
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "recognition-result")
    group_id = os.getenv("KAFKA_GROUP_ID", "iot-platform")
    confirm_count = int(os.getenv("ATTENDANCE_CONFIRM_COUNT", "3"))
    ttl_seconds = int(os.getenv("ATTENDANCE_TTL_SECONDS", "300"))
    tracker = AttendanceTracker(confirm_count=confirm_count, ttl_seconds=ttl_seconds)

    logger.info(
        "Attendance consumer connecting to %s, topic %s (confirm %d consecutive times, TTL %ds)",
        ",".join(servers),
        topic,
        confirm_count,
        ttl_seconds,
    )

    while not stop_event.is_set():
        consumer = None
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=servers,
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            for message in consumer:
                if stop_event.is_set():
                    break
                try:
                    record = to_attendance_record(message.value)
                    student = record["student_name"]
                    decision, count = tracker.on_recognition(student)
                    if decision is None:
                        logger.info("Skipped unidentifiable recognition: %s", student)
                        continue
                    if decision == "pending":
                        logger.info(
                            "Recognition pending %s (%d/%d)",
                            student,
                            count,
                            confirm_count,
                        )
                        continue
                    if decision == "already":
                        logger.info(
                            "%s already checked in (TTL active, no duplicate)",
                            student,
                        )
                        continue
                    validated = validate_record(record)
                    record_id = insert_record(validated)
                    logger.info(
                        "Check-in confirmed id=%s student=%s room=%s device=%s (TTL %ds)",
                        record_id,
                        validated["student_name"],
                        validated["room_id"],
                        validated["device_id"],
                        ttl_seconds,
                    )
                except (ValueError, TypeError, json.JSONDecodeError) as error:
                    logger.warning("Skipped invalid Kafka message: %s", error)
        except Exception as error:
            logger.warning(
                "Attendance consumer error (%s); retrying in %ss",
                error,
                RETRY_DELAY_SECONDS,
            )
            time.sleep(RETRY_DELAY_SECONDS)
        finally:
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    pass

    logger.info("Attendance consumer stopped")


def start_consumer_thread():
    """Start the attendance consumer in a daemon thread. Returns (stop_event, thread)."""
    stop_event = threading.Event()
    thread = threading.Thread(
        target=consume_forever,
        args=(stop_event,),
        daemon=True,
        name="attendance-kafka",
    )
    thread.start()
    logger.info("Attendance consumer thread started")
    return stop_event, thread