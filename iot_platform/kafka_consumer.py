import logging
import threading

from attendance_kafka import consume_forever


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    """
    Optional standalone attendance consumer.

    Normally the consumer runs inside `app.py` as a background thread;
    this script only exists for debugging/isolated runs.
    """
    stop_event = threading.Event()
    try:
        consume_forever(stop_event)
    except KeyboardInterrupt:
        stop_event.set()
        print("\nConsumer stopped")


if __name__ == "__main__":
    main()