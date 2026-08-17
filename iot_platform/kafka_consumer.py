import json
import logging
import os

from kafka import KafkaConsumer

from app import insert_record, validate_record


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def main():
    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    topic = os.getenv("KAFKA_TOPIC", "recognition_result")
    group_id = os.getenv("KAFKA_GROUP_ID", "iot-platform")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    logging.info("Listening to Kafka topic %s at %s", topic, ",".join(servers))
    for message in consumer:
        try:
            record = validate_record(message.value)
            record_id = insert_record(record)
            logging.info("Stored attendance id=%s student=%s", record_id, record["student_name"])
        except (ValueError, TypeError, json.JSONDecodeError) as error:
            logging.warning("Skipped invalid Kafka message: %s", error)


if __name__ == "__main__":
    main()
