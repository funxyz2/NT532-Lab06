from confluent_kafka import Producer
from pathlib import Path
import math
import uuid

producer = Producer({
    "bootstrap.servers": "172.20.66.139:9092",
    "security.protocol": "PLAINTEXT",
})

BASE_DIR = Path("./source 1/Dataset/raw")
TOPIC = "device-subscribe"

CHUNK_SIZE = 512 * 1024  # 512 KB

people = [
    "Le Quang Tien",
    "Nguyen Minh Thong",
    "Nguyen Quang Tung",
]

def delivery_report(err, msg):
    if err:
        print(f"FAILED: {err}")
    else:
        print(
            f"SENT partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

for person in people:
    folder = BASE_DIR / person

    for image_path in folder.iterdir():
        if image_path.suffix.lower() not in [".png", ".jpg", ".jpeg"]:
            continue

        image_bytes = image_path.read_bytes()

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
                    "person": person,
                    "filename": image_path.name,
                    "chunk_index": str(chunk_index),
                    "total_chunks": str(total_chunks),
                },
                callback=delivery_report,
            )

            producer.poll(0)

        print(
            f"Queued: {person}/{image_path.name} "
            f"({total_chunks} chunks)"
        )

producer.flush()
print("All images sent.")