from confluent_kafka import Producer, Consumer, KafkaError
from pathlib import Path
import math
import uuid
import threading
import json
import time

producer = Producer({
    "bootstrap.servers": "172.20.66.139:9092",
    "security.protocol": "PLAINTEXT",
})

BASE_DIR = Path("./source 1/Dataset/raw")
TOPIC = "device-subscribe"
RESULT_TOPIC = "recognition-result"

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


def consume_results():
    """
    Background thread to consume recognition results from Kafka
    """
    consumer_config = {
        'bootstrap.servers': "172.20.66.139:9092",
        'group.id': 'edge-client-results',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT',
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([RESULT_TOPIC])
    
    print(f"[Results Consumer] Listening to topic '{RESULT_TOPIC}'...")
    
    try:
        while True:
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
                print(f"\n[Results] ✓ {result['filename']}")
                print(f"           Label: {result['person_label']}")
                print(f"           Recognized: {result['recognized_name']}")
                print(f"           Time: {result['timestamp']}\n")
            except Exception as e:
                print(f"[Results Consumer] Error decoding result: {e}")
    
    except KeyboardInterrupt:
        print("[Results Consumer] Interrupted")
    finally:
        consumer.close()


# Start result consumer in background thread
result_thread = threading.Thread(target=consume_results, daemon=True)
result_thread.start()

print("[Producer] Sending images from Dataset/raw...")
time.sleep(1)

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
print("\n[Producer] All images sent. Waiting for results (Ctrl+C to exit)...")

# Keep main thread alive to receive results
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\n[Main] Exiting...")