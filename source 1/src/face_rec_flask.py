from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from flask import Flask
from flask import render_template , request
from flask_cors import CORS, cross_origin
import tensorflow as tf
import argparse
import facenet
import os
import sys
import math
import pickle
import align.detect_face
import numpy as np
import cv2
import collections
from sklearn.svm import SVC
import base64
from confluent_kafka import Consumer, KafkaError
import threading
import time
from datetime import datetime

MINSIZE = 20
THRESHOLD = [0.6, 0.7, 0.7]
FACTOR = 0.709
IMAGE_SIZE = 182
INPUT_IMAGE_SIZE = 160
CLASSIFIER_PATH = 'Models/facemodel.pkl'
FACENET_MODEL_PATH = './Models/20180402-114759.pb'

# Load The Custom Classifier
with open(CLASSIFIER_PATH, 'rb') as file:
    model, class_names = pickle.load(file)
print("Custom Classifier, Successfully loaded")

with tf.Graph().as_default():

    # Cai dat GPU neu co
    gpu_options = tf.compat.v1.GPUOptions(per_process_gpu_memory_fraction=0.6)
    sess = tf.compat.v1.Session(config=tf.compat.v1.ConfigProto(gpu_options=gpu_options, log_device_placement=False))

    with sess.as_default():
        # Load the model
        print('Loading feature extraction model')
        facenet.load_model(FACENET_MODEL_PATH)

        # Get input and output tensors
        # images_placeholder = tf.compat.v1.get_default_graph().get_tensor_by_name("input:0")
        # embeddings = tf.compat.v1.get_default_graph().get_tensor_by_name("embeddings:0")
        # phase_train_placeholder = tf.compat.v1.get_default_graph().get_tensor_by_name("phase_train:0")
        # embedding_size = embeddings.get_shape()[1]
        # pnet, rnet, onet = align.detect_face.create_mtcnn(sess, "src/align")
        images_placeholder = tf.compat.v1.get_default_graph().get_tensor_by_name("input:0")
        embeddings = tf.compat.v1.get_default_graph().get_tensor_by_name("embeddings:0")
        phase_train_placeholder = tf.compat.v1.get_default_graph().get_tensor_by_name("phase_train:0")
        embedding_size = embeddings.get_shape()[1]

        pnet, rnet, onet = align.detect_face.create_mtcnn(sess, "src/align")



app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
app.config['MAX_FORM_MEMORY_SIZE'] = 100 * 1024 * 1024
CORS(app)


# ============= Kafka Consumer Configuration =============
KAFKA_BROKER = "172.20.66.139:9092"
KAFKA_TOPIC = "device-subscribe"
KAFKA_GROUP_ID = "face-recognition-consumer"

# Global dictionary to reassemble chunks: {file_id: {metadata, chunks}}
chunks_buffer = {}
CHUNK_TIMEOUT = 60  # seconds


def recognize_face(frame):
    """
    Recognize faces in a frame
    Returns: name (recognized person or "Unknown")
    """
    try:
        bounding_boxes, _ = align.detect_face.detect_face(frame, MINSIZE, pnet, rnet, onet, THRESHOLD, FACTOR)

        faces_found = bounding_boxes.shape[0]

        if faces_found > 0:
            det = bounding_boxes[:, 0:4]
            bb = np.zeros((faces_found, 4), dtype=np.int32)
            for i in range(faces_found):
                bb[i][0] = det[i][0]
                bb[i][1] = det[i][1]
                bb[i][2] = det[i][2]
                bb[i][3] = det[i][3]

                cropped = frame[bb[i][1]:bb[i][3], bb[i][0]:bb[i][2], :]
                scaled = cv2.resize(cropped, (INPUT_IMAGE_SIZE, INPUT_IMAGE_SIZE),
                                    interpolation=cv2.INTER_CUBIC)
                scaled = facenet.prewhiten(scaled)
                scaled_reshape = scaled.reshape(-1, INPUT_IMAGE_SIZE, INPUT_IMAGE_SIZE, 3)
                feed_dict = {images_placeholder: scaled_reshape, phase_train_placeholder: False}
                emb_array = sess.run(embeddings, feed_dict=feed_dict)
                predictions = model.predict_proba(emb_array)
                best_class_indices = np.argmax(predictions, axis=1)
                best_class_probabilities = predictions[
                    np.arange(len(best_class_indices)), best_class_indices]

                if best_class_probabilities > 0.5:
                    name = class_names[best_class_indices[0]]
                else:
                    name = "Unknown"
                
                return name
        else:
            return "Unknown"
    except Exception as e:
        print(f"Error in recognize_face: {e}")
        return "Unknown"


def kafka_consumer_thread():
    """
    Background thread to consume images from Kafka broker
    Reassembles chunks and processes face recognition
    """
    consumer_config = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT',
    }
    
    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])
    
    print(f"[Kafka Consumer] Connected to {KAFKA_BROKER}, topic: {KAFKA_TOPIC}")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # Cleanup expired chunks
                current_time = time.time()
                expired_files = [
                    fid for fid, data in chunks_buffer.items()
                    if current_time - data['timestamp'] > CHUNK_TIMEOUT
                ]
                for fid in expired_files:
                    print(f"[Kafka Consumer] Removing expired chunks for file_id: {fid}")
                    del chunks_buffer[fid]
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[Kafka Consumer] Error: {msg.error()}")
                    continue
            
            # Extract headers
            headers = {h[0]: h[1].decode() if isinstance(h[1], bytes) else h[1] 
                       for h in (msg.headers() or [])}
            
            file_id = headers.get('file_id')
            person = headers.get('person', 'Unknown')
            filename = headers.get('filename', 'unknown.jpg')
            chunk_index = int(headers.get('chunk_index', 0))
            total_chunks = int(headers.get('total_chunks', 1))
            
            print(f"[Kafka Consumer] Received chunk {chunk_index}/{total_chunks} for file_id: {file_id}, person: {person}")
            
            # Initialize buffer for this file if not exists
            if file_id not in chunks_buffer:
                chunks_buffer[file_id] = {
                    'chunks': {},
                    'total_chunks': total_chunks,
                    'person': person,
                    'filename': filename,
                    'timestamp': time.time()
                }
            
            # Store chunk
            chunks_buffer[file_id]['chunks'][chunk_index] = msg.value()
            
            # Check if all chunks received
            if len(chunks_buffer[file_id]['chunks']) == total_chunks:
                # Reassemble image
                sorted_chunks = sorted(chunks_buffer[file_id]['chunks'].items(), key=lambda x: x[0])
                image_bytes = b''.join([chunk[1] for chunk in sorted_chunks])
                
                # Decode image
                try:
                    frame = cv2.imdecode(np.frombuffer(image_bytes, dtype=np.uint8), cv2.IMREAD_ANYCOLOR)
                    
                    if frame is not None:
                        # Run face recognition
                        recognized_name = recognize_face(frame)
                        
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        log_msg = f"[{timestamp}] File: {filename} | Person (label): {person} | Recognized: {recognized_name}"
                        print(log_msg)
                        
                        # TODO: Send result back to Kafka (not implemented yet)
                        
                    else:
                        print(f"[Kafka Consumer] Failed to decode image for file_id: {file_id}")
                
                except Exception as e:
                    print(f"[Kafka Consumer] Error processing image {file_id}: {e}")
                
                # Clean up
                del chunks_buffer[file_id]
    
    except KeyboardInterrupt:
        print("[Kafka Consumer] Interrupted")
    finally:
        consumer.close()



@app.route('/')
@cross_origin()
def index():
    return "OK!";

@app.route('/recog', methods=['POST'])
@cross_origin()
def upload_img_file():
    if request.method == 'POST':
        # base 64
        f = request.form.get('image')
        print(type(f))
        w = int(request.form.get('w'))
        h = int(request.form.get('h'))

        decoded_string = base64.b64decode(f)
        frame = np.frombuffer(decoded_string, dtype=np.uint8)
        frame = cv2.imdecode(frame, cv2.IMREAD_ANYCOLOR)

        name = recognize_face(frame)
        return name


if __name__ == '__main__':
    # Start Kafka consumer in background thread
    consumer_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    consumer_thread.start()
    print("[Main] Kafka consumer thread started")
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port='8000', use_reloader=False)

