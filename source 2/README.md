# Hướng Dẫn Cài Đặt và Demo Sử Dụng Kafka

## 1. Cài Đặt Các Thư Viện

## 2. Cấu Hình Địa Chỉ IP trong các file:
- config.py
- docker-compose.yml

## 3. Cài Đặt Kafka và Zookeeper
```bash
docker-compose -f docker-compose.yml up -d
```
## 4. Connect to Kafka shell
```bash
docker exec -it  kafka bin/bash
cd /opt/kafka/bin
```
## 5. Kiểm tra danh sách các Topic đã tạo
```bash
kafka-topics.sh --list --zookeeper zookeeper:2181
```
## 6. Test gửi nhận dữ liệu
Khi chạy 2 file dưới, Topic TEST sẽ được tạo tự động
```bash
python test_send.py
python test_receive.py
```
## 7. Demo gửi nhận hình ảnh
Khi chạy 2 file dưới, Topic send_image và receive_result sẽ được tạo tự động
```bash
python recognize.py
python send_image.py
```
## 8. Một số câu lệnh với Kafka
```bash
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic send_image

kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic receive_result
```