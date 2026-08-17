


python src/align_dataset_mtcnn.py  Dataset/raw Dataset/processed --image_size 160 --margin 32  --random_order --gpu_memory_fraction 0.25

python src/classifier.py TRAIN Dataset/processed Models/20180402-114759.pb Models/facemodel.pkl --batch_size 1000

python src/face_rec_flask.py 