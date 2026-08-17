import os
import base64
import glob

import requests


URL = 'http://127.0.0.1:8000/recog'
RAW_DIR = 'Dataset/raw'
IMAGE_EXTS = ('.jpg', '.jpeg', '.png')


# Function to encode image to Base64
def encode_image_to_base64(image_path):
    with open(image_path, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_image


def recognize(image_path):
    encoded_image_data = encode_image_to_base64(image_path)
    # Parameters to send in the POST request
    data = {
        'image': encoded_image_data,
        'w': 100,  #  width
        'h': 100   #  height
    }
    # Sending the POST request
    response = requests.post(URL, data=data, timeout=60)
    return response


def main():
    image_files = sorted(
        f for f in glob.glob(os.path.join(RAW_DIR, '**', '*'), recursive=True)
        if os.path.isfile(f) and f.lower().endswith(IMAGE_EXTS)
    )

    results = {}
    for image_path in image_files:
        expected = os.path.basename(os.path.dirname(image_path)).replace('_', ' ')
        try:
            response = recognize(image_path)
            text = response.text.strip()
            if response.status_code != 200 or not text or len(text) > 50:
                predicted = '<error/html>'
            else:
                predicted = text
        except Exception as e:
            predicted = '<error: {}>'.format(type(e).__name__)

        match = 'OK' if predicted == expected else 'MISMATCH'
        results.setdefault(expected, [0, 0])
        results[expected][1] += 1
        if predicted == expected:
            results[expected][0] += 1
        print('[{}] expected={:<20} predicted={:<20} file={}'.format(
            match, expected, predicted, image_path), flush=True)

    print()
    print('=== SUMMARY ===')
    total = correct = 0
    for label, (ok, n) in sorted(results.items()):
        total += n
        correct += ok
        print('{:<20} {}/{} ({:.0%})'.format(label, ok, n, ok / n if n else 0))
    print('{:<20} {}/{} ({:.0%})'.format('TOTAL', correct, total, correct / total if total else 0))


if __name__ == '__main__':
    main()
