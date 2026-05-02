import fasttext
import os
import urllib.request

MODEL_URL = "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
MODEL_PATH = "lid.176.bin"

if not os.path.exists(MODEL_PATH):
    urllib.request.urlretrieve(MODEL_URL, MODEL_PATH)

model = fasttext.load_model(MODEL_PATH)
text = "Hello world this is a test"
predictions = model.predict(text)
print(f"Text: {text}")
print(f"Predictions: {predictions}")
