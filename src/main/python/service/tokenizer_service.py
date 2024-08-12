import argparse

from flask import Flask, request, jsonify
from transformers import AutoTokenizer

parser = argparse.ArgumentParser(description="Start a Tokenizer Service")
parser.add_argument("model_dir", type=str, help="Path of tokenizer model")
parser.add_argument("--port", type=int, default=5000, help="Port to run the tokenizer")
args = parser.parse_args()

app = Flask(__name__)
tokenizer = AutoTokenizer.from_pretrained(args.model_dir)

@app.route("/tokenize", methods=["POST"])
def tokenize():
    data = request.get_json()
    text = data['text']
    encoded_text = tokenizer.batch_encode_plus(text)
    return jsonify(encoded_text.get('input_ids'))

if __name__ == '__main__':
    app.run("0.0.0.0", args.port)
