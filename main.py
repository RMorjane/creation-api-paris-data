from base64 import encode
import logging
from pprint import pprint
from paris_data_api import ParisDataApi
from flask import Flask, render_template, request, jsonify

logging.basicConfig(filename='logs.txt', level=logging.DEBUG)
api = ParisDataApi()
app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

@app.route("/", methods=['GET'])
def home():
    return "Home page"

@app.route("/themes/", methods=['GET'])
def themes():
    api.read_list_themes()
    return jsonify(api.list_themes)

@app.route("/dataset/", methods=['GET'])
def dataset():
    api.read_list_dataset()
    return jsonify(api.list_dataset)

@app.route("/records/", methods=['GET'])
def records():
    api.read_list_records()
    return jsonify(api.list_records)

@app.route("/keywords/", methods=['GET'])
def keywords():
    api.read_list_keywords()
    return jsonify(api.list_keywords)

@app.route("/dataset_keywords/<keywords>", methods=['GET'])
def dataset_keywords(keywords: str):
    api.read_dataset_keywords(keywords.split(','))
    return jsonify(api.list_dataset_keywords)
    
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=3000, debug=True)