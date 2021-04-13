# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 06:35:27 2021

@author: guser
"""


from flask import Flask
from flask import jsonify
from flask import request
import json

app = Flask(__name__)


@app.route('/')
def hello():
    message={"message":"Hello World"}

    return jsonify(message)


@app.route('/input')
def input():
    payload=request.get_json()
    print(payload['orderId'])

    message={"orderId":789}
    return jsonify(message)

@app.route('/transform')
def transform():
    payload=request.data
    #print(payload)

    message={"payload":"payload"}
    return jsonify(message)



@app.route('/<name>')
def hello_name(name):

    message={"name":name,
            "age":32}

    return jsonify(message)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
