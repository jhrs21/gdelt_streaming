#!/bin/bash

source env/bin/activate
pip install -r requirements.txt
export FLASK_DEBUG=1
export FLASK_APP=server.py
flask run &
sleep 2
x-www-browser http://localhost:5000 &
