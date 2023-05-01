from flask import Flask, jsonify, send_from_directory
from flask_sqlalchemy import SQLAlchemy

from .app import app
from .auditor import Auditor
from .logger import mylogger
from .model import db


@app.route("/static/<path:filename>")
def staticfiles(filename):
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


@app.route("/")
def hello_world():
    return jsonify(hello="world")
