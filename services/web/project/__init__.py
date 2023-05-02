from flask import Flask, jsonify, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from functools import wraps
import sys
from time import time

from .app import app
from .auditor import Auditor
from .logger import mylogger, version
from .model import db

debug_route = sys.stderr


def timeit(func):
    """
    :param func: Decorated function
    :return: Execution time for the decorated function
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        print(f"{func.__name__} executed in {end - start:.4f} seconds", file=debug_route)
        return result

    return wrapper


@app.route("/static/<path:filename>")
def staticfiles(filename):
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


@app.route("/")
def hello_world():
    return jsonify(hello="world")
