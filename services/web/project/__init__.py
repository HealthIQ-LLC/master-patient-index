from flask import Flask, jsonify, request, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from functools import wraps
import sys
import threading
from time import time

from .app import app
from .auditor import Auditor
from .coupler import COUPLER, DemographicsGetValidator, db
from .logger import mylogger, version

DEBUG_ROUTE = sys.stderr


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
        print(f"{func.__name__} executed in {end - start:.4f} seconds", 
            file=DEBUG_ROUTE)
        return result

    return wrapper


@app.route("/static/<path:filename>")
def staticfiles(filename):
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


@app.route("/")
def hello_world():
    return jsonify(hello="world")


def get(payload: dict, endpoint: str) -> dict:
    """
    :param payload: the user-initiated data payload to GET with
    :param endpoint: a string denoting the endpoint invoked
    """
    response = COUPLER['query_records']['processor'](payload, endpoint=endpoint)
    
    return response


def post(payload: dict, endpoint: str) -> dict:
    """
    :param payload: the user-initiated data payload to POST with
    :param endpoint: a string denoting the endpoint invoked
    """
    user = payload['user']
    processor = COUPLER[endpoint]['processor']
    with Auditor(user, version, endpoint) as job_auditor:
        thread = threading.Thread(target=processor, 
            args=(payload, job_auditor))
        thread.start()
    try:
        batch_key = job_auditor.batch_id
        response = 200
    except AttributeError:
        batch_key = None
        response = 405
    response = {
        "batch_key": batch_key,
        "status": response
    }

    return response


@timeit
def process_payload():
    response = None
    endpoint = request.endpoint
    method = request.method
    validator = COUPLER[endpoint]['validator']()
    if endpoint == 'demographic' and method == 'GET':
        validator = DemographicsGetValidator()
    try:
        payload_obj = request.get_json()
    except:
        print("Request is not acceptable JSON", file=DEBUG_ROUTE)
        return jsonify(status=405, response=response)
    result, msg = validator.validate(payload_obj)
    if result:
        if method == "GET":
            response = get(payload_obj, endpoint)
        elif method == "POST":
            response = post(payload_obj, endpoint)
    else:
        print(f"Invalid request payload: {msg}", file=DEBUG_ROUTE)
        return jsonify(status=405, response=msg)
    if response is not None:
        return jsonify(status=200, response=response)
    else:
        print(f"Issue encountered with {method} request", 
            file=DEBUG_ROUTE)
        return jsonify(status=405, response=response)


# register all API endpoints via the COUPLER
for endpoint, couplings in COUPLER.items():
    app.add_url_rule(
        f'/api_{version}/{endpoint}',
        endpoint=endpoint, 
        view_func=process_payload,
        methods=couplings['methods']
    )
