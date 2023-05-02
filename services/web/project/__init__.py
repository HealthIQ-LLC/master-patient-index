from flask import Flask, jsonify, send_from_directory
from flask_sqlalchemy import SQLAlchemy
from functools import wraps
import sys
from time import time

from .app import app
from .auditor import Auditor
from .logger import mylogger, version
from .model import db
from .processor import ACTION_MAP, query_records

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


def get(payload: dict, endpoint: str):
    """
    :param payload: the user-initiated data payload to GET with
    :param endpoint: a string which maps endpoint to destination model to act on
    """
    response = query_records(payload, table=endpoint)

    return response


def post(payload: dict, endpoint: str):
    """
    :param payload: the user-initiated data payload to POST with
    :param endpoint: a string which maps endpoint to destination model to act on
    """
    user = payload['user']
    with Auditor(user, version, endpoint) as job_auditor:
        thread = threading.Thread(target=ACTION_MAP[endpoint], args=(payload, job_auditor))
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
def process_payload(client_request, endpoint: str):
    """
    :param client_request: the flask request object produced by the client for use in an operation
    :param endpoint: a string representing the endpoint employed by this API call
    """
    response = None
    try:
        input_json = client_request.get_json()
    except:
        print("Request is not JSON", file=debug_route)
        return jsonify(status=405, response=response)
    try:
        payload_obj = json.loads(input_json)
    except:
        print("Request will not deserialize", file=debug_route)
        return jsonify(status=405, response=response)
    if client_request.method == "GET":
        response = get(payload_obj, endpoint)
    elif client_request.method == "POST":
        response = post(payload_obj, endpoint)
    if response is not None:
        return jsonify(status=200, response=response)
    else:
        print(f"Issue encountered with {client_request.method} request", file=debug_route)
        return jsonify(status=405, response=response)


@app.route(f"/api_{version}/delete_action", methods=["GET", "POST"])
def api_delete():
    """
    delete actions endpoint.
    """
    return process_payload(request, "delete_action")


@app.route(f"/api_{version}/demographic", methods=["GET", "POST"])
def api_demographic():
    """
    demographic endpoint.
    """
    return process_payload(request, "demographic")


@app.route(f"/api_{version}/activate_demographic", methods=["GET", "POST"])
def api_activate_demographic():
    """
    demographic record activation endpoint.
    """
    return process_payload(request, "activate_demographic")


@app.route(f"/api_{version}/archive_demographic", methods=["GET"])
def api_archived_demographic():
    """
    archived demographic endpoint.
    """
    return process_payload(request, "archive_demographic")


@app.route(f"/api_{version}/deactivate_demographic", methods=["GET", "POST"])
def api_deactivate_demographic():
    """
    Demographics record deactivation endpoint.
    """
    return process_payload(request, "deactivate_demographic")


@app.route(f"/api_{version}/delete_demographic", methods=["GET", "POST"])
def api_delete_demographic():
    """
    Demographics record delete endpoint.
    """
    return process_payload(request, "delete_demographic")


@app.route(f"/api_{version}/batch", methods=["GET"])
def api_batch():
    """
    batch endpoint.
    """
    return process_payload(request, "batch")


@app.route(f"/api_{version}/bulletin", methods=["GET"])
def api_bulletin():
    """
    patient graph updates endpoint.
    """
    return process_payload(request, "bulletin")


@app.route(f"/api_{version}/process", methods=["GET"])
def api_process():
    """
    process endpoint.
    """
    return process_payload(request, "process")


@app.route(f"/api_{version}/match_affirm", methods=["GET", "POST"])
def api_match_affirmation():
    """
    match affirmations endpoint.
    """
    return process_payload(request, "match_affirm")


@app.route(f"/api_{version}/match_deny", methods=["GET", "POST"])
def api_match_denial():
    """
    match denials endpoint.
    """
    return process_payload(request, "match_deny")


@app.route(f"/api_{version}/telecom", methods=["GET"])
def api_telecom():
    """
    Telecom endpoint.
    """
    return process_payload(request, "telecom")
