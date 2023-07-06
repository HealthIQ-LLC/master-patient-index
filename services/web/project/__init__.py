from flask import jsonify, request, send_from_directory
import sys
import threading

from .app import app
from .auditor import Auditor
from .coupler import COUPLER, DemographicsGetValidator
from .logger import DEBUG_ROUTE, timeit, version
from .validator import DemographicsGetValidator


@app.route("/static/<path:filename>")
def staticfiles(filename):
    """
    :param filename: The path to a file to download
    """
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


@app.route("/")
def hello_world():
    return jsonify(hello="world")


def get(payload: dict, endpoint: str) -> list:
    """
    :param payload: the user-initiated data payload to GET with
    :param endpoint: a string denoting the endpoint invoked
    Return a list of records SELECTed from the data model
    """
    response = COUPLER['query_records']['processor'](payload, endpoint=endpoint)
    
    return response


def post(payload: dict, endpoint: str) -> dict:
    """
    :param payload: the user-initiated data payload to POST with
    :param endpoint: a string denoting the endpoint invoked
    The auditor provides context management and threading for a POST request
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
    """
    A wrapper for all requests: payload is deserialized, validated, and routed
    """
    response = None
    endpoint = request.endpoint
    method = request.method
    # first, retrieve the appropriate validator for the endpoint
    validator = COUPLER[endpoint]['validator']()
    if endpoint == 'demographic' and method == 'GET':
        validator = DemographicsGetValidator()
    # next, deserialize the JSON request
    try:
        payload_obj = request.get_json()
    except:
        print("Request is not acceptable JSON", file=DEBUG_ROUTE)
        return jsonify(status=405, response=response)
    # next, validate the request payload against its endpoint
    result, msg = validator.validate(payload_obj)
    # do the request itself
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


# register all API endpoints on service start
for endpoint, couplings in COUPLER.items():
    app.add_url_rule(
        f'/api_{version}/{endpoint}',
        endpoint=endpoint, 
        view_func=process_payload,
        methods=couplings['methods']
    )
