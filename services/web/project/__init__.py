from flask import jsonify, request, send_from_directory
import threading
from werkzeug.exceptions import BadRequest

from .app import app
from .auditor import Auditor
from .coupler import COUPLER
from .logger import DEBUG_ROUTE, timeit, version
from .validators import DemographicsGetValidator


@app.route("/")
def hello_world():
    return jsonify(hello="world")


@app.route("/static/<path:filename>")
def staticfiles(filename):
    """
    :param filename: The path to a file to download
    :return send_from_directory(): the file at the location requested
    """
    return send_from_directory(app.config["STATIC_FOLDER"], filename)


def get(payload: dict, endpoint: str) -> list:
    """
    :param payload: the user-initiated data payload to GET with
    :param endpoint: a string denoting the endpoint invoked
    :return response: a list of records selected from the data model
    """
    response = COUPLER['query_records']['processor'](payload, endpoint=endpoint)
    
    return response


def post(payload: dict, endpoint: str) -> dict:
    """
    :param payload: the user-initiated data payload to POST with
    :param endpoint: a string denoting the endpoint invoked
    :return response: a json containing your request locator and a status message
    The auditor provides context management and threading for a POST request
    """
    user = payload['user']
    processor = COUPLER[endpoint]['processor']
    with Auditor(user, version, endpoint) as job_auditor:
        thread = threading.Thread(
            target=processor,
            args=(payload, job_auditor)
        )
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
    :return jsonify(status, response): a JSON object containing the HTTP status and response object
    A wrapper for all requests: payload is deserialized, validated, and routed to GET or POST
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
    except BadRequest as e:
        print(f"Request is not acceptable JSON: {e}", file=DEBUG_ROUTE)
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
        print(
            f"Issue encountered with {method} request",
            file=DEBUG_ROUTE
        )
        return jsonify(status=405, response=response)


# register all API endpoints on service start
for end_point, couplings in COUPLER.items():
    app.add_url_rule(
        f'/api_{version}/{end_point}',
        endpoint=end_point,
        view_func=process_payload,
        methods=couplings['methods']
    )
