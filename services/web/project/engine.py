from sqlalchemy import and_, or_
from time import time

from .logger import DEBUG_ROUTE
from .matching import (
    compare_nameday_equal, 
    compare_ssn_equal, 
    wrap_address_check, 
    wrap_name_check
)
from .model import Demographic


def parse_result(metrics: dict) -> bool:
    """
    :param metrics: the object containing the results of pair-wise analysis
    """
    threshold = metrics.get('threshold')
    score = metrics.get('score')
    if score >= threshold:
        return True
    else:
        return False


def toy_fine_matching(record_a, record_b) -> dict:
    """
    :param record_a: the new demographics record to be networked
    :param record_b: one coarse match record
    :return toy_fine_match: an object containing a "coerced-result" match
    metric object without deterministic tests
    """
    stride = 0.3
    match_score = 0
    threshold = 0.5
    if record_a.postal_code == record_b.postal_code:
        match_score += stride
    if record_a.name_day == record_b.name_day:
        match_score += stride
    if record_a.family_name == record_b.family_name:
        match_score += stride
    toy_fine_match = {
        "record_a_id": record_a.record_id,
        "record_b_id": record_b.record_id,
        "score": match_score,
        "threshold": threshold
    }
    toy_fine_match["match"] = parse_result(toy_fine_match)

    return toy_fine_match


def fine_matching(record_a: dict, record_b: dict) -> dict:
    """
    :param record_a: the new demographics record to be networked
    :param record_b: one coarse match record
    :return fine_match: an object containing a match metric object
    containing all deterministic tests
    """
    start = time()
    # ToDo: score and threshold
    fine_match = {"address_matching": wrap_address_check(record_a, record_b),
                  "model_score": None,
                  "name_matching": wrap_name_check(record_a, record_b),
                  "name_day_matching": compare_nameday_equal(
                    record_a.name_day, record_b.name_day    # type: ignore
                    ),
                  "ssn_matching": compare_ssn_equal(
                    record_a.social_security_number,  # type: ignore
                    record_b.social_security_number,  # type: ignore
                    ),
                  "score": 0,
                  "threshold": 0}
    fine_match["match"] = parse_result(fine_match)
    end = time()
    fine_match["exec_time"] = f"{end - start:.8f}"

    return fine_match


def toy_coarse_matching(demographic_record) -> list:
    """
    :param demographic_record: The input demographics record
    :return coarse_results: a list of all the records against which the
    new record should be compared
    """
    record_id = demographic_record.record_id
    postal_code = demographic_record.postal_code
    name_day = demographic_record.name_day
    family_name = demographic_record.family_name
    coarse_results = []
    source_table = Demographic
    query = Demographic.query
    query = query.filter(or_(
        source_table.__table__.c['postal_code'] == postal_code,
        source_table.__table__.c['name_day'] == name_day,
        source_table.__table__.c['family_name'] == family_name))\
        .filter(and_(source_table.__table__.c['record_id'] != record_id))
    for row in query.all():
        coarse_results.append(row)
        print(row, file=DEBUG_ROUTE)

    return coarse_results


def coarse_matching(demographic_record) -> list:
    """
    :param demographic_record: The input demographics record
    :return coarse_results: a list of all the records against which the new
    record should be compared
    """
    # ToDo: implement data-aware coarse matching technique
    # This bypass leaves coarse matching in guaranteed toy mode for now
    coarse_results = toy_coarse_matching(demographic_record)

    return coarse_results


MODES = {  # implement any kind of blocking / filtering by setting a mode here
    "toy": (toy_coarse_matching, toy_fine_matching),
    "prod": (coarse_matching, fine_matching)
}
MODE = "toy"


def compute_all_matches(demographic_record) -> (list, str):
    """
    :param demographic_record: The input demographics record
    :return computed_matches, exec_time: (the list of all results for all
    coarse matches, the duration of the computation)
    """
    coarse_matcher, fine_matcher = MODES[MODE]
    start = time()
    computed_matches = []
    for coarse_match in coarse_matcher(demographic_record):
        if demographic_record.record_id != coarse_match.record_id:
            computed_matches.append(
                fine_matcher(demographic_record, coarse_match)
            )
    end = time()
    exec_time = f"{end - start:.8f}"

    return computed_matches, exec_time
