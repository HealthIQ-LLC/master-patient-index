from sqlalchemy import and_, or_
from time import time

from .matching import (
    compare_nameday_equal, 
    compare_ssn_equal, 
    wrap_address_check, 
    wrap_name_check
)
from .model import Demographic


def parse_result(metrics: dict) -> bool:
    threshold = metrics.get('threshold')
    score = metrics.get('score')
    if score >= threshold:
        return True
    else:
        return False


def toy_fine_matching(record_a, record_b) -> dict:
    stride = 0.3
    match_score = 0
    threshold = 0.5
    if record_a.get(postal_code) == record_b.get('postal_code'):
        match_score += stride
    if record_a.get(name_day) == record_b.get('name_day'):
        match_score += stride
    if record_a.get(family_name) == record_b.get('family_name'):
        match_score += stride
    toy_fine_match = {
        "record_a_id": record_a.get('record_id'),
        "record_b_id": record_b.get('record_id'),
        "score": match_score,
        "threshold": threshold
    }
    toy_fine_match["match"] = parse_result(toy_fine_match)

    return toy_fine_match


def fine_matching(record_a: dict, record_b: dict) -> dict:
    """
    :param record_a: the new demographics record 
    :param record_b: the coarse match record against which the new record is tested
    """
    start = time()
    # ToDo: score and threshold
    fine_match = {"address_matching": wrap_address_check(record_a, record_b),
                  "model_score": None,
                  "name_matching": wrap_name_check(record_a, record_b),
                  "name_day_matching": compare_nameday_equal(
                    record_a.get("name_day", None), record_b.get("name_day", None)
                    ),
                  "ssn_matching": compare_ssn_equal(
                    record_a.get("social_security_number", None), 
                    record_b.get("social_security_number", None)
                    ),
                  "score": 0,
                  "threshold": 0}
    fine_match["match"] = parse_result(fine_match)
    end = time()
    fine_match["exec_time"] = f"{end - start:.8f}"

    return fine_match


def toy_coarse_matching(demographic_record) -> list:
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

    return coarse_results


def coarse_matching(demographics_record) -> list:
    """
    :param demographics_record: The input demographics record
    """
    #ToDo: implement data-aware coarse matching technique
    #This bypass leaves coarse matching in guaranteed toy mode for now
    coarse_results = toy_coarse_matching(demographics_record)

    return coarse_results

# implement blocking & filtering technique here with coarse, fine matching modes
MODES = {
    "toy": (toy_coarse_matching, toy_fine_matching),
    "prod": (coarse_matching, fine_matching)
}
MODE = "toy"


def compute_all_matches(demographic_record) -> (list, str):
    """
    :param demographic_record: The input demographics record
    """
    coarse_matcher, fine_matcher = MODES[MODE]
    start = time()
    computed_matches = []
    for coarse_match in coarse_matcher(demographic_record):
        if demographic_record.get('record_id') != coarse_match.get('record_id'):
            computed_matches.append(fine_matcher(demographic_record, coarse_match))
    end = time()
    exec_time = f"{end - start:.8f}"

    return computed_matches, exec_time
