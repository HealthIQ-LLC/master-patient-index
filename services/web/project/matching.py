from jellyfish import (
    levenshtein_distance,
    damerau_levenshtein_distance,
    hamming_distance,
    jaro_winkler_similarity as jaro_winkler,
    metaphone
    )
import Levenshtein as Lev
import re


def pairwise_string_metrics(a: str, b: str) -> dict:
    return {
        "damerau_levenshtein_distance": damerau_levenshtein_distance(a, b),
        "equal": a == b,
        "hamming_distance": hamming_distance(a, b),
        "jaro_winkler": jaro_winkler(a, b),
        "levenshtein_distance": levenshtein_distance(a, b),
        "metaphone": metaphone(a) == metaphone(b),
        "ratio": Lev.ratio(a, b),
        "strings": (a, b)
    }


def string_replacer(a: str, b: str, pattern: str, repl: str) -> (str, str):
    return a.replace(pattern, repl), b.replace(pattern, repl)


def string_slicer(a: str, b: str, factor: int) -> (str, str):
    return a[:factor], b[:factor]


def string_trimmer(a: str, b: str) -> (str, str):
    return a.strip(), b.strip()


def compare_strings_equal(a: str, b: str) -> bool:
    return a == b


def compare_nameday_equal(a, b) -> bool: # use this to implement your own datetime-related tests
    return a == b


def compare_ssn_equal(a, b) -> bool: # use this to implement your own heightened security posture
    return a == b


def slice_string_check(a: str, b: str, slice_min=3) -> (bool, int):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    :param slice_min: a value of 3 compare a[:3] to b[:3] at the lower end of the range
    """
    len_a = len(a)
    len_b = len(b)
    if len_a >= len_b:
        slice_max = len_a
    else:
        slice_max = len_b
    slice_weight = 1.0
    for i in range(slice_max, slice_min-1, -1):
        slice_result = compare_strings_equal(*string_slicer(a, b, i))
        if slice_result:
            return slice_result, round(slice_weight, 1)
        slice_weight -= 1 / slice_max
    return slice_result, 0


def alpha_composite_name_check(a: str, b: str) -> (bool, str, str):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    """
    regex = re.compile("[^a-zA-Z]")
    a_sub = regex.sub("", a)
    b_sub = regex.sub("", b)
    result = compare_strings_equal(a_sub, b_sub)

    return result, a_sub, b_sub


def family_name_check(a: str, b: str) -> (bool, dict):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    """
    result = compare_strings_equal(a, b)
    if result:
        return result, {"equal": True}
    metrics = pairwise_string_metrics(a, b)
    trim_a, trim_b = string_trimmer(a, b)
    if compare_strings_equal(trim_a, trim_b):
        metrics["trim_result"] = trim_a
    alpha_composite_result, sub_a, sub_b = alpha_composite_name_check(a, b)
    if alpha_composite_result:
        metrics["sub_result"] = sub_a
    if compare_strings_equal(*string_trimmer(*string_replacer(sub_a, sub_b, "JR", ""))):
        metrics["junior_detected"] = True
    if compare_strings_equal(*string_trimmer(*string_replacer(sub_a, sub_b, "SR", ""))):
        metrics["senior_detected"] = True

    return result, metrics


def given_name_check(a: str, b: str) -> (bool, dict):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    """
    result = compare_strings_equal(a, b)
    if result:
        return result, {"equal": True}
    metrics = pairwise_string_metrics(a, b)
    trim_a, trim_b = string_trimmer(a, b)
    if compare_strings_equal(trim_a, trim_b):
        metrics["trim_result"] = trim_a
    slice_result, slice_weight = slice_string_check(a, b)
    if slice_result:
        metrics["slice_weight"] = slice_weight
    alpha_composite_result, sub_a, _ = alpha_composite_name_check(a, b)
    if alpha_composite_result:
        metrics["sub_result"] = sub_a

    return result, metrics
    

def middle_name_check(a: str, b: str) -> (bool, dict):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    """
    result = compare_strings_equal(a, b)
    if len(a) == 0 or len(b) == 0:
        return result, {"blank": True}
    if result:
        return result, {"equal": True}
    metrics = pairwise_string_metrics(a, b)
    trim_a, trim_b = string_trimmer(a, b)
    if compare_strings_equal(trim_a, trim_b):
        metrics["trim_result"] = trim_a
    if compare_strings_equal(a[:1], b[:1]):
        metrics["initial_result"] = True

    return result, metrics


def address_check(a: str, b: str) -> (bool, dict):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    """
    result = compare_strings_equal(a, b)
    if len(a) == 0 or len(b) == 0:
        return result, {"address_blank": True}
    if result:
        return result, {"equal": True}
    metrics = pairwise_string_metrics(a, b)
    slice_result, slice_weight = slice_string_check(a, b)
    if slice_result:
        metrics["slice_weight"] = slice_weight

    return result, metrics


def postal_check(a: str, b: str) -> (bool, dict):
    """
    :param a: one string value to be compared
    :param b: one string value to be compared
    """
    result = compare_strings_equal(a, b)
    if len(a) == 0 or len(b) == 0:
        return result, {"postal_blank": True}
    if result:
        return result, {"equal": True}
    metrics = pairwise_string_metrics(a, b)

    return result, metrics


def wrap_address_check(record_a: dict, record_b: dict) -> dict:
    """
    :record_a: one record series to be compared
    :record_b: one record series to be compared
    """
    metrics = {}
    
    postal_result, postal_metrics = postal_check(
        record_a.get("postal_code", None), record_b.get("postal_code", None))
    address_1_result, address_1_metrics = address_check(
        record_a.get("address_1", None), record_b.get("address_1", None))
    address_2_result, address_2_metrics = address_check(
        record_a.get("address_2", None), record_b.get("address_2", None))

    return {
        "address_1": address_1_result,
        "address_2": address_2_result,
        "postal_code": postal_result,
        "metrics": {
            "address_1": address_1_metrics,
            "address_2": address_2_metrics,
            "postal_code": postal_metrics
        }
    }


def wrap_name_check(record_a: dict, record_b: dict) -> dict:
    """
    :record_a: one record series to be compared
    :record_b: one record series to be compared
    """
    fam_name_result, fam_name_metrics = family_name_check(
        record_a.get("family_name", None), record_b.get("family_name", None))
    given_name_result, given_name_metrics = given_name_check(
        record_a.get("given_name", None), record_b.get("given_name", None))
    mid_name_result, mid_name_metrics = middle_name_check(
        record_a.get("middle_name", None), record_b.get("middle_name", None))
    
    return {
        "family_name": fam_name_result,
        "given_name": given_name_result,
        "middle_name": mid_name_result,
        "metrics": {
            "family_name": fam_name_metrics,
            "given_name": given_name_metrics,
            "middle_name": mid_name_metrics
        }
    }
