import pytest

from services.web.project import timeit
from services.web.project.data_utils import random_datetime
from services.web.project.matching import (
    pairwise_string_metrics,
    string_replacer,
    string_slicer,
    string_trimmer,
    compare_strings_equal,
    compare_nameday_equal,
    compare_ssn_equal,
    slice_string_check,
    alpha_composite_name_check,
    family_name_check,
    given_name_check,
    middle_name_check,
    address_check,
    postal_check,
    wrap_address_check,
    wrap_name_check
)


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("Jon", "Jon"), {"damerau_levenshtein_distance": 0, "equal": True, "hamming_distance": 0, "jaro_winkler": 1.0,
                      "levenshtein_distance": 0, "metaphone": True, "ratio": 1.0, "strings": ("Jon", "Jon")}),
    (("Jon", "Not Jon"), {"damerau_levenshtein_distance": 4, "equal": False, "hamming_distance": 6,
                          "jaro_winkler": 0.4920634920634921, "levenshtein_distance": 4, "metaphone": False,
                          "ratio": 0.6, "strings": ("Jon", "Not Jon")}),
])
def test_pairwise_string_metrics(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    assert pairwise_string_metrics(*test_input) == expected


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("JR.", "JR", ".", ""), "JR"),
    (("MOM", "POP", "M", "P"), "POP"),
])
def test_string_replacer(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b, pattern, repl = test_input
    assert a != b
    repl_a, repl_b = string_replacer(a, b, pattern, repl)
    assert repl_a == repl_b
    assert repl_a == expected


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("JON", "JONATHAN", 3), "JON"),
    (("MICHAEL", "MIKE", 2), "MI")
])
def test_string_slicer(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b, factor = test_input
    slice_a, slice_b = string_slicer(a, b, factor)
    assert a[0] == slice_a[0]
    assert b[0] == slice_b[0]
    assert len(slice_a) == factor
    assert len(slice_b) == factor


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("   TRIM", "TRIM   "), "TRIM"),
    (("   TRIM   ", "TRIM   "), "TRIM"),
    ((" TRIM    ", "    TRIM"), "TRIM"),
])
def test_string_trimmer(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b = test_input
    trim_a, trim_b = string_trimmer(a, b)
    assert trim_a == trim_b


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("A", "B"), False),
    (("A", "A"), True)
])
def test_compare_strings_equal(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b = test_input
    assert compare_strings_equal(a, b) == expected


@timeit
def test_compare_nameday_equal():
    name_day_1 = random_datetime()
    name_day_2 = random_datetime()
    name_day_3 = name_day_1
    assert compare_nameday_equal(name_day_1, name_day_2) is False
    assert compare_nameday_equal(name_day_1, name_day_3) is True


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("123", "456"), False),
    (("123", "123"), True)
])
def test_compare_ssn_equal(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b = test_input
    assert compare_ssn_equal(a, b) == expected


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("JON", "JONATHAN"), (True, 0.4)),
    (("MARY", "JOSEPH"), (False, 0)),
    (("ROBERT", "ROB"), (True, 0.5)),
])
def test_slice_string_check(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b = test_input
    result, weight = expected
    assert slice_string_check(a, b) == (result, weight)


@timeit
@pytest.mark.parametrize("test_input, expected", [
    (("SR ", "SR."), (True, "SR", "SR")),
    (("MARY-SUE", "MARY SUE"), (True, "MARYSUE", "MARYSUE")),
    (("2 CHAINZ", "TWO CHAINZ"), (False, "CHAINZ", "TWOCHAINZ")),
])
def test_alpha_composite_name_check(test_input, expected):
    """
    :param test_input: a pair of strings to be compared
    :param expected: the intended metric output of string comparison
    """
    a, b = test_input
    result, a_sub, b_sub = expected
    assert alpha_composite_name_check(a, b) == (result, a_sub, b_sub)


@timeit
@pytest.mark.parametrize("test_input, expected_key, expected_metric, expected_result", [
    (("REZNICK", "REZNICK"), "equal", {"equal": True}, True),
    (("DAY-LEWIS", "DAY LEWIS"), "sub_result", {"sub_result": "DAYLEWIS"}, False),
    (("SMITH", "SMITH   "), "trim_result", {"trim_result": "SMITH"}, False),
    (("BRUEGEL JR.", "BRUEGEL"), "junior_detected", {"junior_detected": True}, False),
    (("BRUEGEL, SR.", "BRUEGEL"), "senior_detected", {"senior_detected": True}, False),
])
def test_family_name_check(test_input, expected_key, expected_metric, expected_result):
    """
    :param test_input: a pair of strings to be compared
    :param expected_key: a key which is inserted in this case
    :param expected_metric: the intended metric output of string comparison
    :param expected_result: the intended boolean result of the comparison
    """
    a, b = test_input
    result, metrics = family_name_check(a, b)
    assert metrics[expected_key] == expected_metric[expected_key]
    assert result == expected_result


@timeit
@pytest.mark.parametrize("test_input, expected_key, expected_metric, expected_result", [
    (("JON", "JONATHAN"), "slice_weight",
     {"damerau_levenshtein_distance": 5, "equal": False, "hamming_distance": 5, "jaro_winkler": 0.7916666666666666,
      "levenshtein_distance": 5, "metaphone": False, "ratio": 0.5454545454545454, "strings": ("JON", "JONATHAN"),
      "slice_weight": 0.4}, False),
    (("MIKE", "MICHAEL"), "NA",
     {"damerau_levenshtein_distance": 4, "equal": False, "hamming_distance": 5, "jaro_winkler": 0.7809523809523811,
      "levenshtein_distance": 4, "metaphone": False, "ratio": 0.5454545454545454, "strings": ("MIKE", "MICHAEL")},
     False),
    (("MARY-SUE", "MARY SUE"), "sub_result",
     {"damerau_levenshtein_distance": 1, "equal": False, "hamming_distance": 1, "jaro_winkler": 0.95,
      "levenshtein_distance": 1, "metaphone": False, "ratio": 0.875, "strings": ("MARY-SUE", "MARY SUE"),
      "slice_weight": 0.5, "sub_result": "MARYSUE"}, False),
    (("BEN", "BEN"), "NA", {"equal": True}, True),
])
def test_given_name_check(test_input, expected_key, expected_metric, expected_result):
    """
    :param test_input: a pair of strings to be compared
    :param expected_key: a key which is inserted in this case
    :param expected_metric: the intended metric output of string comparison
    :param expected_result: the intended boolean result of the comparison
    """
    a, b = test_input
    result, metric = given_name_check(a, b)
    if expected_key != "NA":
        assert expected_key in expected_metric
    assert result == expected_result


@timeit
@pytest.mark.parametrize("test_input, expected_key, expected_metric, expected_result", [
    (("H", "HARRIS"), "initial_result",
     {"damerau_levenshtein_distance": 5, "equal": False, "hamming_distance": 5, "jaro_winkler": 0.7222222222222223,
      "levenshtein_distance": 5, "metaphone": False, "ratio": 0.2857142857142857, "strings": ("H", "HARRIS"),
      "initial_result": True}, False),
    (("", "MICHAEL"), "blank", {"blank": True}, False),
    (("ROGER", "ROGER   "), "trim_result",
     {"damerau_levenshtein_distance": 2, "equal": False, "hamming_distance": 2, "jaro_winkler": 0.9428571428571428,
      "levenshtein_distance": 2, "metaphone": False, "ratio": 0.8333333333333334, "strings": ("ROGER", "ROGER  "),
      "trim_result": "ROGER", "initial_result": True}, False),
    (("JANE", "JANE"), "equal", {"equal": True}, True),
])
def test_middle_name_check(test_input, expected_key, expected_metric, expected_result):
    """
        :param test_input: a pair of strings to be compared
        :param expected_key: a key which is inserted in this case
        :param expected_metric: the intended metric output of string comparison
        :param expected_result: the intended boolean result of the comparison
        """
    a, b = test_input
    result, metric = middle_name_check(a, b)
    assert expected_metric[expected_key]
    assert result == expected_result


@timeit
@pytest.mark.parametrize("test_input, expected_key, expected_metric, expected_result", [
    (("1600 Pennsylvania Avenue", "1600 Pennsylvania"), "slice_weight",
     {"damerau_levenshtein_distance": 7, "equal": False, "hamming_distance": 7, "jaro_winkler": 0.9027777777777778,
      "levenshtein_distance": 7, "metaphone": False, "ratio": 0.8292682926829268,
      "strings": ("1600 Pennsylvania Avenue", "1600 Pennsylvania"), "slice_weight": 0.7},
     False),
    (("308 Negra Arroyo Lane", ""), "address_blank", {"address_blank": True}, False),
    (("The North Pole", "The North Pole"), "equal", {"equal": True}, True),
])
def test_address_check(test_input, expected_key, expected_metric, expected_result):
    """
    :param test_input: a pair of strings to be compared
    :param expected_key: a key which is inserted in this case
    :param expected_metric: the intended metric output of string comparison
    :param expected_result: the intended boolean result of the comparison
    """
    a, b = test_input
    result, metric = address_check(a, b)
    assert metric[expected_key] is True or metric[expected_key] == 0.7
    assert result == expected_result


@timeit
@pytest.mark.parametrize("test_input, expected_key, expected_metric, expected_result", [
    (("90210", "90211"), "NA",
     {"damerau_levenshtein_distance": 1, "equal": False, "hamming_distance": 1, "jaro_winkler": 0.8666666666666667,
      "levenshtein_distance": 1, "metaphone": True, "ratio": 0.8, "strings": ("90210", "90211")}, False),
    (("90210", ""), "postal_blank", {"postal_blank": True}, False),
    (("90210", "90210"), "equal", {"equal": True}, True),
])
def test_postal_check(test_input, expected_key, expected_metric, expected_result):
    """
    :param test_input: a pair of strings to be compared
    :param expected_key: a key which is inserted in this case
    :param expected_metric: the intended metric output of string comparison
    :param expected_result: the intended boolean result of the comparison
    """
    a, b = test_input
    result, metric = postal_check(a, b)
    if expected_key != "NA":
        assert expected_key in expected_metric
    assert result == expected_result


record_a = {
    "family_name": "WHITE, SR.",
    "given_name": "WALTER",
    "middle_name": "HARTWELL",
    "address_1": "308 Negra Arroyo Lane",
    "address_2": "",
    "postal_code": "87111"
}

record_b = {
    "family_name": "WHITE JR",
    "given_name": "WALTER",
    "middle_name": "HARTWELL",
    "address_1": "308 Negra Arroyo Lane",
    "address_2": "",
    "postal_code": "87111"
}
record_c = record_a.copy()
record_c["family_name"] = "WHITE"


@timeit
def test_wrap_address_check():
    expected_result = {
        "address_1": True,
        "address_2": True,
        "postal_code": True,
        "metrics": {
            "address_1": {"equal": True},
            "address_2": {"address_blank": True},
            "postal_code": {"equal": True}
        }
    }
    assert wrap_address_check(record_a, record_b) == expected_result


@timeit
def test_wrap_name_check():
    expected_result_1 = {
        "family_name": False,
        "given_name": True,
        "middle_name": True,
        "metrics": {
            "family_name": {
                "damerau_levenshtein_distance": 3,
                "equal": False,
                "hamming_distance": 5,
                "jaro_winkler": 0.915,
                "levenshtein_distance": 3,
                "metaphone": False,
                "ratio": 0.7777777777777778,
                "strings": ("WHITE, SR.", "WHITE JR")},
            "given_name": {"equal": True},
            "middle_name": {"equal": True}
        }
    }
    expected_result_2 = {
        "family_name": False,
        "given_name": True,
        "middle_name": True,
        "metrics": {
            "family_name": {
                "damerau_levenshtein_distance": 3,
                "equal": False,
                "hamming_distance": 3,
                "jaro_winkler": 0.925,
                "levenshtein_distance": 3,
                "metaphone": False,
                "ratio": 0.7692307692307692,
                "strings": ("WHITE", "WHITE JR"),
                "junior_detected": True},
            "given_name": {"equal": True},
            "middle_name": {"equal": True}
        }
    }

    assert wrap_name_check(record_a, record_b) == expected_result_1
    assert wrap_name_check(record_c, record_b) == expected_result_2
