from services.web.project import timeit
from services.web.project.app import app
from services.web.project.data_utils import demographics_record
from services.web.project.engine import (
    compute_all_matches,
    coarse_matching,
    fine_matching,
    parse_result,
)
from services.web.project.model import db

@timeit
def test_compute_all_matches():
    with app.app_context():
        db.create_all()
        key = "test_compute_all_matches"
        input_fixture = demographics_record(key)
        expected_result = {
                'address_matching': None,
                'model_score': None,
                'name_matching': None,
                'name_day_matching': None,
                'ssn_matching': None
        }
        actual_results, _ = compute_all_matches(input_fixture)
        for actual_result in actual_results:
            assert actual_result.keys() == expected_result.keys()


@timeit
def test_coarse_matching():
    with app.app_context():
        db.create_all()
        key = "test_coarse_matching"
        input_fixture = demographics_record(key)
        expected_result = []
        actual_result = coarse_matching(input_fixture)
        assert expected_result == actual_result


@timeit
def test_fine_matching():
    key = "test_fine_matching"
    input_fixture_1 = demographics_record(f'{key}_1')
    input_fixture_2 = demographics_record(f'{key}_2')
    expected_result = {
        'address_matching': None,
        'match': None,
        'exec_time': None,
        'model_score': None,
        'name_matching': None,
        'name_day_matching': None,
        'ssn_matching': None,
        'score': 0,
        'threshold': 0
    }
    actual_result = fine_matching(input_fixture_1, input_fixture_2)
    assert sorted(expected_result.keys()) == sorted(actual_result.keys())


@timeit
def test_parse_result():
    input_fixture = {'score': 0.6, 'threshold': 0.5}
    expected_result = True
    actual_result = parse_result(input_fixture)
    assert expected_result == actual_result

    input_fixture = {'score': 0.4, 'threshold': 0.5}
    expected_result = False
    actual_result = parse_result(input_fixture)
    assert expected_result == actual_result
