from .conftest import (
    THE_CONSTANT_ID,
    CONST_BATCH_ID,
    CONST_PROC_ID,
    transaction_key_for_tests
)


def test_delete(mock_delete):
    my_delete = mock_delete
    assert my_delete.etl_id == THE_CONSTANT_ID


def test_demographic(mock_demographic):
    my_transaction_key = transaction_key_for_tests(
        batch_id=CONST_BATCH_ID, proc_id=CONST_PROC_ID)
    my_demographic = mock_demographic
    assert my_demographic.transaction_key == my_transaction_key


def test_demographic_activation(mock_demographic_activation):
    my_demographic_activation = mock_demographic_activation
    assert my_demographic_activation.etl_id == THE_CONSTANT_ID


def test_demographic_archive(mock_demographic_archive):
    my_transaction_key = transaction_key_for_tests(
        batch_id=CONST_BATCH_ID, proc_id=CONST_PROC_ID)
    my_demographic_archive = mock_demographic_archive
    assert my_demographic_archive.transaction_key == my_transaction_key


def test_demographic_deactivation(mock_demographic_deactivation):
    my_demographic_deactivation = mock_demographic_deactivation
    assert my_demographic_deactivation.etl_id == THE_CONSTANT_ID


def test_demographic_delete(mock_demographic_delete):
    my_demographic_delete = mock_demographic_delete
    assert my_demographic_delete.etl_id == THE_CONSTANT_ID


def test_batch(mock_batch):
    my_batch = mock_batch
    assert my_batch.batch_status == "test_batch"


def test_bulletin(mock_bulletin):
    my_bulletin = mock_bulletin
    assert my_bulletin.etl_id == THE_CONSTANT_ID


def test_process(mock_process):
    my_process = mock_process
    assert my_process.proc_status == "test_process"


def test_match_affirm(mock_match_affirm):
    my_match_affirm = mock_match_affirm
    assert my_match_affirm.etl_id == THE_CONSTANT_ID


def test_match_deny(mock_match_deny):
    my_match_deny = mock_match_deny
    assert my_match_deny.etl_id == THE_CONSTANT_ID


def test_telecom(mock_telecom):
    my_telecom = mock_telecom
    assert my_telecom.etl_id == THE_CONSTANT_ID


def test_enterprise_group(mock_enterprise_group):
    my_enterprise_group = mock_enterprise_group
    assert my_enterprise_group.etl_id == THE_CONSTANT_ID


def test_enterprise_match(mock_enterprise_match):
    my_enterprise_match = mock_enterprise_match
    assert my_enterprise_match.etl_id == THE_CONSTANT_ID
