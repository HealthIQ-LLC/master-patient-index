from datetime import datetime
import pytest

from services.web.project.app import app
from services.web.project.data_utils import (
    demographics_record,
    unique_id,
    unique_text_key
)
from services.web.project.model import (
    Batch,
    Bulletin,
    Delete,
    Demographic,
    DemographicActivation,
    DemographicArchive,
    DemographicDeactivation,
    DemographicDelete,
    EnterpriseGroup,
    EnterpriseMatch,
    ETLIDSource,
    MatchAffirmation,
    MatchDenial,
    Process,
    Telecom
)
from services.web.project import version
now = datetime.now()
time_stamp = now.strftime("%y/%m/%d, %H:%M:%S")
batch_action = created_by = f'test_{time_stamp}'
created_ts = now
THE_CONSTANT_ID = 8675309
CONST_BATCH_ID = 867
CONST_PROC_ID = 5309


def transaction_key_for_tests(batch_id=None, proc_id=None):
    if batch_id is None:
        batch_id = unique_id()
    if proc_id is None:
        proc_id = unique_id()
    transaction_key = f"{batch_id}_{proc_id}"

    return transaction_key, proc_id, batch_id


@pytest.fixture
def client():
    return app.test_client()


@pytest.fixture
def nodes_and_weights():
    return [
        (12345, 12346, 1),
        (12345, 12347, 0.6),
        (12345, 12348, 0.4),
        (12346, 12347, 0),
        (12346, 12348, 0.3),
        (12347, 12348, 0),
    ]


@pytest.fixture
def mock_delete():
    key = "test_delete_action"
    record = {
        "etl_id": THE_CONSTANT_ID,
        "batch_action": key,
        "transaction_key": transaction_key_for_tests(),
    }
    delete = Delete(**record)

    return delete


@pytest.fixture
def mock_demographic():
    key = "test_demographic"
    record = demographics_record(key)
    record['transaction_key'] = transaction_key_for_tests(batch_id=CONST_BATCH_ID,
                                                                proc_id=CONST_PROC_ID)
    demographic = Demographic(**record)

    return demographic


@pytest.fixture
def mock_demographic_activation():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id": unique_id(),
        "transaction_key": transaction_key_for_tests(),
    }
    activate_demographic = DemographicActivation(**record)

    return activate_demographic


@pytest.fixture
def mock_demographic_archive():
    key = "test_archive_demographic"
    record = demographics_record(key)
    record['transaction_key'] = transaction_key_for_tests(batch_id=CONST_BATCH_ID,
                                                                proc_id=CONST_PROC_ID)
    record['archive_transaction_key'] = transaction_key_for_tests()
    archive_demographic = DemographicArchive(**record)

    return archive_demographic


@pytest.fixture
def mock_demographic_deactivation():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id": unique_id(),
        "transaction_key": transaction_key_for_tests(),
    }
    deactivate_demographic = DemographicDeactivation(**record)

    return deactivate_demographic


@pytest.fixture
def mock_demographic_delete():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id": unique_id(),
        "transaction_key": transaction_key_for_tests(),
    }
    delete_demographic = DemographicDelete(**record)

    return delete_demographic


@pytest.fixture
def mock_batch():
    key = "test_batch"
    record = {
        "batch_id": THE_CONSTANT_ID,
        "batch_action": batch_action,
        "batch_status": key
    }
    batch = Batch(**record)

    return batch


@pytest.fixture
def mock_bulletin():
    batch_id = unique_id()
    proc_id = unique_id()
    record = {
        "etl_id": THE_CONSTANT_ID,
        "batch_id": batch_id,
        "proc_id": proc_id,
        "record_id": unique_id(),
        "empi_id": unique_id(),
        "transaction_key": transaction_key_for_tests(batch_id=batch_id, proc_id=proc_id),
        "bulletin_ts": created_ts
    }
    bulletin = Bulletin(**record)

    return bulletin


@pytest.fixture
def mock_process():
    key = "test_process"
    record = {
        "proc_id": unique_id(),
        "batch_id": unique_id(),
        "proc_record_id": unique_id(),
        "proc_status": key,
        "row": unique_id(),
        "foreign_record_id": unique_id()
    }
    process = Process(**record)

    return process


@pytest.fixture
def mock_match_affirm():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id_low": unique_id(),
        "record_id_high": unique_id(),
        "transaction_key": transaction_key_for_tests(),
    }
    affirmation = MatchAffirmation(**record)

    return affirmation


@pytest.fixture
def mock_match_deny():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id_low": unique_id(),
        "record_id_high": unique_id(),
        "transaction_key": transaction_key_for_tests(),
    }
    denial = MatchDenial(**record)

    return denial


@pytest.fixture
def mock_telecom():
    key = "test_telecom"
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id": unique_id(),
        "telecoms_type": unique_text_key(key),
        "telecoms_subtype": unique_text_key(key),
        "telecoms_value": unique_text_key(key),
        "transaction_key": transaction_key_for_tests(),
        "touched_by": created_by,
        "touched_ts": created_ts
    }
    telecom = Telecom(**record)

    return telecom


@pytest.fixture
def mock_enterprise_group():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "enterprise_id": unique_id(),
        "record_id": unique_id(),
        "transaction_key": transaction_key_for_tests(),
        "touched_by": created_by,
        "touched_ts": created_ts
    }
    group = EnterpriseGroup(**record)

    return group


@pytest.fixture
def mock_enterprise_match():
    key = "test_enterprise_match"
    record = {
        "etl_id": THE_CONSTANT_ID,
        "record_id_low": unique_id(),
        "record_id_high": unique_id(),
        "match_weight": unique_text_key(key),
        "is_valid": True,
        "transaction_key": transaction_key_for_tests(),
        "touched_by": created_by,
        "touched_ts": created_ts
    }
    match = EnterpriseMatch(**record)

    return match


@pytest.fixture
def mock_etl_id_source():
    record = {
        "etl_id": THE_CONSTANT_ID,
        "version": version,
        "id_created_ts": created_ts
    }
    id_source = ETLIDSource(**record)

    return id_source
