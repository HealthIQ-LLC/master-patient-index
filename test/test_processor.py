from mock_alchemy.mocking import UnifiedAlchemyMagicMock
import pytest

from .conftest import (
    THE_CONSTANT_ID,
    mock_delete,
    mock_demographic,
    mock_demographic_activation,
    mock_demographic_archive,
    mock_demographic_deactivation,
    mock_demographic_delete,
    mock_batch,
    mock_bulletin,
    mock_process,
    mock_match_affirm,
    mock_match_deny,
    mock_telecom,
    mock_enterprise_group,
    mock_enterprise_match,
    mock_etl_id_source
)
from services.web.project import timeit
from services.web.project.processor import MODEL_MAP


@timeit
@pytest.mark.parametrize(
    "payload, table_name", [
        (mock_delete, "delete_action"),
        (mock_demographic, "demographic"),
        (mock_demographic_activation, "activate_demographic"),
        (mock_demographic_archive, "archive_demographic"),
        (mock_demographic_deactivation, "deactivate_demographic"),
        (mock_demographic_delete, "delete_demographic"),
        (mock_enterprise_group, "enterprise_group"),
        (mock_enterprise_match, "enterprise_match"),
        (mock_batch, "batch"),
        (mock_bulletin, "bulletin"),
        (mock_process, "process"),
        (mock_match_affirm, "match_affirm"),
        (mock_match_deny, "match_deny"),
        (mock_telecom, "telecom"),
        (mock_etl_id_source, "etl_id_source")
    ])
def test_transact_and_query_records(payload, table_name):
    """
    :param payload: an object in the model to test
    :param table_name: the table corresponding to the model to test
    """
    session = UnifiedAlchemyMagicMock()
    model = MODEL_MAP[table_name]
    assert len(session.query(model).all()) == 0
    if table_name in ('demographic', 'archive_demographic'):
        session.add(model(record_id=THE_CONSTANT_ID))
        pkey = model.record_id
        assert len(session.query(model).filter(pkey == THE_CONSTANT_ID).all()) == 1
        session.add(model(record_id=THE_CONSTANT_ID + 1))
    elif table_name == 'batch':
        session.add(model(batch_id=THE_CONSTANT_ID))
        pkey = model.batch_id
        assert len(session.query(model).filter(pkey == THE_CONSTANT_ID).all()) == 1
        session.add(model(batch_id=THE_CONSTANT_ID + 1))
    elif table_name == 'process':
        session.add(model(proc_id=THE_CONSTANT_ID))
        pkey = model.proc_id
        assert len(session.query(model).filter(pkey == THE_CONSTANT_ID).all()) == 1
        session.add(model(proc_id=THE_CONSTANT_ID + 1))
    else:
        session.add(model(etl_id=THE_CONSTANT_ID))
        pkey = model.etl_id
        assert len(session.query(model).filter(pkey == THE_CONSTANT_ID).all()) == 1
        session.add(model(etl_id=THE_CONSTANT_ID + 1))
    query = session.query(model)
    response = list()
    for row in query.all():
        response.append(row.to_dict())
    assert len(response) == 2
