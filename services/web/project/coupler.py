import json
from incoming import datatypes, PayloadValidator

from .model import db, MODEL_MAP
from .processor import PROCESSOR_MAP


class RecordIDValidator(PayloadValidator):
    record_id = datatypes.Integer(required=True)
    user = datatypes.String(required=True)


class DemographicsGetValidator(PayloadValidator):
    record_id = datatypes.Integer(required=False)
    organization_key = datatypes.String(required=False)
    system_key = datatypes.String(required=False)
    system_id = datatypes.String(required=False)
    given_name = datatypes.String(required=False)
    middle_name = datatypes.String(required=False)
    family_name = datatypes.String(required=False)
    gender = datatypes.String(required=False)
    # name_day = db.Column(db.DateTime)
    address_1 = datatypes.String(required=False)
    address_2 = datatypes.String(required=False)
    city = datatypes.String(required=False)
    state = datatypes.String(required=False)
    postal_code = datatypes.String(required=False)
    is_active = datatypes.Boolean(required=False)
    transaction_key = datatypes.String(required=False)
    source_key = datatypes.String(required=False)
    source_value = datatypes.String(required=False)
    user = datatypes.String(required=True)


class DemographicsPostValidator(PayloadValidator):
    demographics = datatypes.Array(required=True)
    user = datatypes.String(required=True)


class DeleteActionValidator(PayloadValidator):
    batch_id = datatypes.Integer(required=True)
    proc_id = datatypes.Integer(required=True)
    action = datatypes.String(required=True)
    user = datatypes.String(required=True)


class MatchValidator(PayloadValidator):
    record_id_low = datatypes.Integer(required=True)
    record_id_high = datatypes.Integer(required=True)
    user = datatypes.String(required=True)


COUPLER = {
    'activate_demographic': {
        'model': MODEL_MAP['activate_demographic'],
        'processor': PROCESSOR_MAP['activate_demographic'],
        'validator': RecordIDValidator,
        'methods': ['GET', 'POST'],
    },
    'archive_demographic': {
        'model': MODEL_MAP['archive_demographic'],
        'processor': None,
        'validator': RecordIDValidator,
        'methods': ['GET'],
    },
    'deactivate_demographic': {
        'model': MODEL_MAP['deactivate_demographic'],
        'processor': PROCESSOR_MAP['deactivate_demographic'],
        'validator': RecordIDValidator,
        'methods': ['GET', 'POST'],
    },
    'delete_action': {
        'model': MODEL_MAP['delete_action'],
        'processor': PROCESSOR_MAP['delete_action'],
        'validator': DeleteActionValidator,
        'methods': ['GET', 'POST'],
    },
    'delete_demographic': {
        'model': MODEL_MAP['delete_demographic'],
        'processor': PROCESSOR_MAP['delete_demographic'],
        'validator': RecordIDValidator,
        'methods': ['GET', 'POST'],
    },
    'demographic': {
        'model': MODEL_MAP['demographic'],
        'processor': PROCESSOR_MAP['demographic'],
        'validator': DemographicsPostValidator,
        'methods': ['GET', 'POST'],
    },
    'match_affirm': {
        'model': MODEL_MAP['match_affirm'],
        'processor': PROCESSOR_MAP['match_affirm'],
        'validator': MatchValidator,
        'methods': ['GET', 'POST'],
    },
    'match_deny': {
        'model': MODEL_MAP['match_deny'],
        'processor': PROCESSOR_MAP['match_deny'],
        'validator': MatchValidator,
        'methods': ['GET', 'POST'],
    },
    'enterprise_group': {
        'model': MODEL_MAP['enterprise_group'],
        'processor': None,
        'validator': None,
        'methods': ['GET'],
    },
    'enterprise_match': {
        'model': MODEL_MAP['enterprise_match'],
        'processor': None,
        'validator': None,
        'methods': ['GET'],
    },
    'batch': {
        'model': MODEL_MAP['batch'],
        'processor': None,
        'validator': None,
        'methods': ['GET'],
    },
    'bulletin': {
        'model': MODEL_MAP['bulletin'],
        'processor': None,
        'validator': None,
        'methods': ['GET'],
    },
    'process': {
        'model': MODEL_MAP['process'],
        'processor': None,
        'validator': None,
        'methods': ['GET'],
    },
    'etl_id_source': {
        'model': MODEL_MAP['etl_id_source'],
        'processor': None,
        'validator': None,
        'methods': ['GET'],
    },
    'query_records': {
        'model': None,
        'processor': PROCESSOR_MAP['query_records'],
        'validator': None,
        'methods': ['GET'],
    },
}
