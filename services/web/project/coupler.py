from .crosswalk import CROSSWALK_MAP
from .model import MODEL_MAP
from .processor import PROCESSOR_MAP
from .validators import (
    CrossWalkValidator,
    CrossWalkBindValidator,
    DeleteActionValidator,
    DemographicsPostValidator,
    MatchValidator,
    RecordIDValidator
)

# define API endpoints: coupling name, model, processor, validator, and methods

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
    'enterprise_group': {  # ToDo: GET validator for ETL ID
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
    'add_crosswalk': {
        'model': MODEL_MAP['crosswalk'],
        'processor': CROSSWALK_MAP['add_crosswalk'],
        'validator': CrossWalkValidator,
        'methods': ['GET', 'POST'],
    },
    'add_crosswalk_bind': {
        'model': MODEL_MAP['crosswalk_bind'],
        'processor': CROSSWALK_MAP['add_crosswalk_bind'],
        'validator': CrossWalkBindValidator,
        'methods': ['GET', 'POST'],
    },
    'activate_crosswalk': {
        'model': MODEL_MAP['crosswalk'],
        'processor': CROSSWALK_MAP['activate_crosswalk'],
        'validator': CrossWalkValidator,
        'methods': ['GET', 'POST'],
    },
    'deactivate_crosswalk': {
        'model': MODEL_MAP['crosswalk'],
        'processor': CROSSWALK_MAP['deactivate_crosswalk'],
        'validator': CrossWalkValidator,
        'methods': ['GET', 'POST'],
    },
    'activate_crosswalk_bind': {
        'model': MODEL_MAP['crosswalk_bind'],
        'processor': CROSSWALK_MAP['activate_crosswalk_bind'],
        'validator': CrossWalkBindValidator,
        'methods': ['GET', 'POST'],
    },
    'deactivate_crosswalk_bind': {
        'model': MODEL_MAP['crosswalk_bind'],
        'processor': CROSSWALK_MAP['deactivate_crosswalk_bind'],
        'validator': CrossWalkBindValidator,
        'methods': ['GET', 'POST'],
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
