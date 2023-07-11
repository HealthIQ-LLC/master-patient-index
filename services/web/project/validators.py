from incoming import datatypes, PayloadValidator

# these objects provide API payload validation


class RecordIDValidator(PayloadValidator):
    record_id = datatypes.Integer(required=True)
    touched_by = datatypes.String(required=True)


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
    touched_by = datatypes.String(required=True)


class DemographicsPostValidator(PayloadValidator):
    demographics = datatypes.Array(required=True)
    touched_by = datatypes.String(required=True)


class DeleteActionValidator(PayloadValidator):
    batch_id = datatypes.Integer(required=True)
    proc_id = datatypes.Integer(required=True)
    action = datatypes.String(required=True)
    touched_by = datatypes.String(required=True)


class MatchValidator(PayloadValidator):
    record_id_low = datatypes.Integer(required=True)
    record_id_high = datatypes.Integer(required=True)
    touched_by = datatypes.String(required=True)


class CrossWalkValidator(PayloadValidator):
    crosswalk_id = datatypes.Integer(required=False)
    crosswalk_name = datatypes.String(required=False)
    key_name = datatypes.String(required=False)
    is_active = datatypes.Boolean(required=False)
    touched_by = datatypes.String(required=True)


class CrossWalkBindValidator(PayloadValidator):
    bind_id = datatypes.Integer(required=False)
    crosswalk_id = datatypes.Integer(required=False)
    batch_id = datatypes.Integer(required=False)
    is_active = datatypes.Boolean(required=False)
    touched_by = datatypes.String(required=True)
