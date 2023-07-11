from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy_serializer import SerializerMixin

from .app import app
db = SQLAlchemy(app)


def key_gen(user: str, version: str) -> int:
    """
    :param user: the username issuing the command
    :param version: the software version employed at the time
    :return etl_id: the unique ID created for this job
    A few truths:
      (1) the patient network can only be adjusted one record at a time, step-wise
      (2) we are serializing requests, records, transactions, and patients
        (and soon, crosswalk IDs)
      (3) it actually makes sense to do this all along one number-line
    This function is employed wherever a new record is staged for insertion
    User, version, and timestamp metadata are inserted into the ETLIDSource,
    this insertion provides the auto-incremented ID which is what's returned.
    """
    with app.app_context():
        staged_key_record = {
            "id_created_ts": datetime.now(),
            "user": user,
            "version": version
        }
        etl_id_source_record = ETLIDSource(**staged_key_record)  # type: ignore
        db.session.add(etl_id_source_record)
        db.session.commit()
        db.session.refresh(etl_id_source_record)
        etl_id = etl_id_source_record.etl_id

    return etl_id


# the record of requests to delete an action
class Delete(db.Model, SerializerMixin):  
    __tablename__ = "delete_action"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    batch_action = db.Column(db.Text, nullable=False)
    archive_proc_id = db.Column(db.BigInteger)
    archive_batch_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


# the record of patient demographic facts
class Demographic(db.Model, SerializerMixin):  
    __tablename__ = "demographic"
    record_id = db.Column(db.BigInteger, primary_key=True)
    organization_key = db.Column(db.Text)
    system_key = db.Column(db.Text)
    system_id = db.Column(db.Text)
    given_name = db.Column(db.Text)
    middle_name = db.Column(db.Text)
    family_name = db.Column(db.Text)
    gender = db.Column(db.Text)
    name_day = db.Column(db.DateTime)
    address_1 = db.Column(db.Text)
    address_2 = db.Column(db.Text)
    city = db.Column(db.Text)
    state = db.Column(db.Text)
    postal_code = db.Column(db.Text)
    social_security_number = db.Column(db.Text)
    uq_hash = db.Column(db.Text, unique=True)
    composite_key = db.Column(db.Text)
    composite_name = db.Column(db.Text)
    composite_name_day_postal_code = db.Column(db.Text)
    is_active = db.Column(db.Boolean)
    transaction_key = db.Column(db.Text, index=True)
    source_key = db.Column(db.Text)
    source_value = db.Column(db.Text)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


# the record of requests to activate a demographic record
class DemographicActivation(db.Model, SerializerMixin):  
    __tablename__ = "activate_demographic"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


# the record of requests to archive a demographic record
class DemographicArchive(db.Model, SerializerMixin):  
    __tablename__ = "archive_demographic"
    record_id = db.Column(db.BigInteger, primary_key=True)
    organization_key = db.Column(db.Text)
    system_key = db.Column(db.Text)
    system_id = db.Column(db.Text)
    given_name = db.Column(db.Text)
    middle_name = db.Column(db.Text)
    family_name = db.Column(db.Text)
    gender = db.Column(db.Text)
    name_day = db.Column(db.DateTime)
    address_1 = db.Column(db.Text)
    address_2 = db.Column(db.Text)
    city = db.Column(db.Text)
    state = db.Column(db.Text)
    postal_code = db.Column(db.Text)
    social_security_number = db.Column(db.Text)
    uq_hash = db.Column(db.Text, unique=True)
    composite_key = db.Column(db.Text)
    composite_name = db.Column(db.Text)
    composite_name_day_postal_code = db.Column(db.Text)
    is_active = db.Column(db.Boolean)
    archive_transaction_key = db.Column(db.Text)
    transaction_key = db.Column(db.Text, index=True)
    source_key = db.Column(db.Text)
    source_value = db.Column(db.Text)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


# the record of requests to deactivate a demographic record
class DemographicDeactivation(db.Model, SerializerMixin):  
    __tablename__ = "deactivate_demographic"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


# the record of requests to delete a demographic record
class DemographicDelete(db.Model, SerializerMixin):  
    __tablename__ = "delete_demographic"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


# the record of API requests
class Batch(db.Model, SerializerMixin):  
    __tablename__ = "batch"
    batch_id = db.Column(db.BigInteger, primary_key=True)
    batch_action = db.Column(db.Text, nullable=False)
    batch_status = db.Column(db.Text, nullable=False)


# the record of patient graph changes
class Bulletin(db.Model, SerializerMixin):  
    __tablename__ = "bulletin"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    batch_id = db.Column(db.BigInteger)
    proc_id = db.Column(db.BigInteger)
    record_id = db.Column(db.BigInteger)
    empi_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)
    bulletin_ts = db.Column(db.DateTime)


# the record of processes spawned by API requests
class Process(db.Model, SerializerMixin):  
    __tablename__ = "process"
    proc_id = db.Column(db.BigInteger, primary_key=True)
    batch_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)
    proc_record_id = db.Column(db.BigInteger)
    proc_status = db.Column(db.Text, nullable=False)
    row = db.Column(db.BigInteger)
    foreign_record_id = db.Column(db.Text)


# the source table for all primary keys, preserving request meta-data
class ETLIDSource(db.Model, SerializerMixin):  
    __tablename__ = "etl_id_source"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    user = db.Column(db.Text)
    version = db.Column(db.Text)
    id_created_ts = db.Column(db.DateTime)


# the record of match affirmation activities
class MatchAffirmation(db.Model, SerializerMixin):  
    __tablename__ = "match_affirm"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id_low = db.Column(db.BigInteger, index=True)
    record_id_high = db.Column(db.BigInteger, index=True)
    transaction_key = db.Column(db.Text, index=True)


# the record of match denial activities
class MatchDenial(db.Model, SerializerMixin):  
    __tablename__ = "match_deny"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id_low = db.Column(db.BigInteger, index=True)
    record_id_high = db.Column(db.BigInteger, index=True)
    transaction_key = db.Column(db.Text, index=True)


# the record of telecoms facts
class Telecom(db.Model, SerializerMixin):  
    __tablename__ = "telecom"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    telecoms_type = db.Column(db.Text)
    telecoms_subtype = db.Column(db.Text)
    telecoms_value = db.Column(db.Text)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


# the record of pairwise assignment btwn record and EMPI ID
class EnterpriseGroup(db.Model, SerializerMixin):  
    __tablename__ = "enterprise_group"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    enterprise_id = db.Column(db.BigInteger)
    record_id = db.Column(db.BigInteger, unique=True, index=True)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


# the record of pairwise match between two demographic records
class EnterpriseMatch(db.Model, SerializerMixin):  
    __tablename__ = "enterprise_match"
    __table_args__ = (
        db.UniqueConstraint(
            'record_id_low', 
            'record_id_high', 
            name='matched_pair_constraint'
        ),
    )
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id_low = db.Column(db.BigInteger, index=True)
    record_id_high = db.Column(db.BigInteger, index=True)
    match_weight = db.Column(db.Float)
    is_valid = db.Column(db.Boolean)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


# the record of a foreign key-system brought in with demographic records
class Crosswalk(db.Model, SerializerMixin):
    __tablename__ = "crosswalk"
    __table_args__ = (
        db.UniqueConstraint(
            'crosswalk_name',
            'key_name',
            name='crosswalk_key_constraint'
        ),
    )
    crosswalk_id = db.Column(db.BigInteger, primary_key=True)
    crosswalk_name = db.Column(db.Text)
    key_name = db.Column(db.Text)
    is_active = db.Column(db.Boolean)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


class CrosswalkBind(db.Model, SerializerMixin):
    __tablename__ = "crosswalk_bind"
    bind_id = db.Column(db.BigInteger, primary_key=True)
    crosswalk_id = db.Column(db.BigInteger)
    batch_id = db.Column(db.BigInteger)
    is_active = db.Column(db.Boolean)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


# Data-model dependencies are shipped with this map
MODEL_MAP = {
    "activate_demographic": DemographicActivation,
    "archive_demographic": DemographicArchive,
    "batch": Batch,
    "bulletin": Bulletin,
    "crosswalk": Crosswalk,
    "crosswalk_bind": CrosswalkBind,
    "deactivate_demographic": DemographicDeactivation,
    "delete_action": Delete,
    "delete_demographic": DemographicDelete,
    "demographic": Demographic,
    "enterprise_group": EnterpriseGroup,
    "enterprise_match": EnterpriseMatch,
    "etl_id_source": ETLIDSource,
    "match_affirm": MatchAffirmation,
    "match_deny": MatchDenial,
    "process": Process,
    "telecom": Telecom,
}
