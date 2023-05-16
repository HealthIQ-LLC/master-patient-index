from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy_serializer import SerializerMixin

from .app import app
db = SQLAlchemy(app)


def key_gen(user: str, version: str) -> int:
    """
    :param user: the username issuing any command is stored in the etl id source table
    :param version: the EMPI software version employed by the command at the time
    :return etl_id: the unique ID created for this job
    """
    with app.app_context():
        staged_key_record = {
            "id_created_ts": datetime.now(),
            "user": user,
            "version": version
        }
        etl_id_source_record = ETLIDSource(**staged_key_record)
        db.session.add(etl_id_source_record)
        db.session.commit()
        db.session.refresh(etl_id_source_record)
        etl_id = etl_id_source_record.etl_id

    return etl_id


class Delete(db.Model, SerializerMixin):  # the record of requests to delete an action
    __tablename__ = "delete_action"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    batch_action = db.Column(db.Text, nullable=False)
    archive_proc_id = db.Column(db.BigInteger)
    archive_batch_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


class Demographic(db.Model, SerializerMixin):  # the record of patient demographic facts
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
    transaction_key = db.Column(db.Text)
    source_key = db.Column(db.Text)
    source_value = db.Column(db.Text)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


class DemographicActivation(db.Model, SerializerMixin):  # the record of requests to activate a demographic record
    __tablename__ = "activate_demographic"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


class DemographicArchive(db.Model, SerializerMixin):  # the record of requests to archive a demographic record
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
    transaction_key = db.Column(db.Text)
    source_key = db.Column(db.Text)
    source_value = db.Column(db.Text)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


class DemographicDeactivation(db.Model, SerializerMixin):  # the record of requests to deactivate a demographic record
    __tablename__ = "deactivate_demographic"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


class DemographicDelete(db.Model, SerializerMixin):  # the record of requests to delete a demographic record
    __tablename__ = "delete_demographic"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)


class Batch(db.Model, SerializerMixin):  # the record of API requests
    __tablename__ = "batch"
    batch_id = db.Column(db.BigInteger, primary_key=True)
    batch_action = db.Column(db.Text, nullable=False)
    batch_status = db.Column(db.Text, nullable=False)


class Bulletin(db.Model, SerializerMixin):  # the record of patient graph changes
    __tablename__ = "bulletin"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    batch_id = db.Column(db.BigInteger)
    proc_id = db.Column(db.BigInteger)
    record_id = db.Column(db.BigInteger)
    empi_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)
    bulletin_ts = db.Column(db.DateTime)


class Process(db.Model, SerializerMixin):  # the record of processes spawned by API requests
    __tablename__ = "process"
    proc_id = db.Column(db.BigInteger, primary_key=True)
    batch_id = db.Column(db.BigInteger)
    transaction_key = db.Column(db.Text, index=True)
    proc_record_id = db.Column(db.BigInteger)
    proc_status = db.Column(db.Text, nullable=False)
    row = db.Column(db.BigInteger)
    foreign_record_id = db.Column(db.Text)


class ETLIDSource(db.Model, SerializerMixin):  # the source table for all primary keys, preserving request meta-data
    __tablename__ = "etl_id_source"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    user = db.Column(db.Text)
    version = db.Column(db.Text)
    id_created_ts = db.Column(db.DateTime)


class MatchAffirmation(db.Model, SerializerMixin):  # the record of match affirmation activities
    __tablename__ = "match_affirm"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id_low = db.Column(db.BigInteger, index=True)
    record_id_high = db.Column(db.BigInteger, index=True)
    transaction_key = db.Column(db.Text, index=True)


class MatchDenial(db.Model, SerializerMixin):  # the record of match denial activities
    __tablename__ = "match_deny"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id_low = db.Column(db.BigInteger, index=True)
    record_id_high = db.Column(db.BigInteger, index=True)
    transaction_key = db.Column(db.Text, index=True)


class Telecom(db.Model, SerializerMixin):  # the record of telecoms facts
    __tablename__ = "telecom"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id = db.Column(db.BigInteger)
    telecoms_type = db.Column(db.Text)
    telecoms_subtype = db.Column(db.Text)
    telecoms_value = db.Column(db.Text)
    transaction_key = db.Column(db.Text)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


class EnterpriseGroup(db.Model, SerializerMixin):  # the record of pairwise assignment btwn record and EMPI ID
    __tablename__ = "enterprise_group"
    etl_id = db.Column(db.BigInteger, primary_key=True)
    enterprise_id = db.Column(db.BigInteger)
    record_id = db.Column(db.BigInteger, unique=True, index=True)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


class EnterpriseMatch(db.Model, SerializerMixin):  # the record of pairwise match between two demographic records
    __tablename__ = "enterprise_match"
    __table_args__ = (
        db.UniqueConstraint('record_id_low', 'record_id_high', name='matched_pair_constraint'),
    )
    etl_id = db.Column(db.BigInteger, primary_key=True)
    record_id_low = db.Column(db.BigInteger, index=True)
    record_id_high = db.Column(db.BigInteger, index=True)
    match_weight = db.Column(db.Float)
    is_valid = db.Column(db.Boolean)
    transaction_key = db.Column(db.Text, index=True)
    touched_by = db.Column(db.Text)
    touched_ts = db.Column(db.DateTime)


MODEL_MAP = {
    "delete_action": Delete,
    "demographic": Demographic,
    "activate_demographic": DemographicActivation,
    "archive_demographic": DemographicArchive,
    "deactivate_demographic": DemographicDeactivation,
    "delete_demographic": DemographicDelete,
    "enterprise_group": EnterpriseGroup,
    "enterprise_match": EnterpriseMatch,
    "batch": Batch,
    "bulletin": Bulletin,
    "process": Process,
    "match_affirm": MatchAffirmation,
    "match_deny": MatchDenial,
    "telecom": Telecom,
    "etl_id_source": ETLIDSource
}
