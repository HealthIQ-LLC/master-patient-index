from datetime import datetime
from .app import app
from .model import (
    Crosswalk,
    CrosswalkBind,
    db,
    key_gen
)
from .processor import mint_transaction_key, transact_records


def add_crosswalk(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed
    :param auditor: native Auditor class object for data warehousing
    :return crosswalk_id: the  ID for the added record
    This processor is accessed when a Crosswalk (e.g. "Cognito") is added
    """
    with app.app_context():
        crosswalk_name = payload.get("crosswalk_name")
        key_name = payload.get("key_name")
        transaction_key, _, _, user, ts = mint_transaction_key(auditor)
        staged_crosswalk = {
            "crosswalk_id": key_gen(user, auditor.version),
            "crosswalk_name": crosswalk_name,
            "key_name": key_name,
            "transaction_key": transaction_key,
            "is_active": True,
            'touched_by': user,
            'touched_ts': ts
        }
        crosswalk_record = Crosswalk(**staged_crosswalk)  # type: ignore

    return transact_records(crosswalk_record, "crosswalk")


def deactivate_crosswalk(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed
    :param auditor: native Auditor class object for data warehousing
    This processor is accessed when a Crosswalk (e.g. "Cognito") is deactivated
    """
    with app.app_context():
        crosswalk_id = payload.get("crosswalk_id")
        db.session.query(Crosswalk). \
            filter(Crosswalk.instruction_id == crosswalk_id,
                   Crosswalk.is_active is True). \
            update(
            {
                Crosswalk.is_active: False,
                Crosswalk.touched_by: auditor.user,
                Crosswalk.touched_ts: datetime.now()
            },
            synchronize_session=False
        )
        db.session.commit()


def activate_crosswalk(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed
    :param auditor: native Auditor class object for data warehousing
    This processor is accessed when a Crosswalk (e.g. "Cognito") is activated
    """
    with app.app_context():
        crosswalk_id = payload.get("crosswalk_id")
        db.session.query(Crosswalk). \
            filter(Crosswalk.instruction_id == crosswalk_id,
                   Crosswalk.is_active is False). \
            update(
            {
                Crosswalk.is_active: True,
                Crosswalk.touched_by: auditor.user,
                Crosswalk.touched_ts: datetime.now()
            },
            synchronize_session=False
        )
        db.session.commit()


def add_crosswalk_bind(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed
    :param auditor: native Auditor class object for data warehousing
    :return bind_id: the  ID for the added record
    This processor is accessed to bind a Batch to a Crosswalk, in effect
    saying that the "foreign key" for each demographic in a given Batch is in a
    given serialization system defined in the Crosswalk. eg "I am uploading Athena records,
    and these are their AthenaIDs going into our foreign key store"
    """
    with app.app_context():
        crosswalk_id = payload.get("crosswalk_id")
        batch_id = payload.get("crosswalk_id")
        transaction_key, _, _, user, ts = mint_transaction_key(auditor)
        staged_crosswalk_instruction = {
            "bind_id": key_gen(user, auditor.version),
            "crosswalk_id": crosswalk_id,
            "batch_id": batch_id,
            "transaction_key": transaction_key,
            "is_active": True,
            'touched_by': user,
            'touched_ts': ts
        }
        crosswalk_instruction_record = \
            CrosswalkInstruction(**staged_crosswalk_instruction)  # type: ignore

        return transact_records(crosswalk_instruction_record, "crosswalk_bind")


def deactivate_crosswalk_bind(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed
    :param auditor: native Auditor class object for data warehousing
    This processor is accessed when a bind is deactivated
    """
    with app.app_context():
        instruction_id = payload.get("instruction_id")
        db.session.query(CrosswalkBind). \
            filter(CrosswalkBind.instruction_id == instruction_id,
                   CrosswalkBind.is_active is True). \
            update(
            {
                CrosswalkBind.is_active: False,
                CrosswalkBind.touched_by: auditor.user,
                CrosswalkBind.touched_ts: datetime.now()
            },
            synchronize_session=False
        )
        db.session.commit()


def activate_crosswalk_bind(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed
    :param auditor: native Auditor class object for data warehousing
    This processor is accessed when a bind is activated
    """
    with app.app_context():
        instruction_id = payload.get("instruction_id")
        db.session.query(CrosswalkBind). \
            filter(CrosswalkBind.instruction_id == instruction_id,
                   CrosswalkBind.is_active is False). \
            update(
            {
                CrosswalkBind.is_active: True,
                CrosswalkBind.touched_by: auditor.user,
                CrosswalkBind.touched_ts: datetime.now()
            },
            synchronize_session=False
        )
        db.session.commit()


# the crosswalk-dependencies are shipped with this map
CROSSWALK_MAP = {
    "add_crosswalk": add_crosswalk,
    "activate_crosswalk": activate_crosswalk,
    "deactivate_crosswalk": deactivate_crosswalk,
    "add_crosswalk_bind": add_crosswalk_bind,
    "activate_crosswalk_bind": activate_crosswalk_bind,
    "deactivate_crosswalk_bind": deactivate_crosswalk_bind,
}
