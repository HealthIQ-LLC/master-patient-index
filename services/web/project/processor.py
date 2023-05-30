from datetime import datetime

from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
import sys
from time import sleep

from .app import app
from .data_utils import apply_record_metadata
from .engine import compute_all_matches
from .graphing import GraphCursor, GraphReCursor
from .logger import version
from .model import (
    db,
    key_gen,
    Batch,
    Delete,
    Demographic,
    DemographicActivation,
    DemographicArchive,
    DemographicDeactivation,
    DemographicDelete,
    EnterpriseMatch,
    EnterpriseGroup,
    Process,
    MatchAffirmation,
    MatchDenial,
    MODEL_MAP
)


def mint_transaction_key(auditor, row=None, foreign_record_id=None):
    """
    :param auditor: native Auditor class object for data warehousing
    :param row: used when counting rows in tables
    :param foreign_record_id: the primary key from demographic record origin
    """
    ts = datetime.now()
    proc_id = auditor.stamp(row, foreign_record_id)
    transaction_key = auditor.stamp.transaction_key

    return transaction_key, proc_id, auditor.batch_id, auditor.user, ts


def transact_records(record, table: str) -> int:
    """
    :param record: a sqla data object for insertion into an appropriate table
    :param table: a string identifying a table
    """
    with app.app_context():
        db.session.add(record)
        db.session.commit()
        db.session.refresh(record)
        if table in ("demographic", "archive_demographic"):
            record_id = record.record_id
        elif table == "batch":
            record_id = record.batch_id
        elif table == "process":
            record_id = record.proc_id
        else:
            record_id = record.etl_id

    return record_id


def query_records(payload: dict, endpoint="demographic") -> list:
    """
    :param payload: a list of key/value constraints to use in filtering
    :param table: a string mapped to the sqla data model of tables
    """
    response = list()
    source_table = MODEL_MAP[endpoint]
    query = source_table.query
    try:
        del payload['user']
    except:
        pass
    for field_name, field_val in payload.items():
        query = query.filter(source_table.__table__.c[field_name] == field_val)
    for row in query.all():
        response.append(row.to_dict())

    return response


def update_status(batch_id, proc_id, message):
    """
    This function is called to log the conclusion of each process in the Process table
    """
    with app.app_context():
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_status: message}, synchronize_session=False)
        db.session.commit()
        batch_check_query = db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_status == 'PENDING')
        if len(batch_check_query.all()) == 0:
            db.session.query(Batch).filter(Batch.batch_id == batch_id).\
                update({Batch.batch_status: "COMPUTED"}, synchronize_session=False)
            db.session.commit()


def demographic(payload: dict, auditor) -> dict:
    """
    :param payload: a list of json/dict-like records to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    metrics = {
        "affected_records": [],
        "bulletin_ids": [],
        "error_count": 0,
        "error_rows": [],
        "proc_ids": [],
        "pending_count": 0,
        "record_count": 0,
        "skipped_count": 0,
        "telecoms_count": 0,
    }
    row = 1
    for record in payload.get('demographics'):
        foreign_record_id = record.get("foreign_record_id")
        transaction_key, proc_id, batch_id, user, _ = mint_transaction_key(
        	auditor,
        	row=row,
        	foreign_record_id=foreign_record_id
        	)
        name_day_input = record.get('name_day', None)
        if type(name_day_input) is str:
            name_day_format = '%Y%m%d'
            name_day_datetime = datetime.strptime(name_day_input, name_day_format)
        elif type(name_day_input) is datetime:
            name_day_datetime = name_day_input
        try:
            staged_record = {
                "record_id": key_gen(user, version),
                "address_1": record.get("address_1", None),
                "address_2": record.get("address_2", None),
                "city": record.get("city", None),
                "family_name": record.get("family_name", None),
                "gender": record.get("gender", None),
                "given_name": record.get("given_name", None),
                "middle_name": record.get("address_1", None),
                "name_day":  name_day_datetime,
                "organization_key": record.get("organization_key", None),
                "postal_code": record.get("postal_code", None),
                "social_security_number": record.get("social_security_number", None),
                "state": record.get("state", None),
                "system_key": record.get("system_key", None),
                "system_id": record.get("system_id", None),
                "transaction_key": transaction_key
            }
        except KeyError:
            metrics["error_count"] += 1
            metrics["error_rows"].append(row)
            staged_record = None
        if staged_record is not None:
            record, _ = apply_record_metadata(staged_record, user)
            metrics["record_count"] += 1
            record_id = None
            with app.app_context():
                try:
                    demographics_record = Demographic(**record)
                    record_id = transact_records(demographics_record, "demographic")
                    metrics["proc_ids"].append(proc_id)
                    metrics["affected_records"].append((batch_id, proc_id, record_id, transaction_key))
                    metrics["pending_count"] += 1
                except IntegrityError:
                    db.session.rollback()
                    metrics["skipped_count"] += 1
                except Exception as error_msg:
                    print(error_msg, file=sys.stderr)
                if record_id is not None:
                    db.session.query(Process). \
                        filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
                        update({Process.proc_record_id: record_id}, synchronize_session=False)
                    db.session.commit()
                    update_status(batch_id, proc_id, "POSTED")
                    activate_demographic({"record_id": record_id}, auditor)
        row += 1

    return metrics


def activate_demographic(payload: dict, auditor) -> int:
    """
    :param payload: a dict representing a json/dict-like record to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    with app.app_context():
        transaction_key, proc_id, batch_id, user, touched_ts = mint_transaction_key(auditor)
        record_id = payload.get("record_id")
        db.session.query(Demographic).\
            filter(Demographic.record_id == record_id).\
            update(
            {
                Demographic.is_active: True,
                Demographic.touched_by: user,
                Demographic.touched_ts: touched_ts
            },
            synchronize_session=False
        )
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_record_id: record_id}, synchronize_session=False)
        db.session.commit()
        db.session.query(EnterpriseMatch).\
            filter(EnterpriseMatch.is_valid is False,
                   or_(
                       EnterpriseMatch.record_id_low == record_id,
                       EnterpriseMatch.record_id_high == record_id,
                   )
                   ).\
            update(
            {
                EnterpriseMatch.is_valid: True,
                EnterpriseMatch.touched_by: user,
                EnterpriseMatch.touched_ts: touched_ts
            },
            synchronize_session=False
        )
        db.session.commit()
        record = db.session.query(Demographic).filter(Demographic.record_id == record_id).first()
        computed_matches, _ = compute_all_matches(record)
        nodes_and_weights = list()
        for computed_match in computed_matches:
            tup = (
                computed_match['record_a_id'],
                computed_match['record_b_id'],
                computed_match['score']
            )
            nodes_and_weights.append(tup)
        graph = GraphCursor(nodes_and_weights, batch_id, proc_id)
        graph()
        update_status(batch_id, proc_id, "ACTIVATED")
        staged_demo_activate_record = {
            "etl_id": key_gen(user, version),
            "record_id": record_id,
            "transaction_key": transaction_key,
        }
        demo_act_record = DemographicActivation(**staged_demo_activate_record)
        transact_records(demo_act_record, "activations")

    return graph.enterprise_id


def archive_demographic(record_id: int, auditor):
    """
    :param record_id: the record ID of the targeted Demographic record
    :param auditor: native Auditor class object for data warehousing
    """
    with app.app_context():
        transaction_key, proc_id, batch_id, user, touched_ts = mint_transaction_key(auditor)
        record_to_archive = db.session.query(Demographic).\
            filter(Demographic.record_id == record_id).first()
        record_to_archive.archive_transaction_key = record_to_archive.transaction_key
        record_to_archive.transaction_key = transaction_key
        record_to_archive.touched_by = user
        record_to_archive.touched_ts = touched_ts
        record_to_archive = record_to_archive.__dict__
        del record_to_archive['_sa_instance_state']
        archive_record = DemographicArchive(**record_to_archive)
        archive_id = transact_records(archive_record, "archive_demographic")
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_record_id: archive_id}, synchronize_session=False)
        db.session.commit()
        update_status(batch_id, proc_id, "ARCHIVED")

    return archive_id


def deactivate_demographic(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    
    with app.app_context():
        transaction_key, proc_id, batch_id, user, touched_ts = mint_transaction_key(auditor)
        record_id = payload.get("record_id")
        recursor = GraphReCursor(record_id)
        print(f'deac nodes and weights 1 {recursor.nodes_and_weights}', file=sys.stderr)
        db.session.query(Demographic).\
            filter(Demographic.record_id == record_id).\
            update(
            {
                Demographic.is_active: False,
                Demographic.touched_by: user,
                Demographic.touched_ts: touched_ts
            },
            synchronize_session=False
        )
        db.session.commit()
        db.session.query(EnterpriseMatch). \
            filter(
            or_(
                EnterpriseMatch.record_id_low == record_id,
                EnterpriseMatch.record_id_high == record_id,
                )
            ).\
            update(
            {
                EnterpriseMatch.is_valid: False,
                EnterpriseMatch.touched_by: user,
                EnterpriseMatch.touched_ts: touched_ts
            },
            synchronize_session=False
        )
        db.session.commit()
        #ToDo: update in case where rm is the enterprise id item
        #results = EnterpriseGroup.query.filter_by(enterprise_id=record_id).all()
        EnterpriseGroup.query.filter_by(record_id=record_id).delete()
        EnterpriseGroup.query.filter_by(enterprise_id=record_id).delete()
        db.session.commit()
        for matched_record in recursor.matched_records:
            print(matched_record, file=sys.stderr)
            #if matched_record != record_id:
            inner_recursor = GraphReCursor(matched_record)
            print(f'deac nodes and weights 2 {inner_recursor.nodes_and_weights}', file=sys.stderr)
            graph = GraphCursor(inner_recursor.nodes_and_weights, batch_id, proc_id)
            graph()
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_record_id: record_id}, synchronize_session=False)
        db.session.commit()
        EnterpriseMatch.query.filter_by(is_valid=False).delete()
        db.session.commit()
        update_status(batch_id, proc_id, "DEACTIVATED")
        staged_demo_deac_record = {
            "etl_id": key_gen(user, version),
            "record_id": record_id,
            "transaction_key": transaction_key,
        }
        demo_deac_record = DemographicDeactivation(**staged_demo_deac_record)

    return transact_records(demo_deac_record, "deactivations")


def delete_demographic(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    with app.app_context():
        transaction_key, proc_id, batch_id, user, _ = mint_transaction_key(auditor)
        record_id = payload["record_id"]
        deactivate_demographic({"record_id": record_id}, auditor)
        archive_demographic(record_id, auditor)
        Demographic.query.filter_by(record_id=record_id).delete()
        db.session.commit()
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_record_id: record_id}, synchronize_session=False)
        db.session.commit()
        update_status(batch_id, proc_id, "DELETED DEMOGRAPHIC")
        staged_demo_delete_record = {
            "etl_id": key_gen(user, version),
            "record_id": record_id,
            "transaction_key": transaction_key,
        }
        demo_delete_record = DemographicDelete(**staged_demo_delete_record)

    return transact_records(demo_delete_record, "demo_deletes")


def delete_action(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    with app.app_context():
        transaction_key, proc_id, batch_id, user, touched_ts = mint_transaction_key(auditor)
        payload_batch_id = payload['batch_id']
        payload_proc_id = payload['proc_id']
        select_transaction_key = f'{payload_batch_id}_{payload_proc_id}'
        action = payload['action']
        if action == 'delete':
            delete_record = DemographicDelete.query.filter_by(transaction_key=select_transaction_key).first()
            record_id = delete_record.record_id
            demographic_record = DemographicArchive.query.filter_by(record_id=record_id).first().__dict__
            del demographic_record['archive_transaction_key']
            demographic_record["transaction_key"] = transaction_key
            demographic_record["touched_by"] = user
            demographic_record["touched_ts"] = touched_ts
            demo_payload = dict()
            demo_payload['demographics'] = [demographic_record]
            demographic(demo_payload, auditor)
            DemographicArchive.query.filter_by(record_id=record_id).delete()
            active_payload = {'record_id': record_id}
            db.session.query(Process). \
                filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
                update({Process.proc_record_id: record_id}, synchronize_session=False)
            db.session.commit()
        else:
            record_id_low = record_id_high = None
            if action == 'affirm':
                affirm_record = MatchAffirmation.query.filter_by(transaction_key=select_transaction_key).first()
                record_id_low = affirm_record.record_id_low
                record_id_high = affirm_record.record_id_high
                del_affirm_payload = {'record_id_low': record_id_low, 'record_id_high': record_id_high}
                deny_matching(del_affirm_payload, auditor)
            elif action == 'deny':
                deny_record = MatchDenial.query.filter_by(transaction_key=select_transaction_key).first()
                record_id_low = deny_record.record_id_low
                record_id_high = deny_record.record_id_high
                del_deny_payload = {'record_id_low': record_id_low, 'record_id_high': record_id_high}
                affirm_matching(del_deny_payload, auditor)
            recursor_low = GraphReCursor(record_id_low)
            recursor_high = GraphReCursor(record_id_high)
            already_matched = list()
            for matched_record in recursor_low.matched_records:
                if matched_record not in already_matched:
                    recursor = GraphReCursor(matched_record)
                    graph = GraphCursor(recursor.nodes_and_weights, batch_id, proc_id)
                    graph()
                    already_matched.append(matched_record)
            for matched_record in recursor_high.matched_records:
                if matched_record not in already_matched:
                    recursor = GraphReCursor(matched_record)
                    graph = GraphCursor(recursor.nodes_and_weights, batch_id, proc_id)
                    graph()
                    already_matched.append(matched_record)
        update_status(batch_id, proc_id, f"DELETED {action}")
        staged_record = {
            "etl_id": key_gen(user, version),
            "batch_action": action,
            "archive_proc_id": payload_proc_id,
            "archive_batch_id": payload_batch_id,
            "transaction_key": transaction_key,
        }
        delete_action_record = Delete(**staged_record)

    return transact_records(delete_action_record, "delete")


def affirm_matching(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    with app.app_context():
        transaction_key, proc_id, batch_id, user, touched_ts = mint_transaction_key(auditor)
        record_id_low = payload.get("record_id_low")
        record_id_high = payload.get("record_id_high")
        record = db.session.query(EnterpriseMatch). \
            filter(
            EnterpriseMatch.record_id_low == record_id_low,
            EnterpriseMatch.record_id_high == record_id_high
            ).first()
        etl_id = record.etl_id
        weight = record.match_weight
        weight += 1
        db.session.query(EnterpriseMatch). \
            filter(EnterpriseMatch.etl_id == etl_id). \
            update(
            {
                EnterpriseMatch.match_weight: weight,
                EnterpriseMatch.touched_by: user,
                EnterpriseMatch.touched_ts: touched_ts
            },
            synchronize_session=False
        )
        db.session.commit()
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_record_id: etl_id}, synchronize_session=False)
        db.session.commit()
        recursor_low = GraphReCursor(record_id_low)
        recursor_high = GraphReCursor(record_id_high)
        already_matched = list()
        for matched_record in recursor_low.matched_records:
            if matched_record not in already_matched:
                recursor = GraphReCursor(matched_record)
                graph = GraphCursor(recursor.nodes_and_weights, batch_id, proc_id)
                graph()
                already_matched.append(matched_record)
        for matched_record in recursor_high.matched_records:
            if matched_record not in already_matched:
                recursor = GraphReCursor(matched_record)
                graph = GraphCursor(recursor.nodes_and_weights, batch_id, proc_id)
                graph()
                already_matched.append(matched_record)
        update_status(batch_id, proc_id, "AFFIRMED")
        staged_record = {
            "etl_id": key_gen(user, version),
            "record_id_low": record_id_low,
            "record_id_high": record_id_high,
            "transaction_key": transaction_key,
        }
        affirmation_record = MatchAffirmation(**staged_record)

    return transact_records(affirmation_record, "affirm")


def deny_matching(payload: dict, auditor):
    """
    :param payload: a dict representing a json/dict-like record to be computed by EMPI
    :param auditor: native Auditor class object for data warehousing
    """
    with app.app_context():
        transaction_key, proc_id, batch_id, user, touched_ts = mint_transaction_key(auditor)
        record_id_low = payload.get("record_id_low")
        record_id_high = payload.get("record_id_high")
        record = db.session.query(EnterpriseMatch). \
            filter(
            EnterpriseMatch.record_id_low == record_id_low,
            EnterpriseMatch.record_id_high == record_id_high
        ).first()
        etl_id = record.etl_id
        weight = record.match_weight
        weight -= 1
        db.session.query(EnterpriseMatch).\
            filter(EnterpriseMatch.etl_id == etl_id).\
            update(
            {
                "match_weight": weight,
                "touched_by": user,
                "touched_ts": touched_ts
            }
        )
        db.session.commit()
        db.session.query(Process). \
            filter(Process.batch_id == batch_id, Process.proc_id == proc_id). \
            update({Process.proc_record_id: etl_id}, synchronize_session=False)
        db.session.commit()
        recursor_low = GraphReCursor(record_id_low)
        recursor_high = GraphReCursor(record_id_high)
        already_matched = list()
        for matched_record in recursor_low.matched_records:
            if matched_record not in already_matched:
                print(f"{recursor.nodes_and_weights} low", file=sys.stderr)
                recursor = GraphReCursor(matched_record)
                graph = GraphCursor(recursor.nodes_and_weights, batch_id, proc_id)
                graph()
                already_matched.append(matched_record)
        for matched_record in recursor_high.matched_records:
            if matched_record not in already_matched:
                print(f"{recursor.nodes_and_weights} high", file=sys.stderr)
                recursor = GraphReCursor(matched_record)
                graph = GraphCursor(recursor.nodes_and_weights, batch_id, proc_id)
                graph()
                already_matched.append(matched_record)
        update_status(batch_id, proc_id, "DENIED")
        staged_record = {
            "etl_id": key_gen(user, version),
            "record_id_low": record_id_low,
            "record_id_high": record_id_high,
            "transaction_key": transaction_key,
        }
        denial_record = MatchDenial(**staged_record)

    return transact_records(denial_record, "deny")

PROCESSOR_MAP = {
    "delete_action": delete_action,
    "demographic": demographic,
    "activate_demographic": activate_demographic,
    "deactivate_demographic": deactivate_demographic,
    "delete_demographic": delete_demographic,
    "match_affirm": affirm_matching,
    "match_deny": deny_matching,
    "query_records": query_records
}