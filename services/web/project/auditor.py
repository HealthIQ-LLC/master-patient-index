from datetime import datetime
import sys

from .app import app
from .logger import mylogger
from .model import (
    db,
    Batch,
    Process,
    key_gen
)


class AuditStamp:
    """
    The AuditStamp manages context at the task level.
    """
    def __init__(self, batch_id, user, version):
        self.batch_id = batch_id
        self.user = user
        self.version = version
        self.proc_id = None
        self.record_id = None
        self.row = None

    def __call__(self, row, foreign_record_id):
        with app.app_context():
            self.proc_id = key_gen(self.user, self.version)
            self.row = row
            self.transaction_key = f"{self.batch_id}_{self.proc_id}"
            staged_proc_record = {
                "batch_id": self.batch_id,
                "proc_id": self.proc_id,
                "proc_status": "PENDING",
                "transaction_key": self.transaction_key,
                "row": self.row,
                "foreign_record_id": foreign_record_id
            }
            proc_record = Process(**staged_proc_record)
            db.session.add(proc_record)
            db.session.commit()

        return self.proc_id

    def __str__(self):
        return f"<AuditStamp: \
{self.batch_id}:{self.proc_id}:{self.record_id}:{self.row}:{self.user}>"


class Auditor:
    """
    The Auditor is a context manager at the batch/api-request level.
    """
    def __init__(self, user, version, action):
        self.user = user
        self.version = version
        self.action = action
        self.batch_id = key_gen(self.user, self.version)
        staged_batch_record = {
            "batch_id": self.batch_id,
            "batch_action": self.action,
            "batch_status": "STARTING"
        }
        batch_record = Batch(**staged_batch_record)
        db.session.add(batch_record)
        db.session.commit()

    def __enter__(self):
        self.stamp = AuditStamp(self.batch_id, self.user, self.version)

        return self

    def __exit__(self, e_type, value, traceback):
        if e_type is not None:
            error_msg = f"{e_type} : {value} : {traceback}"
            mylogger.error(error_msg)
            print(error_msg, file=sys.stderr)
        else:
            # ToDo: wrap QC/exit strategy on activities here
            db.session.query(Batch).filter(Batch.batch_id == self.batch_id).\
                update({Batch.batch_status: "PENDING"}, synchronize_session=False)
            db.session.commit()

        return "ok"

    def __str__(self):
        return f"<Auditor: {self.version}:{self.stamp}>"
