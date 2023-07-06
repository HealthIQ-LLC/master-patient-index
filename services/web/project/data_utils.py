import datetime
from hashlib import sha256
import random
import uuid

HASH_KEYS = [
    "address_1",
    "address_2",
    "city",
    "state",
    "postal_code",
    "organization_key",
    "given_name",
    "family_name",
    "name_day",
    "gender",
]


def choose(input_list: list):
    return random.choice(input_list)


def value_composite(*args) -> str:
    return ":".join(args)


def key_composite(organization_key: str, system_key: str, system_id: str) -> str:
    return value_composite(organization_key, system_key, system_id)


def create_composite_name_day_postal_code(name_day: datetime.date, postal_code: str) -> str:
    name_day_str = name_day.strftime("%Y%m%d")
    return value_composite(name_day_str, postal_code)


def create_composite_name(given_name: str, family_name: str) -> str:
    return value_composite(given_name[:5], family_name).replace(" ", "").replace("-", "")


def apply_hash(record: dict, hash_keys: list):
    value_list = [str(record.get(key) or "") for key in hash_keys]
    string_to_hash = "".join(value_list)
    temp_hash = sha256()
    temp_hash.update(string_to_hash.encode())
    my_hash = temp_hash.hexdigest()

    return my_hash


def apply_record_metadata(record, user):
    composite_ndpc = None
    ts = datetime.datetime.now()
    my_hash = apply_hash(record, HASH_KEYS)
    composite_key = key_composite(
        record.get("organization_key") or "",
        record.get("system_key") or "",
        record.get("system_id") or ""
    )
    composite_name = (record["given_name"])
    if record["given_name"] and record["family_name"]:
        composite_name = create_composite_name(
            record["given_name"],
            record["family_name"]
        )
    if record["postal_code"]:
        composite_ndpc = create_composite_name_day_postal_code(
            record["name_day"],
            record["postal_code"]
        )
    record["uq_hash"] = my_hash
    record["composite_key"] = composite_key
    record["composite_name"] = composite_name
    record["composite_name_day_postal_code"] = composite_ndpc
    record["touched_by"] = user
    record["touched_ts"] = ts

    return record, ts


def random_float() -> float:
    value = random.uniform(0, 1)
    scalar = random.random()

    return value * scalar


def random_datetime(min_year: int = 1901, max_year: int = datetime.date.today().year) -> datetime.datetime:
    parameters = {
        "year": random.randrange(min_year, max_year),
        "month": random.randrange(1, 12),
        "day": random.randrange(1, 28),
        "hour": random.randrange(0, 24),
        "minute": random.randrange(0, 60),
        "second": random.randrange(0, 60)
    }

    return datetime.datetime(**parameters)


def unique_id(low: int = 1, high: int = 1000000000000000) -> int:
    return random.randrange(low, high)


def unique_text_key(key="TEST") -> str:
    return f"{key}_{uuid.uuid4().hex[:8]}"


# these functions generate dummy test records
# ToDo: couple test record gen to Model
def demographics_record(key: str) -> dict:
    record_id = unique_id()
    organization_key = unique_text_key(key)
    system_key = unique_text_key(key)
    system_id = unique_text_key(key)
    given_name = unique_text_key(key)
    middle_name = unique_text_key(key)
    family_name = unique_text_key(key)
    gender = choose("mfu")    # type: ignore
    name_day = random_datetime()
    address_1 = unique_text_key(key)
    address_2 = unique_text_key(key)
    city = unique_text_key(key)
    state = unique_text_key(key)
    postal_code = unique_text_key(key)
    social_security_number = unique_text_key(key)
    staged_record = {
        "record_id": record_id,
        "organization_key": organization_key,
        "system_key": system_key,
        "system_id": system_id,
        "given_name": given_name,
        "middle_name": middle_name,
        "family_name": family_name,
        "gender": gender,
        "name_day":  name_day,
        "address_1": address_1,
        "address_2": address_2,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "social_security_number": social_security_number,
    }
    record, _ = apply_record_metadata(staged_record, "testuser")

    return record


def telecoms_record(key: str, record_id: int) -> dict:
    record = {
        "id": unique_id(),
        "record_id": record_id,
        "telecoms_type": unique_text_key(key),
        "telecoms_subtype": unique_text_key(key),
        "telecoms_value": unique_text_key(key)
    }   

    return record


def enterprise_groups_record(enterprise_id: int, record_id: int) -> dict:
    record = {
        "id": unique_id(),
        "enterprise_id": enterprise_id,
        "record_id": record_id
    }

    return record


def enterprise_match_record(record_id_low: int, record_id_high: int, 
                            key: str, is_valid=True) -> dict:
    record = {
        "id": unique_id(),
        "record_id_low": record_id_low,
        "record_id_high": record_id_high,
        "match_type": unique_text_key(key),
        "det_match_tier": random_float(),
        "prob_match_score": random_float(),
        "is_valid": is_valid
    }

    return record
