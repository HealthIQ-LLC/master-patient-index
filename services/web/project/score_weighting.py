from datetime import datetime
import operator

from .app import app
from .model import Battery, db, key_gen, Test
from .processor import transact_records

ops = {  # ToDo: greater operations support or just these 6?
	'lt': operator.lt,  # less than <
	'le': operator.le,  # less than or equal <=
	'eq': operator.eq,  # equal ==
	'ne': operator.ne,	 # not equal !=
	'ge': operator.ge,	 # greater than or equal >=
	'gt': operator.gt,	 # greater than >
}


def create_test(packet: dict) -> int:
	"""
	This function adds a single test to your database of tests
	:param packet: a dict with 'metric', 'threshold', 'operator', 'weight'
	"""
	user = packet["user"]
	version = packet["version"]
	staged_test_record = {
		"test_id": key_gen(user, version),
		# ToDo: consider disjoint keys for type bool or numeric
		"metric": packet["metric"],
		"threshold": packet["threshold"],
		"operator": packet["operator"],
		"weight": packet["weight"],
		"touched_by": user,
		"touched_ts": datetime.now()
	}
	with app.app_context():
		record = Test(**staged_test_record)  # type: ignore

	return transact_records(record, "test")


def delete_test(test_id: int):
	"""
	This function removes a test from your database of tests and batteries
	:param test_id: the primary key of the test to be removed
	"""
	with app.app_context():
		Test.query.filter_by(test_id=test_id).delete()
		Battery.query.filter_by(test_id=test_id).delete()
		db.session.commit()

	return 'ok'


def create_battery(packet: dict) -> int:
	"""
	This function collates a list of tests into one battery of tests for use later
	:param packet: a dict with
	"""
	user = packet["user"]
	version = packet["version"]
	test_ids = packet["test_ids"]
	battery_id = key_gen(user, version)
	battery_ts = datetime.now()
	with app.app_context():
		for test_id in test_ids:
			staged_battery_record = {
				"battery_id": battery_id,
				"test_id": test_id,
				"touched_by": user,
				"touched_ts": battery_ts
			}
			record = Battery(**staged_battery_record)  # type: ignore
			transact_records(record, "battery")

	return battery_id


def delete_battery(battery_id: int):
	"""
	This function removes a battery from the database
	:param battery_id: the primary key of the battery to be removed
	"""
	with app.app_context():
		Battery.query.filter_by(battery_id=battery_id).delete()
		db.session.commit()

	return 'ok'


def assemble_tests(battery_id: int) -> list:
	"""
	This function collates tests bound to a given battery ID
	:param battery_id: the primary key of the test battery to be run
	"""
	test_ids = list()
	with app.app_context():
		results = Battery.query.filter_by(battery_id=battery_id).all()
		for result in results:
			test_ids.append(result.test_id)

	return test_ids


def make_battery(test_ids: list, metric: dict) -> list:
	"""
	Given a set of tests (by ID), collate them into a battery of such tests
	:param test_ids: a list of primary keys for score tests
	:param metric: the results of pairwise analysis of two records
	"""
	battery = list()
	with app.app_context():
		for test_id in test_ids:
			result = Test.query.filter_by(test_id=test_id).first()
			# threshold is either bool or numeric, cast away from string
			metric_name = result['metric']
			metric_val = metric[metric_name]
			threshold = result['threshold']
			if threshold == 'True':
				treated_threshold = True
			elif threshold == 'False':
				treated_threshold = False
			else:
				treated_threshold = float(threshold)
			op = result['operator']
			weight = result['weight']
			result_tup = (metric_val, treated_threshold, op, weight)
			battery.append(result_tup)

	return battery


def run_test(x, y, op):
	"""
	This function evaluates x and y with a given comparison operator
	:param x: the metric to be compared (num type: bool, int, or float)
	:param y: the test eval threshold (num type: bool, int, or float)
	:param op: the operator of comparison (a callable function)

	"""
	return op(x, y)


def run_threshold(score: float, threshold=0.5) -> bool:
	"""
	This function compares a match score to the threshold only
	:param score: the weighted result of pairwise metric evaluation
	:param threshold: the value above which a score represents a match
	"""
	return score >= threshold


def run_battery(battery: list) -> tuple:
	"""
	This function wraps the battery of tests
	:param battery: a list of evaluations of pairwise string metrics
	"""
	score = 0
	for x, y, op, weight in battery:
		if run_test(x, y, op):
			score += weight
		else:
			score -= weight

	return score, run_threshold(score)


def score_weighting(battery_id: int, metric: dict) -> tuple:
	"""
	This function wraps the entire score-weighting process.
	:param battery_id: the primary key for your test battery
	:param metric: the results of pairwise analysis of two records
	"""
	test_ids = assemble_tests(battery_id)
	battery = make_battery(test_ids, metric)

	return run_battery(battery)
