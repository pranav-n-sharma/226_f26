from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

from pathlib import Path

import csv
import json
import requests
import snowflake.connector



@task
def extract():

	# Using Airflow variables to start pipeline
	params = {
		"latitude": float(Variable.get("latitude")),
		"longitude": float(Variable.get("longitude")),
		"past_days": int(Variable.get("n_days")),
		"forecast_days": 0,
		"daily": [
			"temperature_2m_max",
			"temperature_2m_min",
			"precipitation_sum",
			"weather_code"
		],
		"timezone": "America/Los_Angeles"
	}

	# Adding timeout, catching exceptions
	response = requests.get(Variable.get("src"), params=params, timeout=30)
	response.raise_for_status()

	# Write json to file, return path
	# Using dag and run id to identify files
	context = get_current_context()
	dag_id = context["dag"].dag_id
	run_id = context["run_id"].replace(":", "_").replace("+", "_")

	base = Path("/opt/airflow/data/") / dag_id / run_id
	base.mkdir(parents=True, exist_ok=True)

	ext_path = base / "extract.json"
	with open(ext_path, "w") as ext_json:
		json.dump(response.json(), ext_json, indent=4)

	return str(ext_path)



@task
def transform(ext_path):

	with open(ext_path, "r") as ext_json:
		data = json.load(ext_json)

	lat, lng = data["latitude"], data["longitude"]
	daily = data["daily"]
	n_days = len(daily["time"])

	date, t_max, t_min, prec, wc = \
	daily["time"], daily["temperature_2m_max"], daily["temperature_2m_min"], \
	daily["precipitation_sum"], daily["weather_code"]

	# Write rows to file
	base = Path(ext_path).parent
	tr_path = base / "transformed.csv"
	with open(tr_path, "w", newline="") as dst:
		writer = csv.writer(dst)

		# 2d tuple: 
		writer.writerows(
			(
				lat,
				lng,
				date[i],
				t_max[i],
				t_min[i],
				prec[i],
				wc[i]
			) \
			for i in range(n_days)
		)

	# Return transform path
	return str(tr_path)


@task
def load(tr_path, table):

	hook = SnowflakeHook(
		snowflake_conn_id="snowflake_conn",

	)

	# Create table outside transaction due to default commit
	hook.run(
		"""
			CREATE TABLE IF NOT EXISTS IDENTIFIER(%s) (
				Latitude NUMBER(9,6),
				Longitude NUMBER(9,6),
				Record_Date DATE,
				Temp_Max NUMBER(4,1),
				Temp_Min NUMBER(4,1),
				Precipitation NUMBER(4,2),
				Weather_Code NUMBER(2,0),

				PRIMARY KEY (Latitude,Longitude,Record_Date)
			)
		""",
		parameters=(table,)
	)

	# Stage file outside transaction to isolate delays
	hook.run(
		f"PUT file://{tr_path} @%{table}"
	)

	# Transaction
	conn = hook.get_conn()

	try:
		conn.autocommit(False)

		with conn.cursor() as cur:
			cur.execute(f"DELETE FROM {table}") # Delete existing rows
			cur.execute(f"COPY INTO {table} FROM @%{table}") # Populate table 

		conn.commit()
	except Exception as e:
		conn.rollback()
		raise e
	finally:
		conn.close()



with DAG(
	dag_id="weather_dag",
	start_date=datetime(2026,9,17),
	schedule=timedelta(minutes=5),
	catchup=False
) as dag:

	ext_path = extract()
	tr_path = transform(ext_path)
	load(tr_path=tr_path, table="Weather_A03")
