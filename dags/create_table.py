import os
import sys
import datetime
import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import uuid

@dag(
    dag_id="process-employees",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    create_employees_table = PostgresOperator(
        task_id="create_employees_table",
        postgres_conn_id="postgres_connect",
        sql="""
            CREATE TABLE IF NOT EXISTS table1 (
                id SERIAL PRIMARY KEY,x x   
                track_id VARCHAR(255),
                car_type VARCHAR(255),
                traveled_distance VARCHAR(255),
                avg_speed VARCHAR(255)
            );""",
    )

    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="postgres_connect",
        sql="""
            CREATE TABLE IF NOT EXISTS table2 (
                track_id VARCHAR(255),
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                speed DOUBLE PRECISION,
                lon_acc DOUBLE PRECISION,
                lat_acc DOUBLE PRECISION,
                time VARCHAR(255),
                id SERIAL PRIMARY KEY
            );""",
    )

    @task
    def get_data():
        # Load data and generate unique track_id for each row
        df = pd.read_csv('dags/files/table1.csv')
        df['track_id'] = [str(uuid.uuid4()) for _ in range(len(df))]

        # Save the modified DataFrame with generated track_id back to CSV
        df.to_csv('dags/files/table1_modified.csv', index=False)

        # Insert data into table1
        postgres_hook = PostgresHook(postgres_conn_id="postgres_connect")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()

        with open('dags/files/table1.csv', "r") as file:
            cur.copy_expert(
                "COPY table1 FROM STDIN WITH CSV HEADER DELIMITER AS ',' ",
                file,
            )
        conn.commit()

        # Insert data into table2
        with open('dags/files/table2.csv', "r") as file:
            cur.copy_expert(
                "COPY table2 FROM STDIN WITH CSV HEADER DELIMITER AS ',' ",
                file,
            )
        conn.commit()

    create_employees_table >> create_employees_temp_table >> get_data()

dag = ProcessEmployees()
