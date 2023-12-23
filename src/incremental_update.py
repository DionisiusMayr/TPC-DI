import airflow
from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 12, 20),
}

dag_incremental_update = DAG(
    dag_id = "dag_incremental_update",
    default_args = default_args,
    schedule_interval = None,
)

create_schema_staging = PostgresOperator(
    task_id = "create_schema_staging",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/create_schema_staging.sql",
    dag = dag_incremental_update
)

truncate_staging = PostgresOperator(
    task_id = "truncate_staging",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/truncate_staging.sql",
    dag = dag_incremental_update
)

load_staging = PostgresOperator(
    task_id = "load_staging",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/load_staging.sql",
    dag = dag_incremental_update
)

tl_master_dimtrade = PostgresOperator(
    task_id = "tl_master_dimtrade",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_dimtrade.sql",
    dag = dag_incremental_update
)

tl_master_dimaccount = PostgresOperator(
    task_id = "tl_master_dimaccount",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_dimaccount.sql",
    dag = dag_incremental_update
)

tl_master_dimcustomer = PostgresOperator(
    task_id = "tl_master_dimcustomer",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_dimcustomer.sql",
    dag = dag_incremental_update
)

tl_master_factcashbalances = PostgresOperator(
    task_id = "tl_master_factcashbalances",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_factcashbalances.sql",
    dag = dag_incremental_update
)

tl_master_factholdings = PostgresOperator(
    task_id = "tl_master_factholdings",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_factholdings.sql",
    dag = dag_incremental_update
)

tl_master_factmarkethistory = PostgresOperator(
    task_id = "tl_master_factmarkethistory",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_factmarkethistory.sql",
    dag = dag_incremental_update
)

tl_master_factwatches = PostgresOperator(
    task_id = "tl_master_factwatches",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/tl_master_factwatches.sql",
    dag = dag_incremental_update
)

prospect = PostgresOperator(
    task_id = "prospect",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/prospect.sql",
    dag = dag_incremental_update
)

update_prospect = PostgresOperator(
    task_id = "update_prospect",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/update_prospect.sql",
    dag = dag_incremental_update
)

load_prospect_status_dimessages = PostgresOperator(
    task_id = "load_prospect_status_dimessages",
    postgres_conn_id = "pg_conn",
    sql = "incremental_update/load_prospect_status_dimessages.sql",
    dag = dag_incremental_update
)

create_schema_staging >> truncate_staging
truncate_staging >> load_staging
load_staging >> tl_master_dimcustomer >> tl_master_dimaccount >> tl_master_dimtrade
tl_master_dimtrade >> tl_master_factholdings
tl_master_dimtrade >> tl_master_factmarkethistory
tl_master_dimtrade >> tl_master_factwatches
tl_master_dimtrade >> tl_master_factcashbalances

load_staging >> prospect >> update_prospect >> load_prospect_status_dimessages

