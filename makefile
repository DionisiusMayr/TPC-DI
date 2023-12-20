start:
	docker run --name POSTGRES -v .:/home/workspace --network=host -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres
	sleep 2
	docker exec POSTGRES psql -U postgres -c 'CREATE DATABASE tpc_di;'
	docker run --name AIRFLOW -v .:/home/workspace --network=host -p 8080:8080 -d apache/airflow standalone

stop:
	docker stop POSTGRES
	docker rm POSTGRES
	docker stop AIRFLOW
	docker rm AIRFLOW

generate_data:
	bash ./scripts/generate_data.sh ${SCALE_FACTOR}

create_schemas:
	docker exec POSTGRES psql -U postgres -d tpc_di -q -f /home/workspace/src/sql/create_staging_schema.sql
	docker exec POSTGRES psql -U postgres -d tpc_di -q -f /home/workspace/src/sql/create_master_schema.sql

load_staging:
	docker exec AIRFLOW python /home/workspace/src/py/load_staging_db_script.py

psql:
	docker exec -it POSTGRES psql -U postgres -d tpc_di
