start:
	docker run --name POSTGRES -v .:/home/workspace --network=host -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres
	sleep 2
	docker exec POSTGRES psql -U postgres -c 'CREATE DATABASE tpc_di;'
	docker run --name AIRFLOW -v .:/home/workspace -v ./src/:/opt/airflow/dags/ --network=host -p 8080:8080 -d apache/airflow standalone
	sleep 2
	docker exec AIRFLOW /usr/local/bin/pip install xmltodict
	docker exec AIRFLOW airflow users create --role Admin --username tpcdi --email tpcdi --firstname tpcdi --lastname tpcdi --password tpcdi
	docker exec AIRFLOW airflow connections add 'pg_conn' --conn-type 'postgres' --conn-login 'postgres' --conn-password '' --conn-host 'localhost' --conn-port '5432'

stop:
	docker stop POSTGRES
	docker rm POSTGRES
	docker stop AIRFLOW
	docker rm AIRFLOW

generate_data:
	bash ./scripts/generate_data.sh ${SCALE_FACTOR}
	chmod -R 777 ./data/

historical_load:
	docker exec AIRFLOW airflow dags unpause dw_sf_3
	docker exec AIRFLOW airflow dags trigger dw_sf_3

psql:
	docker exec -it POSTGRES psql -U postgres -d tpc_di
