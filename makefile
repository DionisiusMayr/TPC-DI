start:
	docker run --name POSTGRES -v .:/home/workspace -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres

stop:
	docker stop POSTGRES
	docker rm POSTGRES

generate_data:
	bash ./scripts/generate_data.sh ${SCALE_FACTOR}

create_schemas:
	docker exec POSTGRES psql -U postgres -q -f /home/workspace/src/sql/create_staging_schema.sql
	docker exec POSTGRES psql -U postgres -q -f /home/workspace/src/sql/create_master_schema.sql
