start:
	docker run --name POSTGRES -v .:/home/workspace -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres

stop:
	docker stop POSTGRES
	docker rm POSTGRES

generate_data:
	bash ./scripts/generate_data.sh ${SCALE_FACTOR}
	# mkdir -p ./data/sf${SCALE_FACTOR}
	# chmod +w ./data/sf${SCALE_FACTOR}
	# docker exec POSTGRES bash /home/workspace/scripts/generate_data.sh ${SCALE_FACTOR}
