docker-image-client:
	docker build -f ./client/Dockerfile -t "client:latest" .
.PHONY: docker-image-client

docker-image-system:
#	docker build -f ./base-images/server-base.dockerfile -t "server-base:latest" .
#	docker build -f ./server/clientHandler/Dockerfile -t "client_handler:latest" .
	docker build -f ./server/query1/worker/Dockerfile -t "query1_worker:latest" .
	docker build -f ./server/query1/synchronizer/Dockerfile -t "query1_synchronizer:latest" .	
	docker build -f ./server/query2/worker/Dockerfile -t "query2_worker:latest" .
	docker build -f ./server/query2/synchronizer/Dockerfile -t "query2_synchronizer:latest" .
#	docker build -f ./server/query3/worker/Dockerfile -t "query3_worker:latest" .
	docker build -f ./server/query3/synchronizer/Dockerfile -t "query3_synchronizer:latest" .
#	docker build -f ./server/query5/worker/Dockerfile -t "query5_worker:latest" .
	docker build -f ./server/query5/synchronizer/Dockerfile -t "query5_synchronizer:latest" .
#	docker build -f ./server/resultHandler/Dockerfile -t "result_handler:latest" .
.PHONY: docker-image-system

system-run: docker-image-system
	docker compose -f docker-compose-server.yaml up -d --build
	docker compose -f docker-compose-server.yaml logs -f
.PHONY: system-run	

client-run: docker-image-client
	docker compose -f docker-compose-client.yaml build
	docker compose -f docker-compose-client.yaml run --rm client python main.py
.PHONY: client-run

system-shutdown:
	docker compose -f docker-compose-server.yaml stop -t 10
	docker compose -f docker-compose-server.yaml down
.PHONY: system-shutdown

test: docker-image-system
	docker compose -f docker-compose-test.yaml up -d --build
	docker compose -f docker-compose-test.yaml logs -f
.PHONY: test	

rm-test:
	docker compose -f docker-compose-test.yaml stop -t 1
	docker compose -f docker-compose-test.yaml down
.PHONY: rm-test

system-logs:
	docker compose -f docker-compose-server.yaml logs -f

system-config:
	# python3 set_up_config.py
	python3 set_up_middleware_queues.py
	python3 set_up_docker_compose.py
	
.PHONY: system-logs
