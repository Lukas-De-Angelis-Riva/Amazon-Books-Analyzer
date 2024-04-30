docker-image:
	docker build -f ./base-images/python-base.dockerfile -t "python-base:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./server/clientHandler/Dockerfile -t "client_handler:latest" .
	docker build -f ./server/query1/worker/Dockerfile -t "query1_worker:latest" .
	docker build -f ./server/query2/worker/Dockerfile -t "query2_worker:latest" .
	docker build -f ./server/query2/synchronizer/Dockerfile -t "query2_synchronizer:latest" .
	docker build -f ./server/query3/worker/Dockerfile -t "query3_worker:latest" .
	docker build -f ./server/query3/synchronizer/Dockerfile -t "query3_synchronizer:latest" .
	docker build -f ./server/query5/worker/Dockerfile -t "query5_worker:latest" .
	docker build -f ./server/query5/synchronizer/Dockerfile -t "query5_synchronizer:latest" .
	docker build -f ./server/resultHandler/Dockerfile -t "result_handler:latest" .
.PHONY: docker-image

middleware-run: docker-image
	docker compose -f docker-compose-middleware.yaml up -d --build
	docker compose -f docker-compose-middleware.yaml logs -f
.PHONY: middleware-run

system-run: docker-image
	docker compose -f docker-compose-server.yaml up -d --build
	docker compose -f docker-compose-server.yaml logs -f
.PHONY: system-run	

client-run: docker-image
	docker compose -f docker-compose-client.yaml up -d --build
	docker compose -f docker-compose-client.yaml logs -f
.PHONY: client-run

system-shutdown:
	docker compose -f docker-compose-server.yaml stop -t 10
	docker compose -f docker-compose-server.yaml down
	docker compose -f docker-compose-middleware.yaml stop -t 10
	docker compose -f docker-compose-middleware.yaml down 
.PHONY: system-shutdown

client-shutdown:
	docker compose -f docker-compose-client.yaml stop -t 10
	docker compose -f docker-compose-client.yaml down
.PHONY: client-shutdown

system-logs:
	docker compose -f docker-compose-server.yaml logs -f
.PHONY: system-logs