docker-image:
	docker build -f ./base-images/python-base.dockerfile -t "python-base:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	docker build -f ./server/clientHandler/Dockerfile -t "client_handler:latest" .
	docker build -f ./server/query1/worker/Dockerfile -t "query1_worker:latest" .
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

shutdown:
	docker compose -f docker-compose-client.yaml stop -t 10
	docker compose -f docker-compose-client.yaml down
	docker compose -f docker-compose-server.yaml stop -t 10
	docker compose -f docker-compose-server.yaml down
	docker compose -f docker-compose-middleware.yaml stop -t 10
	docker compose -f docker-compose-middleware.yaml down 
.PHONY: shutdown

system-logs:
	docker compose -f docker-compose-server.yaml logs -f
.PHONY: system-logs