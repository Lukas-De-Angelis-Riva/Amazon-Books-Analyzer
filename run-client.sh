#!/bin/bash
docker compose -f docker-compose-client.yaml build
docker compose -f docker-compose-client.yaml run -e BOOK_FILE_PATH=data/books_$1.csv -e REVIEW_FILE_PATH=data/books_ratings_$1.csv --rm client python main.py