.PHONY: start-db

start-db:
	./scripts/start-db.sh

run-data-gen:
	poetry run python ./scripts/generate-data.py --data-path ./data

run-spark-ingestion:
	poetry run python ./src/step1_ingestion.py --data-path ./data

run-spark-daily-batch:
	poetry run python ./src/step2_aggs.py