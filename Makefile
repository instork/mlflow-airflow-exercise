format:
	black .
	isort .

clean_up:
	sudo rm -rf logs/dag*
	sudo rm -rf logs/scheduler
	sudo rm -rf data/csvs/*
	sudo rm -rf data/minio/*
	sudo rm -rf data/mlflow/*
	sudo rm -rf data/mongodb/*
