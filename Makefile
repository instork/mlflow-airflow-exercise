format:
	black .
	isort .

clean_up:
	sudo rm -rf logs/dag*
	sudo rm -rf logs/scheduler
	sudo rm -rf data/csvs/*
	sudo rm -rf data/mlflow_artifact/*
	sudo rm -rf data/mlflow_db/*
	sudo rm -rf data/mongodb/*
