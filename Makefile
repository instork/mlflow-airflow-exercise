PYTHON=3.9
BASENAME=$(shell basename $(CURDIR))
CONDA_CH=conda-forge defaults

env:
	conda create -n $(BASENAME)  python=$(PYTHON)

setup:
	conda install --file requirements.txt $(addprefix -c ,$(CONDA_CH))
	
format:
	black .
	isort .

clean_up:
	sudo rm -rf logs/dag*
	sudo rm -rf logs/scheduler
	sudo rm -rf data/csvs/*
	sudo rm -rf data/mlflow_artifact/*
	sudo rm -rf data/mlflow_db/*

clean_up_all:
	sudo rm -rf logs/dag*
	sudo rm -rf logs/scheduler
	sudo rm -rf data/csvs/*
	sudo rm -rf data/mlflow_artifact/*
	sudo rm -rf data/mlflow_db/*
	sudo rm -rf data/mongodb/*