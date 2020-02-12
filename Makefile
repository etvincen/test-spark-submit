SHELL := /bin/bash
BASE_DIR := $(shell dirname $(abspath $(lastword $(MAKEFILE_LIST))))
DATASETS_PATH=~/Documents/spark/eval/dataset
PY_FILE=~/Documents/spark/eval/data-ingestion-job/src

prepare-dataset:
	@mkdir -p $(DATASETS_PATH)
	@wget https://cadastre.data.gouv.fr/data/etalab-dvf/latest/csv/2019/full.csv.gz
	@wget https://www.data.gouv.fr/fr/datasets/r/b3b26ad1-a143-4651-afd6-dde3908196fc
	@gunzip ~/Downloads/full.csv.gz
	@mv ~/Downloads/full.csv $(DATASETS_PATH)/full.csv
	@mv ~/Downloads/fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre.csv $(DATASETS_PATH)/fr-en-adresse-et-geolocalisation-etablissements-premier-et-second-degre.csv



run-pyspark:
	@docker run -d -ti -v $(DATASETS_PATH):/data -v $(PY_FILE):/src -p 4040:4040 stebourbi/sio:pyspark



open-spark-ui:
	@open http://localhost:4040


prepare-dev-env:
	@source $(BASE_DIR)/create-virtual-dev-env.sh

