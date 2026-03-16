PROJECT_ID ?= #your project id
REGION ?= #your region
PROJECT_NUMBER ?= $$(gcloud projects list --filter=${PROJECT_ID} --format='value(PROJECT_NUMBER)')
CODE_BUCKET ?= data-lake-code-${PROJECT_NUMBER}
RAW_BUCKET ?= #your raw bucket 
DATA_BUCKET ?= # your data bucket
PHS_BUCKET ?= spark-hist-repo-${PROJECT_NUMBER}
APP_NAME ?= $$(cat pyproject.toml| grep name | cut -d" " -f3 | sed  's/"//g')
VERSION_NO ?= $$(poetry version --short)
SRC_WITH_DEPS ?= src_with_deps
DELTA_JAR_FILE ?= delta-core_2.13-2.1.0.jar
dl_code ?= #unique deal identifier

.PHONY: $(shell sed -n -e '/^$$/ { n ; /^[^ .\#][^ ]*:/ { s/:.*$$// ; p ; } ; }' $(MAKEFILE_LIST))

.DEFAULT_GOAL := help

help: ## This is help
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

setup: ## Setup Buckets and Dataset for Demo
	@echo "Project=${PROJECT_ID}--${PROJECT_NUMBER}--${CODE_BUCKET}"
	@gsutil mb -c standard -l ${REGION} -p ${PROJECT_ID} gs://${CODE_BUCKET}
	@echo "The Following Buckets created - ${CODE_BUCKET},${DATA_BUCKET}"
	@echo "Create Hive Metastore"
	@gcloud metastore services create data-catalog-${PROJECT_ID} --hive-metastore-version=3.1.2 --location=${REGION}

setup_phs: ## Setup Persisten (spark) History Server
	@echo "Project=${PROJECT_ID}--${PROJECT_NUMBER}--${PHS_BUCKET}"
	@gsutil mb -c standard -l ${REGION} -p ${PROJECT_ID} gs://${PHS_BUCKET}
	@echo "The Following Buckets created - ${PHS_BUCKET}"
	@gcloud dataproc clusters create spark-hist-srv-${PROJECT_ID} --region=${REGION} --single-node --enable-component-gateway \
    --properties=spark:spark.history.fs.logDirectory=gs://${PHS_BUCKET}/*/spark-job-history \
    --properties=yarn:yarn.nodemanager.remote-app-log-dir=gs://${PHS_BUCKET}/*/yarn-logs \
    --properties=mapred:mapreduce.jobhistory.read-only.dir-pattern=gs://${PHS_BUCKET}/*/mapreduce-job-history/done \

clean: ## CleanUp Prior to Build
	@rm -Rf ./dist
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -f requirements.txt

build: clean ## Build Python Package with Dependencies
	@echo "Packaging Code and Dependencies for ${APP_NAME}-${VERSION_NO}"
	@mkdir -p ./dist
	@poetry update
	@poetry export -f requirements.txt --without-hashes -o requirements.txt
	@poetry run pip install . -r requirements.txt -t ${SRC_WITH_DEPS}
	@cd ./${SRC_WITH_DEPS}
	@find . -name "*.pyc" -delete
	@cd ./${SRC_WITH_DEPS} && zip -x "*.git*" -x "*.DS_Store" -x "*.pyc" -x "*/*__pycache__*/" -x ".idea*" -r ../dist/${SRC_WITH_DEPS}.zip .
	@rm -Rf ./${SRC_WITH_DEPS}
	@rm -f requirements.txt
	@cp ./src/rmb_main.py ./dist
	@mv ./dist/${SRC_WITH_DEPS}.zip ./dist/${APP_NAME}_${VERSION_NO}.zip
	@gsutil cp -r ./dist gs://${CODE_BUCKET}
	@gsutil cp -r dependencies/*.jar gs://${CODE_BUCKET}/dependencies/



