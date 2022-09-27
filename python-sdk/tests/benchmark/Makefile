all: clean setup_gke container run_job wait teardown_gke
.PHONY: all

GCP_PROJECT ?= astronomer-dag-authoring
GIT_HASH = $(shell git log -1 --format=%h)
APP ?= benchmark
CONTAINER_REGISTRY=gcr.io/$(GCP_PROJECT)/$(APP)
PLATFORM="linux/amd64"

check_google_credentials:
ifndef GOOGLE_APPLICATION_CREDENTIALS
	@echo "The GOOGLE_APPLICATION_CREDENTIALS environment variable is missing."
	exit 1
endif

clean:
	@echo "Deleting unnecessary files"
	@find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$\)" | xargs rm -rf
	@rm -f unittests.cfg
	@rm -f unittests.db
	@rm -f webserver_config.py
	@rm -f ../../unittests.cfg
	@rm -f ../../unittests.db
	@rm -f ../../airflow.cfg
	@rm -f ../../airflow.db

# Takes approximately 7min
setup_gke: clean
	@cd infrastructure/terraform && \
		terraform init  && \
		terraform apply -auto-approve

run: clean
	@AIRFLOW__ASTRO_SDK__SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE=gcs_int_python_sdk GCP_BUCKET=dag-authoring ./run.sh

container: clean check_google_credentials
	@echo "Building and pushing container $(CONTAINER_REGISTRY)"
	@gcloud auth configure-docker gcr.io
	@docker build --platform=$(PLATFORM) --build-arg=GIT_HASH=$(GIT_HASH) --tag=$(CONTAINER_REGISTRY):$(GIT_HASH) --tag=$(CONTAINER_REGISTRY):latest -f ./Dockerfile  ../../
	@gcloud auth activate-service-account --key-file $(GOOGLE_APPLICATION_CREDENTIALS)
	@docker push $(CONTAINER_REGISTRY)

local: clean check_google_credentials
	@echo "Building and pushing container $(CONTAINER_REGISTRY)"
	@gcloud auth configure-docker gcr.io
	@docker build -t benchmark -f ./Dockerfile ../../ --platform=$(PLATFORM) --build-arg=GIT_HASH=$(GIT_HASH)
	@mkdir -p /tmp/docker
	@sudo chmod a+rwx /tmp/docker/
	@docker run -it \
		   -v /tmp/docker:/tmp:rw \
		   -v ${GOOGLE_APPLICATION_CREDENTIALS}:/tmp/gcp.json:ro \
		-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp.json \
		   benchmark
	@rm -rf astro-sdk
	@echo "The results file is available in /tmp/docker"

run_job:
	@gcloud container clusters get-credentials astro-sdk --zone us-central1-a --project ${GCP_PROJECT}
	@kubectl apply -f infrastructure/kubernetes/namespace.yaml
	@kubectl apply -f infrastructure/kubernetes/postgres.yaml
	@kubectl apply -f infrastructure/kubernetes/postgres_svc.yaml
	@kubectl create -f infrastructure/kubernetes/job.yaml

wait:
	sh wait_for_status.sh

# Takes approximately 2min
teardown_gke:
	@cd infrastructure/terraform && \
		terraform init && \
		terraform destroy --target google_container_node_pool.benchmark -auto-approve
