all: clean setup_gke container run_job teardown_gke
.PHONY: all

GCP_PROJECT ?= astronomer-dag-authoring
GIT_HASH = $(shell git log -1 --format=%h)
APP ?= benchmark
CONTAINER_REGISTRY=gcr.io/$(GCP_PROJECT)/$(APP)


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

# Takes approximately 7min
setup_gke:
	@cd infrastructure/terraform && \
		terraform init && \
		terraform apply

container:
	@echo "Building and pushing container $(CONTAINER_REGISTRY)"
	@gcloud auth configure-docker
	@docker build --build-arg=GIT_HASH=$(GIT_HASH) --tag=$(CONTAINER_REGISTRY):$(GIT_HASH) --tag=$(CONTAINER_REGISTRY):latest -f ../../Dockerfile.build ../../
	@docker build --build-arg=GIT_HASH=$(GIT_HASH) --tag=$(CONTAINER_REGISTRY):$(GIT_HASH) --tag=$(CONTAINER_REGISTRY):latest  ../../
	@docker push $(CONTAINER_REGISTRY)

local: check_google_credentials
	@echo "Building and pushing container $(CONTAINER_REGISTRY)"
	@gcloud auth configure-docker
	@docker build -t benchmark . --build-arg=GIT_HASH=`git rev-parse --short HEAD`
	@mkdir -p /tmp/docker
	@sudo chmod a+rwx /tmp/docker/
	@docker run -it \
   		-v /tmp/docker:/tmp:rw 	\
   		-v ${GOOGLE_APPLICATION_CREDENTIALS}:/tmp/gcp.json:ro \
		-e GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp.json \
   		benchmark
	@rm -rf astro-sdk


run_job: check_google_credentials
	@gcloud container clusters get-credentials astro-sdk --zone us-central1-a --project ${GCP_PROJECT}
	@kubectl apply -f infrastructure/kubernetes/namespace.yaml
	@kubectl create -f infrastructure/kubernetes/job.yaml

# Takes approximately 2min
teardown_gke:
	@cd infrastructure/terraform && \
		terraform init && \
		terraform apply -destroy
