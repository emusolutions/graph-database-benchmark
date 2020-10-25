# Go parameters
# DOCKER
DOCKER_APP_NAME=graph-database-benchmark-redisgraph
DOCKER_ORG=redisbench
DOCKER_REPO:=${DOCKER_ORG}/${DOCKER_APP_NAME}
#DOCKER_TAG:=$(shell git log -1 --pretty=format:"%h")
DOCKER_TAG=edge
DOCKER_IMG:="$(DOCKER_REPO):$(DOCKER_TAG)"
DOCKER_LATEST:="${DOCKER_REPO}:latest"

# DOCKER TASKS
# Build the container
docker-build:
	docker build -t $(DOCKER_APP_NAME):latest -f  Dockerfile .

# Build the container without caching
docker-build-nc:
	docker build --no-cache -t $(DOCKER_APP_NAME):latest -f  Dockerfile .

# Make a release by building and publishing the `{version}` ans `latest` tagged containers to ECR
docker-release: docker-build-nc docker-publish

# Docker publish
docker-publish: docker-publish-latest docker-publish-version ## Publish the `{version}` ans `latest` tagged containers to ECR

docker-repo-login: ## login to DockerHub with credentials found in env
	docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}

docker-publish-latest: docker-tag-latest ## Publish the `latest` tagged container to ECR
	@echo 'publish latest to $(DOCKER_REPO)'
	docker push $(DOCKER_LATEST)

docker-publish-version: docker-tag-version ## Publish the `{version}` tagged container to ECR
	@echo 'publish $(DOCKER_IMG) to $(DOCKER_REPO)'
	docker push $(DOCKER_IMG)

# Docker tagging
docker-tag: docker-tag-latest docker-tag-version ## Generate container tags for the `{version}` ans `latest` tags

docker-tag-latest: ## Generate container `{version}` tag
	@echo 'create tag latest'
	docker tag $(DOCKER_APP_NAME) $(DOCKER_LATEST)

docker-tag-version: ## Generate container `latest` tag
	@echo 'create tag $(DOCKER_APP_NAME) $(DOCKER_REPO):$(DOCKER_IMG)'
	docker tag $(DOCKER_APP_NAME) $(DOCKER_IMG)