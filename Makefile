.PHONY: help python_build docker_build_latest run

IMAGE_NAME?=bbp-workflow

define HELPTEXT
Makefile usage
 Targets:
    python_build        Build, test and package python.
    docker_build_latest Build backend local docker image with the latest tag.
    run                 Run locally
endef
export HELPTEXT

help:
	@echo "$$HELPTEXT"

python_build:
	tox -e py3
	pipx run build --sdist

docker_build_latest: # python_build
	docker build -t $(IMAGE_NAME):latest .

run:
	docker run -it --rm --user $$(id -u) -e DEBUG=True $(IMAGE_NAME)
