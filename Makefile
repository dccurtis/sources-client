PYTHON	= $(shell which python)

TOPDIR  = $(shell pwd)
PYDIR	= sources_client

PGSQL_VERSION   = 9.6

OS := $(shell uname)
ifeq ($(OS),Darwin)
	PREFIX	=
else
	PREFIX	= sudo
endif

define HELP_TEXT =
Please use \`make <target>' where <target> is one of:

--- General Commands ---
  clean                    clean the project directory of any scratch files, bytecode, logs, etc.
  help                     show this message
  lint                     run linting against the project

--- Commands using local services ---
  tbd                       Add this later

--- Commands using Docker Compose ---
  docker-up                 run django and database
  docker-down               shut down service containers
  docker-shell              run django and db containers with shell access to server (for pdb)
  docker-logs               connect to console logs for all services


endef
export HELP_TEXT

help:
	@echo "$$HELP_TEXT"

clean:
	git clean -fdx -e .idea/ -e *env/

lint:
	tox -elint

docker-up:
	docker-compose up --build -d

docker-logs:
	docker-compose logs -f

docker-shell:
	docker-compose run --service-ports sources-client

docker-down:
	docker-compose down

.PHONY: docs
