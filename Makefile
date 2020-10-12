.PHONY: build
build:
	docker-compose build address-to-postgres-data-pipeline

.PHONY: shell
shell:
	docker-compose run address-to-postgres-data-pipeline bash

.PHONY: test
test:
	docker-compose build address-to-postgres-data-pipeline-test && docker-compose up address-to-postgres-data-pipeline-test

.PHONY: lint
lint:
	-dotnet tool install -g dotnet-format
	dotnet tool update -g dotnet-format
	dotnet format
