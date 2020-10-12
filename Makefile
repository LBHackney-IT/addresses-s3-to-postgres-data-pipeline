.PHONY: build
build:
	docker-compose build s3-to-postgres-data-pipeline

.PHONY: shell
shell:
	docker-compose run s3-to-postgres-data-pipeline bash

.PHONY: test
test:
	docker-compose build s3-to-postgres-data-pipeline-test && docker-compose up s3-to-postgres-data-pipeline-test

.PHONY: lint
lint:
	-dotnet tool install -g dotnet-format
	dotnet tool update -g dotnet-format
	dotnet format
