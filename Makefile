SHELL    := /bin/bash
TIMEOUT  := 5s
HOST     := localhost:8890
MODULE   := github.com/inna-maikut/dbbatch
PATHS    := `GO111MODULE=on go list -f '{{.Dir}}' ./...`

PWD := $(PWD)
export PATH := $(PWD)/bin:$(PATH)

.DEFAULT_GOAL = test-full

.PHONY: docker-up
docker-up:
	@docker run -d --name dbbatch_postgres_12 \
		-e POSTGRES_DB=master \
		-e POSTGRES_PASSWORD=postgres \
		-p 23340:5432 \
		postgres:12-alpine

.PHONY: docker-down
docker-down:
	@docker rm -f dbbatch_postgres_12
