
build-docker:
	docker build -t ekvs .

run-docker-main:
	docker run -p 4000:8080 --net=ekvsnet --ip=10.0.0.20 ekvs

run-docker-forwarder:
	docker run -p 4001:8080 --net=ekvsnet -e MAINIP=10.0.0.20:8080 ekvs

SHELL:=/bin/bash
kill:
	docker kill $(shell docker ps -q)

build-local:
	rebar3 release

run-local:
	_build/default/rel/ekvs/bin/ekvs foreground

tests:
	- rm test/log/*
	- rm test/pics/*
	- rm test/snapshots/*
	python test/distribution_test.py

eunit:
	rebar3 eunit --dir="test"
