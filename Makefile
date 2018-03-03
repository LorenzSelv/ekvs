
build-docker:
	docker build -t lab4erlang .

run-docker-main:
	docker run -p 4000:8080 --net=lab4net --ip=10.0.0.20 lab4erlang

run-docker-forwarder:
	docker run -p 4001:8080 --net=lab4net -e MAINIP=10.0.0.20:8080 lab4erlang

SHELL:=/bin/bash
kill:
	docker kill $(shell docker ps -q)

build-local:
	rebar3 release

run-local:
	_build/default/rel/lab4kvs/bin/lab4kvs foreground

tests:
	- rm test/log/*
	- rm test/pics/*
	- rm test/snapshots/*
	python test/distribution_test.py
