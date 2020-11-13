ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
TESTCASE ?= io.managed.services.**

clean:
	mvn clean

test:
	mvn test -Dtest=$(TESTCASE)

.PHONY: clean test