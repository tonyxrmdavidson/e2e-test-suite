ROOT_DIR = $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
TESTCASE ?= io.managed.services.**

ifdef PROFILE
	PROFILE_ARGS = "-P$(PROFILE)"
endif

clean:
	mvn clean

build:
	mvn test -DskipTests

test:
	mvn test -Dtest=$(TESTCASE) $(PROFILE_ARGS)

.PHONY: clean build test