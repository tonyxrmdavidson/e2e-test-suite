# MK E2E Test Suite
Junit5 based java test suite focused on e2e testing managed services running on kubernetes.

## Requirements
* java jdk 11
* maven >= 3.3.1
* oc or kubectl command installed
* connected to a running kube cluster

## Build and check checkstyle
maven
```
mvn install -DskipTests
```

make
```
make build
```

## Executing tests

### Running full test suite
maven
```
mvn test
```

make
```
make test
```

### Running single test or subset of tests
maven
```
mvn test -Dtest=ClassName#testName
```
make
```
make TESTCASE=ClassName#testName test
```

### Running subset tests defined by maven profile
maven
```
mvn test -Pci
```
make
```
make PROFILE=ci test
```

## Writing tests
* Test method must be annotated with annotation `@Test` or `@ParametrizedTest`.
* Every new test class must extend `TestBase`
* For access fabric8 or kubeCMD client use variable `cluster` which is defined in TestBase and can be accessed in every inherited class
* Environment variables are stored in static `Environment` class
* Every new and variable used in testsuite must be added to table or env variables

## List of environment variables
| Name        |      Description      |  Default value |
|-------------|:-------------:|------:|
| LOG_DIR     |  path where test suite stores logs from failed tests etc...      | $(pwd)/target/logs |
| CONFIG_PATH | path where is stored config.json with env variables and values   | $(pwd)/config.json |

