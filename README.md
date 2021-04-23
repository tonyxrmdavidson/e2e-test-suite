# MK E2E Test Suite

Junit5 based java test suite focused on e2e testing managed services running on kubernetes.

## Requirements

* java jdk 11
* maven >= 3.3.1

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
mvn verify -Dit.test=ClassName#testName
```

make

```
make TESTCASE=ClassName#testName test
```

### Running subset tests defined by maven profile

maven

```
mvn verify -Pci
```

make

```
make PROFILE=ci test
```

## Writing tests

* Test method must be annotated with annotation `@Test` or `@ParametrizedTest`.
* Every new test class must extend `TestBase`
* Environment variables are stored in static `Environment` class
* Every new and variable used in testsuite must be added to table or env variables

## List of environment variables

| Name        | Description   |  Default value |
|-------------|:-------------:|---------------:|
| LOG_DIR                     | path where test suite stores logs from failed tests etc...                    | $(pwd)/target/logs |
| CONFIG_PATH                 | path where is stored config.json with env variables and values                | $(pwd)/config.json |
| SERVICE_API_URI             | the service-api URI to tests                                                  | https://api.stage.openshift.com |
| SSO_REDHAT_KEYCLOAK_URI     | the SSO URI to retrieve the service-api token                                 | https://sso.redhat.com |
| MAS_SSO_REDHAT_KEYCLOAK_URI | the SSO URI to retrieve the mas-sso token                                     | https://identity.api.stage.openshift.com |
| SSO_USERNAME                | main user for SSO                                                             |  |
| SSO_PASSWORD                | main user password                                                            |  |
| SSO_SECONDARY_USERNAME      | a second user in the same org as the main user                                |  |
| SSO_SECONDARY_PASSWORD      | the secondary user password                                                   |  |
| SSO_ALIEN_USERNAME          | a third user that is part of a different org respect the main user            |  |
| SSO_ALIEN_PASSWORD          | the alien user password                                                       |  |
| SSO_UNAUTHORIZED_USERNAME   | a user that is not authorized to create kafka instances                       |  |
| SSO_UNAUTHORIZED_PASSWORD   | the unauthorized user password                                                |  |
| DEV_CLUSTER_SERVER          | the api server url of a openshift cluster with the binding operator installed |  |
| DEV_CLUSTER_NAMESPACE       | the namespace to use to install the binding operator CRs                      | mk-e2e-tests |
| DEV_CLUSTER_TOKEN           | the cluster user token (this can also be a service account token)             |  |
| BF2_GITHUB_TOKEN            | a github token to download artifacts from the bf2 org                         |  |
| CLI_VERSION                 | the CLI version to download from bf2/cli                                      | latest |
| CLI_PLATFORM                | windows/macOS/linux                                                           | auto-detect |
| CLI_ARCH                    | the CLI arch and os to download from bf2/cli                                  | amd64 |
| KAFKA_POSTFIX_NAME          | A postfix added to kafka and service account names to avoid conflict          |  |

## List of Tags

| Name | Description | Required Envs |
|------|-------------|---------------|
| service-api               | run all tests targeting the service-api | SSO_USERNAME, SSO_PASSWORD, SSO_SECONDARY_USERNAME, SSO_SECONDARY_PASSWORD |
| service-api-permissions   | run all service api permissions tests   | SSO_USERNAME, SSO_PASSWORD, SSO_SECONDARY_USERNAME, SSO_SECONDARY_PASSWORD, SSO_ALIEN_USERNAME, SSO_ALIEN_PASSWORD, SSO_UNAUTHORIZED_USERNAME, SSO_UNAUTHORIZED_PASSWORD  |
| kafka-admin-permissions   | run all kafka admin permissions tests   | SSO_USERNAME, SSO_PASSWORD |
| kafka-admin-api           | run the kafka admin api tests           | SSO_USERNAME, SSO_PASSWORD |
| binding-operator          | run all tests for the binding-operator  | SSO_USERNAME, SSO_PASSWORD, DEV_CLUSTER_SERVER, DEV_CLUSTER_TOKEN |
| cli                       | run all tests for the CLI               | SSO_USERNAME, SSO_PASSWORD, BF2_GITHUB_TOKEN |

## Report to ReportPortal

When executing the tests is possible to send the results and logs in real time to ReportPortal using
the `./hack/testrunner.sh` script and the following ENVs:

| Name | Description | Default value |
|------|-------------|---------------|
| REPORTPORTAL_ENDPOINT | ReportPortal URL                            | https://reportportal-cloud-services.apps.ocp4.prod.psi.redhat.com |
| REPORTPORTAL_UUID     | The Access Token                            |  |
| REPORTPORTAL_LAUNCH   | The launch name to user                     | mk-e2e-test-suite |
| REPORTPORTAL_PROJECT  | The project where to report the result      | rhosak |
| REPORTPORTAL_ENABLE   | Must be set to true to enable ReportPortal  | false |

## Short guides

#### Recreate the mk-e2e-tests namespace in the dev cluster

1. Login to the dev cluster as kubeadmin
    ```
    oc login --token=KUBE_ADMIN_TOKEN --server=DEV_CLUSTER_SERVER
    ```
2. Execute the `./hack/bootstrap-mk-e2e-tests-namespace.sh` script
    ```
   ./hack/bootstrap-mk-e2e-tests-namespace.sh
    ```
3. Update the [vault](https://vault.devshift.net/ui/vault/secrets/managed-services-ci/show/mk-e2e-test-suite/staging)
   with the new dev cluster token

#### Update the rhoas-model dependency

1. Clone the operator the bf2/operator from here:https://github.com/bf2fc6cc711aee1a0c2a/operator
    ```
   git clone https://github.com/bf2fc6cc711aee1a0c2a/operator.git
   cd operator
   ```

2. Checkout the right version you want to update to

   **Example:**
   ```
   git checkout 0.5.0
   ```

3. Build the all the JARs (See operator README.md for the required maven and java versions)
   ```
   mvn package
   ```

4. Copy the built **rhoas-model.VERSION.jar** to the e2e-test-suite **lib/** directory

   **Example:**
   ```
   cp source/model/target/rhoas-model-1.0.0-SNAPSHOT.jar ../e2e-test-suite/lib
   ```

6. If the rhoas-model version has changed you need to update the pom.xml in the e2e-test-suite repository

5. Switch then to the e2e-test-suite repo and verify the build
   ```
   mvn verify -Psmoke
   ```

6. If the build pass, commit the changes and open a PR, otherwise fix the issues and then open a PR

## Maintainers

* David Kornel <dkornel@redhat.com>
* Davide Bizzarri <dbizzarr@redhat.com>
