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
* For access fabric8 or kubeCMD client use variable `cluster` which is defined in TestBase and can be accessed in every
  inherited class
* Environment variables are stored in static `Environment` class
* Every new and variable used in testsuite must be added to table or env variables

## List of environment variables

| Name        | Description   |  Default value |
|-------------|:-------------:|---------------:|
| LOG_DIR                 | path where test suite stores logs from failed tests etc...                    | $(pwd)/target/logs |
| CONFIG_PATH             | path where is stored config.json with env variables and values                | $(pwd)/config.json |
| SERVICE_API_URI         | the service-api URI to tests                                                  | https://api.stage.openshift.com |
| SSO_REDHAT_KEYCLOAK_URI | the SSO URI to retrieve the service-api token                                 | https://sso.redhat.com |
| SSO_REDHAT_REALM        | authentication realm for SSO                                                  | redhat-external |
| SSO_REDHAT_CLIENT_ID    | authentication client_id for SSO                                              | cloud-services |
| SSO_REDHAT_REDIRECT_URI | valid redirect_uri for SSO                                                    | https://qaprodauth.cloud.redhat.com |
| SSO_USERNAME            | main user for SSO                                                             |  |
| SSO_PASSWORD            | main user password                                                            |  |
| SSO_SECONDARY_USERNAME  | a second user in the same org as the main user                                |  |
| SSO_SECONDARY_PASSWORD  | the secondary user password                                                   |  |
| SSO_ALIEN_USERNAME      | a third user that is part of a different org respect the main user            |  |
| SSO_ALIEN_PASSWORD      | the alien user password                                                       |  |
| DEV_CLUSTER_SERVER      | the api server url of a openshift cluster with the binding operator installed | https://api.devexp.imkr.s1.devshift.org:6443 |
| DEV_CLUSTER_NAMESPACE   | the namespace to use to install the binding operator CRs                      | mk-e2e-tests |
| DEV_CLUSTER_TOKEN       | the cluster user token (this can also be a service account token)             |  |
| BF2_GITHUB_TOKEN        | a github token to download artifacts from the bf2 org                         |  |
| CLI_VERSION             | the CLI version to download from bf2/cli                                      | 0.15.1 |
| CLI_ARCH                | the CLI arch and os to download from bf2/cli                                  | linux_amd64 |

## List of Tags

| Name | Description | Required Envs |
|------|-------------|---------------|
| service-api               | run all tests targeting the service-api | SSO_USERNAME, SSO_PASSWORD |
| service-api-permissions   | run all service api permissions tests   | SSO_USERNAME, SSO_PASSWORD, SSO_SECONDARY_USERNAME, SSO_SECONDARY_PASSWORD, SSO_ALIEN_USERNAME, SSO_ALIEN_PASSWORD |
| binding-operator          | run all tests for the binding-operator  | SSO_USERNAME, SSO_PASSWORD, DEV_CLUSTER_TOKEN |
| cli                       | run all tests for the CLI               | SSO_USERNAME, SSO_PASSWORD, BF2_GITHUB_TOKEN |

## Report to ReportPortal

When executing the tests is possible to send the results and logs in real time to ReportPortal by set the following
envs:

| Name | Description | Default value |
|------|-------------|---------------|
| rp.endpoint | ReportPortal URL                            | https://reportportal-cloud-services.cloud.paas.psi.redhat.com |
| rp.uuid     | The Access Token                            |  |
| rp.launch   | The launch name to user                     | mk-e2e-test-suite |
| rp.project  | The project where to report the result      | rhosak |
| rp.enable   | Must be set to true to enable ReportPortal  | false |

rp.endpoint=https://reportportal-reportportal.apps.chiron.intlyqe.com/
rp.api.key=ff7fd6a5-4985-4260-805f-bfeeca919536 rp.launch=mk-e2e-test-suite rp.project=default_personal rp.enable=false

## Troubleshooting

#### Recreate the mk-e2e-tests namespace in the dev cluster

1. Login to the dev cluster as kubeadmin
    ```
    oc login --token=KUBE_ADMIN_TOKEN --server=DEV_CLUSTER_SERVER
    ```
2. Execute the `./hack/bootstrap-mk-e2e-tests-namespace.sh` script
    ```
   ./hack/bootstrap-mk-e2e-tests-namespace.sh
    ```
3. Update the [vault](https://vault.devshift.net/ui/vault/secrets/managed-services-ci/show/mk-e2e-test-suite/staging) with the new dev cluster token

## Maintainers

* David Kornel <dkornel@redhat.com>
* Davide Bizzarri <dbizzarr@redhat.com>

