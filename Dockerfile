FROM registry.access.redhat.com/ubi8/openjdk-11 as builder

COPY --chown=jboss . /home/jboss/test-suite
WORKDIR /home/jboss/test-suite
RUN mvn verify -Psmoke

RUN chmod go+w -R target

ENV _JAVA_OPTIONS="-Duser.home=${HOME}"

ENTRYPOINT ["./hack/testrunner.sh"]
