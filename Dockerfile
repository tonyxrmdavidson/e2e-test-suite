FROM registry.access.redhat.com/ubi8/openjdk-11

USER root

COPY rhit-root-ca.crt /etc/pki/ca-trust/source/anchors/
RUN update-ca-trust

RUN microdnf install bind-utils

# Install Kafkacat from
RUN curl https://ftp.lysator.liu.se/pub/opensuse/distribution/leap/15.3/repo/oss/x86_64/librdkafka1-0.11.6-1.3.1.x86_64.rpm -o /tmp/librdkafka1.rpm \
    && curl https://ftp.lysator.liu.se/pub/opensuse/distribution/leap/15.3/repo/oss/x86_64/libyajl2-2.1.0-2.12.x86_64.rpm -o /tmp/libyajl2.rpm \
    && curl https://ftp.lysator.liu.se/pub/opensuse/repositories/network:/utilities/openSUSE_Leap_15.3/x86_64/kafkacat-1.6.0-lp153.1.1.x86_64.rpm -o /tmp/kafkacat.rpm \
    && rpm -i /tmp/librdkafka1.rpm /tmp/libyajl2.rpm /tmp/kafkacat.rpm \
    && ln -s /usr/bin/kafkacat /usr/bin/kcat \
    && kcat -V

USER jboss

ENV HOME            /tmp
ENV _JAVA_OPTIONS   "-Duser.home=${HOME}"
