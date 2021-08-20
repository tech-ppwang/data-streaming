FROM 192.168.0.134/openjdk/openjdk:8-jdk-alpine as build
WORKDIR /workspace/app

COPY target/*.jar .

RUN mkdir -p dependency && (cd dependency; jar -xf ../*.jar)

FROM 192.168.0.134/metersphere/fabric8-java-alpine-openjdk8-jre

LABEL maintainer="FIT2CLOUD <support@fit2cloud.com>"

ARG MS_VERSION=dev
ARG DEPENDENCY=/workspace/app/dependency

COPY --from=build ${DEPENDENCY}/BOOT-INF/lib /app/lib
COPY --from=build ${DEPENDENCY}/META-INF /app/META-INF
COPY --from=build ${DEPENDENCY}/BOOT-INF/classes /app

ENV JAVA_CLASSPATH=/app:/app/lib/*
ENV JAVA_MAIN_CLASS=io.metersphere.streaming.Application
ENV AB_OFF=true
ENV MS_VERSION=${MS_VERSION}
ENV JAVA_OPTIONS=-Dfile.encoding=utf-8

CMD ["/deployments/run-java.sh"]
