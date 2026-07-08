# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

ARG BASE_IMAGE_TAG=jdk
ARG OTEL_VERSION=2.28.1
FROM otel/autoinstrumentation-java:${OTEL_VERSION} AS otel
RUN /bin/sha256sum /javaagent.jar

FROM europe-docker.pkg.dev/da-images/public/docker/da-base-image:${BASE_IMAGE_TAG}
ARG BASE_IMAGE_TAG
LABEL org.opencontainers.image.base.name="europe-docker.pkg.dev/da-images/public/docker/da-base-image:$BASE_IMAGE_TAG"

COPY LICENSE.txt /LICENSE.txt
COPY --from=otel /javaagent.jar /open-telemetry.jar

ENV JAVA_TOOL_OPTIONS="-javaagent:/open-telemetry.jar -Djdk.attach.allowAttachSelf"
ENV OTEL_INSTRUMENTATION_MICROMETER_ENABLED=true
ENV OTEL_INSTRUMENTATION_RUNTIME_TELEMETRY_EMIT_EXPERIMENTAL_TELEMETRY=true
ENV OTEL_SERVICE_NAME=scribe
ENV OTEL_TRACES_EXPORTER=none
ENV OTEL_LOGS_EXPORTER=none
ENV OTEL_METRICS_EXPORTER=prometheus
ENV OTEL_EXPORTER_PROMETHEUS_PORT=9090
ENV DA_DIAGNOSTICS_ENABLED=true
# ENV DA_DIAGNOSTICS_HOST=0.0.0.0
ENV DA_DIAGNOSTICS_PORT=9091
# ENV DA_DIAGNOSTICS_DUMP_PATH=...
ENV DA_DIAGNOSTICS_METRICS_INTERVAL=PT10S
ENV DA_DIAGNOSTICS_METRICS_BUFFER_SIZE=360
ENV DA_DIAGNOSTICS_METRICS_TAGS=job=scribe
ENV DA_DIAGNOSTICS_THREADS_INTERVAL=PT1M
ENV DA_DIAGNOSTICS_THREADS_BUFFER_SIZE=60

# set default workdir
WORKDIR /

ENTRYPOINT ["java", "-jar", "scribe.jar"]
