FROM eclipse-temurin:17.0.15_6-jdk-jammy

ARG SBT_VERSION=1.10.11
ARG PROTOC_VERSION=25.5
ENV SBT_OPTS="-Xmx2g -Dsbt.override.build.repos=true"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl unzip \
    && rm -rf /var/lib/apt/lists/*

# Pinned protoc binary (libprotoc 25.5) for the smoke.desc descriptor set and
# the proto3 optional check. PB.protocVersion in build.sbt is pinned to match.
RUN curl -fsSL "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip" -o /tmp/protoc.zip \
    && unzip -o /tmp/protoc.zip -d /usr/local \
    && rm /tmp/protoc.zip \
    && protoc --version

RUN curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
    | tar -xz -C /opt \
    && ln -s /opt/sbt/bin/sbt /usr/local/bin/sbt

ENV MIRROR="https://maven-central.storage-download.googleapis.com/maven2/"
ENV COURSIER_REPOSITORIES="${MIRROR}"
RUN mkdir -p /root/.sbt \
    && printf '[repositories]\nlocal\nmaven-central: %s\n' "$MIRROR" > /root/.sbt/repositories

WORKDIR /app
COPY docker/spark-hello/ /app/
COPY docker/check-proto-optional.sh /app/check-proto-optional.sh
COPY proto/ /app/proto/

RUN protoc -I proto --descriptor_set_out=/app/smoke.desc --include_imports proto/smoke.proto

RUN sbt update compile

ENTRYPOINT ["sbt", "run"]
