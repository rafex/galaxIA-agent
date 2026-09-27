# rig-core 0.42 usa la edición 2024; se fija la misma toolchain con la que se
# desarrolla y prueba. prost-build necesita protoc del sistema.
FROM docker.io/library/rust:1.97-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
ARG COMMIT_HASH=dev
ENV GALAXIA_COMMIT=$COMMIT_HASH
COPY Cargo.toml Cargo.lock build.rs ./
COPY protocol ./protocol
# Copia de libp2p-websocket con `tls::Config::from_rustls` ([patch.crates-io]).
COPY vendor ./vendor
COPY src ./src
RUN cargo build --release

FROM docker.io/library/debian:bookworm-slim
ARG BUILD_DATE=""
ENV BUILD_DATE=$BUILD_DATE
RUN useradd --create-home --uid 10001 galaxia
COPY --from=build /src/target/release/galaxia-agent /usr/local/bin/galaxia-agent
# Para reutilizar el volumen `navigator-data` del Navigator TS (archivo de
# identidad de root, 0600) se corre con `--user 0` en podman rootless.
USER galaxia
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/galaxia-agent"]
