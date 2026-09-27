# rig-core 0.42 usa la edición 2024; se fija la misma toolchain con la que se
# desarrolla y prueba. prost-build necesita protoc del sistema.
FROM docker.io/library/rust:1.97-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY protocol ./protocol
COPY src ./src
RUN cargo build --release

FROM docker.io/library/debian:bookworm-slim
RUN useradd --create-home --uid 10001 galaxia
COPY --from=build /src/target/release/galaxia-agent /usr/local/bin/galaxia-agent
USER galaxia
EXPOSE 8090
ENTRYPOINT ["/usr/local/bin/galaxia-agent"]
