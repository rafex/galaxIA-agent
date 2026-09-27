FROM docker.io/library/rust:1.85-bookworm AS build
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
