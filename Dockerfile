FROM rust:1.91-slim-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src
COPY examples ./examples
COPY tests ./tests

RUN cargo build --release --bin pulld

FROM gcr.io/distroless/cc-debian12:nonroot

LABEL org.opencontainers.image.title="pulld"
LABEL org.opencontainers.image.description="Rust registry pull-through proxy with local caching"
LABEL org.opencontainers.image.source="https://github.com/mkoyan44/pulld"
LABEL org.opencontainers.image.licenses="MIT"

COPY --from=builder /app/target/release/pulld /usr/local/bin/pulld

EXPOSE 5050
VOLUME ["/var/lib/pulld/cache"]

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/pulld"]
CMD ["/var/lib/pulld/cache"]
