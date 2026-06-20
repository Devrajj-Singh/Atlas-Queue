# Multi-stage builds keep the runtime image small and avoid shipping build
# tooling or source code with the final application artifact.
FROM rust:1-slim AS builder

WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim

COPY --from=builder /app/target/release/atlas-queue /usr/local/bin/atlas-queue
EXPOSE 3000

CMD ["atlas-queue"]
