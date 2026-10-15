FROM debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY substrate_actor /usr/local/bin/substrate_actor
EXPOSE 80
ENTRYPOINT ["/usr/local/bin/substrate_actor"]
