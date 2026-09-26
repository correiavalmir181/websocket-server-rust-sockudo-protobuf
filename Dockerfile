FROM scratch

COPY ./target/x86_64-unknown-linux-musl/release/meu-websocket-server /app/server

EXPOSE 8080

ENTRYPOINT ["/app/server"]
