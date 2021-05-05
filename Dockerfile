FROM ubuntu:20.04 as builder

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.51.0

ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt update

RUN apt install -y \
    build-essential \
    curl \
    cmake

RUN apt-get update

RUN curl https://sh.rustup.rs -sSf | bash -s -- -y

WORKDIR /kprf
COPY . ./

RUN cargo build --release

FROM ubuntu:20.04

ENV TZ=Europe/Moscow

COPY --from=builder /kprf/target/release/kprf /usr/bin

WORKDIR /usr/bin
RUN chmod +x ./kprf

ENTRYPOINT ["/usr/bin/kprf"]

