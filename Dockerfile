FROM ubuntu:20.04
ENV LC_ALL C.UTF-8
ENV LANG C.UTF-8
ENV DEBIAN_FRONTEND noninteractive
ARG build_type=Release

RUN apt-get -qq update && \
     apt-get -qq install -y --no-install-recommends --no-install-suggests \
     ca-certificates curl wget git-core \
     build-essential cmake valgrind clang-format \
     sqlite3 libsqlite3-dev libzstd-dev \
     python3-pytest pylint black aria2 zstd samtools

ADD . /work
WORKDIR /work

RUN cmake -DCMAKE_BUILD_TYPE=$build_type . -B build && cmake --build build

WORKDIR /work/build
CMD ctest -V
