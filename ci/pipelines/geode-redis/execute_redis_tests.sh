#!/usr/bin/env bash

wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub
wget https://github.com/sgerrand/alpine-pkg-glibc/releases/download/2.31-r0/glibc-2.31-r0.apk
apk add glibc-2.31-r0.apk

pushd geode
./gradlew devbuild installD

./geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --redis-port=6380 \
  --redis-bind-address=127.0.0.1 \
  --redis-password=foobar

cd ../redis-fork
./runtest --host 127.0.0.1 --port 6380 --single unit/auth

../geode/geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --server-port=0 \
  --redis-port=6379 \
  --redis-bind-address=127.0.0.1

./runtest --host 127.0.0.1 --port 6379 --single unit/type/set --single unit/expire
