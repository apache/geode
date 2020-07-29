#!/usr/bin/env bash

cd ..
git clone https://github.com/prettyClouds/redis.git
pushd redis
git co tests-geode-redis
popd

./geode/geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --redis-port=6380 \
  --redis-bind-address=127.0.0.1 \
  --redis-password=foobar

cd redis
./runtest --host 127.0.0.1 --port 6380 --single unit/auth

../geode/geode-assembly/build/install/apache-geode/bin/gfsh start server \
  --J=-Denable-redis-unsupported-commands=true \
  --name=server1 \
  --server-port=0 \
  --redis-port=6379 \
  --redis-bind-address=127.0.0.1

./runtest --host 127.0.0.1 --port 6379 --single unit/type/set --single unit/expire
