#!/usr/bin/env bash

SERVER_TYPE=""
TEST_RUN_COUNT=10
COMMAND_REPETITION_COUNT=100000
REDIS_HOST=localhost
REDIS_PORT=6379

while getopts ":rgt:c:h:p:" opt; do
  case ${opt} in
  r)
    SERVER_TYPE='redis'
    ;;
  g)
    SERVER_TYPE='geode'
    ;;
  t)
    TEST_RUN_COUNT=${OPTARG}
    ;;
  c)
    COMMAND_REPETITION_COUNT=${OPTARG}
    ;;
  h)
    REDIS_HOST=${OPTARG}
    ;;
  p)
    REDIS_PORT=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0} -r (Redis) | -g (Geode) [-h host] [-p port] [-t (test run count)] [-c (command repetition count)]"
    exit 0
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

if [ -z ${SERVER_TYPE} ]; then
  echo "Please specify native Redis (-r) or Geode Redis (-g)"
  exit 1
fi

SCRIPT_DIR=$(
  cd $(dirname $0)
  pwd
)

function kill_geode() {
  pkill -9 -f ServerLauncher || true
  pkill -9 -f LocatorLauncher || true
  rm -rf server1
  rm -rf locator1
}

nc -zv ${REDIS_HOST} ${REDIS_PORT} 1>&2
SERVER_NOT_FOUND=$?

if [ ${SERVER_TYPE} == "geode" ]; then
  if [ ${SERVER_NOT_FOUND} -eq 0 ]; then
    echo "Redis server detected already running at port ${REDIS_PORT}}"
    echo "Please stop sever before running this script"
    exit 1
  fi

  GEODE_BASE=$(
    cd $SCRIPT_DIR/../../..
    pwd
  )

  cd $GEODE_BASE

  kill_geode

  ./gradlew devBuild installD

  GFSH=$PWD/geode-assembly/build/install/apache-geode/bin/gfsh

  $GFSH -e "start locator --name=locator1"

  $GFSH -e "start server
          --name=server1
          --log-level=none
          --locators=localhost[10334]
          --server-port=0
          --redis-port=6379
          --redis-bind-address=127.0.0.1"
else
  if [ ${SERVER_NOT_FOUND} -eq 1 ]; then
    echo "No Redis server detected on host '${REDIS_HOST}' at port '${REDIS_PORT}'"
    exit 1
  fi
fi

cd ${SCRIPT_DIR}

./aggregator.sh -h ${REDIS_HOST} -p ${REDIS_PORT} -t "${TEST_RUN_COUNT}" -c "${COMMAND_REPETITION_COUNT}"

if [ ${SERVER_TYPE} == "geode" ]; then
  kill_geode
  sleep 1 # Back to back runs need this delay or 'nc' doesn't detect shutdown correctly
fi
