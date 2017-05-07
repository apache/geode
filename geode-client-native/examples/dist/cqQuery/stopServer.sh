#!/bin/bash

if [ -z ${GEMFIRE:-} ]; then
  echo GEMFIRE is not set.
  exit 1
fi


echo Stop GemFire Server

export PATH="${PATH}:${GEMFIRE}/bin"


cacheserver stop -dir=gfecs

echo Stopped Server


