#!/bin/bash

if [ -z ${GFCPP:-} ]; then
  echo GFCPP is not set.
  exit 1
fi

exname='Updater'

echo Running GemFire C++ example ${exname} ...

export PATH="${PATH}:${GEMFIRE}/bin"

${exname} $*

echo Finished example ${exname}.


