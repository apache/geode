#!/usr/bin/env bash
set -x -e -o pipefail

useradd -m -s /usr/bin/bash build

if (! which expect > /dev/null); then
  pkg install  -v --accept shell/expect
fi

chmod +x ./changepasswd
./changepasswd build p1votal!