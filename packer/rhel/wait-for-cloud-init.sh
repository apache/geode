#!/usr/bin/env bash

#set -x
set -e
set -o pipefail

# leaves tail running but we should be restarting anyway
{ tail -n +1 -f /var/log/cloud-init.log /var/log/cloud-init-output.log & } | sed \
		-e '/Cloud-init .* finished/q' \
		-e '/Failed at merging in cloud config/q1'
