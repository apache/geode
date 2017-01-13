#!/usr/bin/env bash
#set -x -e -o pipefail

useradd build

(crontab -l -u build ; echo "@reboot /etc/init-user.sh") | sort - | uniq - | crontab -u build -
