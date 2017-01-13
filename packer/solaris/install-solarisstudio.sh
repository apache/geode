#!/bin/bash
set -e

pkg set-publisher \
    -k /var/pkg/ssl/pkg.oracle.com.key.pem \
    -c /var/pkg/ssl/pkg.oracle.com.certificate.pem \
    -G '*' -g https://pkg.oracle.com/solarisstudio/release solarisstudio

pkg install -v solarisstudio-124/c++  solarisstudio-124/dbx

echo 'PATH=$PATH:/opt/solarisstudio12.4/bin; export PATH' >> ~/.profile

echo 'PATH=$PATH:/opt/solarisstudio12.4/bin; export PATH' >> ~/.bashrc
echo 'PATH=$PATH:/opt/csw//bin; export PATH' >> ~/.bashrc
