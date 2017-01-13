#!/bin/bash
set -e

yes | pkgadd -d http://get.opencsw.org/now all 
/opt/csw/bin/pkgutil -U

echo 'PATH=$PATH:/opt/csw/bin; export PATH' >> .profile
echo 'PATH=$PATH:/opt/csw/bin; export PATH' >> .bashrc

echo 'PATH=$PATH:/opt/csw/bin; export PATH' >> /etc/skel/.profile
echo 'PATH=$PATH:/opt/csw/bin; export PATH' >> /etc/skel/.bashrc

