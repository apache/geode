#!/usr/bin/env bash
set -x -e -o pipefail

# Remove meta-packages that prevent installation of updated tools
pkg uninstall -v entire consolidation/userland/userland-incorporation consolidation/java-8/java-8-incorporation

# Install required tools
pkg install -v --accept \
    developer/java/jdk-8 \
    archiver/gnu-tar

# too old developer/build/cmake
/opt/csw/bin/pkgutil -i -y cmake
