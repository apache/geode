#!/usr/bin/env bash
set -x -e -o pipefail

# Remove meta-packages that prevent installation of updated tools
pkg uninstall -v entire consolidation/userland/userland-incorporation consolidation/java-8/java-8-incorporation

# Install required tools
pkg install -v --accept \
    system/header \
    developer/assembler \
    developer/documentation-tool/doxygen \
    developer/java/jdk-8 \
    text/gnu-patch

# broken     developer/build/gnu-make \


# too old developer/build/cmake
/opt/csw/bin/pkgutil -i -y cmake gmake gtar

# dependent perl package conflict developer/versioning/git
/opt/csw/bin/pkgutil -i -y git
