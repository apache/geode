# geode/dev-tools/release
This directory contains scripts to help committers and PMC members validate a Geode
release candidate.

These scripts are intended to be run from the parent directory of your geode develop checkout, e.g.:

    $ cd ..
    $ geode/dev-tools/release-testing/smoketest_rc.sh

## Overview of scripts

**smoketest\_rc.sh** - Run automated testing of a release candidate, such as validating signatures and building from source
