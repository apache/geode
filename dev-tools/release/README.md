# geode/dev-tools/release
This directory contains scripts to help create a release of Geode and manage branches.

Not all release steps have scripts.  Please follow all instructions as documented in the wiki: [Releasing Apache Geode](https://cwiki.apache.org/confluence/display/GEODE/Releasing+Apache+Geode).

These scripts are intended to be run from the parent directory of your geode develop checkout, e.g.:

    $ cd ..
    $ geode/dev-tools/release/foo.sh

## Overview of scripts

**license\_review.sh** compares versions with a previous release and/or checks that all bundled dependencies are noted in appropriate LICENSE file

**create\_support\_branches.sh** cuts support/x.y from develop for all projects and walks you through creating pipelines and setting version numbers

**set\_copyright.sh** updates the copyright year

**set\_versions.sh** updates files that need to contain the version number planned for the next release from this support branch

**prepare\_rc.sh** Checks out the various geode repos, builds a release candidate, and publishes to nexus staging repo

**commit\_rc.sh** Pushes the tags and artifacts staged by prepare\_rc.sh and then runs print\_rc\_email.sh

**print\_rc\_email.sh** Generates an email to send to the dev list announcing a release candidate

**promote\_rc.sh** Tags an RC as the final release, builds docker images, merges to master, uploads to mirrors, and starts the brew process

**print\_annouce\_email.sh** Generates an email to send to all lists announcing a release

**end\_of\_support.sh** cleans up pipelines and branches after N-2 support lifetime is reached
