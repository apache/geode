This directory contains scripts to help create a release of geode and manage branches.

Not all release steps have scripts.  Please follow all instructions as documented in the wiki:
https://cwiki.apache.org/confluence/display/GEODE/Releasing+Apache+Geode

These scripts are intended to be run from the parent directory of your geode develop checkout, e.g.
# cd ..
# geode/dev-tools/release/foo.sh

Overview of scripts:

create_support_branches.sh: cuts support/x.y from develop for all projects and walks you through creating pipelines and setting version numbers
set_versions.sh: updates files that need to contain the version number planned for the next release from this support branch
prepare_rc.sh: Checks out the various geode repos, builds a release candidate, and publishes to nexus staging repo
commit_rc.sh: Pushes the tags and artifacts staged by prepare_rc.sh and then runs print_rc_email.sh
  print_rc_email.sh: Generates an email to send to the dev list announcing a release candidate
promote_rc.sh: Tags an RC as the final release, builds docker images, merges to master, and starts the mirroring and brew processes
end_of_support.sh: cleans up pipelines and branches after 9-month life of support branch is reached
