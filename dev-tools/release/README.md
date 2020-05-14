This directory contains scripts to help create a release of geode.

Not all release steps have scripts.  Please follow all instructions as documented in the wiki:
https://cwiki.apache.org/confluence/display/GEODE/Releasing+Apache+Geode

Scripts
prepare_rc.sh: Checks out the various geode repos, builds a release candidate, and publishes to nexus staging repo
commit_rc.sh: Pushes the tags and artifacts staged by prepare_rc.sh and then runs print_rc_email.sh
  print_rc_email.sh: Generates an email to send to the dev list announcing a release candidate
promote_rc.sh: Tags an RC as the final release, builds docker images, and starts the mirroring and brew processes
finalize_release.sh: merges to master and cleans up pipelines and branches
