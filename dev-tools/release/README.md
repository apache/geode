This directory contains scripts to help create a release of geode.

Most of the release steps are documented in the wiki. 
See https://cwiki.apache.org/confluence/display/GEODE/Releasing+Apache+Geode

Scripts
prepare_rc.sh: Checks out the various geode repos, builds a release candidate, and publishes to nexus staging repo
commit_rc.sh: Pushes the tags and artifacts staged by prepare_rc.sh and then runs print_rc_email.sh 
  print_rc_email.sh: Generates an email to send to the dev list announcing a release candidate
promote_rc.sh: Makes an RC the final release and starts the mirroring process
finalize_release.sh: Makes the docker and brew releases, etc
