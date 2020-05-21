#!/bin/sh
# use this script to get rid of containers when test execution in gradle leaves them
# up, or when you've created the containers for command-line debugging, as in...

# in the sni directory, use
#   docker-compose up
# then use this to rsh to the container
#   docker-compose exec geode sh

docker kill haproxy geode
docker rm haproxy geode
