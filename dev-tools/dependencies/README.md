Bumping dependencies on develop should be performed after a release, but well in advance
of cutting the next release, to allow time for any subtle issues to surface.

The Release Manager will call for a volunteer to update dependencies.  Here are some 
scripts that may be helpful.

Step 1: list all dependencies for which maven is aware of a newer version:

cd geode
dev-tools/dependencies/bump.sh -l

Step 2: filter out certain dependencies that we cannot change, such as:
- jgroups
- classgraph
- gradle-tooling-api
- JUnitParams
- docker-compose-rule
- javax.servlet-api
- protobuf
- lucene
- tomcat 6
- archunit (13.0 and later get OOM on JDK8)

Step 3: in some cases, maven suggests new majors, beta releases, or just wrong releases.
Manually search for those dependencies on mavencentral to see if there is a better choice.
Examples include:
- commons-collections (versioning back in 2004 predated semver)
- springfox-swagger (stay on 2.9, as 2.10 and later is completely re-architected)
- selenium-api (these tests are very old, so stay on version pi)

Step 4: create a PR and start bumping dependencies.  Push to the PR every few to run PR
checks.  Later, review the PR checks and try to narrow down which bump introduced problems
and revert it.  At the end, create separate PRs for each one that was problematic and ask
for help from someone in the community who knows that area better.

To bump a dependency, in most cases there are 2-4 files that will be touched, but
sometimes more.  The bump commands provided in step 1 just attempt to do a search-and-
replace on the old version number, then a git add -p.  Very carefully answer 'y' to
changes where the matched string was the version number of the library you are bumping.
Answer 'n' to anything else.  The bump script will then create a commit and run a gradlew
devBuild to smoke-test the new version.  Minor changes may be needed, e.g. replacing a
call to a deprecated method.  Use commit --amend to add any such fixups.

The first arg to bump.sh (the dependency name) is just used in the commit message.  It
does not affect the grep.  If you have several dependencies with the same version, one 
bump command will bump them all, so shorten, e.g. just "jetty" instead of "jetty-server".