Bumping dependencies on develop should be performed after a release, but well in advance
of cutting the next release, to allow time for any subtle issues to surface.

The Release Manager will call for a volunteer to update dependencies.  Here are some 
scripts that may be helpful.

Step 0: Create a JIRA ticket for this work.

Step 1: List bump commands for all dependencies for which maven offers a newer version:

cd geode
dev-tools/dependencies/bump.sh <jira you will be committing this work under> -l

Step 2: In some cases, maven suggests beta releases, which Geode should not use.
Manually search for those dependencies on mavencentral to see if there is a better choice.
Special cases:
- tomcat6 (do not upgrade)
- tomcat (upgrade to latest patch only for each of 7, 8.5, and 9)

Step 3: Create a PR and start bumping dependencies.  Push to the PR every few to run PR
checks.  Later, review the PR checks and try to narrow down which bump introduced problems
and revert it.  At the end, create separate PRs for each one that was problematic and ask
for help from someone in the community who knows that area better.

To bump a dependency, in most cases there are 2-4 files that will be touched, but
sometimes more.  Each bump command provided in step 1 just attempts to do a search-and-
replace on the old version number, then a git add -p.  Very carefully answer 'y' to
changes where the matched string was the version number of the library you are bumping.
Answer 'n' to anything else.  The bump script will then create a commit and run a gradlew
devBuild to smoke-test the new version.  Minor touchups may be needed, e.g. replacing a
call to a deprecated method.  Use commit --amend to add any such fixups.

Note: the dependency-name argument to bump.sh is just used in the commit message.  It
does not affect the grep.  If you have several dependencies with the same version, one 
bump command will bump them all, so shorten, e.g. just "jetty" instead of "jetty-server".