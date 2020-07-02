# `geode-old-versions` Module
This module provides old Geode product versions for use by DUnit upgrade tests.

Subprojects are added to this project from the outside. For example Geode's `settings.gradle` has code like this:

```groovy
['1.0.0-incubating',
  /* etc */
 '1.12.0'].each {
  include 'geode-old-versions:'.concat(it)
}
```

For each subproject this project will download the artifacts (from whatever Maven repositories) are in effect. It'll unpack artifacts into subproject-specific build dirs.

After everything is downloaded, two manifest-like files will be created for use by e.g. `VersionManager`:

*. `geodeOldVersionInstalls.txt`: a map of version to install directory
*. `geodeOldVersionClasspaths.txt`: a map of version to Java classpath

## Testing Your Upgrade Bug Fixes

If you find a Geode rolling upgrade bug that necessitates a patch to an old line of development, you'd like to be able to branch that old line of development and build-and-publish artifacts locally. You'd then like this module to consume your locally-built artifacts instead of the ones acquired from remote Maven repositories.

Here's an example of how you can convince Gradle to use your locally-published artifacts.

Let's say you've found a rolling upgrade bug that necessitates a change to Geode 1.12.0 (an old version.) 

1\. clear, from the Gradle cache, 1.12.0 artifacts downloaded from non-local Maven repos
```shell script
cd geode # from the root of your Git clone
find ./.gradle -name "*1.12.0*" | xargs rm -fr {} \;
find ~/.gradle -name "*1.12.0*" | xargs rm -fr {} \;
```
2\. delete the (synthesized) 1.12.0 subproject
```shell script
rm -fr ./geode-old-versions/1.12.0
```
3\. manually rebuild (via Gradle, not IntelliJ) the `geode-old-versions` module
```shell script
./gradlew :geode-old-versions:build
```
4\. verify we are seeing the locally-built-and-published code (see sha and branch)
```shell script
./geode-old-versions/1.12.0/build/apache-geode-1.12.0/bin/gfsh version --full
```

e.g. I named my 1.12.0-based feature branch `feature/GEODE-8240-1-12-0-version-ordinal` and I see: 

```
Source-Repository: feature/GEODE-8240-1-12-0-version-ordinal
Source-Revision: c38a0aa0df2f89fc657aa4f1e15fc152df32c99c
```

Now any `upgradeTest` e.g. `RollingUpgradeRollServersOnReplicatedRegion_dataserializable` that calls for Geode 1.12.0 will get the locally-published one.