#!/usr/bin/env bash

echo """
This file is intended only as a reference or starting-point for extension developers.
It is not supported by Apache Geode in any way.
This is merely the path one developer took in creating an early iteration of Geodes configuration classes.

Our ultimate goal is to not require an XSD at all and to not use XML as a data container.
For those developers coming from this now-discouraged cache.xml configuration style, however,
  this script can assist in generating configuration container classes consistent with the schemas.

This script expects the configurationService.xsd and all *.xjb files to exist in the Geode root directory.

The classes are generated from a modified cache xsd.
See comments in the configurationService.xsd for more details.
Additional adjustments are made by the use of JAXB bindings files.
See each *-bindings.xjb for more details.
Even with these generation adjustments, the generated classes require additional, manual adjustment.
To reiterate, this script and style of generation is intended as a _starting point_.

To avoid redundant generation, you may include the generated etc/*.episode files as additional bindings.
See the geode-connectors and geode-lucene generation methods as an example.
"""

printf "Press ENTER to generate classes using xjc..."
read
echo

geodeRoot=$(git rev-parse --show-toplevel)

generate-core () {
  projectDir="${geodeRoot}/geode-core"

  sourceXsd="${geodeRoot}/configurationService.xsd"
  classPath="${projectDir}/src/main/java"
  jaxBindings="${geodeRoot}/core-bindings.xjb"

  outputDir="${projectDir}/build/generated-src/main/"
  outputPackage="org.apache.geode.cache.configuration"
  outputEpisode="${geodeRoot}/etc/geode-core.episode"

  mkdir -p ${outputDir}

  command="xjc -extension -p ${outputPackage} -d ${outputDir} -episode ${outputEpisode} -no-header -b ${jaxBindings} -classpath ${classPath} -contentForWildcard -npa ${sourceXsd}"
  printf "Executing: '${command}'\nPress ENTER to continue..."
  read
  ${command}
  echo

}

generate-connectors () {
  projectDir="${geodeRoot}/geode-connectors"

  sourceXsd="${projectDir}/src/main/resources/META-INF/schemas/geode.apache.org/schema/jdbc/jdbc-1.0.xsd"
  jaxBindings="${geodeRoot}/connectors-bindings.xjb"
  classPath="${geodeRoot}/geode-core/src/main/java"
  coreEpisode="${geodeRoot}/etc/geode-core.episode"

  outputDir="${projectDir}/build/generated-src/main/"
  outputPackage="org.apache.geode.cache.configuration"
  outputEpisode="${geodeRoot}/etc/geode-connectors.episode"

  mkdir -p ${outputDir}

  command="xjc -extension -p ${outputPackage} -d ${outputDir} -episode ${outputEpisode} -no-header -b ${jaxBindings} -classpath ${classPath} -contentForWildcard -npa ${sourceXsd}"
  printf "Executing: '${command}'\nPress ENTER to continue..."
  read
  ${command}
  echo

}

generate-lucene () {
  projectDir="${geodeRoot}/geode-lucene"

  sourceXsd="${projectDir}/src/main/resources/META-INF/schemas/geode.apache.org/schema/lucene/lucene-1.0.xsd"
  jaxBindings="${geodeRoot}/lucene-bindings.xjb"
  classPath="${geodeRoot}/geode-core/src/main/java"
  coreEpisode="${geodeRoot}/etc/geode-core.episode"

  outputDir="${projectDir}/build/generated-src/main/"
  outputPackage="org.apache.geode.cache.configuration"
  outputEpisode="${projectDir}/../etc/geode-lucene.episode"

  mkdir -p ${outputDir}

  command="xjc -extension -p ${outputPackage} -d ${outputDir} -episode ${outputEpisode} -no-header -b ${coreEpisode} -b ${jaxBindings} -classpath ${classPath} -contentForWildcard -npa ${sourceXsd}"
  printf "Executing: '${command}'\nPress ENTER to continue..."
  read
  ${command}
  echo
}

generate-core
generate-connectors
generate-lucene
