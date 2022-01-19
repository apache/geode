/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.internal;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Hosts common utility methods needed by the management package
 *
 * @since Geode 1.0.0
 */
public class AgentUtil {

  private static final Logger logger = LogService.getLogger();

  private final String gemfireVersion;
  private static final String GEODE_HOME = "GEODE_HOME";

  public AgentUtil(String gemfireVersion) {
    this.gemfireVersion = gemfireVersion;
  }

  /**
   * this method will try to find the named war files in the following order: 1. if GEODE_HOME is
   * defined, it will look under tools/Extensions, tools/Pulse and lib folder (in this order) to
   * find either the name-version.war or the name.war file 2. If GEODE_HOME is not defined, it will
   * try to find either the name-version.war/name.war (in that order) on the classpath
   *
   * @param warFilePrefix : the prefix of the war file, e.g. geode-web, geode-pulse, or
   *        geode-web-api
   */
  public URI findWarLocation(String warFilePrefix) {
    final String versionedWarFileName = warFilePrefix + "-" + gemfireVersion + ".war";
    final String unversionedWarFileName = warFilePrefix + ".war";

    // This will attempt to find the war file defined somewhere on the Java classpath,
    // other than the
    URI possiblePath =
        lookupWarLocationFromClasspath(versionedWarFileName, unversionedWarFileName);
    if (possiblePath != null) {
      logger.info("Located war: {} at location: {}", warFilePrefix, possiblePath);
      return possiblePath;
    }
    possiblePath =
        findPossibleWarLocationFromGeodeHome(versionedWarFileName, unversionedWarFileName);
    if (possiblePath != null) {
      logger.info("Located war: {} at location: {}", warFilePrefix, possiblePath);
      return possiblePath;
    }
    // if $GEODE_HOME is not set or we are not able to find it in all the possible locations under
    // $GEODE_HOME, try to find in the classpath
    possiblePath =
        findPossibleWarLocationFromExtraLocations(versionedWarFileName, unversionedWarFileName);
    if (possiblePath != null) {
      logger.info("Located war: {} at location: {}", warFilePrefix, possiblePath);
      return possiblePath;
    }

    logger.warn(warFilePrefix + " war file was not found");
    return null;
  }

  private URI findPossibleWarLocationFromExtraLocations(String versionedWarFileName,
      String unversionedWarFileName) {
    final URL url = Arrays.stream(new String[] {versionedWarFileName,
        "tools/Pulse/" + versionedWarFileName,
        "tools/Extensions/" + versionedWarFileName,
        "lib/" + versionedWarFileName,
        unversionedWarFileName})
        .map(possibleFile -> getClass().getClassLoader().getResource(possibleFile))
        .filter(Objects::nonNull).findFirst().orElse(null);

    URI uri = null;
    if (url != null) {
      try {
        uri = url.toURI();
        logger.info("War file found: {}", uri.toString());
      } catch (URISyntaxException ex) {
        logger.warn("War file URL could not be converted to URI: {}", url.toString());
      }
    }
    return uri;
  }

  private URI findPossibleWarLocationFromGeodeHome(String versionedWarFileName,
      String unversionedWarFileName) {
    String[] possibleFiles = {};
    String geodeHome = getGeodeHome();
    if (StringUtils.isNotBlank(geodeHome)) {
      possibleFiles = new String[] {geodeHome + "/tools/Extensions/" + versionedWarFileName,
          geodeHome + "/tools/Pulse/" + versionedWarFileName,
          geodeHome + "/lib/" + versionedWarFileName,
          geodeHome + "/tools/Extensions/" + unversionedWarFileName,
          geodeHome + "/tools/Pulse/" + unversionedWarFileName,
          geodeHome + "/lib/" + unversionedWarFileName};
    }
    return findPossibleWarLocationFromStream(Arrays.stream(possibleFiles));
  }

  private URI findPossibleWarLocationFromStream(Stream<String> stream) {
    return stream.filter(possiblePath -> new File(possiblePath).isFile())
        .findFirst().map(s -> new File(s).toURI()).orElse(null);
  }

  private URI lookupWarLocationFromClasspath(String versionedWarFileName,
      String unversionedWarFileName) {
    return Arrays
        .stream(System.getProperty("java.class.path").split(File.pathSeparator))
        .filter(pathString -> pathString.endsWith(versionedWarFileName) || pathString
            .endsWith(unversionedWarFileName))
        .findFirst().map(s -> new File(s).toURI()).orElse(null);
  }

  boolean isAnyWarFileAvailable(final URI... warFileLocations) {
    return Arrays.stream(warFileLocations).anyMatch(Objects::nonNull);
  }

  private String getGeodeHome() {

    String geodeHome = System.getenv(GEODE_HOME);

    logger.info(GEODE_HOME + ":" + geodeHome);
    // Check for empty variable. if empty, then log message and exit HTTP server
    // startup
    if (StringUtils.isBlank(geodeHome)) {
      geodeHome = System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "home");
      logger.info("Reading gemfire.home System Property -> {}", geodeHome);
      if (StringUtils.isBlank(geodeHome)) {
        logger.info("GEODE_HOME environment variable not set; HTTP service will not start.");
        geodeHome = null;
      }
    }
    return geodeHome;
  }

  public boolean isGemfireHomeDefined() {
    String gemfireHome = getGeodeHome();
    return StringUtils.isNotBlank(gemfireHome);
  }
}
