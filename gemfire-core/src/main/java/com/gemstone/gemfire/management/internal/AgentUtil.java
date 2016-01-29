/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gemstone.gemfire.management.internal;

import java.io.File;
import java.net.URL;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Hosts common utility methods needed by the management package
 * 
 * @author Wes Williams
 * @since Geode 1.0.0.0
 *
 */
public class AgentUtil {

  private static final Logger logger = LogService.getLogger();

  public static final String ERROR_VARIABLE_NOT_SET = "The GEMFIRE environment variable must be set!";

  private String gemfireVersion = null;

  public AgentUtil(String gemfireVersion) {
    this.gemfireVersion = gemfireVersion;
  }

  /**
   * this method will try to find the named war files in the following order:
   * 1. if GEMFIRE is defined, it will look under tools/Extensions, tools/Pulse and lib folder (in this order) to find
   *    either the name-version.war or the name.war file
   * 2. If GEMFIRE is not defined, it will try to find either the name-version.war/name.war (in that order) on the
   *    classpath
   *
   * @param warFilePrefix : the prefix of the war file, e.g. gemfire-web, gemfire-pulse, or gemfire-web-api
     */
  public String findWarLocation(String warFilePrefix) {
    String gemfireHome = getGemFireHome();
    if(!StringUtils.isBlank(gemfireHome)) {
      String[] possibleFiles = {
              gemfireHome + "/tools/Extensions/" + warFilePrefix + "-" + gemfireVersion + ".war",
              gemfireHome + "/tools/Pulse/" + warFilePrefix + "-" + gemfireVersion + ".war",
              gemfireHome + "/lib/" + warFilePrefix + "-" + gemfireVersion + ".war",
              gemfireHome + "/tools/Extensions/" + warFilePrefix + ".war",
              gemfireHome + "/tools/Pulse/" + warFilePrefix + ".war",
              gemfireHome + "/lib/" + warFilePrefix + ".war"
      };
      for (String possibleFile : possibleFiles) {
        if (new File(possibleFile).isFile()) {
          logger.info(warFilePrefix + " war found: {}", possibleFile);
          return possibleFile;
        }
      }
    }

    // if $GEMFIRE is not set or we are not able to find it in all the possible locations under $GEMFIRE, try to
    // find in the classpath
    String[] possibleFiles = {
            warFilePrefix + "-" + gemfireVersion + ".war",
            warFilePrefix + ".war"
    };
    for(String possibleFile:possibleFiles){
      URL url = this.getClass().getClassLoader().getResource(possibleFile);
      if(url!=null){
        // found the war file
        logger.info(warFilePrefix + " war found: {}", possibleFile);
        return url.getPath();
      }
    }

    // we still couldn't find the war file
    logger.warn(warFilePrefix+" war file was not found");
    return null;
  }

  public boolean isWebApplicationAvailable(final String warFileLocation) {
    return !StringUtils.isBlank(warFileLocation);
  }

  public boolean isWebApplicationAvailable(final String... warFileLocations) {
    for (String warFileLocation : warFileLocations) {
      if (isWebApplicationAvailable(warFileLocation)) {
        return true;
      }
    }

    return false;
  }

  public String getGemFireHome() {

    String gemFireHome = System.getenv("GEMFIRE");

    logger.info("GEMFIRE HOME:" + gemFireHome);
    // Check for empty variable. if empty, then log message and exit HTTP server
    // startup
    if (StringUtils.isBlank(gemFireHome)) {
      gemFireHome = System.getProperty("gemfire.home");
      logger.info("Reading gemfire.home System Property -> {}", gemFireHome);
      if (StringUtils.isBlank(gemFireHome)) {
        logger.info("GEMFIRE environment variable not set; HTTP service will not start.");
        gemFireHome = null;
      }
    }
    return gemFireHome;
  }

  public boolean isGemfireHomeDefined() {
    String gemfireHome = getGemFireHome();
    return !StringUtils.isBlank(gemfireHome);
  }
}
