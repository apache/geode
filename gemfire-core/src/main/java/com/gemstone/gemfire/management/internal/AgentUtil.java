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
  private static final String TOOLS_WEB_API_WAR = "/tools/Extensions/gemfire-web-api-";
  private static final String LIB_WEB_API_WAR = "/lib/gemfire-web-api-";
  private static final String TOOLS_WEB_WAR = "/tools/Extensions/gemfire-web-";
  private static final String LIB_WEB_WAR = "/lib/gemfire-web-";
  private static final String TOOLS_API_WAR = "/tools/Extensions/gemfire-api-";
  private static final String LIB_API_WAR = "/lib/gemfire-api-";
  private static final String TOOLS_PULSE_WAR = "/tools/Pulse/pulse.war";
  private static final String LIB_PULSE_WAR = "/lib/pulse.war";

  private String gemfireVersion = null;

  public AgentUtil(String gemfireVersion) {
    this.gemfireVersion = gemfireVersion;
  }

  String getGemFireWebApiWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";
    logger.warn(gemfireHome + TOOLS_WEB_API_WAR + gemfireVersion + ".war");

    if (new File(gemfireHome + TOOLS_WEB_API_WAR + gemfireVersion + ".war").isFile()) {
      return gemfireHome + TOOLS_WEB_API_WAR + gemfireVersion + ".war";
    } else if (new File(gemfireHome + LIB_WEB_API_WAR + gemfireVersion + ".war").isFile()) {
      return gemfireHome + LIB_WEB_API_WAR + gemfireVersion + ".war";
    } else {
      return null;
    }
  }

  /*
   * Use the GEMFIRE environment variable to find the GemFire product tree.
   * First, look in the $GEMFIRE/tools/Management directory Second, look in the
   * $GEMFIRE/lib directory Finally, if we cannot find Management WAR file then
   * return null...
   */
  String getGemFireWebWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";

    if (new File(gemfireHome + TOOLS_WEB_WAR + gemfireVersion + ".war").isFile()) {
      return gemfireHome + TOOLS_WEB_WAR + gemfireVersion + ".war";
    } else if (new File(gemfireHome + LIB_WEB_WAR + gemfireVersion + ".war").isFile()) {
      return gemfireHome + LIB_WEB_WAR + gemfireVersion + ".war";
    } else {
      return null;
    }
  }

  String getGemfireApiWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";

    if (new File(gemfireHome + TOOLS_API_WAR + gemfireVersion + ".war").isFile()) {
      return gemfireHome + TOOLS_API_WAR + gemfireVersion + ".war";
    } else if (new File(gemfireHome + LIB_API_WAR + gemfireVersion + ".war").isFile()) {
      return gemfireHome + LIB_API_WAR + gemfireVersion + ".war";
    } else {
      return null;
    }
  }

  // Use the GEMFIRE environment variable to find the GemFire product tree.
  // First, look in the $GEMFIRE/tools/Pulse directory
  // Second, look in the $GEMFIRE/lib directory
  // Finally, if we cannot find the Management WAR file then return null...
  String getPulseWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : "The GEMFIRE environment variable must be set!";

    if (new File(gemfireHome + TOOLS_PULSE_WAR).isFile()) {
      return gemfireHome + TOOLS_PULSE_WAR;
    } else if (new File(gemfireHome + LIB_PULSE_WAR).isFile()) {
      return gemfireHome + LIB_PULSE_WAR;
    } else {
      return null;
    }
  }

  boolean isWebApplicationAvailable(final String warFileLocation) {
    return !StringUtils.isBlank(warFileLocation);
  }

  boolean isWebApplicationAvailable(final String... warFileLocations) {
    for (String warFileLocation : warFileLocations) {
      if (isWebApplicationAvailable(warFileLocation)) {
        return true;
      }
    }

    return false;
  }

  String getGemFireHome() {

    String gemFireHome = System.getenv("GEMFIRE");

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

  boolean isGemfireHomeDefined() {
    String gemfireHome = getGemFireHome();
    return !StringUtils.isBlank(gemfireHome);
  }
}