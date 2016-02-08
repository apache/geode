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
  private static final String TOOLS_PULSE_WAR = "/tools/Pulse/pulse.war";
  private static final String LIB_PULSE_WAR = "/lib/pulse.war";
  public static final String ERROR_VARIABLE_NOT_SET = "The GEMFIRE environment variable must be set!";

  private String gemfireVersion = null;

  public AgentUtil(String gemfireVersion) {
    this.gemfireVersion = gemfireVersion;
  }

  public String getGemFireWebApiWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : ERROR_VARIABLE_NOT_SET;

    String toolsWebApiWar = gemfireHome + TOOLS_WEB_API_WAR + gemfireVersion + ".war";
    String libWebApiWar = gemfireHome + LIB_WEB_API_WAR + gemfireVersion + ".war";

    if (new File(toolsWebApiWar).isFile()) {
      logger.info("GemFire Dev REST API war: {}", toolsWebApiWar);
      return toolsWebApiWar;
    } else if (new File(libWebApiWar).isFile()) {
      logger.info("GemFire Dev REST API war: {}", libWebApiWar);
      return libWebApiWar;
    } else {
      logger.warn("GemFire Dev REST API war not found - neither {} or {} exist", toolsWebApiWar, libWebApiWar);
      return null;
    }
  }

  /*
   * Use the GEMFIRE environment variable to find the GemFire product tree.
   * First, look in the $GEMFIRE/tools/Management directory Second, look in the
   * $GEMFIRE/lib directory Finally, if we cannot find Management WAR file then
   * return null...
   */
  public String getGemFireWebWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : ERROR_VARIABLE_NOT_SET;

    String toolsWebWar = gemfireHome + TOOLS_WEB_WAR + gemfireVersion + ".war";
    String libWebWar = gemfireHome + LIB_WEB_WAR + gemfireVersion + ".war";

    if (new File(toolsWebWar).isFile()) {
      logger.info("GemFire Admin REST war: {}", toolsWebWar);
      return toolsWebWar;
    } else if (new File(libWebWar).isFile()) {
      logger.info("GemFire Admin REST war: {}", libWebWar);
      return libWebWar;
    } else {
      logger.warn("GemFire Admin REST war not found - neither {} or {} exist", toolsWebWar, libWebWar);
      return null;
    }
  }

  // Use the GEMFIRE environment variable to find the GemFire product tree.
  // First, look in the $GEMFIRE/tools/Pulse directory
  // Second, look in the $GEMFIRE/lib directory
  // Finally, if we cannot find the Management WAR file then return null...
  public String getPulseWarLocation() {
    String gemfireHome = getGemFireHome();
    assert !StringUtils.isBlank(gemfireHome) : ERROR_VARIABLE_NOT_SET;

    String toolsPulseWar = gemfireHome + TOOLS_PULSE_WAR;
    String libPulseWar = gemfireHome + LIB_PULSE_WAR;

    if (new File(toolsPulseWar).isFile()) {
      logger.info("GemFire Pulse war: {}", toolsPulseWar);
      return toolsPulseWar;
    } else if (new File(libPulseWar).isFile()) {
      logger.info("GemFire Pulse war: {}", libPulseWar);
      return libPulseWar;
    } else {
      logger.warn("GemFire Pulse war not found - neither {} or {} exist", toolsPulseWar, libPulseWar);
      return null;
    }
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