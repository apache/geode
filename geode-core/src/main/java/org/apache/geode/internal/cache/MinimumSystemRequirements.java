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

package com.gemstone.gemfire.internal.cache;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Checks that minimum system requirements are met.
 * 
 *
 * @since GemFire 8.1
 */
public final class MinimumSystemRequirements {

  public static final String JAVA_VERSION = "1.7.0_72";

  private static final Logger logger = LogService.getLogger();

  private MinimumSystemRequirements() {
    // static only
  }

  /**
   * Asserts minimum system requirements, logs any violations and forces exit.
   * 
   * @see #checkAndLog()
   * 
   * @since GemFire 8.1
   */
  public static void assertLogAndExit() {
    if (!checkAndLog()) {
      System.exit(1);
    }
  }

  /**
   * Checks minimum system requirements and logs any violations.
   * 
   * @return true if minimum system requirements met, otherwise false.
   * 
   * @since GemFire 8.1
   */
  public static boolean checkAndLog() {
    boolean minimumSystemRequirementsMet = true;

    minimumSystemRequirementsMet &= checkJavaVersion();

    if (!minimumSystemRequirementsMet) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.MinimumSystemRequirements_NOT_MET));
    }

    return minimumSystemRequirementsMet;
  }

  /**
   * Check Java version at least {@link #JAVA_VERSION}.
   * 
   * @return true if minimum system requirements met, otherwise false.
   * 
   * @since GemFire 8.1
   */
  private static boolean checkJavaVersion() {
    if (SystemUtils.isJavaVersionAtLeast(JAVA_VERSION)) {
      return true;
    }

    logger.warn(LocalizedMessage.create(LocalizedStrings.MinimumSystemRequirements_JAVA_VERSION, JAVA_VERSION));
    return false;
  }

}
