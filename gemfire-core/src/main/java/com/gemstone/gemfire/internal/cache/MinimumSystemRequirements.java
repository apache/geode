/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author jbarrett@pivotal.io
 *
 * @since 8.1
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
   * @since 8.1
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
   * @since 8.1
   */
  public static boolean checkAndLog() {
    boolean minimumSystemRequirementsMet = true;

    minimumSystemRequirementsMet &= checkJavaVersion();

    if (!minimumSystemRequirementsMet) {
      logger.fatal(LocalizedMessage.create(LocalizedStrings.MinimumSystemRequirements_NOT_MET));
    }

    return minimumSystemRequirementsMet;
  }

  /**
   * Check Java version at least {@link #JAVA_VERSION}.
   * 
   * @return true if minimum system requirements met, otherwise false.
   * 
   * @since 8.1
   */
  private static boolean checkJavaVersion() {
    if (SystemUtils.isJavaVersionAtLeast(JAVA_VERSION)) {
      return true;
    }

    logger.fatal(LocalizedMessage.create(LocalizedStrings.MinimumSystemRequirements_JAVA_VERSION, JAVA_VERSION));
    return false;
  }

}
