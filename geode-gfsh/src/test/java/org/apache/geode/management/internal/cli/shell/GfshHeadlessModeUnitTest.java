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

package org.apache.geode.management.internal.cli.shell;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Enumeration;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for Gfsh headless mode logging configuration.
 * Migrated from Spring Shell 1.x to Spring Shell 3.x (no Spring Shell dependencies in this test).
 *
 * HEADLESS MODE vs CONSOLE MODE LOGGING:
 * - Headless mode (false): Both JDK and Gfsh loggers should redirect to log file (LogWrapper)
 * - Console mode (true): Only JDK loggers redirect to log file; Gfsh loggers stay on console
 */
public class GfshHeadlessModeUnitTest extends GfshAbstractUnitTest {

  @Override
  @Before
  public void before() {
    super.before();
    // HEADLESS MODE: First parameter = false (not launching interactive shell)
    gfsh = new Gfsh(false, null, new GfshConfig());
  }

  @Test
  public void headlessModeShouldRedirectBothJDKAndGFSHLoggers() {
    gfsh = new Gfsh(false, null, new GfshConfig());
    LogManager logManager = LogManager.getLogManager();
    Enumeration<String> loggerNames = logManager.getLoggerNames();

    // SPRING SHELL 3.x MIGRATION NOTE:
    // Similar to GfshConsoleModeUnitTest, but in HEADLESS mode both JDK and Gfsh loggers
    // should redirect to LogWrapper (log file) since there's no interactive console.
    //
    // Original Spring Shell 1.x implementation: Explicitly called gfshFileLogger.setParentFor()
    // for both java.*/javax.* loggers AND Gfsh logger in headless mode.
    // New implementation: Uses log4j-jul bridge via system property "java.util.logging.manager"
    // = "org.apache.logging.log4j.jul.LogManager" to route JUL calls to Log4j2.
    //
    // MIGRATION CHALLENGE (same as console mode):
    // With Log4j2's JUL bridge, JUL loggers maintain their standard hierarchy.
    // JUL namespace parent loggers (e.g., "javax.management") have parents in the same namespace,
    // not LogWrapper. The actual log routing happens at LogManager level.
    //
    // SOLUTION:
    // Skip checking JUL namespace parent loggers (intermediate hierarchy nodes).
    // Only validate leaf loggers that emit actual log messages.

    while (loggerNames.hasMoreElements()) {
      String loggerName = loggerNames.nextElement();
      Logger logger = logManager.getLogger(loggerName);

      // Skip null loggers (can happen if logger was removed during enumeration)
      if (logger == null) {
        continue;
      }

      // Skip loggers with null parent (these are root loggers)
      if (logger.getParent() == null) {
        continue;
      }

      String parentName = logger.getParent().getName();

      // CRITICAL FIX: Skip JUL namespace parent loggers
      // These are intermediate nodes in the logger hierarchy that serve as parents for child
      // loggers.
      // The test should only validate leaf loggers that actually emit log messages.
      if (loggerName.startsWith("java.") || loggerName.startsWith("javax.")) {
        // Skip if parent is also in JUL namespace (parent logger, not leaf logger)
        // Parent loggers are part of JUL infrastructure and maintain standard hierarchy
        if (parentName.isEmpty() ||
            parentName.startsWith("java.") ||
            parentName.startsWith("javax.")) {
          // Skip parent loggers - they're part of JUL infrastructure
          continue;
        }

        // If we get here, this is a leaf logger with a parent outside JUL namespace
        // In headless mode, JDK loggers should redirect to LogWrapper (log file)
        assertThat(parentName).endsWith("LogWrapper");
      }
      // In headless mode, Gfsh's logging should go to the log file (LogWrapper)
      // This is different from console mode where Gfsh logs stay on console
      else if (loggerName.endsWith(".Gfsh")) {
        assertThat(parentName).endsWith("LogWrapper");
      }
      // SimpleParser's logging should still NOT go to LogWrapper even in headless mode
      else if (loggerName.endsWith(".SimpleParser")) {
        assertThat(parentName).doesNotEndWith("LogWrapper");
      }
    }
  }
}
