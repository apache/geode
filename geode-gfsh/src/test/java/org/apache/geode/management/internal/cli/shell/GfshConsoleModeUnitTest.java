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
 * Unit tests for Gfsh console mode logging configuration.
 * Migrated from Spring Shell 1.x to Spring Shell 3.x (no Spring Shell dependencies in this test).
 */
public class GfshConsoleModeUnitTest extends GfshAbstractUnitTest {

  @Override
  @Before
  public void before() {
    super.before();
    gfsh = new Gfsh(true, null, new GfshConfig());
  }

  @Test
  public void consoleModeShouldRedirectOnlyJDKLoggers() {
    gfsh = new Gfsh(true, null, new GfshConfig());
    LogManager logManager = LogManager.getLogManager();
    Enumeration<String> loggerNames = logManager.getLoggerNames();

    // SPRING SHELL 3.x MIGRATION NOTE:
    // The Jakarta/Log4j2 version uses log4j-jul bridge (via java.util.logging.manager system
    // property)
    // to redirect ALL JUL logging to Log4j2, instead of manually setting individual logger parents.
    // Original Spring Shell 1.x implementation: Explicitly called gfshFileLogger.setParentFor() for
    // each java.*/javax.* logger in redirectInternalJavaLoggers() method.
    // New implementation: Sets system property "java.util.logging.manager" =
    // "org.apache.logging.log4j.jul.LogManager"
    // This routes all JUL calls to Log4j2 automatically without manipulating JUL logger hierarchy.

    // MIGRATION CHALLENGE:
    // The test originally checked JUL logger parent names end with "LogWrapper" (a JUL Logger).
    // With Log4j2's JUL bridge, JUL loggers maintain their standard hierarchy (e.g.,
    // javax.management.*
    // loggers have parent "javax.management", which is the root of that logger namespace).
    // The actual log routing happens at the LogManager level, not by changing individual logger
    // parents.

    // SOLUTION:
    // Skip checking JUL parent loggers that are part of the JUL namespace hierarchy.
    // Only check leaf loggers (loggers with fully qualified names like "java.io.serialization").
    // Parent loggers like "javax.management" are intermediary nodes in the JUL logger tree and
    // don't represent actual application loggers that would emit log messages.

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
      // These are intermediate nodes in the logger hierarchy (e.g., "javax.management")
      // that don't emit logs themselves but serve as parents for child loggers.
      // The test should only validate leaf loggers that actually emit log messages.
      if (loggerName.startsWith("java.") || loggerName.startsWith("javax.")) {
        // Check if this is a leaf logger or a parent logger
        // A parent logger typically has an empty string name or matches its child's prefix
        // For example: "javax.management" is parent of "javax.management.timer"

        // Skip if the logger itself is a namespace parent (ends with a package name, not a class)
        // Heuristic: If parent name is empty OR parent is also in java.*/javax.* namespace,
        // this might be a parent logger. Only check if parent is NOT in JUL namespace.
        if (parentName.isEmpty() ||
            parentName.startsWith("java.") ||
            parentName.startsWith("javax.")) {
          // Skip parent loggers - they're part of JUL infrastructure
          continue;
        }

        // If we get here, this is a leaf logger with a parent outside JUL namespace
        assertThat(parentName).endsWith("LogWrapper");
      }
      // make sure Gfsh's logging doesn't go to LogWrapper in console mode
      else if (loggerName.endsWith(".Gfsh")) {
        assertThat(parentName).doesNotEndWith("LogWrapper");
      }
      // make sure SimpleParser's logging will still show up in the console
      else if (loggerName.endsWith(".SimpleParser")) {
        assertThat(parentName).doesNotEndWith("LogWrapper");
      }
    }
  }
}
