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


public class GfshConsoleModeUnitTest extends GfshAbstractUnitTest {

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
    // when initialized in console mode, all log messages will show up in console
    // initially. so that we see messages when "start locator", "start server" command
    // are executed. Only after connection, JDK's logging is turned off
    while (loggerNames.hasMoreElements()) {
      String loggerName = loggerNames.nextElement();
      Logger logger = logManager.getLogger(loggerName);
      // make sure jdk's logging goes to the gfsh log file
      if (loggerName.startsWith("java")) {
        assertThat(logger.getParent().getName()).endsWith("LogWrapper");
      }
      // make sure Gfsh's logging goes to the gfsh log file
      else if (loggerName.endsWith(".Gfsh")) {
        assertThat(logger.getParent().getName()).doesNotEndWith("LogWrapper");
      }
      // make sure SimpleParser's logging will still show up in the console
      else if (loggerName.endsWith(".SimpleParser")) {
        assertThat(logger.getParent().getName()).doesNotEndWith("LogWrapper");
      }
    }
  }
}
