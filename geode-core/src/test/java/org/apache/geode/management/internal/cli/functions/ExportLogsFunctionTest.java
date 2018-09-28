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
 *
 */
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.logging.log4j.Level;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.internal.cli.commands.ExportLogsCommand;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsFunctionTest {

  @Test
  public void defaultExportLogLevelShouldBeAll() {
    assertTrue(ExportLogsCommand.DEFAULT_EXPORT_LOG_LEVEL.equals("ALL"));
    assertEquals(LogLevel.getLevel(ExportLogsCommand.DEFAULT_EXPORT_LOG_LEVEL), Level.ALL);
  }

  @Test
  public void defaultExportLogLevelShouldBeAllViaArgs() {
    ExportLogsFunction.Args args = new ExportLogsFunction.Args("", "", "", false, false, false);
    assertEquals(args.getLogLevel(), Level.ALL);
    ExportLogsFunction.Args args2 = new ExportLogsFunction.Args("", "", null, false, false, false);
    assertEquals(args2.getLogLevel(), Level.ALL);
  }

  @Test
  public void argsCorrectlyBuildALogLevelFilter() {
    ExportLogsFunction.Args args =
        new ExportLogsFunction.Args(null, null, "info", false, false, false);
    assertThat(args.getLogLevel().toString()).isEqualTo("INFO");
    assertThat(args.isThisLogLevelOnly()).isFalse();
    assertThat(args.isIncludeLogs()).isTrue();
    assertThat(args.isIncludeStats()).isTrue();
  }

  @Test
  public void argsCorrectlyBuilt() {
    ExportLogsFunction.Args args =
        new ExportLogsFunction.Args(null, null, "error", true, true, false);
    assertThat(args.getLogLevel()).isEqualTo(Level.ERROR);
    assertThat(args.isThisLogLevelOnly()).isTrue();
    assertThat(args.isIncludeLogs()).isTrue();
    assertThat(args.isIncludeStats()).isFalse();
  }

  @Test
  public void argsCorrectlyBuiltWithGeodeLevel() {
    ExportLogsFunction.Args args =
        new ExportLogsFunction.Args(null, null, "fine", true, true, false);
    assertThat(args.getLogLevel()).isEqualTo(Level.DEBUG);
    assertThat(args.isThisLogLevelOnly()).isTrue();
    assertThat(args.isIncludeLogs()).isTrue();
    assertThat(args.isIncludeStats()).isFalse();
  }
}
