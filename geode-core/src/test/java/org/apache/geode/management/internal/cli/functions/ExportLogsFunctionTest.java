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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.internal.cli.commands.ExportLogsCommand;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.apache.logging.log4j.Level;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ExportLogsFunctionTest {
  @Test
  public void defaultExportLogLevelShouldBeAll() throws Exception {
    assertTrue(ExportLogsCommand.DEFAULT_EXPORT_LOG_LEVEL.equals("ALL"));
    assertEquals(LogLevel.getLevel(ExportLogsCommand.DEFAULT_EXPORT_LOG_LEVEL), Level.ALL);
  }

  @Test
  public void defaultExportLogLevelShouldBeAllViaArgs() throws Exception {
    ExportLogsFunction.Args args = new ExportLogsFunction.Args("", "", "", false, false, false);
    assertEquals(args.getLogLevel(), Level.ALL);
    ExportLogsFunction.Args args2 = new ExportLogsFunction.Args("", "", null, false, false, false);
    assertEquals(args2.getLogLevel(), Level.ALL);
  }

}
