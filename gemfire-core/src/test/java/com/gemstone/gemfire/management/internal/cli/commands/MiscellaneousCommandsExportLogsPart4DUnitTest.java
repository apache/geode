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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Dunit class for testing gemfire function commands : export logs
 *
 * @author apande
 */
public class MiscellaneousCommandsExportLogsPart4DUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  public MiscellaneousCommandsExportLogsPart4DUnitTest(String name) {
    super(name);
  }

  public static String getMemberId() {
    Cache cache = new GemfireDataCommandsDUnitTest("test").getCache();
    return cache.getDistributedSystem().getDistributedMember().getId();
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }

  void setupForExportLogs() {
    final VM vm1 = Host.getHost(0).getVM(1);
    createDefaultSetup(null);

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        // no need to close cache as it will be closed as part of teardown2
        Cache cache = getCache();

        RegionFactory<Integer, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        Region region = dataRegionFactory.create("testRegion");
        for (int i = 0; i < 5; i++) {
          region.put("key" + (i + 200), "value" + (i + 200));
        }
      }
    });
  }

  String getCurrentTimeString() {
    SimpleDateFormat sf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS_z");
    Date startDate = new Date(System.currentTimeMillis());
    String formattedStartDate = sf.format(startDate);
    return ("_" + formattedStartDate);
  }

  public void testExportLogsForTimeRange1() throws IOException {
    setupForExportLogs();
    Date startDate = new Date(System.currentTimeMillis() - 1 * 60 * 1000);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd");
    String start = sf.format(startDate);

    Date enddate = new Date(System.currentTimeMillis() + 1 * 60 * 60 * 1000);
    String end = sf.format(enddate);
    String dir = getCurrentTimeString();

    String logLevel = LogWriterImpl.levelToString(LogWriterImpl.INFO_LEVEL);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    getCache();

    Result cmdResult = misc.exportLogsPreprocessing("./testExportLogsForTimeRange1" + dir, null, null, logLevel, false,
        false, start, end, 1);

    getLogWriter().info("testExportLogsForTimeRange1 command result =" + cmdResult);

    if (cmdResult != null) {
      String cmdStringRsult = commandResultToString((CommandResult) cmdResult);
      getLogWriter().info("testExportLogsForTimeRange1 cmdStringRsult=" + cmdStringRsult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testExportLogsForTimeRange1 failed as did not get CommandResult");
    }
    FileUtil.delete(new File("testExportLogsForTimeRange1" + dir));
  }

  public void testExportLogsForTimeRangeForOnlyStartTime() throws IOException {
    setupForExportLogs();
    Date date = new Date();
    date.setTime(System.currentTimeMillis() - 30 * 1000);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd/HH:mm");
    String s = sf.format(date);
    String dir = getCurrentTimeString();

    String logLevel = LogWriterImpl.levelToString(LogWriterImpl.INFO_LEVEL);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    getCache();

    Result cmdResult = misc.exportLogsPreprocessing("./testExportLogsForTimeRangeForOnlyStartTime" + dir, null, null,
        logLevel, false, false, s, null, 1);

    getLogWriter().info("testExportLogsForTimeRangeForOnlyStartTime command result =" + cmdResult);

    if (cmdResult != null) {
      String cmdStringRsult = commandResultToString((CommandResult) cmdResult);
      getLogWriter().info("testExportLogsForTimeRangeForOnlyStartTime cmdStringRsult=" + cmdStringRsult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testExportLogsForTimeRangeForOnlyStartTime failed as did not get CommandResult");
    }
    FileUtil.delete(new File("testExportLogsForTimeRangeForOnlyStartTime" + dir));
  }
}