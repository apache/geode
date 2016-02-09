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
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Dunit class for testing gemfire function commands : export logs
 *
 * @author apande
 */

public class MiscellaneousCommandsExportLogsPart3DUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  public MiscellaneousCommandsExportLogsPart3DUnitTest(String name) {
    super(name);
  }

  public static String getMemberId() {
    Cache cache = new GemfireDataCommandsDUnitTest("test").getCache();
    return cache.getDistributedSystem().getDistributedMember().getId();
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

  public void testExportLogsForGroup() throws IOException {
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, "Manager");
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    createDefaultSetup(localProps);
    String dir = getCurrentTimeString();

    Date startDate = new Date(System.currentTimeMillis() - 2 * 60 * 1000);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd");
    String start = sf.format(startDate);

    Date enddate = new Date(System.currentTimeMillis() + 2 * 60 * 60 * 1000);
    String end = sf.format(enddate);

    String logLevel = LogWriterImpl.levelToString(LogWriterImpl.INFO_LEVEL);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    getCache();
    String[] groups = new String[1];
    groups[0] = "Group1";

    Result cmdResult = misc.exportLogsPreprocessing("./testExportLogsForGroup" + dir, groups, null, logLevel, false,
        false, start, end, 1);

    LogWriterUtils.getLogWriter().info("testExportLogsForGroup command result =" + cmdResult);
    if (cmdResult != null) {
      String cmdStringRsult = commandResultToString((CommandResult) cmdResult);
      LogWriterUtils.getLogWriter().info("testExportLogsForGroup cmdStringRsult=" + cmdStringRsult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testExportLogsForGroup failed as did not get CommandResult");
    }
    FileUtil.delete(new File("testExportLogsForGroup" + dir));
  }

  public void testExportLogsForMember() throws IOException {
    createDefaultSetup(null);

    Date startDate = new Date(System.currentTimeMillis() - 2 * 60 * 1000);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd");
    String start = sf.format(startDate);

    Date enddate = new Date(System.currentTimeMillis() + 2 * 60 * 60 * 1000);
    String end = sf.format(enddate);

    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1MemberId = (String) vm1.invoke(MiscellaneousCommandsDUnitTest.class, "getMemberId");
    String dir = getCurrentTimeString();

    String logLevel = LogWriterImpl.levelToString(LogWriterImpl.INFO_LEVEL);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    getCache();

    Result cmdResult = misc.exportLogsPreprocessing("./testExportLogsForMember" + dir, null, vm1MemberId, logLevel,
        false, false, start, end, 1);

    LogWriterUtils.getLogWriter().info("testExportLogsForMember command result =" + cmdResult);

    if (cmdResult != null) {
      String cmdStringRsult = commandResultToString((CommandResult) cmdResult);
      LogWriterUtils.getLogWriter().info("testExportLogsForMember cmdStringRsult=" + cmdStringRsult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testExportLogsForMember failed as did not get CommandResult");
    }
    FileUtil.delete(new File("testExportLogsForMember" + dir));
  }
}