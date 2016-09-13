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
package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * Dunit class for testing gemfire function commands : export logs
 */
@Category(DistributedTest.class)
public class MiscellaneousCommandsExportLogsPart3DUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  public String getMemberId() {
    Cache cache = getCache();
    return cache.getDistributedSystem().getDistributedMember().getId();
  }

  void setupForExportLogs() {
    final VM vm1 = Host.getHost(0).getVM(1);
    setUpJmxManagerOnVm0ThenConnect(null);

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

  @Category(FlakyTest.class) // GEODE-672: random ports, java.rmi.server.ExportException: Port already in use, HeadlessGfsh, disk IO
  @Test
  public void testExportLogsForGroup() throws IOException {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);
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

    getLogWriter().info("testExportLogsForGroup command result =" + cmdResult);
    if (cmdResult != null) {
      String cmdStringRsult = commandResultToString((CommandResult) cmdResult);
      getLogWriter().info("testExportLogsForGroup cmdStringRsult=" + cmdStringRsult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testExportLogsForGroup failed as did not get CommandResult");
    }
    FileUtil.delete(new File("testExportLogsForGroup" + dir));
  }

  @Test
  public void testExportLogsForMember() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null);

    Date startDate = new Date(System.currentTimeMillis() - 2 * 60 * 1000);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy/MM/dd");
    String start = sf.format(startDate);

    Date enddate = new Date(System.currentTimeMillis() + 2 * 60 * 60 * 1000);
    String end = sf.format(enddate);

    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1MemberId = (String) vm1.invoke(() -> getMemberId());
    String dir = getCurrentTimeString();

    String logLevel = LogWriterImpl.levelToString(LogWriterImpl.INFO_LEVEL);

    MiscellaneousCommands misc = new MiscellaneousCommands();
    getCache();

    Result cmdResult = misc.exportLogsPreprocessing("./testExportLogsForMember" + dir, null, vm1MemberId, logLevel,
        false, false, start, end, 1);

    getLogWriter().info("testExportLogsForMember command result =" + cmdResult);

    if (cmdResult != null) {
      String cmdStringRsult = commandResultToString((CommandResult) cmdResult);
      getLogWriter().info("testExportLogsForMember cmdStringRsult=" + cmdStringRsult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testExportLogsForMember failed as did not get CommandResult");
    }
    FileUtil.delete(new File("testExportLogsForMember" + dir));
  }
}