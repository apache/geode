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
package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.*;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandProcessor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.*;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.management.ObjectName;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;
import static org.apache.geode.distributed.ConfigurationProperties.*;

@Category(DistributedTest.class)
public class ShowMetricsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  private void createLocalSetUp() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Controller");
    getSystem(localProps);
    Cache cache = getCache();
    RegionFactory<Integer, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region region1 = dataRegionFactory.create("REGION1");
    Region region2 = dataRegionFactory.create("REGION2");
  }

  /**
   * tests the default version of "show metrics"
   */
  @Test
  public void testShowMetricsDefault() {
    setUpJmxManagerOnVm0ThenConnect(null);
    createLocalSetUp();
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        getSystem(localProps);

        Cache cache = getCache();
        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create("REGION1");
      }
    });

    SerializableCallable showMetricCmd = new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        WaitCriterion wc = createMBeanWaitCriterion(1, "", null, 0);
        waitForCriterion(wc, 5000, 500, true);
        CommandProcessor commandProcessor = new CommandProcessor();
        Result result = commandProcessor
            .createCommandStatement("show metrics", Collections.EMPTY_MAP).process();
        String resultStr = commandResultToString((CommandResult) result);
        getLogWriter().info(resultStr);
        assertEquals(resultStr, true, result.getStatus().equals(Status.OK));
        return resultStr;
      }
    };

    // Invoke the command in the Manager VM
    final VM managerVm = Host.getHost(0).getVM(0);
    Object managerResultObj = managerVm.invoke(showMetricCmd);

    String managerResult = (String) managerResultObj;

    getLogWriter().info("#SB Manager");
    getLogWriter().info(managerResult);
  }

  public void systemSetUp() {
    setUpJmxManagerOnVm0ThenConnect(null);
    createLocalSetUp();
    final VM vm1 = Host.getHost(0).getVM(1);
    final String vm1Name = "VM" + vm1.getPid();

    vm1.invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, vm1Name);
        getSystem(localProps);

        Cache cache = getCache();
        RegionFactory<Integer, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.REPLICATE);
        Region region = dataRegionFactory.create("REGION1");
      }
    });
  }

  @Test
  public void testShowMetricsRegion() throws InterruptedException {
    systemSetUp();
    final String regionName = "REGION1";
    SerializableCallable showMetricCmd = new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        WaitCriterion wc = createMBeanWaitCriterion(2, regionName, null, 0);
        waitForCriterion(wc, 5000, 500, true);
        CommandProcessor commandProcessor = new CommandProcessor();
        Result result = commandProcessor
            .createCommandStatement("show metrics --region=REGION1", Collections.EMPTY_MAP)
            .process();
        String resultAsString = commandResultToString((CommandResult) result);
        assertEquals(resultAsString, true, result.getStatus().equals(Status.OK));
        return resultAsString;
      }
    };

    // Invoke the command in the Manager VM
    final VM managerVm = Host.getHost(0).getVM(0);
    Object managerResultObj = managerVm.invoke(showMetricCmd);

    String managerResult = (String) managerResultObj;

    getLogWriter().info("#SB Manager");
    getLogWriter().info(managerResult);
  }

  /***
   * Creates WaitCriterion based on creation of different types of MBeans
   */
  private WaitCriterion createMBeanWaitCriterion(final int beanType, final String regionName,
      final DistributedMember distributedMember, final int cacheServerPort) {

    WaitCriterion waitCriterion = new WaitCriterion() {

      @Override
      public boolean done() {
        boolean done = false;
        Cache cache = getCache();
        ManagementService mgmtService = ManagementService.getManagementService(cache);
        if (beanType == 1) {
          DistributedSystemMXBean dsMxBean = mgmtService.getDistributedSystemMXBean();
          if (dsMxBean != null)
            done = true;
        } else if (beanType == 2) {
          DistributedRegionMXBean dsRegionMxBean =
              mgmtService.getDistributedRegionMXBean("/" + regionName);
          if (dsRegionMxBean != null)
            done = true;
        } else if (beanType == 3) {
          ObjectName memberMBeanName = mgmtService.getMemberMBeanName(distributedMember);
          MemberMXBean memberMxBean =
              mgmtService.getMBeanInstance(memberMBeanName, MemberMXBean.class);

          if (memberMxBean != null)
            done = true;
        } else if (beanType == 4) {
          ObjectName regionMBeanName =
              mgmtService.getRegionMBeanName(distributedMember, "/" + regionName);
          RegionMXBean regionMxBean =
              mgmtService.getMBeanInstance(regionMBeanName, RegionMXBean.class);

          if (regionMxBean != null)
            done = true;
        } else if (beanType == 5) {
          ObjectName csMxBeanName =
              mgmtService.getCacheServerMBeanName(cacheServerPort, distributedMember);
          CacheServerMXBean csMxBean =
              mgmtService.getMBeanInstance(csMxBeanName, CacheServerMXBean.class);

          if (csMxBean != null) {
            done = true;
          }
        }

        return done;
      }

      @Override
      public String description() {
        return "Waiting for the mbean to be created";
      }
    };

    return waitCriterion;
  }

  @Category(FlakyTest.class) // GEODE-1764
  @Test
  public void testShowMetricsMember()
      throws ClassNotFoundException, IOException, InterruptedException {
    systemSetUp();
    Cache cache = getCache();
    final DistributedMember distributedMember = cache.getDistributedSystem().getDistributedMember();
    final String exportFileName = "memberMetricReport.csv";

    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    CacheServer cs = getCache().addCacheServer();
    cs.setPort(ports[0]);
    cs.start();
    final int cacheServerPort = cs.getPort();

    SerializableCallable showMetricCmd = new SerializableCallable() {
      @Override
      public Object call() throws Exception {

        WaitCriterion wc = createMBeanWaitCriterion(3, "", distributedMember, 0);
        waitForCriterion(wc, 5000, 500, true);
        wc = createMBeanWaitCriterion(5, "", distributedMember, cacheServerPort);
        waitForCriterion(wc, 10000, 500, true);

        final String command = CliStrings.SHOW_METRICS + " --" + CliStrings.SHOW_METRICS__MEMBER
            + "=" + distributedMember.getId() + " --" + CliStrings.SHOW_METRICS__CACHESERVER__PORT
            + "=" + cacheServerPort + " --" + CliStrings.SHOW_METRICS__FILE + "=" + exportFileName;

        CommandProcessor commandProcessor = new CommandProcessor();
        Result result =
            commandProcessor.createCommandStatement(command, Collections.EMPTY_MAP).process();
        String resultAsString = commandResultToString((CommandResult) result);
        assertEquals(resultAsString, true, result.getStatus().equals(Status.OK));
        assertTrue(result.hasIncomingFiles());
        result.saveIncomingFiles(null);
        File file = new File(exportFileName);
        file.deleteOnExit();
        assertTrue(file.exists());
        file.delete();
        return resultAsString;

      }
    };

    // Invoke the command in the Manager VM
    final VM managerVm = Host.getHost(0).getVM(0);
    Object managerResultObj = managerVm.invoke(showMetricCmd);

    String managerResult = (String) managerResultObj;

    getLogWriter().info("#SB Manager");
    getLogWriter().info(managerResult);
    cs.stop();
  }

  @Test
  public void testShowMetricsRegionFromMember()
      throws ClassNotFoundException, IOException, InterruptedException {
    systemSetUp();
    Cache cache = getCache();
    final DistributedMember distributedMember = cache.getDistributedSystem().getDistributedMember();
    final String exportFileName = "regionOnAMemberReport.csv";
    final String regionName = "REGION1";

    SerializableCallable showMetricCmd = new SerializableCallable() {

      @Override
      public Object call() throws Exception {

        WaitCriterion wc = createMBeanWaitCriterion(4, regionName, distributedMember, 0);
        waitForCriterion(wc, 5000, 500, true);
        CommandProcessor commandProcessor = new CommandProcessor();
        Result result = commandProcessor
            .createCommandStatement("show metrics --region=" + regionName + " --member="
                + distributedMember.getName() + " --file=" + exportFileName, Collections.EMPTY_MAP)
            .process();
        String resultAsString = commandResultToString((CommandResult) result);
        assertEquals(resultAsString, true, result.getStatus().equals(Status.OK));
        assertTrue(result.hasIncomingFiles());
        result.saveIncomingFiles(null);
        File file = new File(exportFileName);
        file.deleteOnExit();
        assertTrue(file.exists());
        file.delete();
        return resultAsString;
      }
    };

    // Invoke the command in the Manager VM
    final VM managerVm = Host.getHost(0).getVM(0);
    Object managerResultObj = managerVm.invoke(showMetricCmd);

    String managerResult = (String) managerResultObj;

    getLogWriter().info("#SB Manager");
    getLogWriter().info(managerResult);
  }

  @Test
  public void testShowMetricsRegionFromMemberWithCategories()
      throws ClassNotFoundException, IOException, InterruptedException {
    systemSetUp();
    Cache cache = getCache();
    final DistributedMember distributedMember = cache.getDistributedSystem().getDistributedMember();
    final String exportFileName = "regionOnAMemberReport.csv";
    final String regionName = "REGION1";

    SerializableCallable showMetricCmd = new SerializableCallable() {

      @Override
      public Object call() throws Exception {

        WaitCriterion wc = createMBeanWaitCriterion(4, regionName, distributedMember, 0);
        waitForCriterion(wc, 5000, 500, true);
        CommandProcessor commandProcessor = new CommandProcessor();
        Result result = commandProcessor.createCommandStatement(
            "show metrics --region=" + regionName + " --member=" + distributedMember.getName()
                + " --file=" + exportFileName + " --categories=region,eviction",
            Collections.EMPTY_MAP).process();
        String resultAsString = commandResultToString((CommandResult) result);
        assertEquals(resultAsString, true, result.getStatus().equals(Status.OK));
        assertTrue(result.hasIncomingFiles());
        result.saveIncomingFiles(null);
        File file = new File(exportFileName);
        file.deleteOnExit();
        assertTrue(file.exists());
        file.delete();
        return resultAsString;
      }
    };

    // Invoke the command in the Manager VM
    final VM managerVm = Host.getHost(0).getVM(0);
    Object managerResultObj = managerVm.invoke(showMetricCmd);

    String managerResult = (String) managerResultObj;

    getLogWriter().info("#SB Manager");
    getLogWriter().info(managerResult);
  }
}
