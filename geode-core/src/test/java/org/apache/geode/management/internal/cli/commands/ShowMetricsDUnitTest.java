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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.awaitility.Awaitility.await;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.OnlineCommandProcessor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import javax.management.ObjectName;

@Category({DistributedTest.class, FlakyTest.class}) // GEODE-1764 GEODE-3530
@SuppressWarnings("serial")
public class ShowMetricsDUnitTest extends CliCommandTestBase {

  private void createLocalSetUp() {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Controller");
    getSystem(localProps);
    Cache cache = getCache();
    RegionFactory<Integer, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.create("REGION1");
    dataRegionFactory.create("REGION2");
  }

  private void systemSetUp() {
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
        dataRegionFactory.create("REGION1");
      }
    });
  }

  private boolean createMBean(int beanType, String regionName, DistributedMember distributedMember,
      int cacheServerPort) {
    Cache cache = getCache();
    ManagementService mgmtService = ManagementService.getManagementService(cache);
    Object bean = null;

    switch (beanType) {
      case 1:
        bean = mgmtService.getDistributedSystemMXBean();
        break;
      case 2:
        bean = mgmtService.getDistributedRegionMXBean("/" + regionName);
        break;
      case 3:
        ObjectName memberMBeanName = mgmtService.getMemberMBeanName(distributedMember);
        bean = mgmtService.getMBeanInstance(memberMBeanName, MemberMXBean.class);
        break;
      case 4:
        ObjectName regionMBeanName =
            mgmtService.getRegionMBeanName(distributedMember, "/" + regionName);
        bean = mgmtService.getMBeanInstance(regionMBeanName, RegionMXBean.class);
        break;
      case 5:
        ObjectName csMxBeanName =
            mgmtService.getCacheServerMBeanName(cacheServerPort, distributedMember);
        bean = mgmtService.getMBeanInstance(csMxBeanName, CacheServerMXBean.class);
        break;
    }

    return bean != null;
  }

  /**
   * tests the default version of "show metrics"
   */
  @Test
  public void testShowMetricsDefault() {
    systemSetUp();
    SerializableCallable showMetricCmd = new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        await().atMost(120, SECONDS).until(() -> createMBean(1, "", null, 0));
        OnlineCommandProcessor OnlineCommandProcessor = new OnlineCommandProcessor();
        Result result = OnlineCommandProcessor.executeCommand("show metrics");
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

  @Test
  public void testShowMetricsRegion() throws InterruptedException {
    systemSetUp();
    final String regionName = "REGION1";
    SerializableCallable showMetricCmd = new SerializableCallable() {

      @Override
      public Object call() throws Exception {
        await().atMost(120, SECONDS).until(() -> createMBean(2, regionName, null, 0));
        OnlineCommandProcessor OnlineCommandProcessor = new OnlineCommandProcessor();
        Result result = OnlineCommandProcessor.executeCommand("show metrics --region=REGION1");
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

  @Test // FlakyTest: GEODE-1764
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

        await().atMost(120, SECONDS).until(() -> createMBean(3, "", distributedMember, 0));
        await().atMost(120, SECONDS)
            .until(() -> createMBean(5, "", distributedMember, cacheServerPort));

        final String command = CliStrings.SHOW_METRICS + " --" + CliStrings.MEMBER + "="
            + distributedMember.getId() + " --" + CliStrings.SHOW_METRICS__CACHESERVER__PORT + "="
            + cacheServerPort + " --" + CliStrings.SHOW_METRICS__FILE + "=" + exportFileName;

        OnlineCommandProcessor OnlineCommandProcessor = new OnlineCommandProcessor();
        Result result = OnlineCommandProcessor.executeCommand(command);
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

        await().atMost(120, SECONDS).until(() -> createMBean(4, regionName, distributedMember, 0));
        OnlineCommandProcessor OnlineCommandProcessor = new OnlineCommandProcessor();
        Result result = OnlineCommandProcessor.executeCommand("show metrics --region=" + regionName
            + " --member=" + distributedMember.getName() + " --file=" + exportFileName);
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

        await().atMost(120, SECONDS).until(() -> createMBean(4, regionName, distributedMember, 0));
        OnlineCommandProcessor OnlineCommandProcessor = new OnlineCommandProcessor();
        Result result = OnlineCommandProcessor.executeCommand(
            "show metrics --region=" + regionName + " --member=" + distributedMember.getName()
                + " --file=" + exportFileName + " --categories=region,eviction");
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
  public void testShowMetricsWithInvalidOptions() throws InterruptedException {
    systemSetUp();
    Cache cache = getCache();
    final DistributedMember distributedMember = cache.getDistributedSystem().getDistributedMember();

    SerializableCallable showMetricCmd = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        OnlineCommandProcessor OnlineCommandProcessor = new OnlineCommandProcessor();
        Result result = OnlineCommandProcessor.executeCommand("show metrics --region=REGION1 "
            + " --member=" + distributedMember.getName() + " --port=0");
        String resultAsString = commandResultToString((CommandResult) result);
        assertEquals(resultAsString, true, result.getStatus().equals(Status.ERROR));
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
