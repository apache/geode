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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.assertj.core.api.Java6Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.dto.Value1;
import org.apache.geode.management.internal.cli.dto.Value2;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

/**
 * Dunit class for testing gemfire data commands : get, put, remove
 */
@SuppressWarnings("serial")
public class GemfireDataCommandsDUnitTestBase {


  private static final String DATA_REGION_NAME = "GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_VM1 = "GemfireDataCommandsTestRegion_Vm1";
  private static final String DATA_REGION_NAME_VM2 = "GemfireDataCommandsTestRegion_Vm2";
  private static final String DATA_REGION_NAME_PATH = "/GemfireDataCommandsTestRegion";
  private static final String DATA_REGION_NAME_VM1_PATH = "/GemfireDataCommandsTestRegion_Vm1";
  private static final String DATA_REGION_NAME_VM2_PATH = "/GemfireDataCommandsTestRegion_Vm2";

  private static final String DATA_PAR_REGION_NAME = "GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_VM1 = "GemfireDataCommandsTestParRegion_Vm1";
  private static final String DATA_PAR_REGION_NAME_VM2 = "GemfireDataCommandsTestParRegion_Vm2";
  private static final String DATA_PAR_REGION_NAME_PATH = "/GemfireDataCommandsTestParRegion";
  private static final String DATA_PAR_REGION_NAME_VM1_PATH =
      "/GemfireDataCommandsTestParRegion_Vm1";
  private static final String DATA_PAR_REGION_NAME_VM2_PATH =
      "/GemfireDataCommandsTestParRegion_Vm2";

  private static final String DATA_REGION_NAME_CHILD_1 = "ChildRegionRegion1";
  private static final String DATA_REGION_NAME_CHILD_1_2 = "ChildRegionRegion12";

  private static final String SERIALIZATION_FILTER =
      "org.apache.geode.management.internal.cli.**";


  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  protected MemberVM locator, server1, server2;

  public void before() throws Exception {
    locator = cluster.startLocatorVM(0, locatorProperties());
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());
    connectToLocator();
  }

  // extracted for convenience overriding in GemfireDataCommandsOverHttpDUnitTest to connect via
  // http
  public void connectToLocator() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  private static void setupRegions(String regionName, String parRegionName) {
    InternalCache cache = ClusterStartupRule.getCache();
    RegionFactory regionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);

    Region dataRegion = regionFactory.create(DATA_REGION_NAME);
    assertNotNull(dataRegion);

    dataRegion =
        dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1, dataRegion.getAttributes());
    assertNotNull(dataRegion);

    dataRegion =
        dataRegion.createSubregion(DATA_REGION_NAME_CHILD_1_2, dataRegion.getAttributes());
    assertNotNull(dataRegion);

    dataRegion = regionFactory.create(regionName);
    assertNotNull(dataRegion);

    PartitionAttributes partitionAttrs =
        new PartitionAttributesFactory().setRedundantCopies(2).create();
    RegionFactory<Object, Object> partitionRegionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    partitionRegionFactory.setPartitionAttributes(partitionAttrs);
    Region dataParRegion = partitionRegionFactory.create(DATA_PAR_REGION_NAME);
    assertNotNull(dataParRegion);
    dataParRegion = partitionRegionFactory.create(parRegionName);
    assertNotNull(dataParRegion);
  }



  void setupForGetPutRemoveLocateEntry(String testName) throws Exception {
    Properties props = locatorProperties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    props.setProperty(NAME, testName + "Manager");

    Properties serverProps = new Properties();
    serverProps.setProperty(SERIALIZABLE_OBJECT_FILTER, SERIALIZATION_FILTER);
    locator = cluster.startLocatorVM(0, props);
    server1 = cluster.startServerVM(1, serverProps, locator.getPort());
    server2 = cluster.startServerVM(2, serverProps, locator.getPort());

    gfsh.connectAndVerify(locator);

    server1.invoke(() -> setupRegions(DATA_REGION_NAME_VM1, DATA_PAR_REGION_NAME_VM1));

    server2.invoke(() -> {
      setupRegions(DATA_REGION_NAME_VM2, DATA_PAR_REGION_NAME_VM2);
    });

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_VM1_PATH, 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_REGION_NAME_VM2_PATH, 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_PATH, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_VM1_PATH, 1);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(DATA_PAR_REGION_NAME_VM2_PATH, 1);

    locator.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      final ManagementService service = ManagementService.getManagementService(cache);

      assertNotNull(service.getMemberMXBean());
      assertNotNull(service.getManagerMXBean());
      DistributedRegionMXBean bean = service.getDistributedRegionMXBean(DATA_REGION_NAME_PATH);
      assertNotNull(bean);

      String regions[] = {DATA_REGION_NAME_PATH, DATA_REGION_NAME_VM1_PATH,
          DATA_REGION_NAME_VM2_PATH, DATA_PAR_REGION_NAME_PATH, DATA_PAR_REGION_NAME_VM1_PATH,
          DATA_PAR_REGION_NAME_VM2_PATH};

      for (String region : regions) {
        bean = service.getDistributedRegionMXBean(region);
        assertNotNull(bean);

        if (bean.getMemberCount() < 1) {
          fail("Even after waiting mbean reports number of member hosting region "
              + DATA_REGION_NAME_VM1_PATH + " is less than one");
        }
      }
    });
  }

  @Test
  public void testPrimitivesWithDataCommands() throws Exception {
    setupForGetPutRemoveLocateEntry("testPrimitives");
    Byte byteKey = Byte.parseByte("41");
    Byte byteValue = Byte.parseByte("31");
    Short shortKey = Short.parseShort("123");
    Short shortValue = Short.parseShort("121");
    Integer integerKey = Integer.parseInt("123456");
    Integer integerValue = Integer.parseInt("12345678");
    Float floatKey = Float.valueOf("12432.2325");
    Float floatValue = Float.valueOf("111111.1111");
    Double doubleKey = Double.valueOf("12432.235425");
    Double doubleValue = Double.valueOf("111111.111111");

    // Testing Byte Wrappers
    testGetPutLocateEntryFromShellAndGemfire(byteKey, byteValue, Byte.class, true, true);
    // Testing Short Wrappers
    testGetPutLocateEntryFromShellAndGemfire(shortKey, shortValue, Short.class, true, true);
    // Testing Integer Wrappers
    testGetPutLocateEntryFromShellAndGemfire(integerKey, integerValue, Integer.class, true, true);
    // Testing Float Wrappers
    testGetPutLocateEntryFromShellAndGemfire(floatKey, floatValue, Float.class, true, true);
    // Testing Double Wrappers
    testGetPutLocateEntryFromShellAndGemfire(doubleKey, doubleValue, Double.class, true, true);
  }



  private void testGetPutLocateEntryFromShellAndGemfire(final Serializable key,
      final Serializable value, Class klass, boolean addRegionPath, boolean expResult) {

    SerializableRunnableIF putTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      region.clear();
      region.put(key, value);
    };

    SerializableRunnableIF getTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      assertEquals(true, region.containsKey(key));
      assertEquals(value, region.get(key));
    };

    SerializableRunnableIF removeTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      assertEquals(true, region.containsKey(key));
      region.remove(key);
    };


    SerializableRunnableIF clearTask = () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion(DATA_REGION_NAME_PATH);
      assertNotNull(region);
      region.clear();
    };

    String canonicalName = klass.getCanonicalName();
    String getCommand = "get --key=" + key + " --key-class=" + canonicalName
        + " --value-class=" + canonicalName;
    if (addRegionPath)
      getCommand += " --region=" + DATA_REGION_NAME_PATH;

    String locateEntryCommand = "locate entry --key=" + key + " --key-class="
        + canonicalName + " --value-class=" + canonicalName;
    if (addRegionPath)
      locateEntryCommand += " --region=" + DATA_REGION_NAME_PATH;

    String removeCommand = "remove --key=" + key + " --key-class=" + canonicalName;
    if (addRegionPath)
      removeCommand += " --region=" + DATA_REGION_NAME_PATH;
    String putCommand = "put --key=" + key + " --key-class=" + canonicalName
        + " --value=" + value + " --value-class=" + canonicalName;
    if (addRegionPath)
      putCommand += " --region=" + DATA_REGION_NAME_PATH;

    if (expResult) {
      // Do put from shell check gemfire get do gemfire remove
      CommandResult cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, true);
      server1.invoke(getTask);
      server1.invoke(removeTask);

      server1.invoke(clearTask);

      // Do put from gemfire check from shell do gemfire remove
      server1.invoke(putTask);
      cmdResult = gfsh.executeCommand(getCommand);
      validateResult(cmdResult, true);
      cmdResult = gfsh.executeCommand(locateEntryCommand);
      validateResult(cmdResult, true);
      server1.invoke(removeTask);

      server1.invoke(clearTask);

      // Do put from shell check from gemfire do remove from shell get from shell expect false
      cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, true);
      server1.invoke(getTask);
      cmdResult = gfsh.executeCommand(removeCommand);
      validateResult(cmdResult, true);
      cmdResult = gfsh.executeCommand(getCommand);
      validateResult(cmdResult, false);
      cmdResult = gfsh.executeCommand(locateEntryCommand);
      validateResult(cmdResult, false);
    } else {
      // Do put from shell check gemfire get do gemfire remove
      CommandResult cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, false);
      server1.invoke(clearTask);

      // Do put from gemfire check from shell do gemfire remove
      server1.invoke(putTask);
      cmdResult = gfsh.executeCommand(getCommand);
      validateResult(cmdResult, false);
      cmdResult = gfsh.executeCommand(locateEntryCommand);
      validateResult(cmdResult, false);
      server1.invoke(removeTask);
      server1.invoke(clearTask);

      // Do put from shell check from gemfire do remove from shell get from shell exepct false
      cmdResult = gfsh.executeCommand(putCommand);
      validateResult(cmdResult, false);
    }
  }

  private void validateResult(CommandResult cmdResult, Boolean expected) {
    if (ResultData.TYPE_MODEL.equals(cmdResult.getType())) {
      ResultModel rd = (ResultModel) cmdResult.getResultData();
      DataResultModel result = rd.getDataSection(DataCommandResult.DATA_INFO_SECTION);
      assertThat(result.getContent().get("Result")).isEqualTo(expected.toString());
    } else
      fail("Expected CompositeResult Returned Result Type " + cmdResult.getType());
  }

  private Properties locatorProperties() {
    int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost");
    props.setProperty(ConfigurationProperties.JMX_MANAGER_PORT, "" + jmxPort);

    return props;
  }

  public static class Value1WithValue2 extends Value1 {
    private Value2 value2 = null;

    public Value1WithValue2(int i) {
      super(i);
      value2 = new Value2(i);
    }

    public Value2 getValue2() {
      return value2;
    }

    public void setValue2(Value2 value2) {
      this.value2 = value2;
    }
  }

}
