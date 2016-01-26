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
package com.gemstone.gemfire.internal.cache.wan.wancommand;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.management.remote.JMXConnectorServer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySender.OrderPolicy;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class WANCommandTestBase extends CliCommandTestBase{

  static Cache cache;
  private JMXConnectorServer jmxConnectorServer;
  private ManagementService managementService;
//  public String jmxHost;
//  public int jmxPort;

  static VM vm0;
  static VM vm1;
  static VM vm2;
  static VM vm3;
  static VM vm4;
  static VM vm5;
  static VM vm6;
  static VM vm7;

  public WANCommandTestBase(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    vm4 = host.getVM(4);
    vm5 = host.getVM(5);
    vm6 = host.getVM(6);
    vm7 = host.getVM(7);
    enableManagement();
  }

  public static Integer createFirstLocatorWithDSId(int dsId) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    return port;
  }

  public static Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, ""+dsId);
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + port + "]");
    props.setProperty(DistributionConfig.START_LOCATOR_NAME, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost[" + remoteLocPort + "]");
    test.getSystem(props);
    return port;
  }

  public static void createCache(Integer locPort){
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public static void createCacheWithGroups(Integer locPort, String groups){
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort + "]");
    props.setProperty(DistributionConfig.GROUPS_NAME, groups);
    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public static void createSender(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManulaStart) {
    File persistentDirectory = new File(dsName +"_disk_"+System.currentTimeMillis()+"_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File [] dirs1 = new File[] {persistentDirectory};
    if(isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if(isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    }else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManulaStart);
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if(isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      }
      else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  public static void startSender(String senderId){
    final ExpectedException exln = addExpectedException("Could not connect");
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.start();
    } finally {
      exln.remove();
    }
  }

  public static void pauseSender(String senderId){
    final ExpectedException exln = addExpectedException("Could not connect");
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      GatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = s;
          break;
        }
      }
      sender.pause();
    } finally {
      exln.remove();
    }
  }

  public static int createAndStartReceiver(int locPort) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
         receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayRecevier");
    }
    return port;
  }

  public static int createReceiver(int locPort) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    return port;
  }

  public static int createReceiverWithGroup(int locPort, String groups) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");
    props.setProperty(DistributionConfig.GROUPS_NAME, groups);

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    return port;
  }

  public static void startReceiver() {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    try {
      Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
      for (GatewayReceiver receiver : receivers) {
        receiver.start();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayRecevier");
    }
  }

  public static void stopReceiver() {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      receiver.stop();
    }
  }

  public static int createAndStartReceiverWithGroup(int locPort, String groups) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + locPort
        + "]");
    props.setProperty(DistributionConfig.GROUPS_NAME, groups);

    InternalDistributedSystem ds = test.getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + test.getName() + " failed to start GatewayRecevier on port " + port);
    }
    return port;
  }

  public static DistributedMember getMember(){
    return cache.getDistributedSystem().getDistributedMember();
  }


  public static int getLocatorPort(){
    return Locator.getLocators().get(0).getPort();
  }

  /**
   * Enable system property gemfire.disableManagement false in each VM.
   *
   * @throws Exception
   */
  public void enableManagement() {
    invokeInEveryVM(new SerializableRunnable("Enable Management") {
      public void run() {
        System.setProperty(InternalDistributedSystem.DISABLE_MANAGEMENT_PROPERTY, "false");
      }
    });

  }

  public static void verifySenderState(String senderId, boolean isRunning, boolean isPaused) {
    final ExpectedException exln = addExpectedException("Could not connect");
    try {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      AbstractGatewaySender sender = null;
      for (GatewaySender s : senders) {
        if (s.getId().equals(senderId)) {
          sender = (AbstractGatewaySender) s;
          break;
        }
      }

      assertEquals(isRunning, sender.isRunning());
      assertEquals(isPaused, sender.isPaused());
    } finally {
      exln.remove();
    }
  }

  public static void verifySenderAttributes(String senderId, int remoteDsID,
      boolean isParallel, boolean manualStart, int socketBufferSize,
      int socketReadTimeout, boolean enableBatchConflation, int batchSize,
      int batchTimeInterval, boolean enablePersistence,
      boolean diskSynchronous, int maxQueueMemory, int alertThreshold,
      int dispatcherThreads, OrderPolicy orderPolicy,
      List<String> expectedGatewayEventFilters,
      List<String> expectedGatewayTransportFilters) {

    Set<GatewaySender> senders = cache.getGatewaySenders();
    AbstractGatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = (AbstractGatewaySender)s;
        break;
      }
    }
    assertEquals("remoteDistributedSystemId", remoteDsID, sender
        .getRemoteDSId());
    assertEquals("isParallel", isParallel, sender.isParallel());
    assertEquals("manualStart", manualStart, sender.isManualStart());
    assertEquals("socketBufferSize", socketBufferSize, sender
        .getSocketBufferSize());
    assertEquals("socketReadTimeout", socketReadTimeout, sender
        .getSocketReadTimeout());
    assertEquals("enableBatchConflation", enableBatchConflation, sender
        .isBatchConflationEnabled());
    assertEquals("batchSize", batchSize, sender.getBatchSize());
    assertEquals("batchTimeInterval", batchTimeInterval, sender
        .getBatchTimeInterval());
    assertEquals("enablePersistence", enablePersistence, sender
        .isPersistenceEnabled());
    assertEquals("diskSynchronous", diskSynchronous, sender.isDiskSynchronous());
    assertEquals("maxQueueMemory", maxQueueMemory, sender
        .getMaximumQueueMemory());
    assertEquals("alertThreshold", alertThreshold, sender.getAlertThreshold());
    assertEquals("dispatcherThreads", dispatcherThreads, sender
        .getDispatcherThreads());
    assertEquals("orderPolicy", orderPolicy, sender.getOrderPolicy());

    // verify GatewayEventFilters
    if (expectedGatewayEventFilters != null) {
      assertEquals("gatewayEventFilters", expectedGatewayEventFilters.size(),
          sender.getGatewayEventFilters().size());

      List<GatewayEventFilter> actualGatewayEventFilters = sender
          .getGatewayEventFilters();
      List<String> actualEventFilterClassnames = new ArrayList<String>(
          actualGatewayEventFilters.size());
      for (GatewayEventFilter filter : actualGatewayEventFilters) {
        actualEventFilterClassnames.add(filter.getClass().getName());
      }

      for (String expectedGatewayEventFilter : expectedGatewayEventFilters) {
        if (!actualEventFilterClassnames.contains(expectedGatewayEventFilter)) {
          fail("GatewayEventFilter " + expectedGatewayEventFilter
              + " is not added to the GatewaySender");
        }
      }
    }

    // verify GatewayTransportFilters
    if (expectedGatewayTransportFilters != null) {
      assertEquals("gatewayTransportFilters", expectedGatewayTransportFilters
          .size(), sender.getGatewayTransportFilters().size());
      List<GatewayTransportFilter> actualGatewayTransportFilters = sender
          .getGatewayTransportFilters();
      List<String> actualTransportFilterClassnames = new ArrayList<String>(
          actualGatewayTransportFilters.size());
      for (GatewayTransportFilter filter : actualGatewayTransportFilters) {
        actualTransportFilterClassnames.add(filter.getClass().getName());
      }

      for (String expectedGatewayTransportFilter : expectedGatewayTransportFilters) {
        if (!actualTransportFilterClassnames
            .contains(expectedGatewayTransportFilter)) {
          fail("GatewayTransportFilter " + expectedGatewayTransportFilter
              + " is not added to the GatewaySender.");
        }
      }
    }
  }

  public static void verifyReceiverState(boolean isRunning) {
    WANCommandTestBase test = new WANCommandTestBase(testName);
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      assertEquals(isRunning, receiver.isRunning());
    }
  }

  public static void verifyReceiverCreationWithAttributes(boolean isRunning,
      int startPort, int endPort, String bindAddress, int maxTimeBetweenPings,
      int socketBufferSize, List<String> expectedGatewayTransportFilters) {

    WANCommandTestBase test = new WANCommandTestBase(testName);
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    assertEquals("Number of receivers is incorrect", 1, receivers.size());
    for (GatewayReceiver receiver : receivers) {
      assertEquals("isRunning", isRunning, receiver.isRunning());
      assertEquals("startPort", startPort, receiver.getStartPort());
      assertEquals("endPort", endPort, receiver.getEndPort());
      assertEquals("bindAddress", bindAddress, receiver.getBindAddress());
      assertEquals("maximumTimeBetweenPings", maxTimeBetweenPings, receiver
          .getMaximumTimeBetweenPings());
      assertEquals("socketBufferSize", socketBufferSize, receiver
          .getSocketBufferSize());

      // verify GatewayTransportFilters
      if (expectedGatewayTransportFilters != null) {
        assertEquals("gatewayTransportFilters", expectedGatewayTransportFilters
            .size(), receiver.getGatewayTransportFilters().size());
        List<GatewayTransportFilter> actualGatewayTransportFilters = receiver
            .getGatewayTransportFilters();
        List<String> actualTransportFilterClassnames = new ArrayList<String>(
            actualGatewayTransportFilters.size());
        for (GatewayTransportFilter filter : actualGatewayTransportFilters) {
          actualTransportFilterClassnames.add(filter.getClass().getName());
        }

        for (String expectedGatewayTransportFilter : expectedGatewayTransportFilters) {
          if (!actualTransportFilterClassnames
              .contains(expectedGatewayTransportFilter)) {
            fail("GatewayTransportFilter " + expectedGatewayTransportFilter
                + " is not added to the GatewayReceiver.");
          }
        }
      }
    }
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
    closeCache();
    vm0.invoke(WANCommandTestBase.class, "closeCache");
    vm1.invoke(WANCommandTestBase.class, "closeCache");
    vm2.invoke(WANCommandTestBase.class, "closeCache");
    vm3.invoke(WANCommandTestBase.class, "closeCache");
    vm4.invoke(WANCommandTestBase.class, "closeCache");
    vm5.invoke(WANCommandTestBase.class, "closeCache");
    vm6.invoke(WANCommandTestBase.class, "closeCache");
    vm7.invoke(WANCommandTestBase.class, "closeCache");
  }

  public static void closeCache() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
