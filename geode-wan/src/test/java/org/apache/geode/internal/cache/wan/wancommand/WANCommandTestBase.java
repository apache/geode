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
package org.apache.geode.internal.cache.wan.wancommand;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.wan.*;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.commands.CliCommandTestBase;
import org.apache.geode.test.dunit.*;

import javax.management.remote.JMXConnectorServer;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.fail;

public abstract class WANCommandTestBase extends CliCommandTestBase {

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

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
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

  public Integer createFirstLocatorWithDSId(int dsId) {
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, ""+dsId);
    props.setProperty(LOCATORS, "localhost[" + port + "]");
    props.setProperty(START_LOCATOR, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    return port;
  }

  public Integer createFirstRemoteLocator(int dsId, int remoteLocPort) {
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, ""+dsId);
    props.setProperty(LOCATORS, "localhost[" + port + "]");
    props.setProperty(START_LOCATOR, "localhost[" + port + "],server=true,peer=true,hostname-for-clients=localhost");
    props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocPort + "]");
    getSystem(props);
    return port;
  }

  public void createCache(Integer locPort){
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public void createCacheWithGroups(Integer locPort, String groups){
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    props.setProperty(GROUPS, groups);
    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
  }

  public void createSender(String dsName, int remoteDsId,
      boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart) {
    File persistentDirectory = new File(dsName +"_disk_"+System.currentTimeMillis()+"_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File [] dirs1 = new File[] {persistentDirectory};
    if(isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
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
      gateway.setManualStart(isManualStart);
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

  public void startSender(String senderId){
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
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

  public void pauseSender(String senderId){
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
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

  public int createAndStartReceiver(int locPort) {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort
        + "]");

    InternalDistributedSystem ds = getSystem(props);
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
      fail("Test " + getName() + " failed to start GatewayReceiver");
    }
    return port;
  }

  public int createReceiver(int locPort) {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort
        + "]");

    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setStartPort(AvailablePort.AVAILABLE_PORTS_LOWER_BOUND);
    fact.setEndPort(AvailablePort.AVAILABLE_PORTS_UPPER_BOUND);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    return receiver.getPort();
  }

  public int createReceiverWithGroup(int locPort, String groups) {
    Properties props =  getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort
        + "]");
    props.setProperty(GROUPS, groups);

    InternalDistributedSystem ds = getSystem(props);
    cache = CacheFactory.create(ds);
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setStartPort(AvailablePort.AVAILABLE_PORTS_LOWER_BOUND);
    fact.setEndPort(AvailablePort.AVAILABLE_PORTS_UPPER_BOUND);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    return receiver.getPort();

  }

  public void startReceiver() {
    try {
      Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
      for (GatewayReceiver receiver : receivers) {
        receiver.start();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail("Test " + getName() + " failed to start GatewayReceiver");
    }
  }

  public void stopReceiver() {
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      receiver.stop();
    }
  }

  public int createAndStartReceiverWithGroup(int locPort, String groups) {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locPort
        + "]");
    props.setProperty(GROUPS, groups);

    InternalDistributedSystem ds = getSystem(props);
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
      fail("Test " + getName() + " failed to start GatewayReceiver on port " + port);
    }
    return port;
  }

  public DistributedMember getMember(){
    return cache.getDistributedSystem().getDistributedMember();
  }


  public int getLocatorPort(){
    return Locator.getLocators().get(0).getPort();
  }

  /**
   * Enable system property gemfire.disableManagement false in each VM.
   */
  public void enableManagement() {
    Invoke.invokeInEveryVM(new SerializableRunnable("Enable Management") {
      public void run() {
        System.setProperty(InternalDistributedSystem.DISABLE_MANAGEMENT_PROPERTY, "false");
      }
    });

  }

  public void verifySenderState(String senderId, boolean isRunning, boolean isPaused) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
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

  public void verifySenderAttributes(String senderId, int remoteDsID,
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
      List<String> actualEventFilterClassNames = new ArrayList<String>(
          actualGatewayEventFilters.size());
      for (GatewayEventFilter filter : actualGatewayEventFilters) {
        actualEventFilterClassNames.add(filter.getClass().getName());
      }

      for (String expectedGatewayEventFilter : expectedGatewayEventFilters) {
        if (!actualEventFilterClassNames.contains(expectedGatewayEventFilter)) {
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
      List<String> actualTransportFilterClassNames = new ArrayList<String>(
          actualGatewayTransportFilters.size());
      for (GatewayTransportFilter filter : actualGatewayTransportFilters) {
        actualTransportFilterClassNames.add(filter.getClass().getName());
      }

      for (String expectedGatewayTransportFilter : expectedGatewayTransportFilters) {
        if (!actualTransportFilterClassNames
            .contains(expectedGatewayTransportFilter)) {
          fail("GatewayTransportFilter " + expectedGatewayTransportFilter
              + " is not added to the GatewaySender.");
        }
      }
    }
  }

  public void verifyReceiverState(boolean isRunning) {
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      assertEquals(isRunning, receiver.isRunning());
    }
  }

  public void verifyReceiverCreationWithAttributes(boolean isRunning,
      int startPort, int endPort, String bindAddress, int maxTimeBetweenPings,
      int socketBufferSize, List<String> expectedGatewayTransportFilters) {

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
        List<String> actualTransportFilterClassNames = new ArrayList<String>(
            actualGatewayTransportFilters.size());
        for (GatewayTransportFilter filter : actualGatewayTransportFilters) {
          actualTransportFilterClassNames.add(filter.getClass().getName());
        }

        for (String expectedGatewayTransportFilter : expectedGatewayTransportFilters) {
          if (!actualTransportFilterClassNames
              .contains(expectedGatewayTransportFilter)) {
            fail("GatewayTransportFilter " + expectedGatewayTransportFilter
                + " is not added to the GatewayReceiver.");
          }
        }
      }
    }
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    closeCacheAndDisconnect();
    vm0.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm1.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm2.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm3.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm4.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm5.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm6.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
    vm7.invoke(() -> WANCommandTestBase.closeCacheAndDisconnect());
  }

  public static void closeCacheAndDisconnect() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
}
