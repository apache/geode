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
package org.apache.geode.internal.cache.wan.wancommand;

import static org.apache.geode.management.MXBeanAwaitility.awaitGatewayReceiverMXBeanProxy;
import static org.apache.geode.management.MXBeanAwaitility.awaitGatewaySenderMXBeanProxy;
import static org.apache.geode.management.MXBeanAwaitility.awaitMemberMXBeanProxy;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.ConnectionSource;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.CacheServerAdvisor;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.management.GatewayReceiverMXBean;
import org.apache.geode.management.GatewaySenderMXBean;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class WANCommandUtils implements Serializable {

  public static void createSender(String dsName, int remoteDsId, boolean isParallel,
      Integer maxMemory, Integer batchSize, boolean isConflation, boolean isPersistent,
      GatewayEventFilter filter, boolean isManualStart) {
    File persistentDirectory =
        new File(dsName + "_disk_" + System.currentTimeMillis() + "_" + VM.getCurrentVMNum());
    persistentDirectory.mkdir();
    Cache cache = ClusterStartupRule.getCache();
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    File[] dirs1 = new File[] {persistentDirectory};
    if (isParallel) {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setParallel(true);
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.setBatchConflationEnabled(isConflation);
      gateway.create(dsName, remoteDsId);

    } else {
      GatewaySenderFactory gateway = cache.createGatewaySenderFactory();
      gateway.setMaximumQueueMemory(maxMemory);
      gateway.setBatchSize(batchSize);
      gateway.setManualStart(isManualStart);
      if (filter != null) {
        gateway.addGatewayEventFilter(filter);
      }
      gateway.setBatchConflationEnabled(isConflation);
      if (isPersistent) {
        gateway.setPersistenceEnabled(true);
        gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
      } else {
        DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
        gateway.setDiskStoreName(store.getName());
      }
      gateway.create(dsName, remoteDsId);
    }
  }

  public static void startSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      Cache cache = ClusterStartupRule.getCache();
      Set<GatewaySender> senders = cache.getGatewaySenders();
      AbstractGatewaySender sender = (AbstractGatewaySender) senders.stream()
          .filter(s -> s.getId().equalsIgnoreCase(senderId)).findFirst().orElse(null);
      sender.start();
    } finally {
      exln.remove();
    }
  }

  public static void pauseSender(String senderId) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      Cache cache = ClusterStartupRule.getCache();
      Set<GatewaySender> senders = cache.getGatewaySenders();
      AbstractGatewaySender sender = (AbstractGatewaySender) senders.stream()
          .filter(s -> s.getId().equalsIgnoreCase(senderId)).findFirst().orElse(null);
      sender.pause();
    } finally {
      exln.remove();
    }
  }

  public static void verifySenderState(String senderId, boolean isRunning, boolean isPaused) {
    GatewaySender sender = ClusterStartupRule.getCache().getGatewaySenders().stream()
        .filter(x -> senderId.equals(x.getId())).findFirst().orElse(null);
    assertThat(sender.isRunning()).isEqualTo(isRunning);
    assertThat(sender.isPaused()).isEqualTo(isPaused);
  }

  public static void verifySenderAttributes(String senderId, int remoteDsID, boolean isParallel,
      boolean manualStart, int socketBufferSize, int socketReadTimeout,
      boolean enableBatchConflation, int batchSize, int batchTimeInterval,
      boolean enablePersistence, boolean diskSynchronous, int maxQueueMemory, int alertThreshold,
      int dispatcherThreads, GatewaySender.OrderPolicy orderPolicy,
      List<String> expectedGatewayEventFilters, List<String> expectedGatewayTransportFilters) {

    GatewaySender sender = ClusterStartupRule.getCache().getGatewaySenders().stream()
        .filter(x -> senderId.equals(x.getId())).findFirst().orElse(null);

    assertEquals("remoteDistributedSystemId", remoteDsID, sender.getRemoteDSId());
    assertEquals("isParallel", isParallel, sender.isParallel());
    assertEquals("manualStart", manualStart, sender.isManualStart());
    assertEquals("socketBufferSize", socketBufferSize, sender.getSocketBufferSize());
    assertEquals("socketReadTimeout", socketReadTimeout, sender.getSocketReadTimeout());
    assertEquals("enableBatchConflation", enableBatchConflation, sender.isBatchConflationEnabled());
    assertEquals("batchSize", batchSize, sender.getBatchSize());
    assertEquals("batchTimeInterval", batchTimeInterval, sender.getBatchTimeInterval());
    assertEquals("enablePersistence", enablePersistence, sender.isPersistenceEnabled());
    assertEquals("diskSynchronous", diskSynchronous, sender.isDiskSynchronous());
    assertEquals("maxQueueMemory", maxQueueMemory, sender.getMaximumQueueMemory());
    assertEquals("alertThreshold", alertThreshold, sender.getAlertThreshold());
    assertEquals("dispatcherThreads", dispatcherThreads, sender.getDispatcherThreads());
    assertEquals("orderPolicy", orderPolicy, sender.getOrderPolicy());

    // verify GatewayEventFilters
    if (expectedGatewayEventFilters != null) {
      assertEquals("gatewayEventFilters", expectedGatewayEventFilters.size(),
          sender.getGatewayEventFilters().size());

      List<GatewayEventFilter> actualGatewayEventFilters = sender.getGatewayEventFilters();
      List<String> actualEventFilterClassNames =
          new ArrayList<String>(actualGatewayEventFilters.size());
      for (GatewayEventFilter filter : actualGatewayEventFilters) {
        actualEventFilterClassNames.add(filter.getClass().getName());
      }

      for (String expectedGatewayEventFilter : expectedGatewayEventFilters) {
        if (!actualEventFilterClassNames.contains(expectedGatewayEventFilter)) {
          fail("GatewayEventFilter " + expectedGatewayEventFilter
              + " is not added to the GatewaySender");
        }
      }


      // verify GatewayTransportFilters
      if (expectedGatewayTransportFilters != null) {
        assertEquals("gatewayTransportFilters", expectedGatewayTransportFilters.size(),
            sender.getGatewayTransportFilters().size());
        List<GatewayTransportFilter> actualGatewayTransportFilters =
            sender.getGatewayTransportFilters();
        List<String> actualTransportFilterClassNames =
            new ArrayList<String>(actualGatewayTransportFilters.size());
        for (GatewayTransportFilter filter : actualGatewayTransportFilters) {
          actualTransportFilterClassNames.add(filter.getClass().getName());
        }

        for (String expectedGatewayTransportFilter : expectedGatewayTransportFilters) {
          if (!actualTransportFilterClassNames.contains(expectedGatewayTransportFilter)) {
            fail("GatewayTransportFilter " + expectedGatewayTransportFilter
                + " is not added to the GatewaySender.");
          }
        }
      }
    }
  }

  public static void verifySenderDoesNotExist(String senderId, boolean isParallel) {
    Cache cache = ClusterStartupRule.getCache();
    Set<GatewaySender> senders = cache.getGatewaySenders();
    Set<String> senderIds = senders.stream().map(AbstractGatewaySender.class::cast)
        .map(AbstractGatewaySender::getId).collect(Collectors.toSet());
    assertThat(senderIds).doesNotContain(senderId);
    String queueRegionNameSuffix = null;
    if (isParallel) {
      queueRegionNameSuffix = ParallelGatewaySenderQueue.QSTRING;
    } else {
      queueRegionNameSuffix = "_SERIAL_GATEWAY_SENDER_QUEUE";
    }
    Set<String> allRegions = ((GemFireCacheImpl) cache).getAllRegions().stream()
        .map(InternalRegion::getName).collect(Collectors.toSet());

    assertThat(allRegions).doesNotContain(senderId + queueRegionNameSuffix);
  }

  public static void startReceiver() {
    try {
      Cache cache = ClusterStartupRule.getCache();
      Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
      for (GatewayReceiver receiver : receivers) {
        receiver.start();
      }
    } catch (IOException e) {
      e.printStackTrace();
      // fail("Test " + getName() + " failed to start GatewayReceiver");
      fail("Failed to start GatewayReceiver");
    }
  }

  public static void stopReceivers() {
    Cache cache = ClusterStartupRule.getCache();
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      receiver.stop();
    }
  }

  public static void createAndStartReceiver(int locPort) {
    createReceiver(locPort);
    startReceiver();
  }

  public static void createReceiver(int locPort) {
    Cache cache = ClusterStartupRule.getCache();
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    fact.setStartPort(AvailablePort.AVAILABLE_PORTS_LOWER_BOUND);
    fact.setEndPort(AvailablePort.AVAILABLE_PORTS_UPPER_BOUND);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
  }

  public static void verifyReceiverState(boolean isRunning) {
    Set<GatewayReceiver> receivers = ClusterStartupRule.getCache().getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      assertEquals(isRunning, receiver.isRunning());
    }
  }

  public static void verifyGatewayReceiverServerLocations(int locatorPort, String expected) {
    PoolFactory pf = PoolManager.createFactory();
    pf.setServerGroup(GatewayReceiver.RECEIVER_GROUP);
    pf.addLocator("localhost", locatorPort);
    PoolImpl pool = (PoolImpl) pf.create("gateway-receiver-pool");
    ConnectionSource connectionSource = pool.getConnectionSource();
    List<ServerLocation> serverLocations = connectionSource.getAllServers();
    for (ServerLocation serverLocation : serverLocations) {
      assertEquals(expected, serverLocation.getHostName());
    }
  }

  public static void verifyGatewayReceiverProfile(String expected) {
    Set<GatewayReceiver> receivers = ((Cache) ClusterStartupRule.getCache()).getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      CacheServerImpl server = (CacheServerImpl) receiver.getServer();
      CacheServerAdvisor.CacheServerProfile profile =
          (CacheServerAdvisor.CacheServerProfile) server.getProfile();
      assertEquals(expected, profile.getHost());
    }
  }

  public static void verifyReceiverCreationWithAttributes(boolean isRunning, int startPort,
      int endPort, String bindAddress, int maxTimeBetweenPings, int socketBufferSize,
      List<String> expectedGatewayTransportFilters, String hostnameForSenders) {

    Set<GatewayReceiver> receivers = ((Cache) ClusterStartupRule.getCache()).getGatewayReceivers();
    assertEquals("Number of receivers is incorrect", 1, receivers.size());
    for (GatewayReceiver receiver : receivers) {
      assertEquals("isRunning", isRunning, receiver.isRunning());
      assertEquals("startPort", startPort, receiver.getStartPort());
      assertEquals("endPort", endPort, receiver.getEndPort());
      assertEquals("bindAddress", bindAddress, receiver.getBindAddress());
      assertEquals("maximumTimeBetweenPings", maxTimeBetweenPings,
          receiver.getMaximumTimeBetweenPings());
      assertEquals("socketBufferSize", socketBufferSize, receiver.getSocketBufferSize());
      assertEquals("hostnameForSenders", hostnameForSenders, receiver.getHostnameForSenders());

      // verify GatewayTransportFilters
      if (expectedGatewayTransportFilters != null) {
        assertEquals("gatewayTransportFilters", expectedGatewayTransportFilters.size(),
            receiver.getGatewayTransportFilters().size());
        List<GatewayTransportFilter> actualGatewayTransportFilters =
            receiver.getGatewayTransportFilters();
        List<String> actualTransportFilterClassNames =
            new ArrayList<String>(actualGatewayTransportFilters.size());
        for (GatewayTransportFilter filter : actualGatewayTransportFilters) {
          actualTransportFilterClassNames.add(filter.getClass().getName());
        }

        for (String expectedGatewayTransportFilter : expectedGatewayTransportFilters) {
          if (!actualTransportFilterClassNames.contains(expectedGatewayTransportFilter)) {
            fail("GatewayTransportFilter " + expectedGatewayTransportFilter
                + " is not added to the GatewayReceiver.");
          }
        }
      }
    }
  }

  public static void verifyReceiverDoesNotExist() {
    Set<GatewayReceiver> receivers = ClusterStartupRule.getCache().getGatewayReceivers();
    assertThat(receivers.size()).isEqualTo(0);
  }

  public static void validateMemberMXBeanProxy(final InternalDistributedMember member) {
    MemberMXBean memberMXBean = awaitMemberMXBeanProxy(member);
    assertThat(memberMXBean).isNotNull();
  }

  public static void validateGatewaySenderMXBeanProxy(final InternalDistributedMember member,
      final String senderId, final boolean isRunning, final boolean isPaused) {
    GatewaySenderMXBean gatewaySenderMXBean = awaitGatewaySenderMXBeanProxy(member, senderId);
    GeodeAwaitility.await(
        "Awaiting GatewaySenderMXBean.isRunning(" + isRunning + ").isPaused(" + isPaused + ")")
        .untilAsserted(() -> {
          assertThat(gatewaySenderMXBean.isRunning()).isEqualTo(isRunning);
          assertThat(gatewaySenderMXBean.isPaused()).isEqualTo(isPaused);
        });
    assertThat(gatewaySenderMXBean).isNotNull();
  }

  public static void validateGatewayReceiverMXBeanProxy(final InternalDistributedMember member,
      final boolean isRunning) {
    GatewayReceiverMXBean gatewayReceiverMXBean = awaitGatewayReceiverMXBeanProxy(member);
    GeodeAwaitility.await("Awaiting GatewayReceiverMXBean.isRunning(" + isRunning + ")")
        .untilAsserted(() -> {
          assertThat(gatewayReceiverMXBean.isRunning()).isEqualTo(isRunning);
        });
    assertThat(gatewayReceiverMXBean).isNotNull();
  }

  public static InternalDistributedMember getMember(final VM vm) {
    return vm.invoke(() -> {
      return ClusterStartupRule.getCache().getMyId();
    });
  }

  public static SerializableCallableIF<DistributedMember> getMemberIdCallable() {
    return () -> ClusterStartupRule.getCache().getDistributedSystem().getDistributedMember();
  }
}
