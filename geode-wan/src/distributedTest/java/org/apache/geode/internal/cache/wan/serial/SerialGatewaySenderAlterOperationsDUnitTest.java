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
package org.apache.geode.internal.cache.wan.serial;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.wan.GatewaySender.DEFAULT_ORDER_POLICY;
import static org.apache.geode.cache.wan.GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getCurrentVMNum;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category(WanTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class SerialGatewaySenderAlterOperationsDUnitTest extends CacheTestCase {
  private static final Logger logger = LogService.getLogger();

  @Parameters(name = "{index}: numDispatchers={0}")
  public static Collection<Integer> data() {
    return asList(1, 3, 5);
  }

  @Parameter
  public int numDispatchers;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;
  private VM vm4;
  private VM vm5;
  private VM vm6;
  private VM vm7;

  private String className;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    addIgnoredException("Broken pipe");
    addIgnoredException("Connection refused");
    addIgnoredException("Connection reset");
    addIgnoredException("could not get remote locator information");
    addIgnoredException("Software caused connection abort");
    addIgnoredException("Unexpected IOException");

    className = getClass().getSimpleName();

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
    vm4 = getVM(4);
    vm5 = getVM(5);
    vm6 = getVM(6);
    vm7 = getVM(7);

    // Stopping the gateway closed the region, which causes this exception to get logged
    addIgnoredException(RegionDestroyedException.class);
  }

  @Test
  public void testStartPauseResumeSerialGatewaySenderUpdateAttributes() throws Exception {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    for (VM vm : toArray(vm2, vm3)) {
      vm.invoke(() -> {
        createCache(nyPort);
        createReceiver();
      });
    }

    vm4.invoke(() -> createCache(lnPort));
    vm5.invoke(() -> createCache(lnPort));
    vm6.invoke(() -> createCache(lnPort));
    vm7.invoke(() -> createCache(lnPort));

    vm4.invoke(() -> createSenderInVm4());
    vm5.invoke(() -> createSenderInVm5());

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));

    vm4.invoke(() -> validateSenderPausedState("ln"));
    vm5.invoke(() -> validateSenderPausedState("ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 1000));

    updateBatchSize(50);
    updateBatchTimeInterval(200);

    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));

    vm4.invoke(() -> validateSenderResumedState("ln"));
    vm5.invoke(() -> validateSenderResumedState("ln"));

    checkBatchSize(50);
    checkBatchTimeInterval(200);

    vm4.invoke(() -> validateQueueContents("ln", 0));
    vm5.invoke(() -> validateQueueContents("ln", 0));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 1000));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 1000));
  }

  @Test
  public void testSerialGatewaySenderUpdateAttributesWhilePutting() throws Exception {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    for (VM vm : toArray(vm2, vm3)) {
      vm.invoke(() -> {
        createCache(nyPort);
        createReceiver();
      });
    }

    vm4.invoke(() -> createCache(lnPort));
    vm5.invoke(() -> createCache(lnPort));
    vm6.invoke(() -> createCache(lnPort));
    vm7.invoke(() -> createCache(lnPort));

    vm4.invoke(() -> createSenderInVm4());
    vm5.invoke(() -> createSenderInVm5());

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    // Do some puts from both vm4 and vm5 while restarting a sender
    AsyncInvocation doPutsInVm4 =
        vm4.invokeAsync(() -> doPuts(className + "_RR", 1000));

    updateBatchSize(50);
    updateBatchTimeInterval(200);

    doPutsInVm4.await();

    checkBatchSize(50);
    checkBatchTimeInterval(200);

    vm4.invoke(() -> validateQueueContents("ln", 0));
    vm5.invoke(() -> validateQueueContents("ln", 0));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 1000));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 1000));
  }

  @Test
  public void testSerialGatewaySenderUpdateGatewayEventFiltersWhilePutting() throws Exception {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    List<GatewayEventFilter> filters = new ArrayList<>();
    filters.add(new MyGatewayEventFilter_AfterAck());
    filters.add(new PDXGatewayEventFilter());

    for (VM vm : toArray(vm2, vm3)) {
      vm.invoke(() -> {
        createCache(nyPort);
        createReceiver();
      });
    }

    vm4.invoke(() -> createCache(lnPort));
    vm5.invoke(() -> createCache(lnPort));
    vm6.invoke(() -> createCache(lnPort));
    vm7.invoke(() -> createCache(lnPort));

    vm4.invoke(() -> createSenderInVm4());
    vm5.invoke(() -> createSenderInVm5());

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    // Do some puts from both vm4 and vm5 while restarting a sender
    AsyncInvocation doPutsInVm4 =
        vm4.invokeAsync(() -> doPuts(className + "_RR", 5000));

    updateBatchSize(40);
    updateGatewayEventFilters(filters);

    doPutsInVm4.await();

    checkBatchSize(40);

    vm4.invoke(() -> validateQueueContents("ln", 0));
    vm5.invoke(() -> validateQueueContents("ln", 0));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 5000));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 5000));
  }

  protected boolean isOffHeap() {
    return false;
  }

  protected void createSenderInVm4() throws IOException {
    createSender("ln", 2, true, true, numDispatchers, DEFAULT_ORDER_POLICY);
  }

  protected void createSenderInVm5() throws IOException {
    createSender("ln", 2, true, true, numDispatchers, DEFAULT_ORDER_POLICY);
  }

  protected final void createSender(String id,
      int remoteDsId,
      boolean isPersistent,
      boolean isManualStart,
      int numDispatchers,
      OrderPolicy policy) throws IOException {
    try (IgnoredException ie = addIgnoredException("Could not connect")) {
      File persistentDirectory =
          temporaryFolder.newFolder(id + "_disk_" + currentTimeMillis() + "_" + getCurrentVMNum());
      DiskStoreFactory diskStoreFactory = getCache().createDiskStoreFactory();
      File[] dirs = new File[] {persistentDirectory};

      InternalGatewaySenderFactory gatewaySenderFactory =
          (InternalGatewaySenderFactory) getCache().createGatewaySenderFactory();

      gatewaySenderFactory.setParallel(false);
      gatewaySenderFactory.setMaximumQueueMemory(100);
      gatewaySenderFactory.setBatchSize(10);
      gatewaySenderFactory.setBatchConflationEnabled(false);
      gatewaySenderFactory.setManualStart(isManualStart);
      gatewaySenderFactory.setDispatcherThreads(numDispatchers);
      gatewaySenderFactory.setOrderPolicy(policy);
      gatewaySenderFactory.setSocketBufferSize(DEFAULT_SOCKET_BUFFER_SIZE);

      if (isPersistent) {
        gatewaySenderFactory.setPersistenceEnabled(true);
        gatewaySenderFactory.setDiskStoreName(
            diskStoreFactory.setDiskDirs(dirs).create(id).getName());
      } else {
        DiskStore store = diskStoreFactory.setDiskDirs(dirs).create(id);
        gatewaySenderFactory.setDiskStoreName(store.getName());
      }

      gatewaySenderFactory.create(id, remoteDsId);
    }
  }

  private Properties getDistributedSystemProperties(int locatorPort) {
    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    return props;
  }

  private void createCache(int locatorPort) {
    getCache(getDistributedSystemProperties(locatorPort));
  }

  private void createReplicatedRegion(String regionName, String senderIds) {
    try (IgnoredException ie1 = addIgnoredException(ForceReattemptException.class);
        IgnoredException ie2 = addIgnoredException(GatewaySenderException.class);
        IgnoredException ie3 = addIgnoredException(InterruptedException.class)) {
      RegionFactory regionFactory = getCache().createRegionFactory(REPLICATE);

      if (senderIds != null) {
        StringTokenizer tokenizer = new StringTokenizer(senderIds, ",");
        while (tokenizer.hasMoreTokens()) {
          String senderId = tokenizer.nextToken();
          regionFactory.addGatewaySenderId(senderId);
        }
      }

      regionFactory.setDataPolicy(DataPolicy.REPLICATE);
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);
      regionFactory.setOffHeap(isOffHeap());

      regionFactory.create(regionName);
    }
  }

  private void doPuts(String regionName, int count) {
    try (IgnoredException ie1 = addIgnoredException(GatewaySenderException.class);
        IgnoredException ie2 = addIgnoredException(InterruptedException.class)) {
      Region<Number, String> region = getCache().getRegion(SEPARATOR + regionName);
      for (int i = 0; i < count; i++) {
        region.put(i, "Value_" + i);
      }
    }
  }

  private void doPuts(String regionName, int from, int count) {
    Region<Number, String> region = getCache().getRegion(SEPARATOR + regionName);
    for (int i = from; i < count; i++) {
      region.put(i, "Value_" + i);
    }
  }

  private int createFirstLocatorWithDSId(int systemId) {
    stopOldLocator();
    int locatorPort = getRandomAvailableTCPPort();
    startLocator(systemId, locatorPort, locatorPort, -1, true);
    return locatorPort;
  }

  private int createFirstRemoteLocator(int systemId, int remoteLocatorPort) {
    stopOldLocator();
    int locatorPort = getRandomAvailableTCPPort();
    startLocator(systemId, locatorPort, locatorPort, remoteLocatorPort, true);
    return locatorPort;
  }

  private void startLocator(int systemId, int locatorPort, int startLocatorPort,
      int remoteLocatorPort, boolean startServerLocator) {
    Properties props = getDistributedSystemProperties();

    props.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(systemId));
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    props.setProperty(START_LOCATOR, "localhost[" + startLocatorPort + "],server="
        + startServerLocator + ",peer=true,hostname-for-clients=localhost");

    if (remoteLocatorPort != -1) {
      props.setProperty(REMOTE_LOCATORS, "localhost[" + remoteLocatorPort + "]");
    }

    // Start start the locator with a LOCATOR_DM_TYPE and not a NORMAL_DM_TYPE
    System.setProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE, "true");
    try {
      getSystem(props);
    } finally {
      System.clearProperty(InternalLocator.FORCE_LOCATOR_DM_TYPE);
    }
  }

  private void stopOldLocator() {
    if (Locator.hasLocator()) {
      Locator.getLocator().stop();
    }
  }

  private InternalGatewaySender findInternalGatewaySender(String senderId) {
    return (InternalGatewaySender) findGatewaySender(senderId, true);
  }

  private GatewaySender findGatewaySender(String senderId) {
    return findGatewaySender(senderId, true);
  }

  private GatewaySender findGatewaySender(String senderId, boolean assertNotNull) {
    Set<GatewaySender> senders = getCache().getGatewaySenders();
    GatewaySender sender = null;
    for (GatewaySender s : senders) {
      if (s.getId().equals(senderId)) {
        sender = s;
        break;
      }
    }

    if (assertNotNull) {
      assertThat(sender).isNotNull();
    }

    return sender;
  }

  private void startSender(String senderId) {
    try (IgnoredException ie1 = addIgnoredException("Could not connect");
        IgnoredException ie2 = addIgnoredException(ForceReattemptException.class);
        IgnoredException ie3 = addIgnoredException(InterruptedException.class)) {
      findGatewaySender(senderId).start();
    }
  }

  private void pauseSender(String senderId) {
    try (IgnoredException ie1 = addIgnoredException("Could not connect");
        IgnoredException ie2 = addIgnoredException(ForceReattemptException.class)) {
      InternalGatewaySender sender = findInternalGatewaySender(senderId);
      sender.pause();
      sender.getEventProcessor().waitForDispatcherToPause();
    }
  }

  private void resumeSender(String senderId) {
    try (IgnoredException ie1 = addIgnoredException("Could not connect");
        IgnoredException ie2 = addIgnoredException(ForceReattemptException.class)) {
      findGatewaySender(senderId).resume();
    }
  }

  private void stopSender(String senderId) {
    try (IgnoredException ie1 = addIgnoredException("Could not connect");
        IgnoredException ie2 = addIgnoredException(ForceReattemptException.class)) {
      InternalGatewaySender sender = findInternalGatewaySender(senderId);

      AbstractGatewaySenderEventProcessor eventProcessor = sender.getEventProcessor();

      sender.stop();

      if (eventProcessor instanceof ConcurrentSerialGatewaySenderEventProcessor) {
        ConcurrentSerialGatewaySenderEventProcessor concurrentEventProcessor =
            (ConcurrentSerialGatewaySenderEventProcessor) eventProcessor;
        Set<RegionQueue> queues = concurrentEventProcessor.getQueues();
        for (RegionQueue queue : queues) {
          if (queue instanceof SerialGatewaySenderQueue) {
            assertThat(((SerialGatewaySenderQueue) queue).isRemovalThreadAlive())
                .isFalse();
          }
        }
      }
    }
  }

  private int createReceiver() throws IOException {
    int receiverPort = getRandomAvailableTCPPort();

    GatewayReceiverFactory gatewayReceiverFactory = getCache().createGatewayReceiverFactory();
    gatewayReceiverFactory.setStartPort(receiverPort);
    gatewayReceiverFactory.setEndPort(receiverPort);
    gatewayReceiverFactory.setManualStart(true);

    GatewayReceiver receiver = gatewayReceiverFactory.create();
    receiver.start();

    return receiverPort;
  }

  private void validateRegionSize(String regionName, int regionSize) {
    try (IgnoredException ie1 = addIgnoredException(CacheClosedException.class);
        IgnoredException ie2 = addIgnoredException(ForceReattemptException.class)) {
      Region region = getCache().getRegion(SEPARATOR + regionName);

      await()
          .untilAsserted(() -> {
            assertThat(region.keySet()).hasSize(regionSize);
          });
    }
  }

  private void validateQueueContents(String senderId, int regionSize) {
    try (IgnoredException ie1 = addIgnoredException(GatewaySenderException.class);
        IgnoredException ie2 = addIgnoredException(InterruptedException.class)) {
      InternalGatewaySender sender = findInternalGatewaySender(senderId);

      if (sender.isParallel()) {
        RegionQueue regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
        await()
            .untilAsserted(() -> {
              assertThat(regionQueue.size())
                  .isEqualTo(regionSize);
            });

      } else {
        Set<RegionQueue> queues = sender.getQueues();
        await()
            .untilAsserted(() -> {
              int size = 0;
              for (RegionQueue queue : queues) {
                size += queue.size();
              }
              assertThat(size)
                  .isEqualTo(regionSize);
            });
      }
    }
  }

  private void validateSenderPausedState(String senderId) {
    GatewaySender sender = findGatewaySender(senderId);

    assertThat(sender.isPaused())
        .isTrue();
  }

  private void validateSenderResumedState(String senderId) {
    GatewaySender sender = findGatewaySender(senderId);

    assertThat(sender.isPaused())
        .isFalse();
    assertThat(sender.isRunning())
        .isTrue();
  }

  private void updateBatchSize(int batchsize) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setBatchSize(batchsize);
      if (paused) {
        sender.resume();
      }
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setBatchSize(batchsize);
      if (paused) {
        sender.resume();
      }
    });
  }

  private void updateBatchTimeInterval(int batchTimeInterval) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setBatchTimeInterval(batchTimeInterval);
      if (paused) {
        sender.resume();
      }
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setBatchTimeInterval(batchTimeInterval);
      if (paused) {
        sender.resume();
      }
    });
  }

  private void updateGroupTransactionEvents(boolean groupTransactionEvents) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setGroupTransactionEvents(groupTransactionEvents);
      if (paused) {
        sender.resume();
      }
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setGroupTransactionEvents(groupTransactionEvents);
      if (paused) {
        sender.resume();
      }
    });
  }

  private void updateGatewayEventFilters(List<GatewayEventFilter> filters) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setGatewayEventFilters(filters);
      if (paused) {
        sender.resume();
      }
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      boolean paused = false;
      if (sender.isRunning() && !sender.isPaused()) {
        sender.pause();
        paused = true;
      }
      sender.setGatewayEventFilters(filters);
      if (paused) {
        sender.resume();
      }
    });
  }

  private void checkBatchSize(int batchsize) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertThat(sender.getBatchSize()).isEqualTo(batchsize);
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertThat(sender.getBatchSize()).isEqualTo(batchsize);
    });
  }

  private void checkBatchTimeInterval(int batchTimeInterval) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertThat(sender.getBatchTimeInterval()).isEqualTo(batchTimeInterval);
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertThat(sender.getBatchTimeInterval()).isEqualTo(batchTimeInterval);
    });
  }

  private void checkGroupTransactionEvents(boolean groupTransactionEvents) {
    vm4.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertThat(sender.mustGroupTransactionEvents()).isEqualTo(groupTransactionEvents);
    });
    vm5.invoke(() -> {
      AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender("ln");
      assertThat(sender.mustGroupTransactionEvents()).isEqualTo(groupTransactionEvents);
    });

  }


  public static class MyGatewayEventFilter implements GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter";

    boolean beforeEnqueueInvoked;
    boolean beforeTransmitInvoked;
    boolean afterAckInvoked;

    public MyGatewayEventFilter() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      this.beforeEnqueueInvoked = true;
      return !((Integer) event.getKey() >= 500 && (Integer) event.getKey() < 600);
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      this.beforeTransmitInvoked = true;
      return !((Integer) event.getKey() >= 600 && (Integer) event.getKey() < 700);
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      this.afterAckInvoked = true;
      // TODO Auto-generated method stub
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter)) {
        return false;
      }
      MyGatewayEventFilter filter = (MyGatewayEventFilter) obj;
      return this.Id.equals(filter.Id);
    }
  }

  public static class MyGatewayEventFilter_AfterAck implements GatewayEventFilter, Serializable {

    String Id = "MyGatewayEventFilter_AfterAck";

    ConcurrentSkipListSet<Integer> ackList = new ConcurrentSkipListSet<Integer>();

    public MyGatewayEventFilter_AfterAck() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      ackList.add((Integer) event.getKey());
    }

    public Set getAckList() {
      return ackList;
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof MyGatewayEventFilter)) {
        return false;
      }
      MyGatewayEventFilter filter = (MyGatewayEventFilter) obj;
      return this.Id.equals(filter.Id);
    }
  }

  public static class PDXGatewayEventFilter implements GatewayEventFilter, Serializable {

    String Id = "PDXGatewayEventFilter";

    public int beforeEnqueueInvoked;
    public int beforeTransmitInvoked;
    public int afterAckInvoked;

    public PDXGatewayEventFilter() {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      System.out.println("Invoked enqueue for " + event);
      this.beforeEnqueueInvoked++;
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      System.out.println("Invoked transmit for " + event);
      this.beforeTransmitInvoked++;
      return true;
    }

    @Override
    public void close() {
      // TODO Auto-generated method stub

    }

    public String toString() {
      return Id;
    }

    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {
      System.out.println("Invoked afterAck for " + event);
      this.afterAckInvoked++;
      // TODO Auto-generated method stub
    }

    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof WANTestBase.MyGatewayEventFilter)) {
        return false;
      }
      MyGatewayEventFilter filter = (MyGatewayEventFilter) obj;
      return this.Id.equals(filter.Id);
    }
  }


}
