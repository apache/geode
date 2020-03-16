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

import static org.apache.geode.cache.wan.GatewaySender.DEFAULT_BATCH_TIME_INTERVAL;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;


public class SerialWANConflationDUnitTest extends WANTestBase {

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  private void createCacheWithLogFile(Integer locPort, String logFile) {
    WANTestBase test = new WANTestBase();
    Properties props = test.getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    String logLevel = System.getProperty(LOG_LEVEL, "info");
    props.setProperty(LOG_LEVEL, logLevel);
    props.setProperty(LOCATORS, "localhost[" + locPort + "]");
    props.setProperty(LOG_FILE, logFile);

    InternalDistributedSystem ds = test.getSystem(props);
    getCache(ds);
  }

  @SuppressWarnings("deprecation")
  private void getCache(InternalDistributedSystem ds) {
    cache = createCache(ds);
  }

  private File createDirectory(String name) {
    assertThat(name).isNotEmpty();

    File directory = new File(temporaryFolder.getRoot(), name);
    if (!directory.exists()) {
      try {
        return temporaryFolder.newFolder(name);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return directory;
  }

  private GatewaySenderFactory configureGateway(DiskStoreFactory dsf, File[] dirs1, String dsName,
      boolean isParallel, Integer maxMemory, Integer batchSize, boolean isConflation,
      boolean isPersistent, GatewayEventFilter filter, int numDispatchers,
      GatewaySender.OrderPolicy policy, int socketBufferSize, int batchTimeInterval) {
    InternalGatewaySenderFactory gateway =
        (InternalGatewaySenderFactory) cache.createGatewaySenderFactory();
    gateway.setParallel(isParallel);
    gateway.setMaximumQueueMemory(maxMemory);
    gateway.setBatchSize(batchSize);
    gateway.setBatchConflationEnabled(isConflation);
    gateway.setDispatcherThreads(numDispatchers);
    gateway.setOrderPolicy(policy);
    gateway.setLocatorDiscoveryCallback(new MyLocatorCallback());
    gateway.setSocketBufferSize(socketBufferSize);
    gateway.setBatchTimeInterval(batchTimeInterval);

    if (filter != null) {
      eventFilter = filter;
      gateway.addGatewayEventFilter(filter);
    }

    if (isPersistent) {
      gateway.setPersistenceEnabled(true);
      gateway.setDiskStoreName(dsf.setDiskDirs(dirs1).create(dsName).getName());
    } else {
      DiskStore store = dsf.setDiskDirs(dirs1).create(dsName);
      gateway.setDiskStoreName(store.getName());
    }

    return gateway;
  }

  private void createSender(String dsName, int remoteDsId, boolean isParallel, Integer maxMemory,
      Integer batchSize, boolean isConflation, boolean isPersistent, GatewayEventFilter filter,
      int batchTimeInterval) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      File persistentDirectory = createDirectory(dsName + "_disk_" + VM.getCurrentVMNum());
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      File[] dirs1 = new File[] {persistentDirectory};
      GatewaySenderFactory gateway = configureGateway(dsf, dirs1, dsName, isParallel, maxMemory,
          batchSize, isConflation, isPersistent, filter, numDispatcherThreadsForTheRun,
          GatewaySender.DEFAULT_ORDER_POLICY,
          GatewaySender.DEFAULT_SOCKET_BUFFER_SIZE, batchTimeInterval);
      gateway.create(dsName, remoteDsId);

    } finally {
      exln.remove();
    }
  }

  private void waitForEventQueueSize(int expectedQueueSize) {
    await().untilAsserted(() -> {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      Optional<GatewaySender> sender =
          senders.stream().filter(s -> s.getId().equals("ln")).findFirst();
      assertThat(sender.isPresent()).isTrue();
      Set<RegionQueue> queues = ((AbstractGatewaySender) sender.get()).getQueues();
      int totalEvents = queues.stream().mapToInt(RegionQueue::size).sum();
      assertThat(totalEvents).isEqualTo(expectedQueueSize);
    });
  }

  @Test
  public void testSerialPropagationPartitionRegionBatchConflation() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));

    vm4.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, false, 100, 50, false, false, null, true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));


    final Map<Object, Object> keyValues = new HashMap<>();

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        keyValues.put(j, i);
      }
      vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), keyValues));
    }

    vm4.invoke(() -> enableConflation("ln"));
    vm5.invoke(() -> enableConflation("ln"));
    vm6.invoke(() -> enableConflation("ln"));
    vm7.invoke(() -> enableConflation("ln"));

    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));
    vm6.invoke(() -> resumeSender("ln"));
    vm7.invoke(() -> resumeSender("ln"));

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    assertTrue("No events conflated in batch",
        (v4List.get(8) + v5List.get(8) + v6List.get(8) + v7List.get(8)) > 0);
  }

  @Test
  public void testSerialPropagationPartitionRegionConflationDuringEnqueue() {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2, vm3);

    vm2.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));
    vm3.invoke(() -> createPartitionedRegion(getTestMethodName(), null, 1, 8, isOffHeap()));

    createReceiverInVMs(vm2, vm3);

    createCacheInVMs(lnPort, vm4, vm5, vm6, vm7);

    vm4.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));
    vm6.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));
    vm7.invoke(() -> createPartitionedRegion(getTestMethodName(), "ln", 1, 8, isOffHeap()));

    vm4.invoke(() -> createSender("ln", 2, false, 100, 50, true, false, null, true));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 50, true, false, null, true));
    vm6.invoke(() -> createSender("ln", 2, false, 100, 50, true, false, null, true));
    vm7.invoke(() -> createSender("ln", 2, false, 100, 50, true, false, null, true));

    startSenderInVMs("ln", vm4, vm5, vm6, vm7);

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));
    vm6.invoke(() -> pauseSender("ln"));
    vm7.invoke(() -> pauseSender("ln"));


    final Map<Object, Object> keyValues = new HashMap<>();

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 10; j++) {
        keyValues.put(j, i);
      }
      vm4.invoke(() -> putGivenKeyValue(getTestMethodName(), keyValues));
    }

    ArrayList<Integer> v4List =
        (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 20));
    assertEquals("After conflation during enqueue, there should be only 20 events", 20,
        (int) v4List.get(0));

    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));
    vm6.invoke(() -> resumeSender("ln"));
    vm7.invoke(() -> resumeSender("ln"));

    v4List = (ArrayList<Integer>) vm4.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v5List =
        (ArrayList<Integer>) vm5.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v6List =
        (ArrayList<Integer>) vm6.invoke(() -> WANTestBase.getSenderStats("ln", 0));
    ArrayList<Integer> v7List =
        (ArrayList<Integer>) vm7.invoke(() -> WANTestBase.getSenderStats("ln", 0));

    assertEquals("No events in secondary queue stats since it's serial sender", 0,
        (v4List.get(10) + v5List.get(10) + v6List.get(10) + v7List.get(10)));
    assertEquals("Total queued events should be 100", 100,
        (v4List.get(2) + v5List.get(2) + v6List.get(2) + v7List.get(2)));

    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 10));

  }

  @Test
  // See GEODE-7079: a NullPointerException was thrown whenever the queue was recovered from disk
  // and the processor started dispatching events before the actual region was available.
  public void persistentSerialGatewayWithConflationShouldNotLooseEventsNorThrowNullPointerExceptionsWhenMemberIsRestartedWhileEventsAreStillOnTheQueue()
      throws IOException {
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    createCacheInVMs(nyPort, vm2);
    vm2.invoke(() -> createReplicatedRegion(getTestMethodName(), null, Scope.DISTRIBUTED_ACK,
        DataPolicy.PERSISTENT_REPLICATE, isOffHeap()));
    createReceiverInVMs(vm2);

    // Create Region, associate gateway and insert some entries.
    vm4.invoke(() -> {
      createCache(lnPort);
      createReplicatedRegion(getTestMethodName(), "ln", Scope.DISTRIBUTED_ACK, DataPolicy.REPLICATE,
          isOffHeap());

      // Large batch time interval and low batch size so no events are processed before the restart.
      createSender("ln", 2, false, 100, 10, true, true, null, 120000);

      Region<Integer, Integer> region = cache.getRegion(getTestMethodName());
      for (int i = 0; i < 5; i++) {
        region.put(i, i);
      }
      waitForEventQueueSize(5);
    });
    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 0));

    // Custom Log File to manually search for exceptions.
    File customLogFile = temporaryFolder.newFile("memberLog.log");

    vm4.invoke(() -> {
      // Restart the cache.
      cache.close();
      createCacheWithLogFile(lnPort, customLogFile.getAbsolutePath());

      // Recover the queue from disk, reduce batch thresholds so processing starts right away.
      createSender("ln", 2, false, 100, 5, true, true, null, DEFAULT_BATCH_TIME_INTERVAL);
      waitForSenderToBecomePrimary("ln");

      // Wait for the processors to start.
      await().until(() -> {
        Set<Thread> threads = Thread.getAllStackTraces().keySet();
        return threads
            .stream()
            .filter(t -> t.getName().contains("Processor for GatewaySender_ln"))
            .allMatch(Thread::isAlive);
      });

      // Create the region, processing will continue and no NPE should be thrown anymore.
      createReplicatedRegion(getTestMethodName(), "ln", Scope.DISTRIBUTED_ACK, DataPolicy.REPLICATE,
          isOffHeap());
    });
    vm2.invoke(() -> validateRegionSize(getTestMethodName(), 5));

    Files.lines(customLogFile.toPath()).forEach((line) -> assertThat(line)
        .as("Dispatchers shouldn't have thrown any errors while processing batches")
        .doesNotContain("An Exception occurred. The dispatcher will continue.")
        .doesNotContain("java.lang.NullPointerException"));
  }
}
