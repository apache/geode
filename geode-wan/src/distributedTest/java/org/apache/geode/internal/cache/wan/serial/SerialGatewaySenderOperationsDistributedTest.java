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
import static java.lang.System.lineSeparator;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
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
import static org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID.getNewProxyMembership;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.greaterThan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.locator.QueueConnectionRequest;
import org.apache.geode.cache.client.internal.locator.QueueConnectionResponse;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.InternalGatewaySender;
import org.apache.geode.internal.cache.wan.InternalGatewaySenderFactory;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
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
public class SerialGatewaySenderOperationsDistributedTest extends CacheTestCase {
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
    className = getClass().getSimpleName();

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
    vm4 = getVM(4);
    vm5 = getVM(5);
    vm6 = getVM(6);
    vm7 = getVM(7);

    addIgnoredException("Broken pipe");
    addIgnoredException("Connection refused");
    addIgnoredException("Connection reset");
    addIgnoredException("could not get remote locator information");
    addIgnoredException("Software caused connection abort");
    addIgnoredException("Unexpected IOException");

    // Stopping the gateway closed the region, which causes this exception to get logged
    addIgnoredException(RegionDestroyedException.class);
  }

  @Test
  public void testSerialGatewaySenderOperationsWithoutStarting() {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 10));
    vm4.invoke(() -> doPuts(className + "_RR", 100));

    vm4.invoke(() -> validateSenderOperations("ln"));
    vm5.invoke(() -> validateSenderOperations("ln"));
  }

  @Test
  public void testStartPauseResumeSerialGatewaySender() throws Exception {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 10));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 100));

    vm4.invoke(() -> pauseSender("ln"));
    vm5.invoke(() -> pauseSender("ln"));

    vm4.invoke(() -> validateSenderPausedState("ln"));
    vm5.invoke(() -> validateSenderPausedState("ln"));

    AsyncInvocation<?> doPutsInVm4 =
        vm4.invokeAsync(() -> doPuts(className + "_RR", 10));

    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));

    vm4.invoke(() -> validateSenderResumedState("ln"));
    vm5.invoke(() -> validateSenderResumedState("ln"));

    doPutsInVm4.await();

    vm4.invoke(() -> validateQueueContents("ln", 0));
    vm5.invoke(() -> validateQueueContents("ln", 0));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 100));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 100));
  }

  @Test
  public void testStopSerialGatewaySender() throws Exception {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 20));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 20));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 20));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 20));

    vm2.invoke(this::stopReceivers);
    vm3.invoke(this::stopReceivers);

    vm4.invoke(() -> doPuts(className + "_RR", 20));

    vm4.invoke(() -> validateQueueSizeStat("ln", 20));
    vm5.invoke(() -> validateQueueSizeStat("ln", 20));

    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> validateSenderStoppedState("ln"));
    vm5.invoke(() -> validateSenderStoppedState("ln"));

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateQueueSizeStat("ln", 0));
    /*
     * Should have no effect on GatewaySenderState
     */
    vm4.invoke(() -> resumeSender("ln"));
    vm5.invoke(() -> resumeSender("ln"));

    vm4.invoke(() -> validateSenderStoppedState("ln"));
    vm5.invoke(() -> validateSenderStoppedState("ln"));

    AsyncInvocation<?> startSenderInVm4 = vm4.invokeAsync(() -> startSender("ln"));
    AsyncInvocation<?> startSenderInVm5 = vm5.invokeAsync(() -> startSender("ln"));

    startSenderInVm4.await();
    startSenderInVm5.await();

    vm4.invoke(() -> validateQueueSizeStat("ln", 20));
    vm5.invoke(() -> validateQueueSizeStat("ln", 20));

    vm5.invoke(() -> doPuts(className + "_RR", 110));

    vm4.invoke(() -> validateQueueSizeStat("ln", 130));
    vm5.invoke(() -> validateQueueSizeStat("ln", 130));

    vm2.invoke(this::startReceivers);
    vm3.invoke(this::startReceivers);

    vm4.invoke(() -> validateSenderResumedState("ln"));
    vm5.invoke(() -> validateSenderResumedState("ln"));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 110));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 110));

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateQueueSizeStat("ln", 0));
  }

  @Test
  public void testRestartSerialGatewaySendersWhilePutting() throws Exception {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    vm7.invoke(() -> doPuts(className + "_RR", 20));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm7.invoke(() -> doPuts(className + "_RR", 20));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 20));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 20));

    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> validateSenderStoppedState("ln"));
    vm5.invoke(() -> validateSenderStoppedState("ln"));

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateQueueSizeStat("ln", 0));


    // do a lot of puts while senders are restarting
    AsyncInvocation<?> doPutsInVm7 = vm7.invokeAsync(() -> doPuts(className + "_RR", 5000));

    AsyncInvocation<?> startSenderInVm4 = vm4.invokeAsync(() -> startSender("ln"));
    AsyncInvocation<?> startSenderInVm5 = vm5.invokeAsync(() -> startSender("ln"));

    startSenderInVm4.await();
    startSenderInVm5.await();

    doPutsInVm7.await();

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateQueueSizeStat("ln", 0));
    vm4.invoke(() -> validateSecondaryQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateSecondaryQueueSizeStat("ln", 0));
  }

  @Test
  public void testSerialGatewaySendersPrintQueueContents() throws Exception {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

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

    vm7.invoke(() -> doPuts(className + "_RR", 20));

    await()
        .until(() -> stream(toArray(vm4, vm5))
            .map(vm -> vm.invoke(() -> {
              InternalGatewaySender sender = findInternalGatewaySender("ln");
              logger.info(displaySerialQueueContent(sender));
              return sender.getEventQueueSize();
            }))
            .mapToInt(i -> i)
            .sum(),
            greaterThan(0));

    AsyncInvocation<?> resumeSenderInVm4 = vm4.invokeAsync(() -> resumeSender("ln"));
    AsyncInvocation<?> resumeSenderInVm5 = vm5.invokeAsync(() -> resumeSender("ln"));

    resumeSenderInVm4.await();
    resumeSenderInVm5.await();

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateQueueSizeStat("ln", 0));
    vm4.invoke(() -> validateSecondaryQueueSizeStat("ln", 0));
    vm5.invoke(() -> validateSecondaryQueueSizeStat("ln", 0));
  }

  @Test
  public void testStopOneSerialGatewaySenderBothPrimary() throws Exception {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 100));

    vm4.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> validateSenderStoppedState("ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 200));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 200));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 200));

    // Do some puts from both vm4 and vm5 while restarting a sender
    AsyncInvocation<?> doPutsInVm4 =
        vm4.invokeAsync(() -> doPuts(className + "_RR", 300));

    Thread.sleep(10);
    vm4.invoke(() -> startSender("ln"));

    doPutsInVm4.await();

    vm2.invoke(() -> validateRegionSize(className + "_RR", 300));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 300));

    vm4.invoke(() -> validateQueueSizeStat("ln", 0));
  }

  @Test
  public void testStopOneSerialGatewaySender_PrimarySecondary() {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 10));

    vm4.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> validateSenderStoppedState("ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 100));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 100));
    vm3.invoke(() -> validateRegionSize(className + "_RR", 100));
  }

  @Test
  public void testStopOneSender_StartAnotherSender() {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> createCache(nyPort));
    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm2.invoke(this::createReceiver);

    vm4.invoke(() -> createCache(lnPort));
    vm4.invoke(this::createSenderInVm4);
    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm4.invoke(() -> startSender("ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 10));
    vm4.invoke(() -> stopSender("ln"));
    vm4.invoke(() -> validateSenderStoppedState("ln"));

    vm5.invoke(() -> createCache(lnPort));
    vm5.invoke(this::createSenderInVm5);
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> startSender("ln"));

    vm5.invoke(() -> doPuts(className + "_RR", 100));

    vm2.invoke(() -> validateRegionSize(className + "_RR", 100));
  }

  @Test
  public void test_Bug44153_StopOneSender_StartAnotherSender_CheckQueueSize() {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm4.invoke(() -> createCache(lnPort));
    vm4.invoke(this::createSenderInVm4);
    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm4.invoke(() -> startSender("ln"));

    vm4.invoke(() -> doPuts(className + "_RR", 10));
    vm4.invoke(() -> validateQueueContents("ln", 10));
    vm4.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> validateSenderStoppedState("ln"));

    vm5.invoke(() -> createCache(lnPort));
    vm5.invoke(this::createSenderInVm5);
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> startSender("ln"));

    vm5.invoke(() -> doPuts(className + "_RR", 10, 110));

    vm5.invoke(() -> validateQueueContents("ln", 100));
    vm5.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> validateSenderStoppedState("ln"));

    vm4.invoke(() -> startSender("ln"));
    vm4.invoke(() -> validateQueueContents("ln", 10));
    vm4.invoke(() -> stopSender("ln"));

    vm5.invoke(() -> startSender("ln"));
    vm2.invoke(() -> createCache(nyPort));
    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm2.invoke(this::createReceiver);

    vm2.invoke(() -> validateRegionSize(className + "_RR", 100));
    vm5.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> startSender("ln"));
    vm2.invoke(() -> validateRegionSize(className + "_RR", 110));
    vm4.invoke(() -> stopSender("ln"));
  }

  /**
   * Destroy SerialGatewaySender on all the nodes.
   */
  @Test
  public void testDestroySerialGatewaySenderOnAllNodes() {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 10));

    // before destroying, stop the sender
    vm4.invoke(() -> stopSender("ln"));
    vm5.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> removeSenderFromTheRegion("ln", className + "_RR"));
    vm5.invoke(() -> removeSenderFromTheRegion("ln", className + "_RR"));

    vm4.invoke(() -> destroySender("ln"));
    vm5.invoke(() -> destroySender("ln"));

    vm4.invoke(() -> validateSenderDestroyed("ln", false));
    vm5.invoke(() -> validateSenderDestroyed("ln", false));
  }

  /**
   * Destroy SerialGatewaySender on a single node.
   */
  @Test
  public void testDestroySerialGatewaySenderOnSingleNode() {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 10));

    // before destroying, stop the sender
    vm4.invoke(() -> stopSender("ln"));

    vm4.invoke(() -> removeSenderFromTheRegion("ln", className + "_RR"));

    vm4.invoke(() -> destroySender("ln"));

    vm4.invoke(() -> validateSenderDestroyed("ln", false));
    vm5.invoke(() -> verifySenderRunningState("ln"));
  }

  /**
   * Since the sender is attached to a region and in use, it can not be destroyed. Hence, exception
   * is thrown by the sender API.
   */
  @Test
  public void testDestroySerialGatewaySenderExceptionScenario() {
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

    vm4.invoke(this::createSenderInVm4);
    vm5.invoke(this::createSenderInVm5);

    vm2.invoke(() -> createReplicatedRegion(className + "_RR", null));
    vm3.invoke(() -> createReplicatedRegion(className + "_RR", null));

    vm4.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm5.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm6.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));
    vm7.invoke(() -> createReplicatedRegion(className + "_RR", "ln"));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    vm4.invoke(() -> doPuts(className + "_RR", 10));

    vm4.invoke(() -> {
      assertThatThrownBy(() -> destroySender("ln")).isInstanceOf(GatewaySenderException.class);
    });

    vm2.invoke(() -> validateRegionSize(className + "_RR", 10));
  }

  @Test
  public void testGatewaySenderNotRegisteredAsCacheServer() {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    for (VM vm : toArray(vm2, vm3)) {
      vm.invoke(() -> {
        createCache(nyPort);
        createReceiver();
      });
    }

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> createCache(lnPort));
    }

    vm4.invoke(() -> createSender("ln", 2, true, true, numDispatchers, DEFAULT_ORDER_POLICY));
    vm5.invoke(() -> createSender("ln", 2, true, true, numDispatchers, DEFAULT_ORDER_POLICY));

    for (VM vm : toArray(vm4, vm5)) {
      vm.invoke(() -> startSender("ln"));
    }

    for (VM vm : toArray(vm0, vm1)) {
      vm.invoke(() -> {
        InternalLocator internalLocator = (InternalLocator) Locator.getLocator();
        ServerLocator serverLocator = internalLocator.getServerLocatorAdvisee();

        final Map<?, ?> loadMap = serverLocator.getLoadMap();
        assertThat(loadMap)
            .as("Empty server load map")
            .isEmpty();

        ClientProxyMembershipID clientProxyMembership = getNewProxyMembership(getSystem());
        QueueConnectionRequest request =
            new QueueConnectionRequest(clientProxyMembership, 1, new HashSet<>(), "", false);
        QueueConnectionResponse response =
            (QueueConnectionResponse) serverLocator.processRequest(request);

        assertThat(response.getServers())
            .as("Empty response servers")
            .isEmpty();
      });
    }
  }

  @Test
  public void registeringInstantiatorsInGatewayShouldNotCauseDeadlock() {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> createReceiverAndServer(nyPort));
    vm3.invoke(() -> createReceiverAndServer(lnPort));

    vm2.invoke(() -> createSender("ln", 1, false, false, numDispatchers, DEFAULT_ORDER_POLICY));
    vm3.invoke(() -> createSender("ny", 2, false, false, numDispatchers, DEFAULT_ORDER_POLICY));

    vm4.invoke(() -> createClientWithLocator(nyPort, "localhost"));

    // Register instantiator
    vm4.invoke(() -> Instantiator.register(new TestObjectInstantiator()));
  }

  @Test
  public void registeringDataSerializableInGatewayShouldNotCauseDeadlock() {
    int lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    int nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> createReceiverAndServer(nyPort));
    vm3.invoke(() -> createReceiverAndServer(lnPort));

    vm2.invoke(() -> createSender("ln", 1, false, false, numDispatchers, DEFAULT_ORDER_POLICY));
    vm3.invoke(() -> createSender("ny", 2, false, false, numDispatchers, DEFAULT_ORDER_POLICY));

    vm4.invoke(() -> createClientWithLocator(nyPort, "localhost"));

    // Register instantiator
    vm4.invoke(() -> DataSerializer.register(TestObjectDataSerializer.class));
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

  @SuppressWarnings("deprecation")
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

  @SuppressWarnings("deprecation")
  private int getCurrentVMNum() {
    return org.apache.geode.test.dunit.VM.getCurrentVMNum();
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
      RegionFactory<?, ?> regionFactory = getCache().createRegionFactory(REPLICATE);

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

  @SuppressWarnings("deprecation")
  private void createClientWithLocator(int locatorPort, String hostName) {
    Properties props = getDistributedSystemProperties();

    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    getCache(props);

    org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.disableShufflingOfEndpoints();
    try {
      PoolManager.createFactory()
          .addLocator(hostName, locatorPort)
          .setPingInterval(250)
          .setSubscriptionEnabled(true)
          .setSubscriptionRedundancy(-1)
          .setReadTimeout(20000)
          .setSocketBufferSize(1000)
          .setMinConnections(6)
          .setMaxConnections(10)
          .setRetryAttempts(3)
          .create("pool");
    } finally {
      org.apache.geode.internal.cache.tier.sockets.CacheServerTestUtil.enableShufflingOfEndpoints();
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

  private void removeSenderFromTheRegion(String senderId, String regionName) {
    Region<?, ?> region = getCache().getRegion(regionName);
    region.getAttributesMutator().removeGatewaySenderId(senderId);
  }

  private void destroySender(String senderId) {
    findGatewaySender(senderId).destroy();
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

  private void createReceiverAndServer(int locatorPort) throws IOException {
    getCache(getDistributedSystemProperties(locatorPort));

    int receiverPort = getRandomAvailableTCPPort();

    GatewayReceiverFactory gatewayReceiverFactory = getCache().createGatewayReceiverFactory();
    gatewayReceiverFactory.setStartPort(receiverPort);
    gatewayReceiverFactory.setEndPort(receiverPort);
    gatewayReceiverFactory.setManualStart(true);

    GatewayReceiver receiver = gatewayReceiverFactory.create();
    receiver.start();

    CacheServer server = getCache().addCacheServer();
    server.setPort(0);
    server.setHostnameForClients("localhost");
    server.start();
  }

  private void startReceivers() throws IOException {
    Set<GatewayReceiver> receivers = getCache().getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      receiver.start();
    }
  }

  private void stopReceivers() {
    Set<GatewayReceiver> receivers = getCache().getGatewayReceivers();
    for (GatewayReceiver receiver : receivers) {
      receiver.stop();
    }
  }

  private void validateRegionSize(String regionName, int regionSize) {
    try (IgnoredException ie1 = addIgnoredException(CacheClosedException.class);
        IgnoredException ie2 = addIgnoredException(ForceReattemptException.class)) {
      Region<?, ?> region = getCache().getRegion(SEPARATOR + regionName);

      await()
          .untilAsserted(() -> assertThat(region.keySet()).hasSize(regionSize));
    }
  }

  private void validateQueueContents(String senderId, int regionSize) {
    try (IgnoredException ie1 = addIgnoredException(GatewaySenderException.class);
        IgnoredException ie2 = addIgnoredException(InterruptedException.class)) {
      InternalGatewaySender sender = findInternalGatewaySender(senderId);

      if (sender.isParallel()) {
        RegionQueue regionQueue = sender.getQueues().toArray(new RegionQueue[1])[0];
        await()
            .untilAsserted(() -> assertThat(regionQueue.size())
                .isEqualTo(regionSize));

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

  private void validateQueueSizeStat(String senderId, int queueSize) {
    InternalGatewaySender sender = findInternalGatewaySender(senderId);

    await()
        .untilAsserted(() -> assertThat(sender.getEventQueueSize())
            .as("Sender event queue size")
            .isEqualTo(queueSize));
  }

  private void validateSecondaryQueueSizeStat(String senderId, int queueSize) {
    InternalGatewaySender sender = findInternalGatewaySender(senderId);

    await()
        .untilAsserted(() -> {
          assertThat(sender.getStatistics().getUnprocessedEventMapSize())
              .as("Sender statistics unprocessed event map contents: "
                  + sender.getEventProcessor().printUnprocessedEvents())
              .isEqualTo(queueSize);
        });
  }

  private void validateSenderDestroyed(String senderId, boolean isParallel) {
    GatewaySender sender = findGatewaySender(senderId, false);

    assertThat(sender)
        .isNull();

    String queueRegionNameSuffix =
        isParallel ? ParallelGatewaySenderQueue.QSTRING : "_SERIAL_GATEWAY_SENDER_QUEUE";

    for (InternalRegion region : getCache().getAllRegions()) {
      assertThat(region.getName())
          .doesNotContain(senderId + queueRegionNameSuffix);
    }
  }

  private void validateSenderOperations(String senderId) {
    GatewaySender sender = findGatewaySender(senderId);

    assertThat(sender.isPaused())
        .isFalse();
    assertThat(sender.isRunning())
        .isFalse();

    sender.pause();

    assertThat(sender.isPaused())
        .isFalse();
    assertThat(sender.isRunning())
        .isFalse();

    sender.resume();

    assertThat(sender.isPaused())
        .isFalse();
    assertThat(sender.isRunning())
        .isFalse();

    sender.stop();

    assertThat(sender.isPaused())
        .isFalse();
    assertThat(sender.isRunning())
        .isFalse();
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

  private void verifySenderRunningState(String senderId) {
    GatewaySender sender = getCache().getGatewaySender(senderId);

    assertThat(sender.isRunning())
        .isTrue();
  }

  private void validateSenderStoppedState(String senderId) {
    GatewaySender sender = findGatewaySender(senderId);

    assertThat(sender.isRunning())
        .isFalse();
    assertThat(sender.isPaused())
        .isFalse();
  }

  private String displaySerialQueueContent(InternalGatewaySender sender) {
    StringBuilder message = new StringBuilder();
    message
        .append("Is Primary: ")
        .append(sender.isPrimary())
        .append(", ")
        .append("Queue Size: ")
        .append(sender.getEventQueueSize());

    if (sender.getQueues() != null) {
      message
          .append(", ")
          .append("Queue Count: ")
          .append(sender.getQueues().size());

      List<Object> list = sender.getQueues().stream()
          .<Object>map(regionQueue -> ((SerialGatewaySenderQueue) regionQueue).displayContent())
          .collect(toList());

      message
          .append(", ")
          .append("Keys: ")
          .append(list);
    }

    AbstractGatewaySenderEventProcessor abstractProcessor = sender.getEventProcessor();
    if (abstractProcessor == null) {
      message
          .append(", ")
          .append("Null Event Processor: ");
    }
    if (!sender.isPrimary()) {
      message
          .append(lineSeparator())
          .append("Unprocessed Events: ")
          .append(abstractProcessor.printUnprocessedEvents())
          .append(lineSeparator());
      message
          .append(lineSeparator())
          .append("Unprocessed Tokens: ")
          .append(abstractProcessor.printUnprocessedTokens())
          .append(lineSeparator());
    }

    return message.toString();
  }

  private static class TestObject implements DataSerializable {

    private String id;

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(id, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException {
      id = DataSerializer.readString(in);
    }
  }

  private static class TestObjectInstantiator extends Instantiator {

    private TestObjectInstantiator() {
      this(TestObject.class, (byte) 99);
    }

    private TestObjectInstantiator(Class<TestObject> c, byte id) {
      super(c, id);
    }

    @Override
    public DataSerializable newInstance() {
      return new TestObject();
    }
  }

  private static class TestObjectDataSerializer extends DataSerializer implements Serializable {

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class<?>[] {TestObject.class};
    }

    @Override
    public boolean toData(Object o, DataOutput out) {
      return o instanceof TestObject;
    }

    @Override
    public Object fromData(DataInput in) {
      return new TestObject();
    }

    @Override
    public int getId() {
      return 99;
    }
  }
}
