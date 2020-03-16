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

import static junit.framework.TestCase.assertEquals;
import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class GatewayReceiverDUnitTest extends WANTestBase {

  private static GatewayReceiver receiver;

  @Test
  public void removingGatewayReceiverUsingReplicatedRegionShouldRemoveCacheServerFlagFromProfile() {
    testRemoveGatewayReceiver(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName(), null, isOffHeap()),
        () -> ((DistributedRegion) WANTestBase.cache.getRegion(getTestMethodName()))
            .getDistributionAdvisor());
  }

  @Test
  public void removingGatewayReceiverUsingPartitionedRegionShouldRemoveCacheServerFlagFromProfile() {
    testRemoveGatewayReceiver(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 10, isOffHeap()),
        () -> ((PartitionedRegion) WANTestBase.cache.getRegion(getTestMethodName()))
            .getDistributionAdvisor());
  }

  @Test
  public void canAddReceiverAfterRemovingFromReplicatedRegion() {
    testCanAddGatewayReceiverAfterOneHasBeenRemoved(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName(), null, isOffHeap()),
        () -> ((DistributedRegion) WANTestBase.cache.getRegion(getTestMethodName()))
            .getDistributionAdvisor());
  }

  @Test
  public void canAddReceiverAfterRemovingFromPartitionedRegion() {
    testCanAddGatewayReceiverAfterOneHasBeenRemoved(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 10, isOffHeap()),
        () -> ((PartitionedRegion) WANTestBase.cache.getRegion(getTestMethodName()))
            .getDistributionAdvisor());
  }

  @Test
  public void canDestroyUnstartedGatewayReceiverFromReplicated() {
    testCanDestroyUnstartedGatewayReceiver(
        () -> WANTestBase.createReplicatedRegion(getTestMethodName(), null, isOffHeap()));
  }

  @Test
  public void canDestroyUnstartedReceiverFromPartitionedRegion() {
    testCanDestroyUnstartedGatewayReceiver(
        () -> WANTestBase.createPartitionedRegion(getTestMethodName(), null, 1, 10, isOffHeap()));
  }

  public void testRemoveGatewayReceiver(SerializableRunnableIF createRegionLambda,
      SerializableCallableIF<DistributionAdvisor> extractAdvisorLambda) {
    InternalDistributedMember[] memberIds = new InternalDistributedMember[8];

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));

    memberIds[2] = (InternalDistributedMember) vm2
        .invoke(() -> WANTestBase.cache.getDistributedSystem().getDistributedMember());

    memberIds[3] = (InternalDistributedMember) vm3
        .invoke(() -> WANTestBase.cache.getDistributedSystem().getDistributedMember());

    vm2.invoke(createRegionLambda);
    vm3.invoke(createRegionLambda);

    vm2.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 100));

    vm2.invoke(() -> {
      GatewayReceiverDUnitTest.receiver = GatewayReceiverDUnitTest.createAndReturnReceiver();
    });
    vm3.invoke(() -> {
      GatewayReceiverDUnitTest.receiver = GatewayReceiverDUnitTest.createAndReturnReceiver();
    });

    vm2.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[3], true, extractAdvisorLambda));
    vm3.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[2], true, extractAdvisorLambda));

    vm2.invoke(() -> {
      GatewayReceiverDUnitTest.receiver.stop();
      GatewayReceiverDUnitTest.receiver.destroy();
    });

    vm2.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[3], true, extractAdvisorLambda));
    // vm3 should still see that vm2's profile still has cache server set to true
    vm3.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[2], false, extractAdvisorLambda));

    vm3.invoke(() -> {
      GatewayReceiverDUnitTest.receiver.stop();
      GatewayReceiverDUnitTest.receiver.destroy();
    });

    vm2.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[3], false, extractAdvisorLambda));
    vm3.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[2], false, extractAdvisorLambda));
  }

  public void testCanAddGatewayReceiverAfterOneHasBeenRemoved(
      SerializableRunnableIF createRegionLambda,
      SerializableCallableIF<DistributionAdvisor> extractAdvisorLambda) {
    InternalDistributedMember[] memberIds = new InternalDistributedMember[8];

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));
    vm3.invoke(() -> WANTestBase.createCache(nyPort));

    memberIds[2] = (InternalDistributedMember) vm2
        .invoke(() -> WANTestBase.cache.getDistributedSystem().getDistributedMember());

    memberIds[3] = (InternalDistributedMember) vm3
        .invoke(() -> WANTestBase.cache.getDistributedSystem().getDistributedMember());

    vm2.invoke(createRegionLambda);
    vm3.invoke(createRegionLambda);

    vm2.invoke(() -> WANTestBase.doPuts(getTestMethodName(), 100));

    vm2.invoke(() -> {
      GatewayReceiverDUnitTest.receiver = GatewayReceiverDUnitTest.createAndReturnReceiver();
    });
    vm3.invoke(() -> {
      GatewayReceiverDUnitTest.receiver = GatewayReceiverDUnitTest.createAndReturnReceiver();
    });

    vm2.invoke(() -> {
      GatewayReceiverDUnitTest.receiver.stop();
      GatewayReceiverDUnitTest.receiver.destroy();
    });

    vm3.invoke(() -> {
      GatewayReceiverDUnitTest.receiver.stop();
      GatewayReceiverDUnitTest.receiver.destroy();
    });

    vm2.invoke(() -> {
      GatewayReceiverDUnitTest.receiver = GatewayReceiverDUnitTest.createAndReturnReceiver();
    });
    vm3.invoke(() -> {
      GatewayReceiverDUnitTest.receiver = GatewayReceiverDUnitTest.createAndReturnReceiver();
    });

    vm2.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[3], true, extractAdvisorLambda));
    vm3.invoke(() -> assertProfileCacheServerFlagEquals(memberIds[2], true, extractAdvisorLambda));

  }

  public void testCanDestroyUnstartedGatewayReceiver(SerializableRunnableIF createRegionLambda) {

    Integer lnPort = vm0.invoke(() -> WANTestBase.createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> WANTestBase.createFirstRemoteLocator(2, lnPort));

    vm2.invoke(() -> WANTestBase.createCache(nyPort));

    vm2.invoke(() -> WANTestBase.cache.getDistributedSystem().getDistributedMember());
    vm2.invoke(createRegionLambda);

    vm2.invoke(() -> {
      GatewayReceiverDUnitTest.receiver =
          GatewayReceiverDUnitTest.createAndReturnUnstartedReceiver();
    });

    vm2.invoke(() -> GatewayReceiverDUnitTest.receiver.destroy());
  }



  private void assertProfileCacheServerFlagEquals(InternalDistributedMember member,
      boolean expectedFlag, SerializableCallableIF<DistributionAdvisor> extractAdvisor)
      throws Exception {
    DistributionAdvisor advisor = extractAdvisor.call();
    CacheDistributionAdvisor.CacheProfile cp =
        (CacheDistributionAdvisor.CacheProfile) advisor.getProfile(member);
    assertEquals(expectedFlag, cp.hasCacheServer);
  }

  public static GatewayReceiver createAndReturnReceiver() {
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    GatewayReceiver receiver = fact.create();
    try {
      receiver.start();
    } catch (IOException e) {
      e.printStackTrace();
      fail(
          "Test " + getTestMethodName() + " failed to start GatewayReceiver on port " + port, e);
    }
    return receiver;
  }

  public static GatewayReceiver createAndReturnUnstartedReceiver() {
    GatewayReceiverFactory fact = cache.createGatewayReceiverFactory();
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    fact.setStartPort(port);
    fact.setEndPort(port);
    fact.setManualStart(true);
    return fact.create();
  }

}
