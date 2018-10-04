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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.AdditionalAnswers;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.HAHelper;
import org.apache.geode.internal.cache.ha.HARegionQueue;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * This is the bugtest for bug no. 36738. When Object of class ClientUpdateMessage gets deserialized
 * it thows NPE if region mentioned in the ClientUpdateMessage is not present on the node. The test
 * performs following operations 1. Create server1 and HARegion. 2. Perform put operations on
 * HARegion with the value as ClientUpdateMessage. 3. Create server2 and HARegion in it so that GII
 * will happen. 4. Perform get operations from server2.
 */
@Category({ClientSubscriptionTest.class})
public class HABug36738DUnitTest extends JUnit4DistributedTestCase {

  private static final String REGION_NAME = "HABug36738DUnitTest_Region";
  private static final String HAREGION_NAME = "haRegion";
  private static final int COUNT = 10;

  private static Cache cache;

  private Region haRegion;

  public HABug36738DUnitTest() {
    super();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnectAllFromDS();
    invokeInEveryVM(() -> cache = null);
  }

  @Test
  public void testBug36768() throws Exception {
    final VM server1 = Host.getHost(0).getVM(0);
    final VM server2 = Host.getHost(0).getVM(1);

    server1.invoke(() -> createServerCacheWithHAAndRegion());
    await().until(() -> regionExists(server1, HAREGION_NAME));
    server1.invoke(() -> checkRegionQueueSize());

    server2.invoke(() -> createServerCacheWithHA());

    server1.invoke(() -> checkRegionQueueSize());
    server2.invoke(() -> checkRegionQueueSize());
  }

  private void createServerCacheWithHAAndRegion() throws Exception {
    createServerCacheWithHA();
    assertNotNull(cache);
    assertNotNull(this.haRegion);

    final AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEnableConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    cache.createVMRegion(REGION_NAME, factory.createRegionAttributes());

    for (int i = 0; i < COUNT; i++) {
      ClientUpdateMessage clientMessage =
          new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE, (LocalRegion) this.haRegion,
              null, ("value" + i).getBytes(), (byte) 0x01, null, new ClientProxyMembershipID(),
              new EventID(("memberID" + i).getBytes(), i, i));

      this.haRegion.put(i, clientMessage);
    }
  }

  private void createServerCacheWithHA() throws Exception {
    cache = CacheFactory.create(getSystem());

    final AttributesFactory factory = new AttributesFactory();
    factory.setMirrorType(MirrorType.KEYS_VALUES);
    factory.setScope(Scope.DISTRIBUTED_ACK);

    // Mock the HARegionQueue and answer the input CachedDeserializable when updateHAEventWrapper is
    // called
    HARegionQueue harq = mock(HARegionQueue.class);
    when(harq.updateHAEventWrapper(any(), any(), any()))
        .thenAnswer(AdditionalAnswers.returnsSecondArg());

    haRegion = HARegion.getInstance(HAREGION_NAME, (GemFireCacheImpl) cache, harq,
        factory.createRegionAttributes());
  }

  private void checkRegionQueueSize() {
    final HARegion region =
        (HARegion) cache.getRegion(Region.SEPARATOR + HAHelper.getRegionQueueName(HAREGION_NAME));
    assertNotNull(region);
    assertEquals(COUNT, region.size());
  }

  private boolean regionExists(final VM vm, final String name) {
    return vm.invoke(() -> regionExists(name));
  }

  private boolean regionExists(final String name) {
    return cache.getRegion(Region.SEPARATOR + HAHelper.getRegionQueueName(HAREGION_NAME)) != null;
  }
}
