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
package org.apache.geode.cache;

import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

public class RetryPutIfAbsentIntegrationTest {

  Cache cache;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void duplicatePutIfAbsentIsAccepted() {
    final String key = "mykey";
    final String value = "myvalue";

    LocalRegion myregion =
        (LocalRegion) CacheUtils.getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .setConcurrencyChecksEnabled(true).create("myregion");

    ClientProxyMembershipID id =
        new ClientProxyMembershipID(new InternalDistributedMember("localhost", 1));
    EventIDHolder clientEvent = new EventIDHolder(new EventID(new byte[] {1, 2, 3, 4, 5}, 1, 1));
    clientEvent.setRegion(myregion);
    byte[] valueBytes = new VMCachedDeserializable("myvalue", 7).getSerializedValue();
    System.out.println("first putIfAbsent");
    Object oldValue =
        myregion.basicBridgePutIfAbsent("mykey", valueBytes, true, null, id, true, clientEvent);
    assertEquals(null, oldValue);
    assertTrue(myregion.containsKey(key));

    myregion.getEventTracker().clear();

    clientEvent = new EventIDHolder(new EventID(new byte[] {1, 2, 3, 4, 5}, 1, 1));
    clientEvent.setRegion(myregion);
    clientEvent.setPossibleDuplicate(true);
    clientEvent.setOperation(Operation.PUT_IF_ABSENT);
    assertFalse(myregion.getEventTracker().hasSeenEvent(clientEvent));

    System.out.println("second putIfAbsent");
    oldValue =
        myregion.basicBridgePutIfAbsent("mykey", valueBytes, true, null, id, true, clientEvent);
    assertEquals(null, oldValue);
  }

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

}
