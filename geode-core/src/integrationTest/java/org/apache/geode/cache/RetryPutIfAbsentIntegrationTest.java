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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.EventIDHolder;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

class RetryPutIfAbsentIntegrationTest {

  private Cache cache;
  private LocalRegion myRegion;
  private final ClientProxyMembershipID id =
      new ClientProxyMembershipID(new InternalDistributedMember("localhost", 1));
  private final EventIDHolder clientEvent =
      new EventIDHolder(new EventID(new byte[] {1, 2, 3, 4, 5}, 1, 1));
  private final String key = "key";
  private int updateCount;

  @Test
  void duplicatePutIfAbsentIsAccepted() {
    myRegion = (LocalRegion) cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setConcurrencyChecksEnabled(true).create("myRegion");

    clientEvent.setRegion(myRegion);
    byte[] valueBytes = new VMCachedDeserializable("myValue", 7).getSerializedValue();
    Object oldValue =
        myRegion.basicBridgePutIfAbsent(key, valueBytes, true, null, id, true, clientEvent);
    assertThat(oldValue).isNull();
    assertThat(myRegion.containsKey(key)).isTrue();

    myRegion.getEventTracker().clear();

    clientEvent.setPossibleDuplicate(true);
    clientEvent.setOperation(Operation.PUT_IF_ABSENT);
    assertThat(myRegion.getEventTracker().hasSeenEvent(clientEvent)).isFalse();

    oldValue = myRegion.basicBridgePutIfAbsent(key, valueBytes, true, null, id, true, clientEvent);
    assertThat(oldValue).isNull();
  }

  @Test
  void duplicatePutIfAbsentOfNullValueDoesNotInvokeAfterUpdateListener() {
    myRegion = (LocalRegion) cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setConcurrencyChecksEnabled(true)
        .addCacheListener(new CacheListenerAdapter<Object, Object>() {
          @Override
          public void afterUpdate(EntryEvent<Object, Object> event) {
            ++updateCount;
          }
        }).create("myRegion");

    clientEvent.setRegion(myRegion);
    Object oldValue =
        myRegion.basicBridgePutIfAbsent(key, null, true, null, id, true, clientEvent);
    assertThat(oldValue).isNull();
    assertThat(myRegion.containsKey(key)).isTrue();

    myRegion.getEventTracker().clear();

    clientEvent.setPossibleDuplicate(true);
    clientEvent.setOperation(Operation.PUT_IF_ABSENT);
    assertThat(myRegion.getEventTracker().hasSeenEvent(clientEvent)).isFalse();

    oldValue = myRegion.basicBridgePutIfAbsent(key, null, true, null, id, true, clientEvent);
    assertThat(oldValue).isNull();
    assertThat(updateCount).isEqualTo(0);
  }

  @BeforeEach
  void setUp() {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
  }

  @AfterEach
  void tearDown() {
    CacheUtils.closeCache();
  }

}
