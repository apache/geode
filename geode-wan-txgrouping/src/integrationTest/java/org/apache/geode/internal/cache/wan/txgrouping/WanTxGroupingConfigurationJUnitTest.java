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
package org.apache.geode.internal.cache.wan.txgrouping;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.Set;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.internal.cache.wan.GatewaySenderException;

public class WanTxGroupingConfigurationJUnitTest {

  private Cache cache;

  /**
   * Test to validate that serial gateway sender group transaction events can
   * be correctly set to true
   */
  @Test
  public void test_ValidateSerialGatewaySenderGroupTransactionEventsAttributeSetToTrue() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(true);
    fact.setBatchConflationEnabled(false);
    boolean groupTransactionEvents = true;
    fact.setManualStart(true);
    fact.setGroupTransactionEvents(groupTransactionEvents);
    GatewaySender sender1 = fact.create("TKSender", 2);
    RegionFactory factory = cache.createRegionFactory(RegionShortcut.PARTITION);
    factory.addGatewaySenderId(sender1.getId());
    Set<GatewaySender> senders = cache.getGatewaySenders();
    assertEquals(senders.size(), 1);
    GatewaySender gatewaySender = senders.iterator().next();
    assertThat(sender1.mustGroupTransactionEvents())
        .isEqualTo(gatewaySender.mustGroupTransactionEvents());
  }

  @Test
  public void test_create_SerialGatewaySender_ThrowsException_when_GroupTransactionEvents_isTrue_and_DispatcherThreads_is_greaterThanOne() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setParallel(false);
    fact.setDispatcherThreads(2);
    fact.setGroupTransactionEvents(true);
    assertThatThrownBy(() -> fact.create("NYSender", 2))
        .isInstanceOf(GatewaySenderException.class)
        .hasMessageContaining(
            "SerialGatewaySender NYSender cannot be created with group transaction events set to true when dispatcher threads is greater than 1");
  }

  @Test
  public void test_create_GatewaySender_ThrowsException_when_GroupTransactionEvents_isTrue_and_BatchConflation_is_enabled() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    GatewaySenderFactory fact = cache.createGatewaySenderFactory();
    fact.setBatchConflationEnabled(true);
    fact.setGroupTransactionEvents(true);
    assertThatThrownBy(() -> fact.create("NYSender", 2))
        .isInstanceOf(GatewaySenderException.class)
        .hasMessageContaining(
            "GatewaySender NYSender cannot be created with both group transaction events set to true and batch conflation enabled");
  }

  @After
  public void tearDown() {
    if (this.cache != null) {
      this.cache.close();
    }
  }
}
