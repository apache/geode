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
package org.apache.geode.internal.cache;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.Disconnect.disconnectFromDS;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.SearchLoadAndWriteProcessor.NetSearchRequestMessage;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class NetSearchMessagingDUnitTest implements Serializable {
  private static final int DEFAULT_MAXIMUM_ENTRIES = 5;
  private static final AtomicBoolean listenerHasFinished = new AtomicBoolean();
  private static Logger logger = LogService.getLogger();
  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() {
    regionName = "region";

    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);
  }

  @After
  public void tearDown() {
    for (VM vm : toArray(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        DistributionMessageObserver.setInstance(null);
        cacheRule.closeAndNullCache();
      });
    }
    disconnectAllFromDS();
  }

  @Test
  public void testOneMessageWithReplicates() {
    createReplicate(vm0);
    createReplicate(vm1);
    createNormal(vm2);
    createEmpty(vm3);

    // Test with a null value
    long vm0Count = getReceivedMessages(vm0);
    long vm1Count = getReceivedMessages(vm1);
    long vm2Count = getReceivedMessages(vm2);
    long vm3Count = getReceivedMessages(vm3);

    assertThat(get(vm3, "a")).isNull();

    // Make sure we only processed one message
    assertThat(vm3Count + 1).isEqualTo(getReceivedMessages(vm3));

    // Make sure the replicates only saw one message between them
    assertThat(vm0Count + vm1Count + 1)
        .isEqualTo(getReceivedMessages(vm0) + getReceivedMessages(vm1));

    // Make sure the normal vm didn't see any messages
    assertThat(vm2Count).isEqualTo(getReceivedMessages(vm2));

    // Test with a real value value
    put(vm3, "a", "b");

    vm0Count = getReceivedMessages(vm0);
    vm1Count = getReceivedMessages(vm1);
    vm2Count = getReceivedMessages(vm2);
    vm3Count = getReceivedMessages(vm3);

    assertThat("b").isEqualTo(get(vm3, "a"));

    // Make sure we only processed one message
    assertThat(vm3Count + 1).isEqualTo(getReceivedMessages(vm3));

    // Make sure the replicates only saw one message between them

    assertThat(vm0Count + vm1Count + 1)
        .isEqualTo(getReceivedMessages(vm0) + getReceivedMessages(vm1));

    // Make sure the normal vm didn't see any messages
    assertThat(vm2Count).isEqualTo(getReceivedMessages(vm2));
  }

  @Test
  public void testNetSearchNormals() {
    createNormal(vm0);
    createNormal(vm1);
    createNormal(vm2);
    createEmpty(vm3);

    // Test with a null value
    long vm0Count = getReceivedMessages(vm0);
    long vm1Count = getReceivedMessages(vm1);
    long vm2Count = getReceivedMessages(vm2);
    long vm3Count = getReceivedMessages(vm3);

    assertThat(get(vm3, "a")).isNull();

    // Make sure we only processed one message
    waitForReceivedMessages(vm3, vm3Count + 3);

    // Make sure the normal vms each saw 1 query message.
    assertThat(vm0Count + vm1Count + vm2Count + 3)
        .isEqualTo(getReceivedMessages(vm0) + getReceivedMessages(vm1) + getReceivedMessages(vm2));

    // Test with a real value value
    put(vm3, "a", "b");

    vm0Count = getReceivedMessages(vm0);
    vm1Count = getReceivedMessages(vm1);
    vm2Count = getReceivedMessages(vm2);
    vm3Count = getReceivedMessages(vm3);

    assertThat("b").isEqualTo(get(vm3, "a"));

    // Make sure we only processed one message
    waitForReceivedMessages(vm2, vm2Count + 1);

    waitForReceivedMessages(vm3, vm3Count + 3);

    // Make sure the normal vms each saw 1 query message.
    assertThat(vm0Count + vm1Count + vm2Count + 3)
        .isEqualTo(getReceivedMessages(vm0) + getReceivedMessages(vm1) + getReceivedMessages(vm2));
  }

  /**
   * In bug #48186 a deadlock occurs when a netsearch pulls in a value from the disk and causes a
   * LRU eviction of another entry. Here we merely demonstrate that a netsearch that gets the value
   * of an overflow entry does not update the LRU status of that entry.
   */
  @Test
  public void testNetSearchNoLRU() {
    createOverflow(vm2);
    createEmpty(vm1);

    // Test with a null value
    put(vm2, "a", "1");
    put(vm2, "b", "2");
    put(vm2, "c", "3");
    put(vm2, "d", "4");
    put(vm2, "e", "5");
    // the cache in vm0 is now full and LRU will occur on this next put()
    put(vm2, "f", "6");

    boolean evicted = vm2.invoke("verify eviction of 'a'", () -> {
      InternalRegion region1 = (InternalRegion) getCache().getRegion(regionName);
      RegionEntry re1 = region1.getRegionEntry("a");
      Object o1 = re1.getValueInVM(region1);
      return o1 == null || o1 == Token.NOT_AVAILABLE;
    });
    assertThat(evicted).isTrue();

    // now netsearch for 'a' from the other VM and verify again
    Object value = get(vm1, "a");
    assertThat("1").isEqualTo(value);

    evicted = vm2.invoke("verify eviction of 'a'", () -> {
      InternalRegion region1 = (InternalRegion) getCache().getRegion(regionName);
      RegionEntry re1 = region1.getRegionEntry("a");
      Object o1 = re1.getValueInVM(region1);
      return o1 == null || o1 == Token.NOT_AVAILABLE;
    });
    assertThat(evicted).isTrue();
    vm2.invoke("verify other entries are not evicted", () -> {
      LocalRegion region = (LocalRegion) getCache().getRegion(regionName);
      String[] keys = new String[] {"b", "c", "d", "e", "f"};
      for (String key : keys) {
        RegionEntry re = region.getRegionEntry(key);
        Object o = re.getValueInVM(region);
        assertThat((o != null) && (o != Token.NOT_AVAILABLE)).isTrue();
      }
    });
  }

  /**
   * The system prefers net searching replicates. If a replicate fails after it responds to a query
   * message and before it returns a value, the system should fall back to net searching normal
   * members.
   */
  @Test
  public void testNetSearchFailoverFromReplicate() {
    installListenerToDisconnectOnNetSearchRequest(vm0);
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate entered");
    createReplicate(vm0);
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 1");

    createNormal(vm1);
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 2");
    createNormal(vm2);
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 3");
    createEmpty(vm3);
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 4");

    put(vm3, "a", "b");
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 5");

    assertThat("b").isEqualTo(get(vm3, "a"));
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 6");

    vm0.invoke(() -> await("system to shut down")
        .untilAsserted(
            () -> {
              logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 7");
              assertThat(InternalDistributedSystem.getConnectedInstance()).isNull();
              logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 8");

            }));
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate 9");

    waitForListenerToFinish(vm0);
    logger.info("DBG GEODE-7474: testNetSearchFailoverFromReplicate finished");
  }

  /**
   * When a replicate fails after responding to a query message, the net search should fail over to
   * the next replicate that responded.
   */
  @Test
  public void testNetSearchFailoverFromOneReplicateToAnother() {
    installListenerToDisconnectOnNetSearchRequest(vm0);

    createReplicate(vm0);
    createReplicate(vm1);
    createEmpty(vm2);

    put(vm2, "a", "b");

    boolean disconnected = false;
    while (!disconnected) {
      // get() causes vm2 to send a query message to all replicated members. All members with that
      // key reply. vm2 then sends a net search request to the first one that replies. If that
      // fails, it sends a net search request to the next one that replied. And so on.
      //
      // Because we can't be sure which member will respond first to each query, we repeat the loop
      // until vm0 is the first responder. vm0 will then disconnect when it receives the net search
      // request, forcing failover to another vm (in this case vm1). If we got the correct answer
      // AND the system is disconnected, then failover occurred.
      assertThat("b").isEqualTo(get(vm2, "a"));

      // Make sure we were disconnected in vm0
      disconnected =
          vm0.invoke("check disconnected",
              () -> InternalDistributedSystem.getConnectedInstance() == null);
    }

    waitForListenerToFinish(vm0);
  }

  private void put(VM vm, final String key, final String value) {
    vm.invoke(() -> {
      Region<String, String> region = getCache().getRegion(regionName);
      region.put(key, value);
    });
  }

  private String get(VM vm, final String key) {
    return vm.invoke("get " + key, () -> {
      Region<String, String> region = getCache().getRegion(regionName);
      return region.get(key);
    });
  }

  private void waitForReceivedMessages(final VM vm, final long expected) {
    await().untilAsserted(() -> assertThat(getReceivedMessages(vm)).isEqualTo(expected));
  }

  private long getReceivedMessages(VM vm) {
    return vm.invoke(() -> InternalDistributedSystem.getDMStats().getReceivedMessages());
  }

  private void createEmpty(VM vm) {
    logger.info("DBG GEODE-7474: createEmpty entered");
    vm.invoke(() -> {
      logger.info("DBG GEODE-7474: createEmpty 1");


      getCache().createRegionFactory(RegionShortcut.REPLICATE_PROXY)
          .setConcurrencyChecksEnabled(false).create(regionName);
      logger.info("DBG GEODE-7474: createEmpty 2");

    });
    logger.info("DBG GEODE-7474: createEmpty finished");
  }

  private void createNormal(VM vm) {
    logger.info("DBG GEODE-7474: createNormal entered");

    vm.invoke(() -> {
      logger.info("DBG GEODE-7474: createNormal 1");

      getCache().createRegionFactory()
          .setScope(Scope.DISTRIBUTED_ACK)
          .setConcurrencyChecksEnabled(false)
          .setDataPolicy(DataPolicy.NORMAL)
          .setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL))
          .create(regionName);
      logger.info("DBG GEODE-7474: createNormal 2");

    });
    logger.info("DBG GEODE-7474: createNormal finished");

  }

  private void createOverflow(VM vm) {
    logger.info("DBG GEODE-7474: createOverflow entered");
    vm.invoke(() -> {
      logger.info("DBG GEODE-7474: createOverflow 1");

      getCache().createRegionFactory(RegionShortcut.REPLICATE)
          .setEvictionAttributes(
              EvictionAttributes.createLRUEntryAttributes(DEFAULT_MAXIMUM_ENTRIES,
                  EvictionAction.OVERFLOW_TO_DISK))
          .create(regionName);
      logger.info("DBG GEODE-7474: createOverflow 2");

    });
    logger.info("DBG GEODE-7474: createOverflow finished");

  }

  private void createReplicate(VM vm) {
    logger.info("DBG GEODE-7474: createReplicate entered");
    vm.invoke(() -> {
      logger.info("DBG GEODE-7474: createReplicate 1");
      getCache().createRegionFactory(RegionShortcut.REPLICATE)
          .setConcurrencyChecksEnabled(false)
          .create(regionName);
      logger.info("DBG GEODE-7474: createReplicate 2");
    });
    logger.info("DBG GEODE-7474: createReplicate finished");
  }

  private void installListenerToDisconnectOnNetSearchRequest(VM vm) {
    logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest entered");

    vm.invoke("install listener", () -> {
      logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest 1");
      listenerHasFinished.set(false);
      logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest 2");

      DistributionMessageObserver observer = new DistributionMessageObserver() {
        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest 3");

          if (message instanceof NetSearchRequestMessage) {
            DistributionMessageObserver.setInstance(null);
            logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest 4");
            disconnectFromDS();

            logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest 5");
            listenerHasFinished.set(true);
          }
          logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest 6");
        }
      };
      DistributionMessageObserver.setInstance(observer);
    });

    logger.info("DBG GEODE-7474: installListenerToDisconnectOnNetSearchRequest finished");
  }

  private void waitForListenerToFinish(VM vm) {
    logger.info("DBG GEODE-7474: waitForListenerToFinish entered");

    vm.invoke("wait for listener to finish", () -> {
      logger.info("DBG GEODE-7474: waitForListenerToFinish 1");

      assertThat(DistributionMessageObserver.getInstance())
          .withFailMessage("listener was not invoked")
          .isNull();
      logger.info("DBG GEODE-7474: waitForListenerToFinish 2");

      await("listener to finish")
          .untilAsserted(() -> {
            assertThat(listenerHasFinished).isTrue();
            logger.info("DBG GEODE-7474: waitForListenerToFinish 3");
          });
    });

    logger.info("DBG GEODE-7474: waitForListenerToFinish finished");
  }

  private InternalCache getCache() {
    return cacheRule.getOrCreateCache();
  }
}
