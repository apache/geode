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
import static org.awaitility.Awaitility.await;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ConcurrentTransactionDistributedTest {

  private static final String CREATES = "CREATES";
  private static final String REMOVES = "REMOVES";
  private static final String EXPIRE_DESTROYS = "EXPIRE_DESTROYS";

  private String regionName;
  private Random randomGenerator;

  private static final int MAX_ENTRY_COUNT = 50000;


  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public SharedCountersRule sharedCountersRule = new SharedCountersRule();

  @Before

  public void setUp() throws Exception {
    regionName = getClass().getSimpleName();

    randomGenerator = new Random();

    sharedCountersRule.initialize(CREATES);
    sharedCountersRule.initialize(REMOVES);
    sharedCountersRule.initialize(EXPIRE_DESTROYS);

  }

  protected Region<Integer, Integer> createRegion(final String name) {
    ExpirationAttributes expirationAttributes =
        new ExpirationAttributes(1, ExpirationAction.DESTROY);

    RegionFactory<Integer, Integer> regionFactory = cacheRule.getCache().createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);
    regionFactory.addCacheListener(new CountingCacheListener());
    regionFactory.setEntryTimeToLive(expirationAttributes);

    return regionFactory.create(name);
  }

  @Test
  public void testCachePerfStatsEntriesIsNotNegativeAfterTransactionsWithExpiration() {
    Region<Integer, Integer> region = createRegion(regionName);

    int creates = 0;
    int removes = 0;
    int equalInts = 0;

    long endTime = System.currentTimeMillis() + 10000; // 30 seconds
    do {
      int randomIntForCreate = randomGenerator.nextInt(MAX_ENTRY_COUNT);
      int randomIntForDestroy = randomGenerator.nextInt(MAX_ENTRY_COUNT);
      if (randomIntForCreate == randomIntForDestroy) {
        equalInts++;
      }

      cacheRule.getCache().getCacheTransactionManager().begin();
      Integer oldValue = region.putIfAbsent(randomIntForCreate, randomIntForCreate);
      boolean created = (oldValue == null);

      oldValue = region.remove(randomIntForDestroy);
      boolean removed = (oldValue != null);

      if (created && (randomIntForCreate != randomIntForDestroy)) {
        creates++;
      }
      if (removed && ((randomIntForCreate != randomIntForDestroy) || !created)) {
        removes++;
      }
      cacheRule.getCache().getCacheTransactionManager().commit();

    } while (System.currentTimeMillis() < endTime);

    await("Waiting for region to be empty...")
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> region.isEmpty());

    int actualExpireDestroys = sharedCountersRule.getTotal(EXPIRE_DESTROYS);
    int actualRemoves = sharedCountersRule.getTotal(REMOVES);
    CachePerfStats cacheStats = ((InternalRegion) region).getCachePerfStats();

    System.out.println("Test Create & Destroy on same key:" + equalInts);
    System.out.println("Test Creates within tx:" + creates);
    System.out.println("Test Removes within tx:" + removes);
    System.out.println("Listener removes:" + actualRemoves);
    System.out.println("Listener Expire destroys:" + actualExpireDestroys);
    System.out
        .println("Listener Total Destroys counted: " + (actualRemoves + actualExpireDestroys));

    System.out.println("CachePerfStats.creates:" + cacheStats.getCreates());
    System.out.println("CachePerfStats.destroys:" + cacheStats.getDestroys());
    System.out.println("CachePerfStats.entries:" + cacheStats.getEntries());
    System.out.println("CachePerfStats.tombstones:" + cacheStats.getTombstoneCount());

    assertThat(cacheStats.getCreates()).isEqualTo(creates);

    assertThat(actualRemoves).isEqualTo(removes);
    assertThat(cacheStats.getDestroys()).isEqualTo(actualExpireDestroys + actualRemoves);

    assertThat(creates - (actualExpireDestroys + removes)).isEqualTo(0);
    assertThat(cacheStats.getEntries()).isEqualTo(0);
  }

  private class CountingCacheListener extends CacheListenerAdapter<Integer, Integer> {
    @Override
    public void afterDestroy(final EntryEvent<Integer, Integer> event) {
      if (event.getOperation().isExpiration()) {
        sharedCountersRule.increment(EXPIRE_DESTROYS);
      } else {
        sharedCountersRule.increment(REMOVES);
      }
    }
  }
}
