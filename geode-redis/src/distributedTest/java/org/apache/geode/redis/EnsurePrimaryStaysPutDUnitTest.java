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
 *
 */

package org.apache.geode.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceFactory;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class EnsurePrimaryStaysPutDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  private static final String KEY = "foo";
  private static final String VALUE = "bar";
  private static final String REGION = "TEST";

  @Before
  public void setup() throws Exception {
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1, cf -> cf.withConnectionToLocator(locatorPort));
    server2 = cluster.startServerVM(2, cf -> cf.withConnectionToLocator(locatorPort));

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat(
        String.format("create region --name=%s --type=PARTITION_REDUNDANT", REGION))
        .statusIsSuccess();

    server1.invoke(() -> FunctionService.registerFunction(new CheckPrimaryBucketFunction()));
    server2.invoke(() -> FunctionService.registerFunction(new CheckPrimaryBucketFunction()));
  }

  @Test
  public void primaryRemainsWhileLocalFunctionExecutes() throws InterruptedException {
    primaryRemainsWhileFunctionExecutes(true, false);
  }

  @Test
  public void primaryRemainsWhileRemoteFunctionExecutes() throws InterruptedException {
    primaryRemainsWhileFunctionExecutes(false, false);
  }

  @Test
  public void localFunctionRetriesIfNotOnPrimary() throws InterruptedException {
    primaryRemainsWhileFunctionExecutes(true, true);
  }

  @Test
  public void remoteFunctionRetriesIfNotOnPrimary() throws InterruptedException {
    primaryRemainsWhileFunctionExecutes(false, true);
  }

  private void primaryRemainsWhileFunctionExecutes(boolean runLocally, boolean releaseLatchEarly)
      throws InterruptedException {
    // Create entry and return name of primary
    String memberForPrimary = server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region<String, String> region = cache.getRegion("TEST");
      region.put(KEY, VALUE);

      GeodeAwaitility.await()
          .until(() -> PartitionRegionHelper.getRedundantMembersForKey(region, KEY).size() == 1);

      rebalanceRegions(cache, region);

      return awaitForPrimary(region);
    });

    // who is primary?
    MemberVM primary = memberForPrimary.equals("server-1") ? server1 : server2;
    MemberVM secondary = memberForPrimary.equals("server-1") ? server2 : server1;

    MemberVM memberToRunOn = runLocally ? primary : secondary;

    AsyncInvocation<Boolean> asyncChecking =
        memberToRunOn.invokeAsync("CheckPrimaryBucketFunction", () -> {
          InternalCache cache = ClusterStartupRule.getCache();
          Region<String, String> region = cache.getRegion(REGION);

          @SuppressWarnings("unchecked")
          ResultCollector<?, List<Boolean>> rc = FunctionService.onRegion(region)
              .withFilter(Collections.singleton(KEY))
              .setArguments(releaseLatchEarly)
              .execute(CheckPrimaryBucketFunction.class.getName());

          return rc.getResult().get(0);
        });

    primary.invoke("waitForFunctionToStart", () -> {
      CheckPrimaryBucketFunction fn =
          (CheckPrimaryBucketFunction) FunctionService.getRegisteredFunctions()
              .get(CheckPrimaryBucketFunction.ID);
      fn.waitForFunctionToStart();
    });

    // switch primary to secondary while running test fn()
    secondary.invoke("Switching bucket", () -> {
      InternalCache cache = ClusterStartupRule.getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion(REGION);

      // get bucketId
      int bucketId = PartitionedRegionHelper.getHashKey(region, KEY);
      BucketAdvisor bucketAdvisor = region.getRegionAdvisor().getBucketAdvisor(bucketId);

      bucketAdvisor.becomePrimary(false);

      CheckPrimaryBucketFunction fn =
          (CheckPrimaryBucketFunction) FunctionService.getRegisteredFunctions()
              .get(CheckPrimaryBucketFunction.ID);
      fn.finishedMovingPrimary();
    });

    primary.invoke("finishedMovingPrimary", () -> {
      CheckPrimaryBucketFunction fn =
          (CheckPrimaryBucketFunction) FunctionService.getRegisteredFunctions()
              .get(CheckPrimaryBucketFunction.ID);
      fn.finishedMovingPrimary();
    });

    assertThat(asyncChecking.get())
        .as("CheckPrimaryBucketFunction determined that the primary has moved")
        .isTrue();
  }

  private static String awaitForPrimary(Region<String, String> region) {
    AtomicReference<String> lastPrimary =
        new AtomicReference<>(PartitionRegionHelper.getPrimaryMemberForKey(region, KEY).getName());
    GeodeAwaitility.await()
        .during(10, TimeUnit.SECONDS)
        .atMost(60, TimeUnit.SECONDS)
        .until(() -> {
          String currentPrimary =
              PartitionRegionHelper.getPrimaryMemberForKey(region, KEY).getName();
          return lastPrimary.getAndSet(currentPrimary).equals(currentPrimary);
        });

    return lastPrimary.get();
  }

  private static void rebalanceRegions(Cache cache, Region<?, ?> region) {
    ResourceManager manager = cache.getResourceManager();
    Set<String> includeRegions = new HashSet<>();
    includeRegions.add(region.getName());

    RebalanceFactory factory = manager.createRebalanceFactory();
    factory.includeRegions(includeRegions);

    try {
      factory.start().getResults();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
