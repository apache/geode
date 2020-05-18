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

import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.internal.cache.BucketAdvisor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class EnsurePrimaryStaysPut {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  private static final String KEY = "foo";
  private static final String VALUE = "bar";

  @Before
  public void setup() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=TEST --type=PARTITION_REDUNDANT")
        .statusIsSuccess();

    server1.invoke(() -> {
      FunctionService.registerFunction(new CheckPrimaryBucketFunction());
    });
    server2.invoke(() -> {
      FunctionService.registerFunction(new CheckPrimaryBucketFunction());
    });
  }

  @Test
  public void sanity() throws InterruptedException {
    // Create entry and return name of primary
    String memberForPrimary = server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region<String, String> region = cache.getRegion("TEST");
      region.put(KEY, VALUE);

      return PartitionRegionHelper.getPrimaryMemberForKey(region, KEY).getName();
    });

    // who is primary?
    MemberVM primary = memberForPrimary.equals("server-1") ? server1 : server2;
    MemberVM secondary = memberForPrimary.equals("server-1") ? server2 : server1;

    AsyncInvocation<?> asyncChecking = primary.invokeAsync(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      Region<String, String> region = cache.getRegion("TEST");

      @SuppressWarnings("unchecked")
      ResultCollector<?, ?> rc = FunctionService.onRegion(region)
          .withFilter(Collections.singleton(KEY))
          .execute(CheckPrimaryBucketFunction.class.getName());

      rc.getResult();
    });

    Thread.sleep(1000);
    primary.invoke(CheckPrimaryBucketFunction::awaitLatch);

    // switch primary to secondary while running test fn()
    secondary.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      PartitionedRegion region = (PartitionedRegion) cache.getRegion("TEST");

      // get bucketId
      int bucketId = PartitionedRegionHelper.getHashKey(region, KEY);
      BucketAdvisor bucketAdvisor = region.getRegionAdvisor().getBucketAdvisor(bucketId);

      bucketAdvisor.becomePrimary(false);
    });

    asyncChecking.get();

  }
}
