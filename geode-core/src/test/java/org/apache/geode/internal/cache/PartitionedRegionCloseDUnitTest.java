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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class PartitionedRegionCloseDUnitTest implements Serializable {

  private static final int TOTAL_NUM_BUCKETS = 3;
  private static final int NUM_PUTS = 3;
  private static final int REDUNDANT_COPIES = 1;

  private String regionName;
  private VM accessor;
  private VM[] datastores;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Before
  public void setUp() throws Exception {
    regionName = getClass().getSimpleName();

    accessor = Host.getHost(0).getVM(0);
    datastores = new VM[3];
    datastores[0] = Host.getHost(0).getVM(1);
    datastores[1] = Host.getHost(0).getVM(2);
    datastores[2] = Host.getHost(0).getVM(3);
  }

  @Test
  @Parameters({"CLOSE_REGION", "LOCAL_DESTROY_REGION"})
  @TestCaseName("{method}({params})")
  public void redundantDataIsAvailableAfterRemovingOneDatastore(final RegionRemoval regionRemoval)
      throws Exception {
    accessor.invoke("create accessor", () -> createAccessor());
    for (VM vm : datastores) {
      vm.invoke("create datastore", () -> createDataStore());
    }

    accessor.invoke("put operations", () -> {
      Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < NUM_PUTS; i++) {
        region.put(i, "VALUE-" + i);
      }
    });

    Node datastoreToRemove = datastores[0].invoke("get datastore node to remove", () -> {
      Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);
      return ((PartitionedRegion) region).getNode();
    });

    datastores[0].invoke("remove PR from one datastore", () -> {
      Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);
      regionRemoval.remove(region);
    });

    datastores[1].invoke("validate PR metadata", () -> {
      InternalCache cache = cacheRule.getCache();
      Region<Integer, String> region = cache.getRegion(regionName);

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      RegionAdvisor advisor = partitionedRegion.getRegionAdvisor();
      for (int bucketId : advisor.getBucketSet()) {
        assertThat(advisor.getBucketOwners(bucketId))
            .doesNotContain(datastoreToRemove.getMemberId());
      }

      Region<String, PartitionRegionConfig> prMetaData = PartitionedRegionHelper.getPRRoot(cache);
      PartitionRegionConfig prConfig = prMetaData.get(partitionedRegion.getRegionIdentifier());
      assertThat(prConfig.containsNode(datastoreToRemove)).isFalse();
    });

    accessor.invoke("get operations", () -> {
      Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < NUM_PUTS; i++) {
        assertThat(region.get(i)).isEqualTo("VALUE-" + i);
      }
    });
  }

  /**
   * This test case checks that a closed PR (accessor/datastore) can be recreated.
   */
  @Test
  public void closeAndRecreateInAllHasNoData() throws Exception {
    accessor.invoke("create accessor", () -> createAccessor());
    for (VM vm : datastores) {
      vm.invoke("create datastore", () -> createDataStore());
    }

    accessor.invoke("put operations", () -> {
      Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < NUM_PUTS; i++) {
        region.put(i, "VALUE-" + i);
      }
    });

    for (VM vm : datastores) {
      vm.invoke("close datastore", () -> cacheRule.getCache().getRegion(regionName).close());
    }
    accessor.invoke("close accessor", () -> cacheRule.getCache().getRegion(regionName).close());

    accessor.invoke("recreate accessor", () -> createAccessor());
    for (VM vm : datastores) {
      vm.invoke("recreate datastore", () -> createDataStore());
    }

    accessor.invoke("get operations", () -> {
      Region<Integer, String> region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < NUM_PUTS; i++) {
        assertThat(region.get(i)).isNull();
      }
    });
  }

  private void createAccessor() {
    createRegion(true);
  }

  private void createDataStore() {
    createRegion(false);
  }

  private void createRegion(final boolean accessor) {
    PartitionAttributesFactory partitionFactory = new PartitionAttributesFactory();
    partitionFactory.setRedundantCopies(REDUNDANT_COPIES);
    if (accessor) {
      partitionFactory.setLocalMaxMemory(0);
    }
    partitionFactory.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

    RegionFactory<String, Integer> regionFactory = cacheRule.getCache().createRegionFactory();
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    regionFactory.setPartitionAttributes(partitionFactory.create());

    regionFactory.create(regionName);
  }

  private enum RegionRemoval {
    CLOSE_REGION((region) -> region.close()),
    LOCAL_DESTROY_REGION((region) -> region.localDestroyRegion());

    private final Consumer<Region> strategy;

    RegionRemoval(final Consumer<Region> strategy) {
      this.strategy = strategy;
    }

    void remove(final Region region) {
      strategy.accept(region);
    }
  }
}
