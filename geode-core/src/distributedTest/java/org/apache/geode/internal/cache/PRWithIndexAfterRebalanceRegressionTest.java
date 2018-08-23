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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PROXY;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Extracted from {@link PRQueryDistributedTest}.
 *
 * <p>
 * TRAC #50749: RegionDestroyedException when running a OQL inside function for a colocated region
 */
@Category(OQLIndexTest.class)
@SuppressWarnings("serial")
public class PRWithIndexAfterRebalanceRegressionTest implements Serializable {

  public static final String INDEX_NAME = "prIndex";

  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  /**
   * 1. Indexes and Buckets are created on several nodes <br>
   * 2. Buckets are moved <br>
   * 3. Check to make sure we don't have lingering bucket indexes with bucket regions already
   * destroyed
   */
  @Test
  public void testRebalanceWithIndex() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm2.invoke(() -> createPartitionedRegion());

    vm1.invoke(() -> createIndex(INDEX_NAME, "r.score", SEPARATOR + regionName + " r"));

    // Do Puts
    vm1.invoke("putting data", () -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      for (int i = 0; i < 2000; i++) {
        region.put(i, new TestObject(i));
      }
    });

    vm3.invoke(() -> createPartitionedRegion());

    // Rebalance
    vm1.invoke("rebalance", () -> {
      cacheRule.getCache().getResourceManager().createRebalanceFactory().start().getResults();
    });

    vm1.invoke(() -> checkForLingeringBucketIndexes(INDEX_NAME));
    vm2.invoke(() -> checkForLingeringBucketIndexes(INDEX_NAME));
  }

  private void createAccessor() {
    cacheRule.createCache();
    PartitionAttributesFactory paf =
        new PartitionAttributesFactory().setTotalNumBuckets(10).setLocalMaxMemory(0);
    cacheRule.getCache().createRegionFactory(PARTITION_PROXY).setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createPartitionedRegion() {
    cacheRule.createCache();
    PartitionAttributesFactory paf = new PartitionAttributesFactory().setTotalNumBuckets(10);
    cacheRule.getCache().createRegionFactory(PARTITION).setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createIndex(String indexName, String indexedExpression, String regionPath)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    cacheRule.getCache().getQueryService().createIndex(indexName, indexedExpression, regionPath);
  }

  private void checkForLingeringBucketIndexes(String indexName) {
    Region region = cacheRule.getCache().getRegion(regionName);
    QueryService queryService = cacheRule.getCache().getQueryService();
    PartitionedIndex index = (PartitionedIndex) queryService.getIndex(region, indexName);
    for (Index bucketIndex : (List<Index>) index.getBucketIndexes()) {
      assertThat(bucketIndex.getRegion().isDestroyed()).isFalse();
    }
  }

  private static class TestObject implements DataSerializable, Comparable {

    private Double score;

    public TestObject() {
      // nothing
    }

    public TestObject(double score) {
      this.score = score;
    }

    @Override
    public int compareTo(Object o) {
      if (o instanceof TestObject) {
        return score.compareTo(((TestObject) o).score);
      }
      return 1;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeDouble(score);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      score = in.readDouble();
    }
  }
}
