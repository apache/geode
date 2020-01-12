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

import java.io.Serializable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.partitioned.QueryMessage;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Extracted from {@link PRQueryDistributedTest}.
 *
 * <p>
 * TRAC #43102: hang while executing query with pdx objects
 */
@Category(OQLQueryTest.class)
@SuppressWarnings("serial")
public class PRQueryWithPdxDuringRebalanceRegressionTest implements Serializable {

  private static final AtomicReference<RebalanceResults> REBALANCE_RESULTS =
      new AtomicReference<>();

  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);

    regionName = getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  /**
   * 1. Buckets are created on several nodes <br>
   * 2. A query is started 3. While the query is executing, several buckets are moved.
   */
  @Test
  public void testRebalanceDuringQueryEvaluation() throws Exception {
    vm0.invoke(() -> createAccessor());
    vm1.invoke(() -> createPartitionedRegion());
    vm1.invoke(() -> createBuckets());
    vm2.invoke(() -> createPartitionedRegion());

    // Add a listener that will trigger a rebalance as soon as the query arrives on this node.
    vm1.invoke("add listener", () -> {
      DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

        @Override
        public void beforeProcessMessage(ClusterDistributionManager dm,
            DistributionMessage message) {
          if (message instanceof QueryMessage) {
            RebalanceOperation rebalance =
                cacheRule.getCache().getResourceManager().createRebalanceFactory().start();
            // wait for the rebalance
            try {
              REBALANCE_RESULTS.compareAndSet(null, rebalance.getResults());
            } catch (CancellationException | InterruptedException e) {
              errorCollector.addError(e);
            }
          }
        }
      });
    });

    vm0.invoke(() -> executeQuery());

    vm1.invoke("check rebalance happened", () -> {
      assertThat(REBALANCE_RESULTS.get()).isNotNull();
      assertThat(REBALANCE_RESULTS.get().getTotalBucketTransfersCompleted()).isEqualTo(5);
    });
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

  private void createBuckets() {
    Region region = cacheRule.getCache().getRegion(regionName);
    for (int i = 0; i < 10; i++) {
      region.put(i, i);
    }
  }

  private void executeQuery() throws NameResolutionException, TypeMismatchException,
      QueryInvocationTargetException, FunctionDomainException {
    Query query = cacheRule.getCache().getQueryService()
        .newQuery("select * from " + SEPARATOR + regionName + " r where r > 0");
    SelectResults results = (SelectResults) query.execute();
    assertThat(results.asSet()).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
  }
}
