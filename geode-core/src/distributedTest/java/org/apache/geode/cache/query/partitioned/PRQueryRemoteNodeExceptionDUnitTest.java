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
package org.apache.geode.cache.query.partitioned;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.query.Utils.createPortfolioData;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * This test verifies exception handling on coordinator node for remote as well as local querying.
 */
@Category({OQLQueryTest.class})
public class PRQueryRemoteNodeExceptionDUnitTest extends CacheTestCase {

  private static final String PARTITIONED_REGION_NAME = "Portfolios";
  private static final String LOCAL_REGION_NAME = "LocalPortfolios";
  private static final int CNT = 0;
  private static final int CNT_DEST = 50;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private int redundancy;
  private int numOfBuckets;
  private PortfolioData[] portfolio;
  private PRQueryDUnitHelper prQueryDUnitHelper;

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);

    redundancy = 0;
    numOfBuckets = 10;
    portfolio = createPortfolioData(CNT, CNT_DEST);
    prQueryDUnitHelper = new PRQueryDUnitHelper();
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
    invokeInEveryVM(QueryObserverHolder::reset);
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.**");
    return config;
  }

  /**
   * 1. Creates PR regions across with scope = DACK, 2 data-stores
   * <p>
   * 2. Creates a Local region on one of the VM's
   * <p>
   * 3. Puts in the same data both in PR region & the Local Region
   * <p>
   * 4. Queries the data both in local & PR
   * <p>
   * 5. Puts a QueryObservers in both local as well as remote data-store node, to throw some test
   * exceptions.
   * <p>
   * 6. then re-executes the query on one of the data-store node.
   * <p>
   * 7. Verifies the exception thrown is from local node not from remote node
   * <p>
   */
  @Test
  public void testPRWithLocalAndRemoteException() throws Exception {
    setCacheInVMs(vm0, vm1);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));

    // Putting the data into the accessor node
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Execute query first time. This is to make sure all the buckets are created
    // (lazy bucket creation).
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));

    // Insert the test hooks on local and remote node.
    // Test hook on remote node will throw CacheException while Test hook on local node will throw
    // QueryException.
    vm1.invoke(() -> {
      QueryObserverHolder.setInstance(new RuntimeExceptionQueryObserver("vm1"));
    });

    vm0.invoke(() -> {
      DefaultQuery query = (DefaultQuery) PRQueryDUnitHelper.getCache().getQueryService()
          .newQuery("Select * from " + SEPARATOR + PARTITIONED_REGION_NAME);
      QueryObserverHolder.setInstance(new RuntimeExceptionQueryObserver("vm0"));
      assertThatThrownBy(query::execute).isInstanceOf(RuntimeException.class)
          .hasMessageContaining("vm0");
    });
  }

  @Test
  public void testRemoteException() throws Exception {
    setCacheInVMs(vm0, vm1);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));

    // Putting the data into the accessor node
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Insert the test hooks on local and remote node.
    // Test hook on remote node will throw CacheException while Test hook on local node will throw
    // QueryException.
    vm1.invoke(() -> {
      QueryObserverHolder.setInstance(new RuntimeExceptionQueryObserver("vm1"));
    });

    vm0.invoke(() -> {
      DefaultQuery query = (DefaultQuery) PRQueryDUnitHelper.getCache().getQueryService()
          .newQuery("Select * from " + SEPARATOR + PARTITIONED_REGION_NAME);
      assertThatThrownBy(query::execute).isInstanceOf(RuntimeException.class)
          .hasMessageContaining("vm1");
    });
  }

  @Test
  public void testCacheCloseExceptionFromLocalAndRemote() throws Exception {
    setCacheInVMs(vm0, vm1);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));

    // Putting the data into the accessor node
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Insert the test hooks on local and remote node.
    // Test hook on remote node will throw CacheException while Test hook on local node will throw
    // QueryException.
    vm1.invoke(() -> {
      QueryObserverHolder.setInstance(new DestroyRegionQueryObserver());
    });

    vm0.invoke(() -> {
      DefaultQuery query = (DefaultQuery) PRQueryDUnitHelper.getCache().getQueryService()
          .newQuery("Select * from " + SEPARATOR + PARTITIONED_REGION_NAME + " p where p.ID > 0");
      QueryObserverHolder.setInstance(new CacheCloseQueryObserver());
      assertThatThrownBy(query::execute).isInstanceOfAny(CacheClosedException.class,
          QueryInvocationTargetException.class);
    });
  }

  @Test
  public void testCacheCloseExceptionFromLocalAndRemote2() throws Exception {
    setCacheInVMs(vm0, vm1);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));

    // Putting the data into the accessor node
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Insert the test hooks on local and remote node.
    // Test hook on remote node will throw CacheException while Test hook on local node will throw
    // QueryException.
    vm1.invoke(() -> {
      QueryObserverHolder.setInstance(new DestroyRegionQueryObserver());
    });

    vm0.invoke(() -> {
      DefaultQuery query = (DefaultQuery) PRQueryDUnitHelper.getCache().getQueryService()
          .newQuery("Select * from " + SEPARATOR + PARTITIONED_REGION_NAME + " p where p.ID > 0");
      QueryObserverHolder.setInstance(new SleepingQueryObserver());
      assertThatThrownBy(query::execute).isInstanceOf(QueryInvocationTargetException.class);
    });
  }

  @Test
  public void testForceReattemptExceptionFromLocal() throws Exception {
    redundancy = 1;
    setCacheInVMs(vm0, vm1, vm2);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreateLimitedBuckets(
        PARTITIONED_REGION_NAME, redundancy, numOfBuckets));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));

    // Putting the data into the accessor node
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, CNT, CNT_DEST));

    // Insert the test hooks on local and remote node.
    // Test hook on remote node will throw CacheException while Test hook on local node will throw
    // QueryException.
    vm1.invoke(() -> {
      QueryObserverHolder.setInstance(new CountingBucketDestroyQueryObserver(1));
    });

    vm0.invoke(() -> {
      DefaultQuery query = (DefaultQuery) PRQueryDUnitHelper.getCache().getQueryService()
          .newQuery("Select * from " + SEPARATOR + PARTITIONED_REGION_NAME);
      QueryObserverHolder.setInstance(new CountingBucketDestroyQueryObserver(2));
      query.execute();
    });
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  class RuntimeExceptionQueryObserver extends IndexTrackingQueryObserver {

    private final String vm;

    RuntimeExceptionQueryObserver(String vm) {
      this.vm = vm;
    }

    @Override
    public void startQuery(Query query) {
      throw new RuntimeException("Thrown in " + vm);
    }
  }

  class DestroyRegionQueryObserver extends IndexTrackingQueryObserver {

    @Override
    public void afterIterationEvaluation(Object result) {
      PRQueryDUnitHelper.getCache().getRegion(PARTITIONED_REGION_NAME).destroyRegion();
    }
  }

  class CacheCloseQueryObserver extends QueryObserverAdapter {

    @Override
    public void afterIterationEvaluation(Object result) {
      PRQueryDUnitHelper.getCache().close();
    }
  }

  class SleepingQueryObserver extends QueryObserverAdapter {

    @Override
    public void afterIterationEvaluation(Object result) {
      for (int i = 0; i <= 10; i++) {
        Region region = PRQueryDUnitHelper.getCache().getRegion(PARTITIONED_REGION_NAME);
        if (region == null || region.isDestroyed()) {
          break;
        }
        try {
          Thread.sleep(10);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }

  class CountingBucketDestroyQueryObserver extends QueryObserverAdapter {

    private final int queryCountToDestroy;
    private final AtomicInteger queryCount = new AtomicInteger();

    CountingBucketDestroyQueryObserver(int queryCountToDestroy) {
      this.queryCountToDestroy = queryCountToDestroy;
    }

    @Override
    public void startQuery(Query query) {
      Object region = ((DefaultQuery) query).getRegionsInQuery(null).iterator().next();
      if (queryCount.incrementAndGet() == queryCountToDestroy) {
        PartitionedRegion partitionedRegion =
            (PartitionedRegion) PRQueryDUnitHelper.getCache().getRegion(PARTITIONED_REGION_NAME);
        List<Integer> localPrimaryBuckets = partitionedRegion.getLocalPrimaryBucketsListTestOnly();
        int bucketId = localPrimaryBuckets.get(0);
        partitionedRegion.getDataStore().getLocalBucketById(bucketId).destroyRegion();
      }
    }
  }
}
