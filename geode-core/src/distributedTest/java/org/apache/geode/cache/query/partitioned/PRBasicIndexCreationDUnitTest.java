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
import static org.apache.geode.cache.query.Utils.createPortfoliosAndPositions;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;

/**
 * TODO: most of these tests log statements instead of using assertions
 */
@Category({OQLIndexTest.class})
public class PRBasicIndexCreationDUnitTest extends CacheTestCase {

  private static final String PARTITIONED_REGION_NAME = "PartionedPortfolios";
  private static final String LOCAL_REGION_NAME = "LocalPortfolios";

  private static final int cnt = 0;
  private static final int cntDest = 1003;
  private static final int redundancy = 0;

  // TODO: delete this helper class
  private final PRQueryDUnitHelper prQueryDUnitHelper = new PRQueryDUnitHelper();

  @After
  public void tearDown() {
    disconnectAllFromDS();
    invokeInEveryVM(() -> {
      GemFireCacheImpl.testCacheXml = null;
      PRQueryDUnitHelper.setCache(null);
    });
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties config = new Properties();
    config.put(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.data.*");
    return config;
  }

  /**
   * Tests basic index creation on a partitioned system.
   */
  @Test
  public void testPRBasicIndexCreate() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));

    // Creating the Datastores Nodes in the VM1.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    // creating a duplicate index, should throw a IndexExistsException
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));
  }

  /**
   * Tests creation of multiple index creation on a partitioned region system.
   */
  @Test
  public void testPRMultiIndexCreation() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    // should create a successful index.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    // creating a duplicate index should throw a IndexExistsException
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnID", "p.ID", null, "p"));
  }

  /**
   * Tests creation of multiple index creation on a partitioned region system and test
   * QueryService.getIndex(Region, indexName) API.
   */
  @Test
  public void testPRMultiIndexCreationAndGetIndex() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    // should create a successful index.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    // creating a duplicate index should throw a IndexExistsException
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnID", "p.ID", null, "p"));

    // Check all QueryService.getIndex APIS for a region.
    SerializableRunnable getIndexCheck = new CacheSerializableRunnable("Get Index Check") {

      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        // Check for ID index
        Index idIndex = cache.getQueryService().getIndex(cache.getRegion(PARTITIONED_REGION_NAME),
            "PrIndexOnID");
        assertNotNull(idIndex);
        assertEquals("PrIndexOnID", idIndex.getName());
        assertEquals("p.ID", idIndex.getIndexedExpression());
        assertEquals(SEPARATOR + PARTITIONED_REGION_NAME + " p", idIndex.getFromClause());
        assertNotNull(idIndex.getStatistics());

        // Check for status index
        Index statusIndex = cache.getQueryService()
            .getIndex(cache.getRegion(PARTITIONED_REGION_NAME), "PrIndexOnStatus");
        assertNotNull(statusIndex);
        assertEquals("PrIndexOnStatus", statusIndex.getName());
        assertEquals("p.status", statusIndex.getIndexedExpression());
        assertEquals(SEPARATOR + PARTITIONED_REGION_NAME + " p", statusIndex.getFromClause());
        assertNotNull(statusIndex.getStatistics());

        // Check for all Indexes on the region.
        Collection<Index> indexes =
            cache.getQueryService().getIndexes(cache.getRegion(PARTITIONED_REGION_NAME));
        for (Index ind : indexes) {
          assertNotNull(ind);
          assertNotNull(ind.getName());
          assertNotNull(ind.getIndexedExpression());
          assertNotNull(ind.getFromClause());
          assertNotNull(ind.getStatistics());
        }
      }
    };

    // Check getIndex() on accessor
    vm0.invoke(getIndexCheck);
    // Check getIndex() on datastore
    vm1.invoke(getIndexCheck);
  }

  /**
   * Creates partitioned index from an xml description.
   */
  @Test
  public void testCreatePartitionedIndexThroughXML() throws Exception {
    IgnoredException ignoredException =
        IgnoredException.addIgnoredException(IndexNameConflictException.class.getName());

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    closeCache();

    String fileName = "PRIndexCreation.xml";
    setCacheInVMsUsingXML(fileName, vm0, vm1);

    AsyncInvocation async0 = vm0.invokeAsync(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME));
    AsyncInvocation async1 = vm1.invokeAsync(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME));

    async0.await();
    async1.await();

    ignoredException.remove();

    // printing all the indexes are created.
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm1.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
  }

  /**
   * Test creation of mutilple index on partitioned regions and then adding a new node to the system
   * and checking it has created all the indexes already in the sytem.
   */
  @Test
  public void testCreatePartitionedRegionThroughXMLAndAPI() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    // creating all the prs
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnId", "p.ID", null, "p"));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnPKID", "p.pkid", null, "p"));

    // adding a new node to an already existing system.
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    // putting some data in.
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm1.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm2.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm3.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
  }

  /**
   * Test to see if index creation works with index creation like in serialQueryEntry.conf hydra
   * test before putting the data in the partitioned region.
   */
  @Test
  public void testCreatePartitionedIndexWithNoAliasBeforePuts() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm3);

    // creating all the prs
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "status", null, ""));

    // putting some data in.
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm1.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm3.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
  }

  /**
   * Test creating index on partitioned region like created in test serialQueryEntry.conf but after
   * putting some data in.
   */
  @Test
  public void testCreatePartitionedIndexWithNoAliasAfterPuts() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm3);

    // creating all the prs
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    // putting some data in.
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "status", null, ""));

    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm1.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
    vm3.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForIndexCreationCheck(PARTITIONED_REGION_NAME));
  }

  /**
   * Test index usage with query on a partitioned region with bucket indexes.
   */
  @Test
  public void testPartitionedIndexUsageWithPRQuery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnId", "p.ID", null, "p"));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));

    // validation on index usage with queries over a pr
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForIndexUsageCheck());
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForIndexUsageCheck());
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForIndexUsageCheck());
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForIndexUsageCheck());
  }

  /**
   * Test index usage with query on a partitioned region with bucket indexes.
   */
  @Test
  public void testPartitionedIndexCreationDuringPersistentRecovery() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    int redundancy = 1;

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPersistentPRCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPersistentPRCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));


    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    // Restart a single member
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForCloseCache());
    setCacheInVMs(vm0);

    AsyncInvocation regionCreateFuture =
        vm0.invokeAsync(prQueryDUnitHelper.getCacheSerializableRunnableForPersistentPRCreate(
            PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));
    AsyncInvocation indexCreateFuture =
        vm1.invokeAsync(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
            PARTITIONED_REGION_NAME, "PrIndexOnId", "p.ID", null, "p"));

    regionCreateFuture.await();
    indexCreateFuture.await();

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * Test for bug 37089 where if there is an index on one attribute (CompiledComparision) of the
   * where clause query produces wrong results.
   */
  @Test
  public void testPartitionedQueryWithIndexOnIdBug37089() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnId", "p.ID", null, "p"));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForLocalRegionCreation(
        LOCAL_REGION_NAME, PortfolioData.class));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(LOCAL_REGION_NAME,
        portfolio, cnt, cntDest));
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
    // validation on index usage with queries over a pr
  }

  /**
   * Creates partitioned index on keys and values of a bucket regions.
   */
  @Test
  public void testCreatePartitionedIndexWithKeysValuesAndFunction() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0, vm1);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "index8", "k", SEPARATOR + PARTITIONED_REGION_NAME + ".keys k",
        ""));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "index7", "nvl(k.status.toString(),'nopes')",
        SEPARATOR + PARTITIONED_REGION_NAME + ".values k", ""));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));
  }

  /**
   * Bug Fix 37201, creating index from a data accessor.
   */
  @Test
  public void testCreateIndexFromAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));

    // create more vms to host data.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    // create the index form asscessor.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));
  }

  /**
   * Test for bug fix 37985, NullPointerException in IndexCreationMsg operateOnPartitionedRegion.
   * This bug show up when an accessor (PR with max memory = 0) vm joins the PR system when there
   * are index on the PR and an index creation message is sent to this accessor VM.
   */
  @Test
  public void testCreateIndexAndAddAnAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPuts(PARTITIONED_REGION_NAME,
        portfolio, cnt, cntDest));

    // create index from a data store.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    // create an accessor vm.
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRAccessorCreate(
        PARTITIONED_REGION_NAME, redundancy, PortfolioData.class));
  }

  /**
   * This test does the following using full queries with projections and drill-down<br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verifies the size ,type , contents of both the resultSets Obtained
   */
  @Test
  public void testIndexQueryingWithOrderBy() throws Exception {
    int dataSize = 10;
    int step = 2;
    int totalDataSize = 90;

    Class valueConstraint = Portfolio.class;

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, valueConstraint));

    // Generating portfolio object array to be populated across the PR's & Local Regions

    Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, 0, dataSize));

    // create index from a data store.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnID", "p.ID", null, "p"));

    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(PARTITIONED_REGION_NAME,
            "PrIndexOnKeyID", "key.ID", SEPARATOR + PARTITIONED_REGION_NAME + ".keys key", null));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  /**
   * This test does the following using full queries with projections and drill-down<br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verifies the size ,type , contents of both the resultSets Obtained
   */
  @Test
  public void testIndexQueryingWithOrderAndVerify() throws Exception {
    int dataSize = 10;
    int step = 2;
    int totalDataSize = 90;

    Class valueConstraint = Portfolio.class;

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, valueConstraint));

    // Generating portfolio object array to be populated across the PR's & Local Regions

    Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, 0, dataSize));

    // create index from a data store.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnID", "p.ID", null, "p"));

    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(PARTITIONED_REGION_NAME,
            "PrIndexOnKeyID", "key.ID", SEPARATOR + PARTITIONED_REGION_NAME + ".keys key", null));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  @Test
  public void testIndexQueryingWithOrderByLimit() throws Exception {
    int dataSize = 10;
    int step = 2;
    int totalDataSize = 90;

    Class valueConstraint = Portfolio.class;

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    // Creating PR's on the participating VM's
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRCreate(PARTITIONED_REGION_NAME,
        redundancy, valueConstraint));

    // creating a local region on one of the JVM's
    vm0.invoke(prQueryDUnitHelper
        .getCacheSerializableRunnableForLocalRegionCreation(LOCAL_REGION_NAME, valueConstraint));

    // Generating portfolio object array to be populated across the PR's & Local Regions

    Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, 0, step));
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, step, (2 * step)));
    vm2.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (2 * step), (3 * step)));
    vm3.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(
        PARTITIONED_REGION_NAME, portfoliosAndPositions, (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRPutsKeyValue(LOCAL_REGION_NAME,
        portfoliosAndPositions, 0, dataSize));

    // create index from a data store.
    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnStatus", "p.status", null, "p"));

    vm1.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(
        PARTITIONED_REGION_NAME, "PrIndexOnID", "p.ID", null, "p"));

    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(PARTITIONED_REGION_NAME,
            "PrIndexOnKeyID", "key.ID", SEPARATOR + PARTITIONED_REGION_NAME + ".keys key", null));

    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(PARTITIONED_REGION_NAME,
            "PrIndexOnKeyStatus", "key.status", SEPARATOR + PARTITIONED_REGION_NAME + ".keys key",
            null));

    vm1.invoke(
        prQueryDUnitHelper.getCacheSerializableRunnableForPRIndexCreate(PARTITIONED_REGION_NAME,
            "PrIndexOnsecID", "p.position1.secId", SEPARATOR + PARTITIONED_REGION_NAME + " p",
            null));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRRIndexCreate(LOCAL_REGION_NAME,
        "rrIndexOnStatus", "p.status", null, "p"));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRRIndexCreate(LOCAL_REGION_NAME,
        "rrIndexOnID", "p.ID", null, "p"));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRRIndexCreate(LOCAL_REGION_NAME,
        "rrIndexOnKeyID", "key.ID", SEPARATOR + LOCAL_REGION_NAME + ".keys key", null));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRRIndexCreate(LOCAL_REGION_NAME,
        "rrIndexOnsecID", "p.position1.secId", SEPARATOR + LOCAL_REGION_NAME + " p", null));

    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForRRIndexCreate(LOCAL_REGION_NAME,
        "rrIndexOnKeyStatus", "key.status", SEPARATOR + LOCAL_REGION_NAME + ".keys key", null));

    // querying the VM for data
    vm0.invoke(prQueryDUnitHelper.getCacheSerializableRunnableForPROrderByQueryWithLimit(
        PARTITIONED_REGION_NAME, LOCAL_REGION_NAME));
  }

  private void setCacheInVMsUsingXML(String xmlFile, VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> GemFireCacheImpl.testCacheXml = prQueryDUnitHelper.findFile(xmlFile));
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
}
