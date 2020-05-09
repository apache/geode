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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class PRBasicMultiIndexCreationDUnitTest extends CacheTestCase {

  private final PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  private static final String name = "PartionedPortfolios";
  private static final String localName = "LocalPortfolios";

  private static final int cnt = 0;
  private static final int cntDest = 1003;
  private static final int redundancy = 0;

  @After
  public void tearDown() {
    disconnectAllFromDS();
    invokeInEveryVM(() -> PRQueryDUnitHelper.setCache(null));
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

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));

    // Creating the Datastores Nodes in the VM1.
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // creating a duplicate index, should throw a IndexExistsException
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(name,
        "PrIndexOnStatus", "p.status", null, "p"));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(name,
        "PrIndexOnStatus", "p.status", null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(name,
        "PrIndexOnStatus", "p.status", null, "p"));
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

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));

    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // Check all QueryService.getIndex APIS for a region.
    SerializableRunnable getIndexCheck = new CacheSerializableRunnable("Get Index Check") {

      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        // Check for ID index
        Index idIndex = cache.getQueryService().getIndex(cache.getRegion(name), "PrIndexOnID");
        assertNotNull(idIndex);
        assertEquals("PrIndexOnID", idIndex.getName());
        assertEquals("ID", idIndex.getIndexedExpression());
        assertEquals(SEPARATOR + name, idIndex.getFromClause());
        assertNotNull(idIndex.getStatistics());

        // Check for status index
        Index statusIndex =
            cache.getQueryService().getIndex(cache.getRegion(name), "PrIndexOnStatus");
        assertNotNull(statusIndex);
        assertEquals("PrIndexOnStatus", statusIndex.getName());
        assertEquals("status", statusIndex.getIndexedExpression());
        assertEquals(SEPARATOR + name, statusIndex.getFromClause());
        assertNotNull(statusIndex.getStatistics());

        // Check for all Indexes on the region.
        Collection<Index> indexes = cache.getQueryService().getIndexes(cache.getRegion(name));
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
    vm2.invoke(getIndexCheck);
    vm3.invoke(getIndexCheck);
  }

  /**
   * Test creation of multiple index on partitioned regions and then adding a new node to the system
   * and checking it has created all the indexes already in the system.
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
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // adding a new node to an already existing system.
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    // putting some data in.
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
  }

  /**
   * Test to see if index creation works with index creation before putting the data in the
   * partitioned region.
   */
  @Test
  public void testCreatePartitionedIndexWithNoAliasBeforePuts() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm3);

    // creating all the prs
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // putting some data in.
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));

    // vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
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
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    // putting some data in.
    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
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

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("ID");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));

    // validation on index usage with queries over a pr
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
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

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name, redundancy,
        PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name, redundancy,
        PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    // Restart a single member
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());
    setCacheInVMs(vm0);

    AsyncInvocation regionCreateFuture = vm0.invokeAsync(PRQHelp
        .getCacheSerializableRunnableForPersistentPRCreate(name, redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("ID");

    AsyncInvocation indexCreateFuture =
        vm1.invokeAsync(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    regionCreateFuture.await();
    indexCreateFuture.await();

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));

    // validation on index usage with queries over a pr
    // The indexes may not have been completely created yet, because the buckets
    // may still be recovering from disk.
  }

  /**
   * Test for bug 37089 where if there is an index on one attribute (CompiledComparision) of the
   * where clause query produces wrong results.
   *
   * <p>
   * TRAC #37089: Query returns incorrect result after create index in parReg
   */
  @Test
  public void testPartitionedQueryWithIndexOnIdBug37089() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("ID");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName, portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(name, localName));
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

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    ArrayList<String> names = new ArrayList<>();
    names.add("index8");
    names.add("index7");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("k");
    exps.add("nvl(k.status.toString(),'nopes')");

    ArrayList<String> fromClause = new ArrayList<>();
    fromClause.add("/PartionedPortfolios.keys k");
    fromClause.add("/PartionedPortfolios.values k");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps, fromClause));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));
  }

  /**
   * Bug Fix 37201, creating index from a data accessor.
   *
   * <p>
   * TRAC #37201: NPE creating index with empty partitioned region
   */
  @Test
  public void testCreateIndexFromAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));

    // create more vms to host data.
    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    // create the index form accessor.
    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("ID");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));
  }

  /**
   * Test for bug fix 37985, NullPointerException in IndexCreationMsg operateOnPartitionedRegion.
   * This bug show up when an accessor (PR with max memory = 0) vm joins the PR system when there
   * are index on the PR and an index creation message is sent to this accessor VM.
   *
   * <p>
   * TRAC #37985: NullPointerException in IndexCreationMsg.operateOnPartitionedRegion
   */
  @Test
  public void testCreateIndexAndAddAnAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0, vm1, vm2, vm3);

    vm1.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(
        PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);

    // Putting the data into the PR's created
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio, cnt, cntDest));

    // create index from a data store.
    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("ID");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // create an accessor vm.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name, redundancy,
        PortfolioData.class));
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
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));

    // creating a local region on one of the JVM's
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));

    // Generating portfolio object array to be populated across the PR's & Local Regions

    Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        0, step));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        step, (2 * step)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        (2 * step), (3 * step)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
        portfoliosAndPositions, 0, dataSize));

    // create index from a data store.
    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // querying the VM for data
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(name, localName));
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
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));

    // creating a local region on one of the JVM's
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));

    // Generating portfolio object array to be populated across the PR's & Local Regions

    Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        0, step));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        step, (2 * step)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        (2 * step), (3 * step)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        (3 * (step)), dataSize));

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
        portfoliosAndPositions, 0, dataSize));

    // create index from a data store.
    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // querying the VM for data
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(name, localName));
  }

  @Test
  public void testIndexQueryingWithOrderByLimit() throws Exception {
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
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name, redundancy, valueConstraint));

    // creating a local region on one of the JVM's
    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));

    // Generating portfolio object array to be populated across the PR's & Local Regions

    Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        0, step));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        step, (2 * step)));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        (2 * step), (3 * step)));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
        (3 * (step)), totalDataSize));

    // Putting the same data in the local region created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
        portfoliosAndPositions, 0, totalDataSize));

    // create index from a data store.

    ArrayList<String> names = new ArrayList<>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnKeyID");
    names.add("PrIndexOnKeyStatus");
    names.add("PrIndexOnsecID");

    ArrayList<String> exps = new ArrayList<>();
    exps.add("status");
    exps.add("ID");
    exps.add("key.ID");
    exps.add("key.status");
    exps.add("position1.secId");

    ArrayList<String> fromClause = new ArrayList<>();
    fromClause.add(SEPARATOR + name);
    fromClause.add(SEPARATOR + name);
    fromClause.add(SEPARATOR + name + ".keys key");
    fromClause.add(SEPARATOR + name + ".keys key");
    fromClause.add(SEPARATOR + name);

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps, fromClause));

    ArrayList<String> names2 = new ArrayList<>();
    names2.add("rrIndexOnStatus");
    names2.add("rrIndexOnID");
    names2.add("rrIndexOnKeyID");
    names2.add("rrIndexOnKeyStatus");
    names2.add("rrIndexOnsecID");

    ArrayList<String> exps2 = new ArrayList<>();
    exps2.add("status");
    exps2.add("ID");
    exps2.add("key.ID");
    exps2.add("key.status");
    exps2.add("position1.secId");

    ArrayList<String> fromClause2 = new ArrayList<>();
    fromClause2.add(SEPARATOR + localName);
    fromClause2.add(SEPARATOR + localName);
    fromClause2.add(SEPARATOR + localName + ".keys key");
    fromClause2.add(SEPARATOR + localName + ".keys key");
    fromClause2.add(SEPARATOR + localName);

    vm0.invoke(
        PRQHelp.getCacheSerializableRunnableForDefineIndex(localName, names2, exps2, fromClause2));

    // querying the VM for data
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryWithLimit(name, localName));
  }

  private void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }
}
