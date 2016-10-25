/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache.query.partitioned;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import static org.apache.geode.cache.query.Utils.*;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.internal.cache.PartitionedRegionDUnitTestCase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

@Category(DistributedTest.class)
public class PRBasicMultiIndexCreationDUnitTest extends PartitionedRegionDUnitTestCase {

  public PRBasicMultiIndexCreationDUnitTest() {
    super();
  }

  public void setCacheInVMs(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(() -> PRQueryDUnitHelper.setCache(getCache()));
    }
  }

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper();

  final String name = "PartionedPortfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 1003;

  final int redundancy = 0;

  /**
   * Tests basic index creation on a partitioned system.
   * 
   * @throws Exception  if an exception is generated
   */
  @Test
  public void testPRBasicIndexCreate() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    LogWriterUtils.getLogWriter().info(
        "PRBasicIndexCreationDUnitTest.testPRBasicIndexCreate started ....");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));
    // Creating local region on vm0 to compare the results of query.
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(localName,
    // Scope.DISTRIBUTED_ACK, redundancy));

    // Creating the Datastores Nodes in the VM1.
    LogWriterUtils.getLogWriter()
        .info("PRBasicIndexCreationDUnitTest : creating all the prs ");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));

    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("status");
    exps.add("ID");
    
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // creating a duplicate index, should throw a IndexExistsException and if not
    // will throw a RuntimeException.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        name, "PrIndexOnStatus", "p.status",null, "p"));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        name, "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        name, "PrIndexOnStatus", "p.status",null, "p"));
    LogWriterUtils.getLogWriter().info(
        "PRBasicIndexCreationDUnitTest.testPRBasicIndexCreate is done ");
  }


  
  /*
   * Tests creation of multiple index creation on a partitioned region system
   * and test QueryService.getIndex(Region, indexName) API.
   *
   * @throws Exception if any exception are generated
   */
  @Test
  public void testPRMultiIndexCreationAndGetIndex() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    LogWriterUtils.getLogWriter().info(
        "PRBasicIndexCreation.testPRMultiIndexCreation Test Started");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));

    vm1.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy, PortfolioData.class));

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
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
        assertEquals("/" + name , idIndex.getFromClause());
        assertNotNull(idIndex.getStatistics());

        // Check for status index
        Index statusIndex = cache.getQueryService().getIndex(cache.getRegion(name), "PrIndexOnStatus");
        assertNotNull(statusIndex);
        assertEquals("PrIndexOnStatus", statusIndex.getName());
        assertEquals("status", statusIndex.getIndexedExpression());
        assertEquals("/" + name , statusIndex.getFromClause());
        assertNotNull(statusIndex.getStatistics());

        //Check for all Indexes on the region.
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

    LogWriterUtils.getLogWriter().info("PRQBasicIndexCreationTest.testPRMultiIndexCreation ENDED");
  }
  


  /**
   * Test creation of multiple index on partitioned regions and then adding a
   * new node to the system and checking it has created all the indexes already
   * in the system.
   * 
   */ 
  @Test
  public void testCreatePartitionedRegionThroughXMLAndAPI()
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
    // fileName));
    LogWriterUtils.getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testCreatePartitionedRegionThroughXMLAndAPI started ");
    // creating all the prs
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

//  adding a new node to an already existing system.
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    // putting some data in.
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));

  } 
  
  
  /**
   * Test to see if index creation works with index creation like in
   * serialQueryEntry.conf hydra test before putting the data in the partitioned
   * region.
   */ 
  @Test
  public void testCreatePartitionedIndexWithNoAliasBeforePuts () throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm3);
    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
    // fileName));
    LogWriterUtils.getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testCreatePartitionedIndexWithNoAliasAfterPuts started ");
    // creating all the prs
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));

    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("status");
    exps.add("ID");
    exps.add("pkid");
    
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    // putting some data in.
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
  //  vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
  }
  
  /**
   * Test creating index on partitioned region like created in test
   * serialQueryEntry.conf but after putting some data in.
   */
  @Test
  public void testCreatePartitionedIndexWithNoAliasAfterPuts () throws Exception { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm3);
    LogWriterUtils.getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testCreatePartitionedIndexWithNoAliasBeforePuts started ");
    // creating all the prs
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));

    // putting some data in.
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnStatus");
    names.add("PrIndexOnID");
    names.add("PrIndexOnPKID");
    
    ArrayList<String> exps = new ArrayList<String>();
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
  public void testPartitionedIndexUsageWithPRQuery () throws Exception { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    LogWriterUtils.getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testPartitionedIndexUsageWithPRQuery started ");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("ID");
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    // validation on index usage with queries over a pr
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck());
    LogWriterUtils.getLogWriter()
    .info(
        "PRBasicIndexCreationDUnitTest.testPartitionedIndexUsageWithPRQuery done ");
  }
  
  /**
   * Test index usage with query on a partitioned region with bucket indexes.
   * @throws Throwable 
   */
  @Test
  public void testPartitionedIndexCreationDuringPersistentRecovery() throws Throwable { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0,vm1);

    int redundancy = 1;
    LogWriterUtils.getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testPartitionedIndexCreationDuringPersistentRecovery started ");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
        redundancy, PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
        redundancy, PortfolioData.class));

    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    
    
    //Restart a single member
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());
    setCacheInVMs(vm0);
    AsyncInvocation regionCreateFuture = vm0.invokeAsync(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
        redundancy, PortfolioData.class));
    
    //Ok, I want to do this in parallel
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("ID");
    
    AsyncInvocation indexCreateFuture = vm1.invokeAsync(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));
    
    regionCreateFuture.getResult(20 * 1000);
    
    indexCreateFuture.getResult(20 * 1000);
    
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    // validation on index usage with queries over a pr
    //The indexes may not have been completely created yet, because the buckets
    //may still be recovering from disk.
    LogWriterUtils.getLogWriter()
    .info(
        "PRBasicIndexCreationDUnitTest.testPartitionedIndexCreationDuringPersistentRecovery done ");
  }
  
  
  /**
   * Test for bug 37089 where if there is an index on one attribute
   * (CompiledComparision) of the where clause query produces wrong results.
   */
  @Test
  public void testPartitionedQueryWithIndexOnIdBug37089 () throws Exception { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    LogWriterUtils.getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testPartitionedQueryWithIndexOnIdBug37089 started ");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("ID");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));
    
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName, PortfolioData.class));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    // validation on index usage with queries over a pr
    LogWriterUtils.getLogWriter()
    .info(
        "PRBasicIndexCreationDUnitTest.testPartitionedQueryWithIndexOnIdBug37089 done ");
  }
  
  /**
   * Creats partitioned index on keys and values of a bucket regions.
   */
  @Test
  public void testCreatePartitionedIndexWithKeysValuesAndFunction() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    setCacheInVMs(vm0,vm1);
    final String fileName = "PRIndexCreation.xml";
    LogWriterUtils.getLogWriter().info(
        "PRBasicIndexCreation.testCreatePartitionedIndexThroughXML started");
    LogWriterUtils.getLogWriter().info(
        "Starting and initializing partitioned regions and indexes using xml");
    LogWriterUtils.getLogWriter().info(
        "Starting a pr asynchronously using an xml file name : " + fileName);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    
    ArrayList<String> names = new ArrayList<String>();
    names.add("index8");
    names.add("index7");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("k");
    exps.add("nvl(k.status.toString(),'nopes')");
    
    ArrayList<String> fromClause = new ArrayList<String>();
    fromClause.add("/PartionedPortfolios.keys k");
    fromClause.add("/PartionedPortfolios.values k");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps, fromClause));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));

    LogWriterUtils.getLogWriter().info(
        "PRBasicIndexCreation.testCreatePartitionedIndexThroughXML is done  " );
    

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
    setCacheInVMs(vm0,vm1,vm2,vm3);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));
    
    // create more vms to host data.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    //  Putting the data into the PR's created
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
     cnt, cntDest));
    
    // create the index form asscessor.
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("ID");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));
 
  }


  /**
   * Test for bug fix 37985, NullPointerException in IndexCreationMsg 
   * operateOnPartitionedRegion. This bug show up when an accessor 
   * (PR with max memory = 0) vm joins the PR system when there are 
   * index on the PR and an index creation message is sent to this 
   * accessor VM.
   */
  
  @Test
  public void testCreateIndexAndAddAnAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    setCacheInVMs(vm0,vm1,vm2,vm3);
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy, PortfolioData.class));
    
    final PortfolioData[] portfolio = createPortfolioData(cnt, cntDest);
    //  Putting the data into the PR's created
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
     cnt, cntDest));
    //create index from a data store.
    ArrayList<String> names = new ArrayList<String>();
    names.add("PrIndexOnID");
    
    ArrayList<String> exps = new ArrayList<String>();
    exps.add("ID");

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));
    
    // create an accessor vm.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy, PortfolioData.class));
  }

  /**
   * This test does the following using full queries with projections and drill-down<br>
   * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
   * 2. Creates a Local region on one of the VM's <br>
   * 3. Puts in the same data both in PR region & the Local Region <br>
   * 4. Queries the data both in local & PR <br>
   * 5. Verfies the size ,type , contents of both the resultSets Obtained
   *
   * @throws Exception
   */
  @Test
  public void testIndexQueryingWithOrderBy() throws Exception
 {
   int dataSize = 10;
   int step = 2;
   int totalDataSize = 90;
   final int i = 0;
   
   LogWriterUtils.getLogWriter()
   .info(
         "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);
   setCacheInVMs(vm0,vm1,vm2,vm3);
   // Creating PR's on the participating VM's
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
              .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (3 * (step)), dataSize));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
                                                            portfoliosAndPositions, i, dataSize));

   //create index from a data store.
   ArrayList<String> names = new ArrayList<String>();
   names.add("PrIndexOnStatus");
   names.add("PrIndexOnID");
   names.add("PrIndexOnPKID");
   
   ArrayList<String> exps = new ArrayList<String>();
   exps.add("status");
   exps.add("ID");
   exps.add("pkid");
   
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));
  
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
                                                                              name, localName));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }

 /**
  * This test does the following using full queries with projections and drill-down<br>
  * 1. Creates PR regions on 4 VMs (all datastores) with scope = D_ACK <br>
  * 2. Creates a Local region on one of the VM's <br>
  * 3. Puts in the same data both in PR region & the Local Region <br>
  * 4. Queries the data both in local & PR <br>
  * 5. Verfies the size ,type , contents of both the resultSets Obtained
  *
  * @throws Exception
  */
  @Test
  public void testIndexQueryingWithOrderAndVerify() throws Exception
 {
   int dataSize = 10;
   int step = 2;
   int totalDataSize = 90;
   final int i = 0;

   LogWriterUtils.getLogWriter()
   .info(
   "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);
   setCacheInVMs(vm0,vm1,vm2,vm3);
   // Creating PR's on the participating VM's
   LogWriterUtils.getLogWriter()
   .info(
   "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
       redundancy, valueConstraint));
   LogWriterUtils.getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   LogWriterUtils.getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
       .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   LogWriterUtils.getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       (3 * (step)), dataSize));
   LogWriterUtils.getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
       portfoliosAndPositions, i, dataSize));

   //create index from a data store.
   ArrayList<String> names = new ArrayList<String>();
   names.add("PrIndexOnStatus");
   names.add("PrIndexOnID");
   names.add("PrIndexOnPKID");
   
   ArrayList<String> exps = new ArrayList<String>();
   exps.add("status");
   exps.add("ID");
   exps.add("pkid");
   
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps));

   LogWriterUtils.getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(
       name, localName));
   LogWriterUtils.getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }

  @Test
  public void testIndexQueryingWithOrderByLimit() throws Exception
 {
  int step = 2;
  int totalDataSize = 90;
  final int i = 0;
   
   LogWriterUtils.getLogWriter()
   .info(
         "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);
   setCacheInVMs(vm0,vm1,vm2,vm3);
   // Creating PR's on the participating VM's
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating PR's on VM0, VM1 , VM2 , VM3");
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
                                                              redundancy, valueConstraint));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
              .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (3 * (step)), totalDataSize));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
                                                            portfoliosAndPositions, i, totalDataSize));

   // create index from a data store.
  
   ArrayList<String> names = new ArrayList<String>();
   names.add("PrIndexOnStatus");
   names.add("PrIndexOnID");
   names.add("PrIndexOnKeyID");
   names.add("PrIndexOnKeyStatus");
   names.add("PrIndexOnsecID");
   
   ArrayList<String> exps = new ArrayList<String>();
   exps.add("status");
   exps.add("ID");
   exps.add("key.ID");
   exps.add("key.status");
   exps.add("position1.secId");
   
   ArrayList<String> fromClause = new ArrayList<String>();
   fromClause.add("/" + name);
   fromClause.add("/" + name);
   fromClause.add("/" + name + ".keys key");
   fromClause.add("/" + name + ".keys key");
   fromClause.add( "/" + name);
   
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(name, names, exps, fromClause));

   ArrayList<String> names2 = new ArrayList<String>();
   names2.add("rrIndexOnStatus");
   names2.add("rrIndexOnID");
   names2.add("rrIndexOnKeyID");
   names2.add("rrIndexOnKeyStatus");
   names2.add("rrIndexOnsecID");
   
   ArrayList<String> exps2 = new ArrayList<String>();
   exps2.add("status");
   exps2.add("ID");
   exps2.add("key.ID");
   exps2.add("key.status");
   exps2.add("position1.secId");
   
   ArrayList<String> fromClause2 = new ArrayList<String>();
   fromClause2.add("/" + localName);
   fromClause2.add("/" + localName);
   fromClause2.add("/" + localName + ".keys key");
   fromClause2.add("/" + localName + ".keys key");
   fromClause2.add( "/" + localName);
   
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForDefineIndex(localName, names2, exps2, fromClause2));
 
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryWithLimit(
                                                                              name, localName));
   LogWriterUtils.getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }
}
