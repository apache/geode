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
package com.gemstone.gemfire.cache.query.partitioned;

import java.util.Collection;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDUnitTestCase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author rdubey
 * 
 */
public class PRBasicIndexCreationDUnitTest extends
    PartitionedRegionDUnitTestCase

{
  /**
   * constructor
   * 
   * @param name
   */

  public PRBasicIndexCreationDUnitTest(String name) {
    super(name);
  }

  // int totalNumBuckets = 131;

  int queryTestCycle = 10;

  PRQueryDUnitHelper PRQHelp = new PRQueryDUnitHelper("");

  final String name = "PartionedPortfolios";

  final String localName = "LocalPortfolios";

  final int cnt = 0, cntDest = 1003;

  final int redundancy = 0;

  /**
   * Tests basic index creation on a partitioned system.
   * 
   * @throws Exception  if an exception is generated
   */
  public void testPRBasicIndexCreate() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    getLogWriter().info(
        "PRBasicIndexCreationDUnitTest.testPRBasicIndexCreate started ....");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));
    // Creating local region on vm0 to compare the results of query.
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(localName,
    // Scope.DISTRIBUTED_ACK, redundancy));

    // Creating the Datastores Nodes in the VM1.
    getLogWriter()
        .info("PRBasicIndexCreationDUnitTest : creating all the prs ");
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    // creating a duplicate index, should throw a IndexExistsException and if not
    // will throw a RuntimeException.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        name, "PrIndexOnStatus", "p.status",null, "p"));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        name, "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForDuplicatePRIndexCreate(
        name, "PrIndexOnStatus", "p.status",null, "p"));
    getLogWriter().info(
        "PRBasicIndexCreationDUnitTest.testPRBasicIndexCreate is done ");
  }

  /**
   * Tests creation of multiple index creation on a partitioned region system.
   * @throws Exception if any excpetion are generated
   */
  public void testPRMultiIndexCreation() throws Exception
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    getLogWriter().info(
        "PRBasicIndexCreation.testPRMultiIndexCreation Test Started");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));

    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    // should create a sucessful index.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    // creating a duplicate index should throw a IndexExistsException and if not
    // will throw a RuntimeException.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnID", "p.ID", null, "p"));
    getLogWriter().info(
        "PRQBasicIndexCreationTest.testPRMultiIndexCreation ENDED");

  }

  /*
   * Tests creation of multiple index creation on a partitioned region system
   * and test QueryService.getIndex(Region, indexName) API.
   *
   * @throws Exception if any exception are generated
   */
  public void testPRMultiIndexCreationAndGetIndex() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    getLogWriter().info(
        "PRBasicIndexCreation.testPRMultiIndexCreation Test Started");

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));

    vm1.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy));
    vm2.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy));
    vm3.invoke(PRQHelp
        .getCacheSerializableRunnableForPRCreate(name, redundancy));

    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    // should create a sucessful index.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status", null, "p"));
    // creating a duplicate index should throw a IndexExistsException and if not
    // will throw a RuntimeException.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnID", "p.ID", null, "p"));

    // Check all QueryService.getIndex APIS for a region.
    SerializableRunnable getIndexCheck = new CacheSerializableRunnable("Get Index Check") {

      @Override
      public void run2() throws CacheException {
        Cache cache = getCache();
        // Check for ID index
        Index idIndex = cache.getQueryService().getIndex(cache.getRegion(name), "PrIndexOnID");
        assertNotNull(idIndex);
        assertEquals("PrIndexOnID", idIndex.getName());
        assertEquals("p.ID", idIndex.getIndexedExpression());
        assertEquals("/" + name + " p", idIndex.getFromClause());
        assertNotNull(idIndex.getStatistics());

        // Check for status index
        Index statusIndex = cache.getQueryService().getIndex(cache.getRegion(name), "PrIndexOnStatus");
        assertNotNull(statusIndex);
        assertEquals("PrIndexOnStatus", statusIndex.getName());
        assertEquals("p.status", statusIndex.getIndexedExpression());
        assertEquals("/" + name + " p", statusIndex.getFromClause());
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

    getLogWriter().info("PRQBasicIndexCreationTest.testPRMultiIndexCreation ENDED");
  }
  
  /**
   * Creats partitioned index from an xml discription.
   */
  public void testCreatePartitionedIndexThroughXML() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
//    VM vm3 = host.getVM(3);
    // closeAllCache();
    final String fileName = "PRIndexCreation.xml";
    getLogWriter().info(
        "PRBasicIndexCreation.testCreatePartitionedIndexThroughXML started");
    getLogWriter().info(
        "Starting and initializing partitioned regions and indexes using xml");
    getLogWriter().info(
        "Starting a pr asynchronously using an xml file name : " + fileName);
    AsyncInvocation asyInvk0 = vm0.invokeAsync(PRQHelp
        .getCacheSerializableRunnableForPRCreateThrougXML(name, fileName));
    AsyncInvocation asyInvk1 = vm1.invokeAsync(PRQHelp
        .getCacheSerializableRunnableForPRCreateThrougXML(name, fileName));
    DistributedTestCase.join(asyInvk1, 30 * 1000, getLogWriter());
    if (asyInvk1.exceptionOccurred()) {
      fail("asyInvk1 failed", asyInvk1.getException());
    }
    DistributedTestCase.join(asyInvk0, 30 * 1000, getLogWriter());
    if (asyInvk0.exceptionOccurred()) {
      fail("asyInvk0 failed", asyInvk0.getException());
    }
    // printing all the indexes are created.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    getLogWriter().info(
        "PRBasicIndexCreation.testCreatePartitionedIndexThroughXML is done  " );

  }

  /**
   * Test creation of mutilple index on partitioned regions and then adding a
   * new node to the system and checking it has created all the indexes already
   * in the sytem.
   * 
   */ 
  public void testCreatePartitionedRegionThroughXMLAndAPI()
  {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name,
    // fileName));
    getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testCreatePartitionedRegionThroughXMLAndAPI started ");
    // creating all the prs
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));

    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnPKID", "p.pkid",null, "p"));
//  adding a new node to an already existing system.
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    // putting some data in.
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
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
  public void testCreatePartitionedIndexWithNoAliasBeforePuts () throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name,
    // fileName));
    getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testCreatePartitionedIndexWithNoAliasAfterPuts started ");
    // creating all the prs
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "status",null, ""));
    //vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
    //    "PrIndexOnId", "p.ID", "p"));

    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
    //    "PrIndexOnPKID", "p.pkid", "p"));
//  adding a new node to an already existing system.
    //vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
    //    Scope.DISTRIBUTED_ACK, redundancy));
    // putting some data in.
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
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
  public void testCreatePartitionedIndexWithNoAliasAfterPuts () throws Exception { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name,
    // fileName));
    getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testCreatePartitionedIndexWithNoAliasBeforePuts started ");
    // creating all the prs
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));

    // vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
    // "PrIndexOnId", "p.ID", "p"));

    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
    // "PrIndexOnPKID", "p.pkid", "p"));
    // adding a new node to an already existing system.
    // vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
    // Scope.DISTRIBUTED_ACK, redundancy));
    // putting some data in.
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    // Putting the data into the PR's created
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "status",null, ""));
    /*
    vm1.invoke(new CacheSerializableRunnable("IndexCreationOnPosition") {
      public void run2(){
        try {
          Cache cache = getCache();
          QueryService qs = cache.getQueryService();
          Region region = cache.getRegion(name);
          LogWriter logger = cache.getLogger();
         // logger.info("Test Creating index with Name : [ "+indexName+" ] " +
         //               "IndexedExpression : [ "+indexedExpression+" ] Alias : [ "+alias+" ] FromClause : [ "+region.getFullPath() + " " + alias+" ] " );
          Index parIndex = qs.createIndex("IndexOnPotionMktValue", IndexType.FUNCTIONAL, "pVal.mktValue"
              ,region.getFullPath()+" pf, pf.positions pVal TYPE Position", "import parReg.\"query\".Position;");
          logger.info(
              "Index creted on partitioned region : " + parIndex);
          logger.info(
              "Number of buckets indexed in the partitioned region locally : "
                  + "" + ((PartitionedIndex)parIndex).getNumberOfIndexedBucket()
                  + " and remote buckets indexed : "
                  + ((PartitionedIndex)parIndex).getNumRemoteBucketsIndexed());
          /*
           * assertEquals("Max num of buckets in the partiotion regions and
           * the " + "buckets indexed should be equal",
           * ((PartitionedRegion)region).getTotalNumberOfBuckets(),
           * (((PartionedIndex)parIndex).getNumberOfIndexedBucket()+((PartionedIndex)parIndex).getNumRemtoeBucketsIndexed()));
           * should put all the assetion in a seperate function.
           */ 
       /* } 
        catch (Exception ex) {
          fail("Creating Index in this vm failed : ", ex);
        }
      
      }
    });*/
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    // vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
  } 
  
  /**
   * Test index usage with query on a partitioned region with bucket indexes.
   */
  public void testPartitionedIndexUsageWithPRQuery () throws Exception { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name,
    // fileName));
    getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testPartitionedIndexUsageWithPRQuery started ");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    // validation on index usage with queries over a pr
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck(name));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck(name));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck(name));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck(name));
    getLogWriter()
    .info(
        "PRBasicIndexCreationDUnitTest.testPartitionedIndexUsageWithPRQuery done ");
  }
  
  /**
   * Test index usage with query on a partitioned region with bucket indexes.
   * @throws Throwable 
   */
  public void testPartitionedIndexCreationDuringPersistentRecovery() throws Throwable { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // final String fileName = "PRIndexCreation.xml";
    // vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name,
    // fileName));
    
    int redundancy = 1;
    getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testPartitionedIndexCreationDuringPersistentRecovery started ");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
        redundancy, PRQHelp.valueConstraint));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
        redundancy, PRQHelp.valueConstraint));
//    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
//        redundancy, PRQHelp.valueConstraint));
    
    
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    
    
    //Restart a single member
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForCloseCache());
    AsyncInvocation regionCreateFuture = vm0.invokeAsync(PRQHelp.getCacheSerializableRunnableForPersistentPRCreate(name,
        redundancy, PRQHelp.valueConstraint));
    
    //Ok, I want to do this in parallel
    AsyncInvocation indexCreateFuture = vm1.invokeAsync(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    
    regionCreateFuture.getResult(20 * 1000);
    
    indexCreateFuture.getResult(20 * 1000);
    
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    // validation on index usage with queries over a pr
    //The indexes may not have been completely created yet, because the buckets
    //may still be recovering from disk.
//    vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexUsageCheck(name));
    getLogWriter()
    .info(
        "PRBasicIndexCreationDUnitTest.testPartitionedIndexCreationDuringPersistentRecovery done ");
  }
  
  
  /**
   * Test for bug 37089 where if there is an index on one attribute
   * (CompiledComparision) of the where clause query produces wrong results.
   */
  public void testPartitionedQueryWithIndexOnIdBug37089 () throws Exception { 
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    getLogWriter()
        .info(
            "PRBasicIndexCreationDUnitTest.testPartitionedQueryWithIndexOnIdBug37089 started ");
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnId", "p.ID",null, "p"));
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    vm0.invoke(PRQHelp
        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(localName,
        portfolio, cnt, cntDest));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRQueryAndCompareResults(
        name, localName));
    // validation on index usage with queries over a pr
    getLogWriter()
    .info(
        "PRBasicIndexCreationDUnitTest.testPartitionedQueryWithIndexOnIdBug37089 done ");
  }
  
  /**
   * Creats partitioned index on keys and values of a bucket regions.
   */
  public void testCreatePartitionedIndexWithKeysValuesAndFunction() throws Exception
  {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
//    VM vm3 = host.getVM(3);
    // closeAllCache();
    final String fileName = "PRIndexCreation.xml";
    getLogWriter().info(
        "PRBasicIndexCreation.testCreatePartitionedIndexThroughXML started");
    getLogWriter().info(
        "Starting and initializing partitioned regions and indexes using xml");
    getLogWriter().info(
        "Starting a pr asynchronously using an xml file name : " + fileName);
   // AsyncInvocation asyInvk0 = vm0.invokeAsync(PRQHelp
   //     .getCacheSerializableRunnableForPRCreateThrougXML(name, fileName));
   // AsyncInvocation asyInvk1 = vm1.invokeAsync(PRQHelp
   //     .getCacheSerializableRunnableForPRCreateThrougXML(name, fileName));
   // asyInvk1.join();
   // if (asyInvk1.exceptionOccurred()) {
   //   fail("asyInvk1 failed", asyInvk1.getException());
   // }
   // asyInvk0.join();
   // if (asyInvk0.exceptionOccurred()) {
    //  fail("asyInvk0 failed", asyInvk0.getException());
   // }
    // printing all the indexes are created.
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    //vm1.invoke(PRQHelp.getCacheSerializableRunnableForIndexCreationCheck(name));
    /*
    <index name="index8">
    <functional from-clause="/PartionedPortfolios.keys k" expression="k" />
  </index> */
  //  vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreateThrougXML(name, fileName));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
//    vm0.invoke(PRQHelp
//        .getCacheSerializableRunnableForLocalRegionCreation(localName));
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "index8","k", "/PartionedPortfolios.keys k" , ""));
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "index7","nvl(k.status.toString(),'nopes')", "/PartionedPortfolios.values k" , ""));
    
    //vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(localName,
    //    "index8","k", "/LocalPortfolios.keys k" , ""));
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
        cnt, cntDest));
    
    
    getLogWriter().info(
        "PRBasicIndexCreation.testCreatePartitionedIndexThroughXML is done  " );
    

  }
  
  /**
   * Bug Fix 37201, creating index from a data accessor.
   */
  public void testCreateIndexFromAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));
    
    // create more vms to host data.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    //  Putting the data into the PR's created
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
     cnt, cntDest));
    
    
    // create the index form asscessor.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    
    

  }


  /**
   * Test for bug fix 37985, NullPointerException in IndexCreationMsg 
   * operateOnPartitionedRegion. This bug show up when an accessor 
   * (PR with max memory = 0) vm joins the PR system when there are 
   * index on the PR and an index creation message is sent to this 
   * accessor VM.
   */
  
  public void testCreateIndexAndAddAnAccessor() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRCreate(name,
        redundancy));
    
    final PortfolioData[] portfolio = PRQHelp.createPortfolioData(cnt, cntDest);
    //  Putting the data into the PR's created
    vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPuts(name, portfolio,
     cnt, cntDest));
    //create index from a data store.
    vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
        "PrIndexOnStatus", "p.status",null, "p"));
    
    // create an accessor vm.
    vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRAccessorCreate(name,
        redundancy));
    
    
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
 public void testIndexQueryingWithOrderBy() throws Exception
 {
   int dataSize = 10;
   int step = 2;
   int totalDataSize = 90;
   final int i = 0;
   
   getLogWriter()
   .info(
         "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);

   // Creating PR's on the participating VM's
   getLogWriter()
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
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
              .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (3 * (step)), dataSize));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
                                                            portfoliosAndPositions, i, dataSize));

   //create index from a data store.
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnStatus", "p.status",null, "p"));

   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnID", "p.ID",null, "p"));

   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnKeyID", "key.ID","/" + name + ".keys key", null));
   
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndCompareResults(
                                                                              name, localName));
   getLogWriter()
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
 public void testIndexQueryingWithOrderAndVerify() throws Exception
 {
   int dataSize = 10;
   int step = 2;
   int totalDataSize = 90;
   final int i = 0;

   getLogWriter()
   .info(
   "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);

   // Creating PR's on the participating VM's
   getLogWriter()
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
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
       .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       (3 * (step)), dataSize));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
       portfoliosAndPositions, i, dataSize));

   //create index from a data store.
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnStatus", "p.status",null, "p"));

   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnID", "p.ID",null, "p"));

   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnKeyID", "key.ID","/" + name + ".keys key", null));

   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryAndVerifyOrder(
       name, localName));
   getLogWriter()
   .info(
       "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }

public void testIndexQueryingWithOrderByLimit() throws Exception
 {
   int dataSize = 10;
   int step = 2;
   int totalDataSize = 90;
   final int i = 0;
   
   getLogWriter()
   .info(
         "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Querying PR Test with DACK Started*****");

   Class valueConstraint = Portfolio.class;
   Host host = Host.getHost(0);
   VM vm0 = host.getVM(0);
   VM vm1 = host.getVM(1);
   VM vm2 = host.getVM(2);
   VM vm3 = host.getVM(3);

   // Creating PR's on the participating VM's
   getLogWriter()
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
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created PR's on VM0, VM1 , VM2 , VM3");

   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Creating Local region on VM0 to compare result Sets");
   // creating a local region on one of the JVM's
   vm0.invoke(PRQHelp
              .getCacheSerializableRunnableForLocalRegionCreation(localName, valueConstraint));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Successfully Created Local Region on VM0");

   // Generating portfolio object array to be populated across the PR's & Local
   // Regions

   final Portfolio[] portfoliosAndPositions = PRQHelp.createPortfoliosAndPositions(totalDataSize);

   // Putting the data into the PR's created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            0, step));
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
       step, (2 * step)));
   vm2.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (2 * step), (3 * step)));
   vm3.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(name, portfoliosAndPositions,
                                                            (3 * (step)), dataSize));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data across PR's");

   // Putting the same data in the local region created
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPRPutsKeyValue(localName,
                                                            portfoliosAndPositions, i, dataSize));

   // create index from a data store.
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnStatus", "p.status", null, "p"));

   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnID", "p.ID", null, "p"));

   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnKeyID", "key.ID", "/" + name + ".keys key", null));
   
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnKeyStatus", "key.status", "/" + name + ".keys key", null));
   
   vm1.invoke(PRQHelp.getCacheSerializableRunnableForPRIndexCreate(name,
       "PrIndexOnsecID", "p.position1.secId", "/" + name + " p", null));

   vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRIndexCreate(localName,
       "rrIndexOnStatus", "p.status", null, "p"));

   vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRIndexCreate(localName,
       "rrIndexOnID", "p.ID", null, "p"));

   vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRIndexCreate(localName,
       "rrIndexOnKeyID", "key.ID", "/" + localName + ".keys key", null));
   
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRIndexCreate(localName,
       "rrIndexOnsecID", "p.position1.secId", "/" + localName + " p", null));
   
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForRRIndexCreate(localName,
       "rrIndexOnKeyStatus", "key.status", "/" + localName + ".keys key", null));

   
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : Inserted Portfolio data over Local Region on VM0");

   // querying the VM for data
   vm0.invoke(PRQHelp.getCacheSerializableRunnableForPROrderByQueryWithLimit(
                                                                              name, localName));
   getLogWriter()
     .info(
           "PRQueryDUnitTest#testPRDAckCreationAndQuerying : *Querying PR's with DACK Test ENDED*****");
 }

  
}
