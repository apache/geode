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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class CopyOnReadIndexDUnitTest extends CacheTestCase {

  VM vm0;
  VM vm1;
  VM vm2;

  
  public CopyOnReadIndexDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
  }
  
  public void tearDown2() throws Exception {
    disconnectAllFromDS();
  }
  
  
  //test different queries against partitioned region
  public void testPRQueryOnLocalNode() throws Exception {
    QueryTestUtils utils = new QueryTestUtils();
    configureServers();
    helpTestPRQueryOnLocalNode(utils.queries.get("545"), 100, 100, true);
    helpTestPRQueryOnLocalNode(utils.queries.get("546"), 100, 100, true);
    helpTestPRQueryOnLocalNode(utils.queries.get("543"), 100, 100, true);
    helpTestPRQueryOnLocalNode(utils.queries.get("544"), 100, 100, true);
    helpTestPRQueryOnLocalNode("select * from /portfolios p where p.ID = 1", 100, 1, true);

    helpTestPRQueryOnLocalNode(utils.queries.get("545"), 100, 100, false);
    helpTestPRQueryOnLocalNode(utils.queries.get("546"), 100, 100, false);
    helpTestPRQueryOnLocalNode(utils.queries.get("543"), 100, 100, false);
    helpTestPRQueryOnLocalNode(utils.queries.get("544"), 100, 100, false);
    helpTestPRQueryOnLocalNode("select * from /portfolios p where p.ID = 1", 100, 1, false);
  }
  
  //tests different queries with a transaction for replicated region
  public void testTransactionsOnReplicatedRegion() throws Exception {
    QueryTestUtils utils = new QueryTestUtils();
    configureServers();
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("545"), 100, 100, true);
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("546"), 100, 100, true);
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("543"), 100, 100, true);
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("544"), 100, 100, true);
    helpTestTransactionsOnReplicatedRegion("select * from /portfolios p where p.ID = 1", 100, 1, true);
    
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("545"), 100, 100, false);
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("546"), 100, 100, false);
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("543"), 100, 100, false);
    helpTestTransactionsOnReplicatedRegion(utils.queries.get("544"), 100, 100, false);
    helpTestTransactionsOnReplicatedRegion("select * from /portfolios p where p.ID = 1", 100, 1, false);
  }
  
  private void configureServers() throws Exception {
    final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    startCacheServer(vm0, port[0]);
    startCacheServer(vm1, port[1]);
    startCacheServer(vm2, port[2]);

  }
  
  //The tests sets up a partition region across 2 servers
  //It does puts in each server, checking instance counts of portfolio objects
  //Querying the data will result in deserialization of portfolio objects.
  //In cases where index is present, the objects will be deserialized in the cache
  public void helpTestPRQueryOnLocalNode(final String queryString, final int numPortfolios, final int numExpectedResults, final boolean hasIndex) throws Exception {
    final int numPortfoliosPerVM = numPortfolios / 2;
  
    resetInstanceCount(vm0);
    resetInstanceCount(vm1);
    
    createPartitionRegion(vm0, "portfolios");
    createPartitionRegion(vm1, "portfolios");
    
    if (hasIndex) {
      vm0.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          QueryTestUtils utils = new QueryTestUtils();
          utils.createIndex("idIndex", "p.ID", "/portfolios p");
          return null;
        }
      });
    }
   
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        for (int i = 0 ; i < numPortfoliosPerVM; i++) {
          Portfolio p = new Portfolio(i);
          p.status = "testStatus";
          p.positions = new HashMap();
          p.positions.put("" + i, new Position("" + i, 20));
          region.put("key " + i, p);
        }
        
        if (hasIndex) {
          //operations we have done on this vm consist of:
          //numPortfoliosPerVM instances of Portfolio created for put operation
          //Due to index, we have deserialized all of the entries this vm currently host
          Index index = getCache().getQueryService().getIndex(region, "idIndex");
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)index.getStatistics().getNumberOfValues() + numPortfoliosPerVM), 5000, 200, true);
        }
        else {
          //operations we have done on this vm consist of:
          //numPortfoliosPerVM instances of Portfolio created for put operation
          //We do not have an index, so we have not deserialized any values
          DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfoliosPerVM), 5000, 200, true);
        }
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        for (int i = numPortfoliosPerVM ; i < numPortfolios; i++) {
          Portfolio p = new Portfolio(i);
          p.status = "testStatus";
          p.positions = new HashMap();
          p.positions.put("" + i, new Position("" + i, 20));
          region.put("key " + i, p);
        }
        //PR indexes are created across nodes unlike Replicated Region Indexes
        if (hasIndex) {
          //operations we have done on this vm consist of:
          //numPortfoliosPerVM instances of Portfolio created for put operation
          //Due to index, we have deserialized all of the entries this vm currently host
          Index index = getCache().getQueryService().getIndex(region, "idIndex");
          if (index == null) {
            QueryTestUtils utils = new QueryTestUtils();
            index = utils.createIndex("idIndex", "p.ID", "/portfolios p");
          }
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)index.getStatistics().getNumberOfValues() + numPortfoliosPerVM), 5000, 200, true);
        }
        else {
          //operations we have done on this vm consist of:
          //numPortfoliosPerVM instances of Portfolio created for put operation
          //We do not have an index, so we have not deserialized any values
          DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfoliosPerVM), 5000, 200, true);
        }
        return null;
      }
    });
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        Iterator it = results.iterator();
        assertEquals("Failed:" + queryString, numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            p.status = "discardStatus";
          }
          else {
            Struct struct = (Struct) o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            p.status = "discardStatus";
          }
        }
        if (hasIndex) {
          //operations we have done on this vm consist of:
          //50 instances of Portfolio created for put operation
          //Due to index, we have deserialized all of the entries this vm currently host
          //Since we have deserialized and cached these values, we just need to add the number of results we did a copy of due to copy on read
          Index index = getCache().getQueryService().getIndex(region, "idIndex");
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)index.getStatistics().getNumberOfValues() + numPortfoliosPerVM + numExpectedResults), 5000, 200, true);
        }
        else {
          //operations we have done on this vm consist of:
          //50 instances of Portfolio created for put operation
          //Due to the query we deserialized the number of entries this vm currently hosts
          //We had to deserialized the results from the other data nodes when we iterated through the results as well as our own
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)((PartitionedRegion)region).getLocalSize() + numExpectedResults + numPortfoliosPerVM), 5000, 200, true);
        }
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        if (hasIndex) {
          //After vm0 executed the query, we already had the values deserialized in our cache
          //So it's the same total as before
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)((PartitionedRegion)region).getLocalSize() + numPortfoliosPerVM), 5000, 200, true);
        }
        else {
          //After vm0 executed the query, we had to deserialize the values in our vm
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)((PartitionedRegion)region).getLocalSize() + numPortfoliosPerVM), 5000, 200, true);
        }
        return null;
      }
    });
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
        }
        if (hasIndex) {
          //operations we have done on this vm consist of:
          //50 instances of Portfolio created for put operation
          //Due to index, we have deserialized all of the entries this vm currently host
          //This is the second query, because we have deserialized and cached these values, we just need to add the number of results a second time
          Index index = getCache().getQueryService().getIndex(region, "idIndex");
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)index.getStatistics().getNumberOfValues() + numExpectedResults + numExpectedResults + numPortfoliosPerVM), 5000, 200, true);
        }
        else {
        //operations we have done on this vm consist of:
          //50 instances of Portfolio created for put operation
          //Due to index, we have deserialized all of the entries this vm currently host
          //This is the second query, because we have deserialized and cached these values, we just need to add the number of results a second time
          //Because we have no index, we have to again deserialize all the values that this vm is hosting
          DistributedTestCase.waitForCriterion(verifyPortfolioCount((int)(((PartitionedRegion)region).getLocalSize() + ((PartitionedRegion)region).getLocalSize() + numExpectedResults + numExpectedResults + numPortfoliosPerVM)), 5000, 200, true);
        }
        return null;
      }
    });
    
    destroyRegion("portfolio", vm0);
    
  }

  
  public void helpTestTransactionsOnReplicatedRegion(final String queryString, final int numPortfolios, final int numExpectedResults, final boolean hasIndex) throws Exception {

    resetInstanceCount(vm0);
    resetInstanceCount(vm1);
    resetInstanceCount(vm2);
    createReplicatedRegion(vm0, "portfolios");
    createReplicatedRegion(vm1, "portfolios");
    createReplicatedRegion(vm2, "portfolios");
    
    //In the case of replicated region hasPR really has no effect on serialization/deserialization counts
    if (hasIndex) {
      vm0.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          QueryTestUtils utils = new QueryTestUtils();
          utils.createHashIndex("idIndex", "p.ID", "/portfolios p");
          return null;
        }
      });
      //let's not create index on vm1 to check different scenarios
      
      vm2.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          QueryTestUtils utils = new QueryTestUtils();
          utils.createHashIndex("idIndex", "p.ID", "/portfolios p");
          return null;
        }
      });
    }
    

    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        for (int i = 0 ; i < numPortfolios; i++) {
          Portfolio p = new Portfolio(i);
          p.status = "testStatus";
          p.positions = new HashMap();
          p.positions.put("" + i, new Position("" + i, 20));
          region.put("key " + i, p);
        }
        
        //We should have the same number of portfolio objects that we created for the put
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfolios), 5000, 200, true);
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        //At this point, we should only have serialized values in this vm
        Region region = getCache().getRegion("/portfolios");
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(0), 0, 200, true);
        return null;
      }
    });
    
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        //There is an index for vm2, so we should have deserialized values at this point,
        Region region = getCache().getRegion("/portfolios");
        if (hasIndex) {
          DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfolios), 0, 200, true);
        }
        else {
          DistributedTestCase.waitForCriterion(verifyPortfolioCount(0), 0, 200, true);
        }
        return null;
      }
    });
    
    //start transaction
    //execute query
    //modify results
    //check instance count
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        CacheTransactionManager txManager =
            region.getCache().getCacheTransactionManager();
        try {
        txManager.begin();

        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            p.status = "discardStatus";
          }
          else {
            Struct struct = (Struct) o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            p.status = "discardStatus";
          }
        }
        
        txManager.commit();
        }
        catch (CommitConflictException conflict) {
          fail("commit conflict exception", conflict);
        }
        
        //We have created puts from our previous callable
        //Now we have copied the results from the query 
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(numExpectedResults + numPortfolios), 0, 200, true);
        return null;
      }
    });
    
    //Check objects in cache on vm1
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
            p.status = "discardStatus";
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
            p.status = "discardStatus";
          }
        }
        //first it must deserialize the portfolios in the replicated region
        //then we do a copy on read of these deserialized objects for the final result set
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(numExpectedResults + numPortfolios), 0, 200, true);

        results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
        }
      
        //we never created index on vm1
        //so in this case, we always have to deserialize the value from the region
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfolios * 2 + numExpectedResults * 2), 0, 200, true);
        return null;
      }
    });
    
    //Check objects in cache on vm2
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
            p.status = "discardStatus";
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
            p.status = "discardStatus";
          }
        }
        //with or without index, the values had to have been deserialized at one point
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfolios + numExpectedResults), 0, 200, true);        
        results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
        }
      

        if (hasIndex) {
          //we have an index, so the values are already deserialized
          //total is now our original deserialization amount : numPortfolios
          //two query results copied.
          DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfolios + numExpectedResults * 2), 0, 200, true);        
        }
        else {
          //we never created index on vm1
          //so in this case, we always have to deserialize the value from the region
          DistributedTestCase.waitForCriterion(verifyPortfolioCount(numPortfolios * 2 + numExpectedResults * 2), 0, 200, true);        
        }
        return null;
      }
    });
    
    //Check objects in cache on vm0
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(numExpectedResults, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
        }
        
        //with or without index, the values we put in the region were already deserialized values
        DistributedTestCase.waitForCriterion(verifyPortfolioCount(numExpectedResults * 2 + numPortfolios), 0, 200, true);        
        return null;
      }
    });
    
    destroyRegion("portfolio", vm0);
  }
  
  private void destroyRegion(String regionName, VM ...vms) {
    for (VM vm: vms) {
      vm.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          QueryTestUtils utils = new QueryTestUtils();
          utils.getCache().getQueryService().removeIndexes();
          Region region = getCache().getRegion("/portfolios");
          region.destroyRegion();
          return null;
        }
      });
    }
  }
 

  private void createPartitionRegion(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        QueryTestUtils utils = new QueryTestUtils();
        utils.createPartitionRegion("portfolios", Portfolio.class);
        return null;
      }
    });
  }
  
  private void createReplicatedRegion(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        QueryTestUtils utils = new QueryTestUtils();
        utils.createReplicateRegion("portfolios");
        return null;
      }
    });
  }
  
  private void resetInstanceCount(VM vm) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        Portfolio.instanceCount.set(0);
      }
    });
  }
  private void startCacheServer(VM server, final int port) throws Exception {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getServerProperties());
        
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setCopyOnRead(true);
        AttributesFactory factory = new AttributesFactory();        
        
        CacheServer cacheServer = getCache().addCacheServer();
        cacheServer.setPort(port);
        cacheServer.start();  
        
        QueryTestUtils.setCache(cache);
        return null;
      }
    });
  }
  
  private void startClient(VM client, final VM server, final int port) {
    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        Properties props = getClientProps();
        getSystem(props);
        
        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(server.getHost()), port);
        ccf.setPoolSubscriptionEnabled(true);
        
        ClientCache cache = (ClientCache)getClientCache(ccf);
      }
    });
  }
 
  protected Properties getClientProps() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }

  protected Properties getServerProperties() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["+getDUnitLocatorPort()+"]");
    return p;
  }

  private WaitCriterion verifyPortfolioCount(final int expected) {
    return new WaitCriterion() {
      private int expectedCount = expected;
      public boolean done() {
        return expectedCount == Portfolio.instanceCount.get();
      }
      
      public String description() {
        return "verifying number of object instances created";
      }
    };
  }
  

}

