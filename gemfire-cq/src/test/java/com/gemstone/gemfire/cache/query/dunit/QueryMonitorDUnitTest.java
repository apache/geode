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
package com.gemstone.gemfire.cache.query.dunit;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryExecutionTimeoutException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryDUnitTest;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests for QueryMonitoring service.
 * @author agingade
 * @since 6.0
 */
public class QueryMonitorDUnitTest extends CacheTestCase {

  private static int bridgeServerPort;

  private final String exampleRegionName = "exampleRegion";
  private final String exampleRegionName2 = "exampleRegion2";
  private final String poolName = "serverConnectionPool";
  
  
  /* Some of the queries are commented out as they were taking less time */
  String[]  queryStr = {
      "SELECT ID FROM /root/exampleRegion p WHERE  p.ID > 100",
      "SELECT DISTINCT * FROM /root/exampleRegion x, x.positions.values WHERE  x.pk != '1000'",
      "SELECT DISTINCT * FROM /root/exampleRegion x, x.positions.values WHERE  x.pkid != '1'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values WHERE  p.pk > '1'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values WHERE  p.pkid != '53'",
      "SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos WHERE  pos.Id > 100",
      "SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos WHERE  pos.Id > 100 and pos.secId IN SET('YHOO', 'IBM', 'AMZN')",
      "SELECT * FROM /root/exampleRegion p WHERE  p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT * FROM /root/exampleRegion WHERE  ID > 100 and status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p WHERE  p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT DISTINCT ID FROM /root/exampleRegion WHERE  status = 'active'",
      "SELECT DISTINCT ID FROM /root/exampleRegion p WHERE  p.status = 'active'",
      "SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos WHERE  pos.secId IN SET('YHOO', 'IBM', 'AMZN')",
      "SELECT DISTINCT proj1:p, proj2:itrX FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos"
      + " WHERE  pos.secId = 'YHOO') as itrX",
      "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion p, p.positions.values pos"
      + " WHERE  pos.secId = 'YHOO') as itrX",
      "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT p.ID FROM /root/exampleRegion x"
      + " WHERE  x.ID = p.ID) as itrX",
      "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion x, x.positions.values pos"
      + " WHERE  x.ID = p.ID) as itrX",
      "SELECT DISTINCT x.ID FROM /root/exampleRegion x, x.positions.values v WHERE  "
      + "v.secId = element(SELECT DISTINCT vals.secId FROM /root/exampleRegion p, p.positions.values vals WHERE  vals.secId = 'YHOO')",
      "SELECT DISTINCT * FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.status = 'active'",
      "SELECT DISTINCT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.status = 'active' and p2.status = 'active'",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.status = 'active' and p.status = p2.status",
      "SELECT DISTINCT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 100000",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 100000 or p.status = p2.status",
      "SELECT p.ID FROM /root/exampleRegion p, /root/exampleRegion2 p2 WHERE  p.ID = p2.ID and p.ID > 100 and p2.ID < 100000 or p.status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, positions.values pos WHERE   (p.ID > 1 or p.status = 'active') or (true AND pos.secId ='IBM')", 
      "SELECT DISTINCT * FROM /root/exampleRegion p, positions.values pos WHERE   (p.ID > 1 or p.status = 'active') or (true AND pos.secId !='IBM')",
      "SELECT DISTINCT structset.sos, structset.key " 
      + "FROM /root/exampleRegion p, p.positions.values outerPos, " 
      + "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "
      + "FROM /root/exampleRegion.entries pf, pf.value.positions.values pos "
      + "where outerPos.secId != 'IBM' AND "
      + "pf.key IN (SELECT DISTINCT * FROM pf.value.collectionHolderMap['0'].arr)) structset "
      + "where structset.sos > 2000",
      "SELECT DISTINCT * "
      + "FROM /root/exampleRegion p, p.positions.values outerPos, "
      + "(SELECT DISTINCT key: key, sos: pos.sharesOutstanding "
      + "FROM /root/exampleRegion.entries pf, pf.value.positions.values pos "
      + "where outerPos.secId != 'IBM' AND " 
      + "pf.key IN (SELECT DISTINCT * FROM pf.value.collectionHolderMap['0'].arr)) structset "
      + "where structset.sos > 2000",
      "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values position "
      + "WHERE (true = null OR position.secId = 'SUN') AND true", 
  };

  String[]  prQueryStr = {
      "SELECT ID FROM /root/exampleRegion p WHERE  p.ID > 100 and p.status = 'active'",
      "SELECT * FROM /root/exampleRegion WHERE  ID > 100 and status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p WHERE   p.ID > 100 and p.status = 'active' and p.ID < 100000",
      "SELECT DISTINCT p.ID FROM /root/exampleRegion p WHERE p.ID > 100 and p.ID < 100000 and p.status = 'active'",
      "SELECT DISTINCT * FROM /root/exampleRegion p, positions.values pos WHERE (p.ID > 1 or p.status = 'active') or (pos.secId != 'IBM')", 
  };
  
  private int numServers;

  public QueryMonitorDUnitTest(String name) {
    super(name);
  }

  public void setup(int numServers) throws Exception {
    super.setUp();
    Host host = Host.getHost(0);
    this.numServers = numServers;
    
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating connection pools
    getSystem();
    
    SerializableRunnable r = new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    };
    
    for (int i=0; i<numServers; i++) {
      host.getVM(i).invoke(r);
    }
    
    r = new SerializableRunnable("getClientSystem") {
      public void run() {
        Properties props = getAllDistributedSystemProperties(new Properties());
        props.put(DistributionConfigImpl.LOCATORS_NAME, "");
        getSystem(props);
      }
    };
    
    for (int i=numServers; i<4; i++) {
      host.getVM(i).invoke(r);
    }
  }
  
  @Override
  public void tearDown2() throws Exception {
    Host host = Host.getHost(0);
    disconnectFromDS();
    // shut down clients before servers
    for (int i=numServers; i<4; i++) {
      host.getVM(i).invoke(CacheTestCase.class, "disconnectFromDS");
    }
    super.tearDown2();
  }
  
  public void createRegion(VM vm){
    createRegion(vm, false, null);
  }

  public void createRegion(VM vm, final boolean eviction, final String dirName){
    vm.invoke(new CacheSerializableRunnable("Create Regions") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        // setting the eviction attributes.
        if (eviction) {
          File []f = new File[1];            
          f[0] = new File(dirName);
          f[0].mkdir();
          DiskStoreFactory dsf = GemFireCacheImpl.getInstance().createDiskStoreFactory();
          DiskStore ds1 = dsf.setDiskDirs(f).create("ds1");
          factory.setDiskStoreName("ds1");
          EvictionAttributes evictAttrs = EvictionAttributes
            .createLRUEntryAttributes(100, EvictionAction.OVERFLOW_TO_DISK);
          factory.setEvictionAttributes(evictAttrs);
        }
        // Create region
        createRegion(exampleRegionName, factory.create());
        createRegion(exampleRegionName2, factory.create());
      }
    });
  }

  public void createPRRegion(VM vm){
    vm.invoke(new CacheSerializableRunnable("Create Regions") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        //factory.setDataPolicy(DataPolicy.PARTITION);
        factory.setPartitionAttributes((new PartitionAttributesFactory()).setTotalNumBuckets(8).create());
        
        createRegion(exampleRegionName, factory.create());
        createRegion(exampleRegionName2, factory.create());
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        exampleRegion.getCache().getLogger().fine("#### CREATING PR REGION....");
      }
    });
  }

  public void configServer(VM server, final int queryMonitorTime, final String testName){
    SerializableRunnable initServer = new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        try {
          startBridgeServer(0, false);
        } catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
        Cache cache = getCache();
        GemFireCacheImpl.getInstance().TEST_MAX_QUERY_EXECUTION_TIME = queryMonitorTime;
        cache.getLogger().fine("#### RUNNING TEST : " + testName);
        DefaultQuery.testHook = new QueryTimeoutHook(queryMonitorTime);
        //((GemFireCache)cache).TEST_MAX_QUERY_EXECUTION_TIME = queryMonitorTime;
        System.out.println("MAX_QUERY_EXECUTION_TIME is set to: " + ((GemFireCacheImpl)cache).TEST_MAX_QUERY_EXECUTION_TIME);
      }
    };
    server.invoke(initServer);
  }

  // Stop server
  public void stopServer(VM server){
    SerializableRunnable stopServer = new SerializableRunnable("Stop CacheServer") {      
      public void run() {
        // Reset the test flag.
        Cache cache = getCache();
        DefaultQuery.testHook = null;
        GemFireCacheImpl.getInstance().TEST_MAX_QUERY_EXECUTION_TIME = -1;
        stopBridgeServer(getCache());
        System.out.println("MAX_QUERY_EXECUTION_TIME is set to: " + ((GemFireCacheImpl)cache).TEST_MAX_QUERY_EXECUTION_TIME);
      }
    };
    server.invoke(stopServer);
  }
  
  public void configClient(VM client, VM[] server){
    final int[] port = new int[server.length];
    for (int i=0; i < server.length; i++){
      port[i] = server[i].invokeInt(QueryMonitorDUnitTest.class, "getCacheServerPort");
    }
    final String host0 = getServerHostName(server[0].getHost());

    SerializableRunnable initClient = new CacheSerializableRunnable("Init client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        PoolFactory poolFactory = PoolManager.createFactory();
        poolFactory.setReadTimeout(10 * 60 * 1000); // 10 mins.
        ClientServerTestCase.configureConnectionPoolWithNameAndFactory(factory, host0, port, true, -1, -1, null, poolName, poolFactory);
      }
    };
    client.invoke(initClient);    
  }
  
  public void verifyException(Exception e) {
    e.printStackTrace();
    String error = e.getMessage();
    if (e.getCause() != null) {
      error = e.getCause().getMessage();
    }
    
    if (error.contains("Query execution cancelled after exceeding max execution time") ||
        error.contains("The Query completed sucessfully before it got canceled") ||
        error.contains("The QueryMonitor thread may be sleeping longer than the set sleep time") ||
        error.contains("The query task could not be found but the query is marked as having been canceled")){
      // Expected exception.
      return;      
    }

    System.out.println("Unexpected exception:");
    if (e.getCause() != null) {
      e.getCause().printStackTrace();
    } else {
      e.printStackTrace();
    }
    
    fail("Expected exception Not found. Expected exception with message: \n" +
        "\"Query execution taking more than the max execution time\"" + "\n Found \n" +
        error);    
  }
  
  /**
   * Tests query execution from client to server (single server).
   */
  public void testQueryMonitorClientServer() throws Exception {

    setup(1);
    
    final Host host = Host.getHost(0);
    
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    VM client3 = host.getVM(3);

    final int numberOfEntries = 100;

    // Start server
    configServer(server, 20, "testQueryMonitorClientServer"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server);
    
    // Initialize server regions.
    server.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize Client1 and create client regions.
    configClient(client1, new VM[] {server});
    createRegion(client1);
    
    // Initialize Client2 and create client regions.
    configClient(client2, new VM[] {server});
    createRegion(client2);
    
    // Initialize Client3 and create client regions.
    configClient(client3, new VM[] {server});
    createRegion(client3);

    // Execute client queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Pool pool = PoolManager.find(poolName);
          QueryService queryService = pool.getQueryService();
          for (int k=0; k < queryStr.length; k++) {
            String qStr = queryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    client1.invoke(executeQuery);
    client2.invoke(executeQuery);
    client3.invoke(executeQuery);

    stopServer(server);
  }

  /**
   * Tests query execution from client to server (multi server).
   */
  public void testQueryMonitorMultiClientMultiServer() throws Exception {

    setup(2);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    // Start server
    configServer(server1, 20, "testQueryMonitorMultiClientMultiServer"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server1);

    configServer(server2, 20, "testQueryMonitorMultiClientMultiServer"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server2);

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize Client1 and create client regions.
    configClient(client1, new VM[] {server1, server2});
    createRegion(client1);
    
    // Initialize Client2 and create client regions.
    configClient(client2, new VM[] {server1, server2});
    createRegion(client2);
    
    // Execute client queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Pool pool = PoolManager.find(poolName);
          QueryService queryService = pool.getQueryService();
          for (int k=0; k < queryStr.length; k++) {
            String qStr = queryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);            
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    client1.invoke(executeQuery);
    client2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on local vm.
   */
  public void testQueryExecutionLocally() throws Exception {

    setup(2);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    configServer(server1, 20, "testQueryExecutionLocally"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server1);

    configServer(server2, 20, "testQueryExecutionLocally"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server2);

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          for (int k=0; k < queryStr.length; k++) {
            String qStr = queryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    server1.invoke(executeQuery);
    server2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on local vm.
   */
  public void testQueryExecutionLocallyAndCacheOp() throws Exception {

    setup(2);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 1000;

    // Start server
    configServer(server1, 20, "testQueryExecutionLocally"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server1);

    configServer(server2, 20, "testQueryExecutionLocally"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server2);

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= 200; i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          String qStr = "SELECT DISTINCT * FROM /root/exampleRegion p, (SELECT DISTINCT pos FROM /root/exampleRegion x, x.positions.values pos"
            + " WHERE  x.ID = p.ID) as itrX";
          try {
            GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
            Query query = queryService.newQuery(qStr);
            query.execute();
            fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
          }catch (Exception e){
            verifyException(e);
          }

          // Create index and Perform cache op. Bug#44307
          queryService.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/root/exampleRegion");
          queryService.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/root/exampleRegion");
          Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
          for (int i= (1 + 100); i <= (numberOfEntries + 200); i++) {
            exampleRegion.put(""+i, new Portfolio(i));
          }
          
        } catch (Exception ex){
          fail("Exception creating the query service", ex);
        }
      }      
    };

    server1.invoke(executeQuery);
    server2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }
  
  /**
   * Tests query execution from client to server (multiple server) on Partition Region .
   */
  public void testQueryMonitorOnPR() throws Exception {

    setup(2);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    // Start server
    configServer(server1, 100, "testQueryMonitorMultiClientMultiServerOnPR"); // All the queries taking more than 100ms should be canceled by Query monitor.
    createPRRegion(server1);

    configServer(server2, 100, "testQueryMonitorMultiClientMultiServerOnPR"); // All the queries taking more than 100ms should be canceled by Query monitor.
    createPRRegion(server2);
    
    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (numberOfEntries + 100); i <= (numberOfEntries + numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize Client1 and create client regions.
    configClient(client1, new VM[] {server1});
    createRegion(client1);
    
    // Initialize Client2 and create client regions.
    configClient(client2, new VM[] {server2});
    createRegion(client2);
    
    // Execute client queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Pool pool = PoolManager.find(poolName);
          QueryService queryService = pool.getQueryService();
          for (int k=0; k < prQueryStr.length; k++) {
            String qStr = prQueryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);            
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    client1.invoke(executeQuery);
    client2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on Partition Region, executes query locally.
   */
  public void testQueryMonitorWithLocalQueryOnPR() throws Exception {

    setup(2);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);

    final int numberOfEntries = 100;

    // Start server
    configServer(server1, 100, "testQueryMonitorMultiClientMultiServerOnPR"); // All the queries taking more than 100ms should be canceled by Query monitor.
    createPRRegion(server1);

    configServer(server2, 100, "testQueryMonitorMultiClientMultiServerOnPR"); // All the queries taking more than 100ms should be canceled by Query monitor.
    createPRRegion(server2);

    
    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (numberOfEntries + 100); i <= (numberOfEntries + numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });
    
    // Execute client queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          for (int k=0; k < prQueryStr.length; k++) {
            String qStr = prQueryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);            
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    server1.invoke(executeQuery);
    server2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution from client to server (multiple server) with eviction to disk.
   */
  public void BUG46770WORKAROUNDtestQueryMonitorRegionWithEviction() throws CacheException {

    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    // Start server
    configServer(server1, 20, "testQueryMonitorRegionWithEviction"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server1, true, "server1_testQueryMonitorRegionWithEviction");

    configServer(server2, 20, "testQueryMonitorRegionWithEviction"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server2, true, "server2_testQueryMonitorRegionWithEviction");

    
    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        for (int i= (numberOfEntries + 100); i <= (numberOfEntries + numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize Client1 and create client regions.
    configClient(client1, new VM[] {server1});
    createRegion(client1);
    
    // Initialize Client2 and create client regions.
    configClient(client2, new VM[] {server2});
    createRegion(client2);
    
    // Execute client queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Pool pool = PoolManager.find(poolName);
          QueryService queryService = pool.getQueryService();
          for (int k=0; k < prQueryStr.length; k++) {
            String qStr = prQueryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    client1.invoke(executeQuery);
    client2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }

  /**
   * Tests query execution on region with indexes.
   */
  public void testQueryMonitorRegionWithIndex() throws Exception {

    setup(2);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);

    final int numberOfEntries = 100;

    // Start server
    configServer(server1, 20, "testQueryMonitorRegionWithIndex"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server1);

    configServer(server2, 20, "testQueryMonitorRegionWithIndex"); // All the queries taking more than 20ms should be canceled by Query monitor.
    createRegion(server2);

//    pause(1000);
    

    // Initialize server regions.
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
       
        try {
        // create index.
        QueryService cacheQS = GemFireCacheImpl.getInstance().getQueryService();
        cacheQS.createIndex("idIndex", IndexType.FUNCTIONAL,"p.ID","/root/exampleRegion p");
        cacheQS.createIndex("statusIndex", IndexType.FUNCTIONAL,"p.status","/root/exampleRegion p");
        cacheQS.createIndex("secIdIndex", IndexType.FUNCTIONAL,"pos.secId","/root/exampleRegion p, p.positions.values pos");
        cacheQS.createIndex("posIdIndex", IndexType.FUNCTIONAL,"pos.Id","/root/exampleRegion p, p.positions.values pos");
        cacheQS.createIndex("pkIndex", IndexType.PRIMARY_KEY, "pk", "/root/exampleRegion");
        cacheQS.createIndex("pkidIndex", IndexType.PRIMARY_KEY, "pkid", "/root/exampleRegion");
        cacheQS.createIndex("idIndex2", IndexType.FUNCTIONAL,"p2.ID","/root/exampleRegion2 p2");
        cacheQS.createIndex("statusIndex2", IndexType.FUNCTIONAL,"p2.status","/root/exampleRegion2 p2");
        cacheQS.createIndex("secIdIndex2", IndexType.FUNCTIONAL,"pos.secId","/root/exampleRegion2 p2, p2.positions.values pos");
        cacheQS.createIndex("posIdIndex2", IndexType.FUNCTIONAL,"pos.Id","/root/exampleRegion2 p2, p2.positions.values pos");
        cacheQS.createIndex("pkIndex2", IndexType.PRIMARY_KEY, "pk", "/root/exampleRegion2");
        cacheQS.createIndex("pkidIndex2", IndexType.PRIMARY_KEY, "pkid", "/root/exampleRegion2");
        } catch (Exception ex) {
        }
        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= (200 + 100); i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    server2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        Region exampleRegion2 = getRootRegion().getSubregion(exampleRegionName2);
        // create index.
        try {
        QueryService cacheQS = GemFireCacheImpl.getInstance().getQueryService();
        cacheQS.createIndex("idIndex", IndexType.FUNCTIONAL,"p.ID","/root/exampleRegion p");
        cacheQS.createIndex("statusIndex", IndexType.FUNCTIONAL,"p.status","/root/exampleRegion p");
        cacheQS.createIndex("secIdIndex", IndexType.FUNCTIONAL,"pos.secId","/root/exampleRegion p, p.positions.values pos");
        cacheQS.createIndex("posIdIndex", IndexType.FUNCTIONAL,"pos.Id","/root/exampleRegion p, p.positions.values pos");
        cacheQS.createIndex("pkIndex", IndexType.PRIMARY_KEY, "pk", "/root/exampleRegion");
        cacheQS.createIndex("pkidIndex", IndexType.PRIMARY_KEY, "pkid", "/root/exampleRegion");
        cacheQS.createIndex("idIndex2", IndexType.FUNCTIONAL,"p2.ID","/root/exampleRegion2 p2");
        cacheQS.createIndex("statusIndex2", IndexType.FUNCTIONAL,"p2.status","/root/exampleRegion2 p2");
        cacheQS.createIndex("secIdIndex2", IndexType.FUNCTIONAL,"pos.secId","/root/exampleRegion2 p2, p2.positions.values pos");
        cacheQS.createIndex("posIdIndex2", IndexType.FUNCTIONAL,"pos.Id","/root/exampleRegion2 p2, p2.positions.values pos");
        cacheQS.createIndex("pkIndex2", IndexType.PRIMARY_KEY, "pk", "/root/exampleRegion2");
        cacheQS.createIndex("pkidIndex2", IndexType.PRIMARY_KEY, "pkid", "/root/exampleRegion2");
        } catch (Exception ex) {
        }

        for (int i= (1 + 100); i <= (numberOfEntries + 100); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
        for (int i= (1 + 100); i <= (200 + 100); i++) {
          exampleRegion2.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize Client1 and create client regions.
    configClient(client1, new VM[] {server1});
    createRegion(client1);
    
    // Initialize Client2 and create client regions.
    configClient(client2, new VM[] {server2});
    createRegion(client2);
    
    // Execute client queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Pool pool = PoolManager.find(poolName);
          QueryService queryService = pool.getQueryService();
          for (int k=0; k < queryStr.length; k++) {
            String qStr = queryStr[k];
            try {
              GemFireCacheImpl.getInstance().getLogger().fine("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
              fail("The query should have been canceled by the QueryMonitor. Query: " + qStr);
            }catch (Exception e){
              verifyException(e);
            }
          }
        } catch (Exception ex){
          GemFireCacheImpl.getInstance().getLogger().fine("Exception creating the query service", ex);
        }
      }
    };

    client1.invoke(executeQuery);
    client2.invoke(executeQuery);

    stopServer(server1);
    stopServer(server2);
  }
  
  
  protected CqQueryDUnitTest cqDUnitTest = new CqQueryDUnitTest("CqDataDUnitTest");

  /**
   * The following CQ test is added to make sure TEST_MAX_QUERY_EXECUTION_TIME is reset
   * and is not affecting other query related tests.
   * @throws Exception
   */
  public void testCQWithDestroysAndInvalidates() throws Exception
  {
    setup(1);
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producerClient = host.getVM(2);
    
    cqDUnitTest.createServer(server, 0, true);
    final int port = server.invokeInt(CqQueryDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    // producer is not doing any thing.
    cqDUnitTest.createClient(producerClient, port, host0);

    final int size = 10;
    final String name = "testQuery_4";
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], size);

    cqDUnitTest.createCQ(client, name, cqDUnitTest.cqs[4]);
    cqDUnitTest.executeCQ(client, name, true, null);
    
    // do destroys and invalidates.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        System.out.println("TEST CQ MAX_QUERY_EXECUTION_TIME is set to: " + ((GemFireCacheImpl)cache).TEST_MAX_QUERY_EXECUTION_TIME);

        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.destroy( CqQueryDUnitTest.KEY + i);
        }
      }
    });
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForDestroyed(client, name , CqQueryDUnitTest.KEY+i);
    }
    // recreate the key values from 1 - 5
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 5);
    // wait for all creates to arrive.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForCreated(client, name , CqQueryDUnitTest.KEY+i);
    }
    
    // do more puts to push first five key-value to disk.
    cqDUnitTest.createValues(server, cqDUnitTest.regions[0], 10);
    // do invalidates on fisrt five keys.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        System.out.println("TEST CQ MAX_QUERY_EXECUTION_TIME is set to: " + ((GemFireCacheImpl)cache).TEST_MAX_QUERY_EXECUTION_TIME);

        Region region1 = getRootRegion().getSubregion(cqDUnitTest.regions[0]);
        for (int i = 1; i <= 5; i++) {
          region1.invalidate( CqQueryDUnitTest.KEY + i);
        }
      }
    });
    // wait for invalidates now.
    for (int i = 1; i <= 5; i++) {
      cqDUnitTest.waitForInvalidated(client, name , CqQueryDUnitTest.KEY+i);
    }
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  
  }

  /**
   * Tests cache operation right after query cancellation.
   */
  public void testCacheOpAfterQueryCancel() throws Exception {

    setup(4);
    
    final Host host = Host.getHost(0);
    
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    VM server4 = host.getVM(3);
    
    final int numberOfEntries = 1000;

    // Start server
    configServer(server1, 5, "testQueryExecutionLocally"); 
    createPRRegion(server1);

    configServer(server2, 5, "testQueryExecutionLocally"); 
    createPRRegion(server2);

    configServer(server3, 5, "testQueryExecutionLocally"); 
    createPRRegion(server3);
    
    configServer(server4, 5, "testQueryExecutionLocally"); 
    createPRRegion(server4);
    
    server1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        try {
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          queryService.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/root/exampleRegion");
          queryService.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId", "/root/exampleRegion p, p.positions.values pos");
        } catch (Exception ex) {
          fail("Failed to create index.");
        }
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        for (int i= 100; i <= (numberOfEntries); i++) {
          exampleRegion.put(""+i, new Portfolio(i));
        }
      }
    });

    // Initialize server regions.
    AsyncInvocation ai1 = server1.invokeAsync(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        for (int j=0; j < 5; j++) {
          for (int i= 1; i <= (numberOfEntries + 1000); i++) {
            exampleRegion.put(""+i, new Portfolio(i));
          }
        }
        getLogWriter().info("### Completed updates in server1 in testCacheOpAfterQueryCancel");
      }
    });

   
    AsyncInvocation ai2 = server2.invokeAsync(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
        for (int j=0; j < 5; j++) {
          for (int i= (1 + 1000); i <= (numberOfEntries + 2000); i++) {
            exampleRegion.put(""+i, new Portfolio(i));
          }
        }
        getLogWriter().info("### Completed updates in server2 in testCacheOpAfterQueryCancel");
      }
    });

    // Execute server queries
    SerializableRunnable executeQuery = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        try {
          Region exampleRegion = getRootRegion().getSubregion(exampleRegionName);
          QueryService queryService = GemFireCacheImpl.getInstance().getQueryService();
          String qStr = "SELECT DISTINCT * FROM /root/exampleRegion p, p.positions.values pos1, p.positions.values pos" +
            " where p.ID < pos.sharesOutstanding OR p.ID > 0 OR p.position1.mktValue > 0 " +
            " OR pos.secId in SET ('SUN', 'IBM', 'YHOO', 'GOOG', 'MSFT', " +
            " 'AOL', 'APPL', 'ORCL', 'SAP', 'DELL', 'RHAT', 'NOVL', 'HP')" +
            " order by p.status, p.ID desc";
          for (int i=0; i < 500; i++) {
            try {
              GemFireCacheImpl.getInstance().getLogger().info("Executing query :" + qStr);
              Query query = queryService.newQuery(qStr);
              query.execute();
            } catch (QueryExecutionTimeoutException qet) {
              getLogWriter().info("### Got Expected QueryExecutionTimeout exception. " +
                  qet.getMessage());
              if (qet.getMessage().contains("cancelled after exceeding max execution")){
                getLogWriter().info("### Doing a put operation");
                exampleRegion.put(""+i, new Portfolio(i));
              }
            } catch (Exception e){
              fail("Exception executing query." + e.getMessage());
            }
          }
          getLogWriter().info("### Completed Executing queries in testCacheOpAfterQueryCancel");
        } catch (Exception ex){
          fail("Exception creating the query service", ex);
        }
      }      
    };

    AsyncInvocation ai3 = server3.invokeAsync(executeQuery);
    AsyncInvocation ai4 = server4.invokeAsync(executeQuery);
    
    getLogWriter().info("### Waiting for async threads to join in testCacheOpAfterQueryCancel");
    try {
      DistributedTestCase.join(ai1, 5 * 60 * 1000, null);
      DistributedTestCase.join(ai2, 5 * 60 * 1000, null);
      DistributedTestCase.join(ai3, 5 * 60 * 1000, null);
      DistributedTestCase.join(ai4, 5 * 60 * 1000, null);
    } catch (Exception ex) {
      fail("Async thread join failure");
    }
    getLogWriter().info("### DONE Waiting for async threads to join in testCacheOpAfterQueryCancel");
    
    validateQueryMonitorThreadCnt(server1, 0, 1000);
    validateQueryMonitorThreadCnt(server2, 0, 1000);
    validateQueryMonitorThreadCnt(server3, 0, 1000);
    validateQueryMonitorThreadCnt(server4, 0, 1000);
    
    getLogWriter().info("### DONE validating query monitor threads testCacheOpAfterQueryCancel");
    
    stopServer(server1);
    stopServer(server2);
    stopServer(server3);
    stopServer(server4);
  }

  public void validateQueryMonitorThreadCnt(VM vm, final int threadCount, final int waitTime){
    SerializableRunnable validateThreadCnt = new CacheSerializableRunnable("validateQueryMonitorThreadCnt") {
      public void run2() throws CacheException {
        Cache cache = getCache();
        QueryMonitor qm = ((GemFireCacheImpl)cache).getQueryMonitor();
        if (qm == null) {
          fail("Didn't found query monitor.");
        }
        int waited = 0;
        while (true) {
          if (qm.getQueryMonitorThreadCount() != threadCount) {
            if (waited <= waitTime) {
              pause(10);
              waited+=10;
              continue;
            } else {
              fail ("Didn't found expected monitoring thread. Expected: " + threadCount +
                  " found :" + qm.getQueryMonitorThreadCount());
            }
          }
          break;
        }
        //((GemFireCache)cache).TEST_MAX_QUERY_EXECUTION_TIME = queryMonitorTime;
      }
    };
    vm.invoke(validateThreadCnt);
  }
  
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {

    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer bridge =
      (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  private static int getCacheServerPort() {
    return bridgeServerPort;
  }
  
  private class QueryTimeoutHook implements DefaultQuery.TestHook {
    long timeout;
    public QueryTimeoutHook(long timeout) {
      this.timeout = timeout;
    }
    
    public void doTestHook(String description) {
      if (description.equals("6")) {
        try {
          Thread.sleep(timeout * 2);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    public void doTestHook(int spot) {
      doTestHook("" + spot);
    }
    
  }

}

