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
package com.gemstone.gemfire.cache.query.cq.dunit;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.InternalCqQuery;
import com.gemstone.gemfire.cache.query.internal.cq.ServerCQImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

/**
 * This class tests the ContiunousQuery mechanism in GemFire.
 *
 * @author anil
 */
public class CqResultSetUsingPoolDUnitTest extends CacheTestCase {

  protected CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("CqResultSetUsingPoolDUnitTest");
  
  private final String selStr = "SELECT * FROM /root/regionA";
  
  /** Supported queries */
  public final String[] condition = new String [] {
    /* 0  */ " p WHERE p.ID > 3",
    /* 1  */ " p WHERE p.ID < 3",
    /* 2  */ "   WHERE   ID = 3",
    /* 3  */ " p WHERE p.ID >= 3",
    /* 4  */ " p WHERE p.ID <= 3",
    /* 5  */ " p WHERE p.ID > 3 AND p.status = 'active'",
    /* 6  */ "   WHERE   status = 'active' AND ID < 3",
    /* 7  */ " p WHERE p.names[0] = 'aaa'",
    /* 8  */ " p WHERE p.status LIKE 'active'",
    /* 9  */ " p WHERE p.collectionHolderMap.get('1').arr[0] = '0'",
    /* 10 */ " p WHERE p.position1.portfolioId > 3",
    /* 11 */ " p where p.position3[1].portfolioId = 2",
    /* 12 */ " p where NOT(SELECT DISTINCT * FROM positions.values pos " +
             " WHERE pos.secId in SET('YHOO', 'SUN', 'IBM', 'YHOO', 'GOOG', " +
             " 'MSFT', 'AOL', 'APPL', 'ORCL', 'SAP', 'DELL', 'RHAT', 'NOVL', 'HP')).isEmpty",
    /* 13  */ " p WHERE p.ID != 3 AND p.ID !=4",
    /* 14  */ " p WHERE (p.ID = 2 OR p.ID = 4) AND p.status = 'active'",

  };

  /** For intial size of 5 */
  public final int[] resultSize = new int [] {
    /* 0  */ 2,
    /* 1  */ 2,
    /* 2  */ 1,
    /* 3  */ 3,
    /* 4  */ 3,
    /* 5  */ 1,
    /* 6  */ 1,
    /* 7  */ 5,
    /* 8  */ 2,
    /* 9  */ 5,
    /* 10 */ 2,
    /* 11 */ 5,
    /* 12 */ 5,
    /* 13 */ 3,
    /* 14 */ 2,
  };

  /** For intial size of 5 */
  public final String[][] expectedKeys = new String [][] {
    /* 0  */ {"key-4", "key-5"},
    /* 1  */ {"key-1", "key-2"},
    /* 2  */ {"key-3"},
    /* 3  */ {"key-3", "key-4", "key-5"},
    /* 4  */ {"key-1", "key-2", "key-3"},
    /* 5  */ {"key-4"},
    /* 6  */ {"key-2"},
    /* 7  */ {"key-1", "key-2", "key-3","key-4", "key-5"},
    /* 8  */ {"key-2", "key-4"},
    /* 9  */ {"key-1", "key-2", "key-3","key-4", "key-5"},
    /* 10 */ {"key-4", "key-5"},
    /* 11 */ {"key-1", "key-2", "key-3","key-4", "key-5"},
    /* 12 */ {"key-1", "key-2", "key-3","key-4", "key-5"},
    /* 13 */ {"key-1", "key-2", "key-5"},
    /* 14  */ {"key-2", "key-4"},
  };


  
  /** Unsupported queries */
  public final String[] condition2 = new String [] {
    /* 0  */ " p1, /root/regionB p2 WHERE p1.status = p2.status",
    /* 1  */ " p, p.positions.values p1 WHERE p1.secId = 'IBM'",
    /* 2  */ " p, p.positions.values AS pos WHERE pos.secId != '1'",
    /* 3  */ " p WHERE p.ID in (SELECT p1.ID FROM /root/regionA p1 WHERE p1.ID > 3)",
    /* 4  */ ".entries entry WHERE entry.key = '1'",
    /* 5  */ ".entries entry WHERE entry.value.ID > '3'",
    /* 6  */ ".values p WHERE p.ID > '3' and p.status = 'active'",
    /* 7  */ " p, p.position3 pos where pos.portfolioId  = 1",
  };
  

  public CqResultSetUsingPoolDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating ConnectionPools
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    
  }
  
  
  /**
   * Tests CQ Result Set.
   * 
   * @throws Exception
   */
  public void testCqResults() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCqResults";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // Put 5 entries into the region.
    cqDUnitTest.createValues(server, "regionA", 5);
    
    // Test for supported queries.
    String cqQuery = "";
    for (int queryCnt=0; queryCnt < condition.length; queryCnt++) {
      cqQuery = selStr + condition[queryCnt];
      cqDUnitTest.createCQ(client, poolName, "testCqResultsP_" + queryCnt, cqQuery);
      cqDUnitTest.executeCQ(client, "testCqResultsP_" + queryCnt, true, resultSize[queryCnt], expectedKeys[queryCnt], null);
    }
        
    // Test unsupported queries.
    for (int queryCnt=0; queryCnt < condition2.length; queryCnt++) {
      cqQuery = selStr + condition2[queryCnt];
      
      try {
        cqDUnitTest.createCQ(client, poolName, "testCqResultsF_" + queryCnt, cqQuery);
        //cqDUnitTest.executeCQ(client, "testCqResultsF_" + queryCnt, true, cqDUnitTest.noTest, null);
        fail("UnSupported CQ Query, Expected to fail. " +
            " CQ :" + "testCqResultsF_" + queryCnt +
            " Query : " + cqQuery);
      } catch (Exception ex) {
        // Expected.
      }
    }
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server); 
  }


  /**
   * Tests CQ Result Set with Compact Range Index.
   * 
   * @throws Exception
   */
  public void testCqResultsWithCompactRangeIndex() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCqResults";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // Create index.
    cqDUnitTest.createFunctionalIndex(server, "IdIndex", "p.ID", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server, "statusIndex", "p.status", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server, "portfolioIdIndex", "p.position1.portfolioId", "/root/regionA p");
    
    // Put 5 entries into the region.
    cqDUnitTest.createValues(server, "regionA", 5);
    
    // Test for supported queries.
    String cqQuery = "";
    for (int queryCnt=0; queryCnt < condition.length; queryCnt++) {
      cqQuery = selStr + condition[queryCnt];
      cqDUnitTest.createCQ(client, poolName, "testCqResultsP_" + queryCnt, cqQuery);
      cqDUnitTest.executeCQ(client, "testCqResultsP_" + queryCnt, true, resultSize[queryCnt], expectedKeys[queryCnt], null);
    }
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server); 
  }

  /**
   * Tests CQ Result Set with Range Index.
   * 
   * @throws Exception
   */
  public void testCqResultsWithRangeIndex() throws Exception
  {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCqResults";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // Create index.
    server.invoke(new CacheSerializableRunnable("Set RangeIndex Falg") {
      public void run2() throws CacheException {
        IndexManager.TEST_RANGEINDEX_ONLY = true;
      }
    });

    cqDUnitTest.createFunctionalIndex(server, "IdIndex", "p.ID", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server, "statusIndex", "p.status", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server, "portfolioIdIndex", "p.position1.portfolioId", "/root/regionA p");
    
    // Put 5 entries into the region.
    cqDUnitTest.createValues(server, "regionA", 5);
    
    // Test for supported queries.
    String cqQuery = "";
    for (int queryCnt=0; queryCnt < condition.length; queryCnt++) {
      cqQuery = selStr + condition[queryCnt];
      cqDUnitTest.createCQ(client, poolName, "testCqResultsP_" + queryCnt, cqQuery);
      cqDUnitTest.executeCQ(client, "testCqResultsP_" + queryCnt, true, resultSize[queryCnt], expectedKeys[queryCnt], null);
    }

    // Unset the flag.
    server.invoke(new CacheSerializableRunnable("Set RangeIndex Falg") {
      public void run2() throws CacheException {
        IndexManager.TEST_RANGEINDEX_ONLY = false;
      }
    });
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server); 
  }

  /**
   * Tests CQ Result Set.
   * 
   * @throws Exception
   */
  public void testCqResultsOnPR() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    cqDUnitTest.createServerWithPR(server1, 0, false, 0);
    cqDUnitTest.createServerWithPR(server2, 0, false, 0);
    
    final int port = server1.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCqResults";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // Put 5 entries into the region.
    cqDUnitTest.createValues(server1, "regionA", 5);
    
    // Test for supported queries.
    String cqQuery = "";
    for (int queryCnt=0; queryCnt < condition.length; queryCnt++) {
      cqQuery = selStr + condition[queryCnt];
      cqDUnitTest.createCQ(client, poolName, "testCqResultsP_" + queryCnt, cqQuery);
      cqDUnitTest.executeCQ(client, "testCqResultsP_" + queryCnt, true, resultSize[queryCnt], expectedKeys[queryCnt], null);
    }
        
    // Test unsupported queries.
    for (int queryCnt=0; queryCnt < condition2.length; queryCnt++) {
      cqQuery = selStr + condition2[queryCnt];
      
      try {
        cqDUnitTest.createCQ(client, poolName, "testCqResultsF_" + queryCnt, cqQuery);
        //cqDUnitTest.executeCQ(client, "testCqResultsF_" + queryCnt, true, cqDUnitTest.noTest, null);
        fail("UnSupported CQ Query, Expected to fail. " +
            " CQ :" + "testCqResultsF_" + queryCnt +
            " Query : " + cqQuery);
      } catch (Exception ex) {
        // Expected.
      }
    }
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }

  /**
   * Tests CQ Result Set with Compact Range Index.
   * 
   * @throws Exception
   */
  public void testCqResultsWithCompactRangeIndexOnPR() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    cqDUnitTest.createServerWithPR(server1, 0, false, 0);
    cqDUnitTest.createServerWithPR(server2, 0, false, 0);

    final int port = server1.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCqResults";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // Create index.
    cqDUnitTest.createFunctionalIndex(server1, "IdIndex", "p.ID", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server1, "statusIndex", "p.status", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server1, "portfolioIdIndex", "p.position1.portfolioId", "/root/regionA p");
    
    // Put 5 entries into the region.
    cqDUnitTest.createValues(server1, "regionA", 5);
    
    // Test for supported queries.
    String cqQuery = "";
    for (int queryCnt=0; queryCnt < condition.length; queryCnt++) {
      cqQuery = selStr + condition[queryCnt];
      cqDUnitTest.createCQ(client, poolName, "testCqResultsP_" + queryCnt, cqQuery);
      cqDUnitTest.executeCQ(client, "testCqResultsP_" + queryCnt, true, resultSize[queryCnt], expectedKeys[queryCnt], null);
    }
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1); 
    cqDUnitTest.closeServer(server2); 

  }

  /**
   * Tests CQ Result Set with Range Index.
   * 
   * @throws Exception
   */
  public void testCqResultsWithRangeIndexOnPR() throws Exception
  {
    
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    cqDUnitTest.createServerWithPR(server1, 0, false, 0);
    cqDUnitTest.createServerWithPR(server2, 0, false, 0);

    final int port = server1.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCqResults";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // Create index.
    server1.invoke(new CacheSerializableRunnable("Set RangeIndex Falg") {
      public void run2() throws CacheException {
        IndexManager.TEST_RANGEINDEX_ONLY = true;
      }
    });

    server2.invoke(new CacheSerializableRunnable("Set RangeIndex Falg") {
      public void run2() throws CacheException {
        IndexManager.TEST_RANGEINDEX_ONLY = true;
      }
    });

    cqDUnitTest.createFunctionalIndex(server1, "IdIndex", "p.ID", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server1, "statusIndex", "p.status", "/root/regionA p");
    cqDUnitTest.createFunctionalIndex(server1, "portfolioIdIndex", "p.position1.portfolioId", "/root/regionA p");
    
    // Put 5 entries into the region.
    cqDUnitTest.createValues(server1, "regionA", 5);
    
    // Test for supported queries.
    String cqQuery = "";
    for (int queryCnt=0; queryCnt < condition.length; queryCnt++) {
      cqQuery = selStr + condition[queryCnt];
      cqDUnitTest.createCQ(client, poolName, "testCqResultsP_" + queryCnt, cqQuery);
      cqDUnitTest.executeCQ(client, "testCqResultsP_" + queryCnt, true, resultSize[queryCnt], expectedKeys[queryCnt], null);
    }
        
    // Create index.
    server1.invoke(new CacheSerializableRunnable("Set RangeIndex Falg") {
      public void run2() throws CacheException {
        IndexManager.TEST_RANGEINDEX_ONLY = false;
      }
    });

    server2.invoke(new CacheSerializableRunnable("Set RangeIndex Falg") {
      public void run2() throws CacheException {
        IndexManager.TEST_RANGEINDEX_ONLY = false;
      }
    });
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1); 
    cqDUnitTest.closeServer(server2); 
  }

  /**
   * Tests CQ Result Set.
   * 
   * @throws Exception
   */
  public void testCqResultsCaching() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    cqDUnitTest.createServer(server);

    final int port = server.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCqResults";
    final String cqName = "testCqResultsP_0";
    
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // create CQ.
    cqDUnitTest.createCQ(client, poolName, cqName, cqDUnitTest.cqs[0]);
    
    final int numObjects = 300;
    final int totalObjects = 500;
    
    // initialize Region.
    server.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
    
    // Keep updating region (async invocation).
    server.invokeAsync(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        // Update (totalObjects - 1) entries.
        for (int i = 1; i < totalObjects; i++) {
          // Destroy entries.
          if (i > 25 && i < 201) {
            region.destroy(""+i);
            continue;
          }
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
        // recreate destroyed entries.
        for (int j = 26; j < 201; j++) {
          Portfolio p = new Portfolio(j);
          region.put(""+j, p);
        }
        // Add the last key.
        Portfolio p = new Portfolio(totalObjects);
        region.put(""+totalObjects, p);
      }
    });

    // Execute CQ.
    // While region operation is in progress execute CQ.
    cqDUnitTest.executeCQ(client, cqName, true, null);
    
    // Verify CQ Cache results.
    server.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        // Wait till all the region update is performed.
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        while(true){
          if (region.get(""+ totalObjects) == null){
            try {
              Thread.sleep(50);
            } catch (Exception ex){
              //ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            if (size != totalObjects) {
              LogWriterUtils.getLogWriter().info("The number of Cached events " + size + 
                  " is not equal to the expected size " + totalObjects);
              HashSet expectedKeys = new HashSet();
              for (int i = 1; i < totalObjects; i++) {
                expectedKeys.add("" + i);
              }
              Set cachedKeys = cqQuery.getCqResultKeyCache();
              expectedKeys.removeAll(cachedKeys);
              LogWriterUtils.getLogWriter().info("Missing keys from the Cache : " + expectedKeys);
            }
            assertEquals("The number of keys cached for cq " + cqName + " is wrong.", 
                totalObjects, cqQuery.getCqResultKeysSize());              
          }
        }
      }
    });
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server); 
  }

  /**
   * Tests CQ Result Set.
   * 
   * @throws Exception
   */
  public void testCqResultsCachingForMultipleCQs() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);

    cqDUnitTest.createServer(server);

    final int port = server.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCqResults";
    final String cqName1 = "testCqResultsP_0";
    final String cqName2 = "testCqResultsP_1";
    
    cqDUnitTest.createPool(client1, poolName, host0, port);
    cqDUnitTest.createPool(client2, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client1, port, host0);
    cqDUnitTest.createClient(client2, port, host0);
    
    // create CQ.
    cqDUnitTest.createCQ(client1, poolName, cqName1, cqDUnitTest.cqs[0]);
    cqDUnitTest.createCQ(client2, poolName, cqName2, cqDUnitTest.cqs[0]);
    
    final int numObjects = 300;
    final int totalObjects = 500;
    
    // initialize Region.
    server.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
    
    // Keep updating region (async invocation).
    server.invokeAsync(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        // Update (totalObjects - 1) entries.
        for (int i = 1; i < totalObjects; i++) {
          // Destroy entries.
          if (i > 25 && i < 201) {
            region.destroy(""+i);
            continue;
          }
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
        // recreate destroyed entries.
        for (int j = 26; j < 201; j++) {
          Portfolio p = new Portfolio(j);
          region.put(""+j, p);
        }
        // Add the last key.
        Portfolio p = new Portfolio(totalObjects);
        region.put(""+totalObjects, p);
      }
    });

    // Execute CQ.
    // While region operation is in progress execute CQ.
    cqDUnitTest.executeCQ(client1, cqName1, true, null);
    cqDUnitTest.executeCQ(client2, cqName2, true, null);
    
    // Verify CQ Cache results.
    server.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        // Wait till all the region update is performed.
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        while(true){
          if (region.get(""+ totalObjects) == null){
            try {
              Thread.sleep(50);
            } catch (Exception ex){
              //ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          int size = cqQuery.getCqResultKeysSize();
          if (size != totalObjects) {
            LogWriterUtils.getLogWriter().info("The number of Cached events " + size + 
                " is not equal to the expected size " + totalObjects);
            HashSet expectedKeys = new HashSet();
            for (int i = 1; i < totalObjects; i++) {
              expectedKeys.add("" + i);
            }
            Set cachedKeys = cqQuery.getCqResultKeyCache();
            expectedKeys.removeAll(cachedKeys);
            LogWriterUtils.getLogWriter().info("Missing keys from the Cache : " + expectedKeys);
          }
          assertEquals("The number of keys cached for cq " + cqQuery.getName() + " is wrong.", 
              totalObjects, cqQuery.getCqResultKeysSize());              
        }
      }
    });
    
    // Close.
    cqDUnitTest.closeClient(client1);
    cqDUnitTest.closeClient(client2);
    cqDUnitTest.closeServer(server); 
  }

  /**
   * Tests CQ Result Set.
   * 
   * @throws Exception
   */
  public void testCqResultsCachingForPR() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    cqDUnitTest.createServerWithPR(server1, 0, false, 0);
    cqDUnitTest.createServerWithPR(server2, 0, false, 0);
    
    final int port = server1.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCqResults";
    final String cqName = "testCqResultsP_0";
    
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // create CQ.
    cqDUnitTest.createCQ(client, poolName, cqName, cqDUnitTest.cqs[0]);
    
    final int numObjects = 300;
    final int totalObjects = 500;
    
    // initialize Region.
    server1.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
    
    // Keep updating region (async invocation).
    server2.invokeAsync(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        // Update (totalObjects - 1) entries.
        for (int i = 1; i < totalObjects; i++) {
          // Destroy entries.
          if (i > 25 && i < 201) {
            region.destroy(""+i);
            continue;
          }
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
        // recreate destroyed entries.
        for (int j = 26; j < 201; j++) {
          Portfolio p = new Portfolio(j);
          region.put(""+j, p);
        }
        // Add the last key.
        Portfolio p = new Portfolio(totalObjects);
        region.put(""+totalObjects, p);
      }
    });

    // Execute CQ.
    // While region operation is in progress execute CQ.
    cqDUnitTest.executeCQ(client, cqName, true, null);
    
    // Verify CQ Cache results.
    server1.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        // Wait till all the region update is performed.
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        while(true){
          if (region.get(""+ totalObjects) == null){
            try {
              Thread.sleep(50);
            } catch (Exception ex){
              //ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getCqResultKeysSize() <= 0) {
            fail("The Result Cache for CQ on PR is not working. CQ :" + cqName);              
          }
        }
      }
    });
    
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }

  /**
   * Tests CQ Result Set caching for destroy events.
   * 
   * @throws Exception
   */
  public void testCqResultsCachingForDestroyEventsOnPR() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);

    cqDUnitTest.createServerWithPR(server1, 0, false, 0);
    cqDUnitTest.createServerWithPR(server2, 0, false, 0);
    
    final int port = server1.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    String poolName = "testCqResults";
    final String cqName = "testCqResultsCachingForDestroyEventsOnPR_0";
    
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);

    // create CQ.
    cqDUnitTest.createCQ(client, poolName, cqName, cqDUnitTest.cqs[0]);

    // Execute CQ.
    cqDUnitTest.executeCQ(client, cqName, true, null);

    final int numObjects = 50;
    
    // initialize Region.
    server1.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });

    // Update from server2.
    server2.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });

    // Destroy entries.
    server2.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.destroy(""+i);
        }
      }
    });
    
    // Wait for events to be sent.
    cqDUnitTest.waitForDestroyed(client, cqName, "" + numObjects);
    
    // Verify CQ Cache results.
    server1.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getCqResultKeysSize() > 0) {
            fail("The CQ Result Cache on PR should have been empty for CQ :" + cqName + " keys=" + cqQuery.getCqResultKeyCache());              
          }
        }
      }
    });

    server2.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getCqResultKeysSize() > 0) {
            fail("The CQ Result Cache on PR should have been empty for CQ :" + cqName);              
          }
        }
      }
    });

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server1);
    cqDUnitTest.closeServer(server2);
  }


  /**
   * Tests CQ Result Caching with CQ Failover.
   * 
   * @throws Exception
   */
  public void testCqResultsCachingWithFailOver() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    cqDUnitTest.createServer(server1);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    
    String poolName = "testCQFailOver";
    final String cqName = "testCQFailOver_0";
    
    cqDUnitTest.createPool(client, poolName, new String[] {host0, host0}, new int[] {port1, ports[0]});

    // create CQ.
    cqDUnitTest.createCQ(client, poolName, cqName, cqDUnitTest.cqs[0]);
    
    final int numObjects = 300;
    final int totalObjects = 500;
    
    // initialize Region.
    server1.invoke(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        for (int i = 1; i <= numObjects; i++) {
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
      }
    });
    
    // Keep updating region (async invocation).
    server1.invokeAsync(new CacheSerializableRunnable("Update Region"){
      public void run2()throws CacheException {
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        // Update (totalObjects - 1) entries.
        for (int i = 1; i < totalObjects; i++) {
          // Destroy entries.
          if (i > 25 && i < 201) {
            region.destroy(""+i);
            continue;
          }
          Portfolio p = new Portfolio(i);
          region.put(""+i, p);
        }
        // recreate destroyed entries.
        for (int j = 26; j < 201; j++) {
          Portfolio p = new Portfolio(j);
          region.put(""+j, p);
        }
        // Add the last key.
        Portfolio p = new Portfolio(totalObjects);
        region.put(""+totalObjects, p);
      }
    });

    // Execute CQ.
    // While region operation is in progress execute CQ.
    cqDUnitTest.executeCQ(client, cqName, true, null);
    
    // Verify CQ Cache results.
    server1.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        // Wait till all the region update is performed.
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        while(true){
          if (region.get(""+ totalObjects) == null){
            try {
              Thread.sleep(50);
            } catch (Exception ex){
              //ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            if (size != totalObjects) {
              LogWriterUtils.getLogWriter().info("The number of Cached events " + size + 
                  " is not equal to the expected size " + totalObjects);
              HashSet expectedKeys = new HashSet();
              for (int i = 1; i < totalObjects; i++) {
                expectedKeys.add("" + i);
              }
              Set cachedKeys = cqQuery.getCqResultKeyCache();
              expectedKeys.removeAll(cachedKeys);
              LogWriterUtils.getLogWriter().info("Missing keys from the Cache : " + expectedKeys);
            }
            assertEquals("The number of keys cached for cq " + cqName + " is wrong.", 
                totalObjects, cqQuery.getCqResultKeysSize());              
          }
        }
      }
    });
    
    cqDUnitTest.createServer(server2, ports[0]);
    final int thePort2 = server2.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    System.out.println("### Port on which server1 running : " + port1 + 
        " Server2 running : " + thePort2);
    Wait.pause(3 * 1000);
    
    // Close server1 for CQ fail over to server2.
    cqDUnitTest.closeServer(server1); 
    Wait.pause(3 * 1000);
    
    // Verify CQ Cache results.
    server2.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqService cqService = null;
        try {
          cqService = ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to get the internal CqService.", ex);
          Assert.fail ("Failed to get the internal CqService.", ex);
        }
        
        // Wait till all the region update is performed.
        Region region = getCache().getRegion("/root/" + cqDUnitTest.regions[0]);
        while(true){
          if (region.get(""+ totalObjects) == null){
            try {
              Thread.sleep(50);
            } catch (Exception ex){
              //ignore.
            }
            continue;
          }
          break;
        }
        Collection<? extends InternalCqQuery> cqs = cqService.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            if (size != totalObjects) {
              LogWriterUtils.getLogWriter().info("The number of Cached events " + size + 
                  " is not equal to the expected size " + totalObjects);
              HashSet expectedKeys = new HashSet();
              for (int i = 1; i < totalObjects; i++) {
                expectedKeys.add("" + i);
              }
              Set cachedKeys = cqQuery.getCqResultKeyCache();
              expectedKeys.removeAll(cachedKeys);
              LogWriterUtils.getLogWriter().info("Missing keys from the Cache : " + expectedKeys);
            }
            assertEquals("The number of keys cached for cq " + cqName + " is wrong.", 
                totalObjects, cqQuery.getCqResultKeysSize());              
          }
        }
      }
    });    
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server2); 
  }

}
