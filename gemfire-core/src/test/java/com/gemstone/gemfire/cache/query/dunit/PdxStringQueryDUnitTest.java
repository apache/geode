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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.cache.query.data.PositionPdx;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.index.CompactRangeIndex;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexStore.IndexStoreEntry;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache.query.internal.index.RangeIndex;
import com.gemstone.gemfire.cache.query.types.CollectionType;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.persistence.query.CloseableIterator;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class PdxStringQueryDUnitTest extends CacheTestCase{
  private static int bridgeServerPort;

  public PdxStringQueryDUnitTest(String name) {
    super(name);
   }

  private final String rootRegionName = "root";
  private final String regionName = "PdxTest";
  private final String regName = "/" + rootRegionName + "/" + regionName;
  private final static int orderByQueryIndex = 11;
  private final static int [] groupByQueryIndex = new int[]{7, 8, 9,10};
  
  private final String[] queryString = new String[] { 
      "SELECT pos.secId FROM " + regName + " p, p.positions.values pos WHERE pos.secId LIKE '%L'",//0
      "SELECT pos.secId FROM " + regName + " p, p.positions.values pos where pos.secId = 'IBM'",//1
      "SELECT pos.secId, p.status FROM " + regName + " p, p.positions.values pos where pos.secId > 'APPL'",//2
      "SELECT pos.secId FROM " + regName + " p, p.positions.values pos WHERE pos.secId > 'APPL' and pos.secId < 'SUN'",//3
      "select pos.secId from " + regName + " p, p.positions.values pos where pos.secId  IN SET ('YHOO', 'VMW')",//4
      "select pos.secId from " + regName + " p, p.positions.values pos where NOT (pos.secId = 'VMW')",//5
      "select pos.secId from " + regName + " p, p.positions.values pos where NOT (pos.secId IN SET('SUN', 'ORCL')) ",//6
      "select pos.secId , count(pos.id) from " + regName + " p, p.positions.values pos where  pos.secId > 'APPL' group by pos.secId ",//7
      "select pos.secId , sum(pos.id) from " + regName + " p, p.positions.values pos where  pos.secId > 'APPL' group by pos.secId ",//8,
      "select pos.secId , count(distinct pos.secId) from " + regName + " p, p.positions.values pos where  pos.secId > 'APPL' group by pos.secId ",//9
      "select  count(distinct pos.secId) from " + regName + " p, p.positions.values pos where  pos.secId > 'APPL' ",//10
      "SELECT distinct pos.secId FROM " + regName + " p, p.positions.values pos order by pos.secId",//11
      "SELECT distinct pos.secId FROM " + regName + " p, p.positions.values pos WHERE p.ID > 1 order by pos.secId limit 5",//12
 };

 private final String[] queryString2 = new String[] { 
      "SELECT pos.secIdIndexed FROM " + regName + " p, p.positions.values pos WHERE pos.secIdIndexed LIKE '%L'",//0
      "SELECT pos.secIdIndexed FROM " + regName + " p, p.positions.values pos where pos.secIdIndexed = 'IBM'",//1
      "SELECT pos.secIdIndexed, p.status FROM " + regName + " p, p.positions.values pos where pos.secIdIndexed > 'APPL'",//2
      "SELECT pos.secIdIndexed FROM " + regName + " p, p.positions.values pos WHERE pos.secIdIndexed > 'APPL' and pos.secIdIndexed < 'SUN'",//3
      "select pos.secIdIndexed from " + regName + " p, p.positions.values pos where pos.secIdIndexed  IN SET ('YHOO', 'VMW')",//4
      "select pos.secIdIndexed from " + regName + " p, p.positions.values pos where NOT (pos.secIdIndexed = 'VMW')",//5
      "select pos.secIdIndexed from " + regName + " p, p.positions.values pos where NOT (pos.secIdIndexed IN SET('SUN', 'ORCL')) ",//6
      "select pos.secIdIndexed , count(pos.id) from " + regName + " p, p.positions.values pos where  pos.secIdIndexed > 'APPL' group by pos.secIdIndexed ",//7
      "select pos.secIdIndexed , sum(pos.id) from " + regName + " p, p.positions.values pos where  pos.secIdIndexed > 'APPL' group by pos.secIdIndexed ",//8
      "select pos.secIdIndexed , count(distinct pos.secIdIndexed) from " + regName + " p, p.positions.values pos where  pos.secIdIndexed > 'APPL' group by pos.secIdIndexed ",//9
      "select  count(distinct pos.secIdIndexed) from " + regName + " p, p.positions.values pos where  pos.secIdIndexed > 'APPL'  ",//10
      "SELECT distinct pos.secIdIndexed FROM " + regName + " p, p.positions.values pos order by pos.secIdIndexed",//11
      "SELECT distinct pos.secIdIndexed FROM " + regName + " p, p.positions.values pos WHERE p.ID > 1 order by pos.secIdIndexed limit 5",//12
 };

  public void testReplicatedRegionNoIndex() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
        Index index = null;
        // create an index on statusIndexed is created
         try {
           index = localQueryService.createIndex("secIdIndex2", "pos.secIdIndexed", regName  + " p, p.positions.values pos");
             if(!(index instanceof RangeIndex)){
               fail("Range Index should have been created instead of " + index.getClass());
             }
            } catch (Exception ex) {
           fail("Failed to create index." + ex.getMessage());
         }
      }
    });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        getLogWriter().info("Put PortfolioPdx");
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new PortfolioPdx(i));
         }
      }
    });
    
     // Execute queries from client to server and locally on client
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        SelectResults[] resWithoutIndexRemote = new SelectResults[queryString.length];
        SelectResults[] resWithIndexRemote = new SelectResults[queryString2.length];
        SelectResults[] resWithoutIndexLocal = new SelectResults[queryString.length];
        SelectResults[] resWithIndexLocal = new SelectResults[queryString2.length];

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        for (int i=0; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on remote server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            resWithoutIndexRemote[i] = rs[0][0];
            getLogWriter().info("RR remote indexType: no index  size of resultset: "+ rs[0][0].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][0].asList(), queryString[i]);
            
            getLogWriter().info("### Executing Query locally on client:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            resWithoutIndexLocal[i] = rs[0][1];
            getLogWriter().info("RR  client local indexType:no index size of resultset: "+ rs[0][1].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][1].asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
          try{
            // to compare remote query results with and without index
            getLogWriter().info("### Executing Query on remote server for region2:" + queryString2[i]);
            Query query = remoteQueryService.newQuery(queryString2[i]);
            resWithIndexRemote[i] = (SelectResults)query.execute();
            getLogWriter().info("RR  remote region2 size of resultset: "+ resWithIndexRemote[i].size() + " for query: " + queryString2[i]);;
            checkForPdxString(resWithIndexRemote[i].asList(), queryString2[i]);

           // to compare local query results with and without index
            getLogWriter().info("### Executing Query on local for region2:" + queryString2[i]);
            query = localQueryService.newQuery(queryString2[i]);
            resWithIndexLocal[i] = (SelectResults)query.execute();
            getLogWriter().info("RR  local region2 size of resultset: "+ resWithIndexLocal[i].size() + " for query: " + queryString2[i]);;
            checkForPdxString(resWithIndexLocal[i].asList(), queryString2[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString2[i], e);
          }

            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, false);
            }
          
        }
        // compare remote query results with and without index
           for (int i=0; i < queryString.length; i++){
            rs[0][0] = resWithoutIndexRemote[i]; 
            rs[0][1] = resWithIndexRemote[i];
            if(i < orderByQueryIndex){
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Results with and without index are not matching for query :" + queryString2[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, false);
            }
          }
           // compare local query results with and without index
          for (int i=0; i < queryString.length; i++){
            rs[0][0] = resWithoutIndexLocal[i]; 
            rs[0][1] = resWithIndexLocal[i];
            if(i < orderByQueryIndex){
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Results with and without index are not matching for query :" + queryString2[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, false);
            }
          }
        
       }
    };

    client.invoke(executeQueries);

    // Put Non Pdx objects on server execute queries locally 
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        getLogWriter().info("Put Objects locally on server");
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, new Portfolio(i));
         }
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server local indexType: no  size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);

          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
          try{
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString2[i]).execute();
            getLogWriter().info("RR server local indexType: no size of resultset: " + rs.size() + " for query: " + queryString2[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString2[i]);
          }catch (Exception e) {
            fail("Failed executing " + queryString2[i], e);
          }
        }
       }
    });
    
   // test for readSerialized flag
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("isPR: false server local readSerializedTrue: indexType: false size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    // test for readSerialized flag on client
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService  remoteQueryService = (PoolManager.find(poolName)).getQueryService();

        // Query server1 remotely to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) remoteQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server remote readSerializedTrue: indexType: false size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }

  public void testRepliacatedRegionCompactRangeIndex() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
        // Verify the type of index created  
        Index index = null;
         try {
           index = localQueryService.createIndex("statusIndex", "status", regName);
             if(!(index instanceof CompactRangeIndex)){
               fail("CompactRange Index should have been created instead of " + index.getClass());
             }
            } catch (Exception ex) {
           fail("Failed to create index." + ex.getMessage());
         }
      }
    });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        getLogWriter().info("Put PortfolioPdx");
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new PortfolioPdx(i));
         }
      }
    });
    
    //Verify if all the index keys are PdxStrings
    server0.invoke(new CacheSerializableRunnable("Create Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService localQueryService = getCache().getQueryService();

        Index index = localQueryService.getIndex(region, "statusIndex");
        CloseableIterator<IndexStoreEntry> iter = ((CompactRangeIndex) index)
            .getIndexStorage().iterator(null);
        while (iter.hasNext()) {
          Object key = iter.next().getDeserializedKey();
          if (!(key instanceof PdxString)) {
            fail("All keys of the CompactRangeIndex should be PdxStrings and not "
                + key.getClass());
          }
        }
      }
    });
 
    // Execute queries from client to server and locally on client
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        for (int i=0; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on remote server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            getLogWriter().info("RR remote indexType: CompactRange size of resultset: "+ rs[0][0].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][0].asList(), queryString[i]);
             
            getLogWriter().info("### Executing Query locally on client:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            getLogWriter().info("RR  client local indexType: CompactRange size of resultset: "+ rs[0][1].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][1].asList(), queryString[i]);
             
            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, false);
            }
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
        
        }
    };

    client.invoke(executeQueries);
    
    // Put Non Pdx objects on server execute queries locally 
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        getLogWriter().info("Put Objects locally on server");
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, new Portfolio(i));
         }
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server local indexType:Range  size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
       }
    });
   
    // test for readSerialized flag
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = getCache().getQueryService();
        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server local readSerializedTrue: indexType: CompactRange size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    // test for readSerialized flag on client
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService  remoteQueryService = (PoolManager.find(poolName)).getQueryService();

        // Query server1 remotely to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) remoteQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server remote readSerializedTrue: indexType:CompactRange size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }
  
  public void testReplicatedRegionRangeIndex() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
        // Verify the type of index created  
        Index index = null;
          try {
           index = localQueryService.createIndex("secIdIndex", "pos.secId", regName  + " p, p.positions.values pos");
            if(!(index instanceof RangeIndex) ){
               fail("Range Index should have been created instead of "+ index.getClass());
             }
          } catch (Exception ex) {
             fail("Failed to create index." + ex.getMessage());
          }
       }
    });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        getLogWriter().info("Put PortfolioPdx");
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new PortfolioPdx(i));
         }
      }
    });
    
    //Verify if all the index keys are PdxStrings
    server0.invoke(new CacheSerializableRunnable("Create Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService localQueryService = getCache().getQueryService();
          Index index = localQueryService.getIndex(region, "secIdIndex");
            for(Object key: ((RangeIndex)index).getValueToEntriesMap().keySet()){
              if(!(key instanceof PdxString)){
                fail("All keys of the RangeIndex should be PdxStrings and not " + key.getClass());
              }
            }
      }
    });
 
    // Execute queries from client to server and locally on client
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        for (int i=0; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on remote server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            getLogWriter().info("RR remote indexType: Range size of resultset: "+ rs[0][0].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][0].asList(), queryString[i]);
           
            getLogWriter().info("### Executing Query locally on client:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            getLogWriter().info("RR  client local indexType: Range size of resultset: "+ rs[0][1].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][1].asList(), queryString[i]);
            
            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, false);
            }
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
        }
    };

    client.invoke(executeQueries);
    
    // Put Non Pdx objects on server execute queries locally 
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        getLogWriter().info("Put Objects locally on server");
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, new Portfolio(i));
         }
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server local indexType:Range  size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
       }
    });
    // test for readSerialized flag
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = getCache().getQueryService();
       // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR  server local readSerializedTrue: indexType: Range size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    // test for readSerialized flag on client
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService  remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        // Query server1 remotely to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) remoteQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server remote readSerializedTrue: indexType: Range size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }
  
  public void testPartitionRegionNoIndex() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    final boolean isPr = true;
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
       
        Index index = null;
        // In the NO_INDEX case where no indexes are used an index on another field statusIndexed is created
         try {
           index = localQueryService.createIndex("secIdIndex", "pos.secIdIndexed", regName  + " p, p.positions.values pos");
           if(index instanceof PartitionedIndex){
               for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                 if(!(o instanceof RangeIndex) ){
                   fail("RangeIndex Index should have been created instead of "+ index.getClass());
                 }
               }
             }
             else{
               fail("Partitioned index expected");
             }
            } catch (Exception ex) {
           fail("Failed to create index." + ex.getMessage());
         }
      }
    });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        getLogWriter().info("Put PortfolioPdx");
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new PortfolioPdx(i));
         }
      }
    });
    
    // Execute queries from client to server and locally on client
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        SelectResults[] resWithoutIndexRemote = new SelectResults[queryString.length];
        SelectResults[] resWithIndexRemote = new SelectResults[queryString.length];
        SelectResults[] resWithoutIndexLocal = new SelectResults[queryString.length];
        SelectResults[] resWithIndexLocal = new SelectResults[queryString.length];

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        for (int i=0; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on remote server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            resWithoutIndexRemote[i] = rs[0][0];
            getLogWriter().info("RR remote no index size of resultset: "+ rs[0][0].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][0].asList(), queryString[i]);

            getLogWriter().info("### Executing Query locally on client:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            resWithoutIndexLocal[i] = rs[0][1];
            getLogWriter().info("isPR: " + isPr+ "  client local indexType:no index size of resultset: "+ rs[0][1].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][1].asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
          try{  
            // to compare remote query results with and without index
            getLogWriter().info("### Executing Query on remote server for region2:" + queryString2[i]);
            Query query = remoteQueryService.newQuery(queryString2[i]);
            resWithIndexRemote[i] = (SelectResults)query.execute();
            getLogWriter().info("isPR: " + isPr+ "  remote region2 size of resultset: "+ resWithIndexRemote[i].size() + " for query: " + queryString2[i]);;
            checkForPdxString(resWithIndexRemote[i].asList(), queryString2[i]);

           // to compare local query results with and without index
            getLogWriter().info("### Executing Query on local for region2:" + queryString2[i]);
            query = localQueryService.newQuery(queryString2[i]);
            resWithIndexLocal[i] = (SelectResults)query.execute();
            getLogWriter().info("isPR: " + isPr+ "  local region2 size of resultset: "+ resWithIndexLocal[i].size() + " for query: " + queryString2[i]);;
            checkForPdxString(resWithIndexLocal[i].asList(), queryString2[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString2[i], e);
          }

            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
                getLogWriter().info("result0="+rs[0][0].asList());
                getLogWriter().info("result1="+rs[0][1].asList());
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, isPr);
            }
        }
        
          for (int i=0; i < queryString.length; i++){
            rs[0][0] = resWithoutIndexRemote[i]; 
            rs[0][1] = resWithIndexRemote[i];
            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Results with and without index are not matching for query :" + queryString2[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, isPr);
            }
          }
          
          for (int i=0; i < queryString.length; i++){
            rs[0][0] = resWithoutIndexLocal[i]; 
            rs[0][1] = resWithIndexLocal[i];
            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Results with and without index are not matching for query :" + queryString2[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, isPr);
            }
          }
       }
    };
    client.invoke(executeQueries);

    // Put Non Pdx objects on server execute queries locally 
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        getLogWriter().info("Put Objects locally on server");
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, new Portfolio(i));
         }
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("PR server local indexType:no  size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
          try{
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString2[i]).execute();
            getLogWriter().info("PR server local indexType: no size of resultset: " + rs.size() + " for query: " + queryString2[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString2[i]);
          }catch (Exception e) {
            fail("Failed executing " + queryString2[i], e);
          }

        }
       }
    });
   
    // test for readSerialized flag
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = getCache().getQueryService();
       // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("isPR: " + isPr+ " server local readSerializedTrue: indexType: no index size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    // test for readSerialized flag on client
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService  remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        // Query server1 remotely to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) remoteQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server remote readSerializedTrue: indexType:no index size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }
  
  public void testPartitionRegionCompactRangeIndex() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    final boolean isPr = true;
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
        // Verify the type of index created  
        Index index = null;
          try {
           index = localQueryService.createIndex("statusIndex", "status", regName);
             if(index instanceof PartitionedIndex){
               for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                 if(!(o instanceof CompactRangeIndex) ){
                   fail("CompactRangeIndex Index should have been created instead of "+ index.getClass());
                 }
               }
             }
             else{
               fail("Partitioned index expected");
             }
         } catch (Exception ex) {
           fail("Failed to create index." + ex.getMessage());
         }
       }
    });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        getLogWriter().info("Put PortfolioPdx");
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new PortfolioPdx(i));
         }
      }
    });
    
    //Verify if all the index keys are PdxStrings
    server0.invoke(new CacheSerializableRunnable("Create Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService localQueryService = getCache().getQueryService();
         Index index = localQueryService.getIndex(region, "statusIndex");
            if(index instanceof PartitionedIndex){
              for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                CloseableIterator<IndexStoreEntry> iter = ((CompactRangeIndex) o)
                    .getIndexStorage().iterator(null);
                while (iter.hasNext()) {
                  Object key = iter.next().getDeserializedKey();
                  if (!(key instanceof PdxString)) {
                    fail("All keys of the CompactRangeIndex in the Partitioned index should be PdxStrings and not "
                        + key.getClass());
                  }
                }
              }
            }
            else{
              fail("Partitioned index expected");
            }
      }
    });
 
    // Execute queries from client to server and locally on client
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        for (int i=0; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on remote server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            getLogWriter().info("RR remote indexType:CompactRange size of resultset: "+ rs[0][0].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][0].asList(), queryString[i]);
           
            getLogWriter().info("### Executing Query locally on client:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            getLogWriter().info("isPR: " + isPr+ "  client local indexType:CompactRange size of resultset: "+ rs[0][1].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][1].asList(), queryString[i]);
 
            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, isPr);
            }
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }       
     };

    client.invoke(executeQueries);
    // Put Non Pdx objects on server execute queries locally 
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        getLogWriter().info("Put Objects locally on server");
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, new Portfolio(i));
         }
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server local indexType:Range  size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
       }
    });
    
    // test for readSerialized flag
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("isPR: " + isPr+ " server local readSerializedTrue: indexType:CompactRange size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    // test for readSerialized flag on client
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService  remoteQueryService = (PoolManager.find(poolName)).getQueryService();

        // Query server1 remotely to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) remoteQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server remote readSerializedTrue: indexType: indexType:CompactRange size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }
  
  public void testPartitionRegionRangeIndex() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    final boolean isPr = true;
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
        // Verify the type of index created  
        Index index = null;
          try {
           index = localQueryService.createIndex("secIdIndex", "pos.secId", regName  + " p, p.positions.values pos");
            if(index instanceof PartitionedIndex){
               for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                 if(!(o instanceof RangeIndex) ){
                   fail("Range Index should have been created instead of "+ index.getClass());
                 }
               }
             }
             else{
               fail("Partitioned index expected");
             }
          } catch (Exception ex) {
             fail("Failed to create index." + ex.getMessage());
          }
        }
  });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        getLogWriter().info("Put PortfolioPdx");
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new PortfolioPdx(i));
         }
      }
    });
    
    //Verify if all the index keys are PdxStrings
    server0.invoke(new CacheSerializableRunnable("Create Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService localQueryService = getCache().getQueryService();
        
          Index index = localQueryService.getIndex(region, "secIdIndex");
             if(index instanceof PartitionedIndex){
              for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                for(Object key: ((RangeIndex)o).getValueToEntriesMap().keySet()){
                  if(!(key instanceof PdxString)){
                    fail("All keys of the RangeIndex in the Partitioned index should be PdxStrings and not " + key.getClass());
                  }
                }
              }
            }
            else{
              fail("Partitioned index expected");
            }
      }
    });
 
    // Execute queries from client to server and locally on client
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
 
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        for (int i=0; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on remote server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            getLogWriter().info("RR remote indexType: Range size of resultset: "+ rs[0][0].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][0].asList(), queryString[i]);
                      
            getLogWriter().info("### Executing Query locally on client:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            getLogWriter().info("isPR: " + isPr+ "  client local indexType: Range size of resultset: "+ rs[0][1].size() + " for query: " + queryString[i]);;
            checkForPdxString(rs[0][1].asList(), queryString[i]);
                   
            if(i < orderByQueryIndex){
              // Compare local and remote query results.
              if (!compareResultsOfWithAndWithoutIndex(rs)){
                getLogWriter().info("result0="+rs[0][0].asList());
                getLogWriter().info("result1="+rs[0][1].asList());
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
              }
            }
            else{
              //compare the order of results returned 
              compareResultsOrder(rs, isPr);
            }
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
       }
    };

    client.invoke(executeQueries);

    // Put Non Pdx objects on server execute queries locally 
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);

        getLogWriter().info("Put Objects locally on server");
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, new Portfolio(i));
         }
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server local indexType:Range  size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
       }
    });
    
    // test for readSerialized flag
    server0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = getCache().getQueryService();

        // Query server1 locally to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) localQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("isPR: " + isPr+ " server local readSerializedTrue: indexType: Range size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    // test for readSerialized flag on client
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService  remoteQueryService = (PoolManager.find(poolName)).getQueryService();

        // Query server1 remotely to check if PdxString is not being returned
        for (int i = 0; i < queryString.length; i++) {
          try {
            getLogWriter().info("### Executing Query locally on server:" + queryString[i]);
            SelectResults rs = (SelectResults) remoteQueryService.newQuery(queryString[i]).execute();
            getLogWriter().info("RR server remote readSerializedTrue: indexType: Range size of resultset: " + rs.size() + " for query: " + queryString[i]);
            // The results should not be PdxString
            checkForPdxString(rs.asList(), queryString[i]);
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }

  public void testNullPdxString() throws CacheException {
    final Host host = Host.getHost(0);
    VM server0 = host.getVM(0);
    VM server1 = host.getVM(1);
    VM server2 = host.getVM(2);
    VM client = host.getVM(3);
    final int numberOfEntries = 10;
    final boolean isPr = true;
    // Start server1 and create index
    server0.invoke(new CacheSerializableRunnable("Create Server1") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false,false);
        // create a local query service
        QueryService localQueryService = null;
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        } 
        // Verify the type of index created  
        Index index = null;
          try {
           index = localQueryService.createIndex("statusIndex", "status", regName);
             if(index instanceof PartitionedIndex){
               for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                 if(!(o instanceof CompactRangeIndex) ){
                   fail("CompactRangeIndex Index should have been created instead of "+ index.getClass());
                 }
               }
             }
             else{
               fail("Partitioned index expected");
             }
         } catch (Exception ex) {
           fail("Failed to create index." + ex.getMessage());
         }
       }
    });

    // Start server2
    server1.invoke(new CacheSerializableRunnable("Create Server2") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Start server3
    server2.invoke(new CacheSerializableRunnable("Create Server3") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(isPr,false, false);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = server0.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = server1.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = server2.invokeInt(PdxStringQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(server0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(client, poolName, new String[]{host0}, new int[]{port0, port1, port2}, true);

    // Create client region and put PortfolioPdx objects (PdxInstances)
    client.invoke(new CacheSerializableRunnable("Create client") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
      
        getLogWriter().info("Put PortfolioPdx");
        // Put some PortfolioPdx objects with null Status and secIds 
        for (int i=0; i<numberOfEntries*2; i++) {
          PortfolioPdx portfolioPdx = new PortfolioPdx(i);
          portfolioPdx.status = null; // this will create NULL PdxStrings
          portfolioPdx.positions = new HashMap();
          portfolioPdx.positions.put(null, new PositionPdx(null, PositionPdx.cnt * 1000));
          region.put("key-"+i, portfolioPdx);
         }
        // Put some PortfolioPdx with non null status to reproduce bug#45351
        for (int i=0; i<numberOfEntries; i++) {
          PortfolioPdx portfolioPdx = new PortfolioPdx(i);
          region.put("key-"+i, portfolioPdx);
         }
      }
    });
    
    //Verify if all the index keys are PdxStrings
    server0.invoke(new CacheSerializableRunnable("Create Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService localQueryService = getCache().getQueryService();
         Index index = localQueryService.getIndex(region, "statusIndex");
             if(index instanceof PartitionedIndex){
              for(Object o : ((PartitionedIndex)index).getBucketIndexes()){
                CloseableIterator<IndexStoreEntry> iter = ((CompactRangeIndex) o).getIndexStorage().iterator(null);
                while (iter.hasNext()) {
                  Object key = iter.next().getDeserializedKey();
                  if (!(key instanceof PdxString) && !(key == IndexManager.NULL)) {
                    fail("All keys of the CompactRangeIndex in the Partitioned index should be PdxStrings and not "
                        + key.getClass());
                  }
                }
              }
            }
            else{
              fail("Partitioned index expected");
            }
       }
    });
 
    // Execute queries from client to server and locally on client
    client.invoke( new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];

        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        
        // Querying the fields with null values
        String[] qs = {"SELECT pos.secId FROM " + regName + "  p, p.positions.values pos where p.status = null",
                      "SELECT p.pkid FROM " + regName + "  p, p.positions.values pos where pos.secId = null"};
        
        for(int i = 0; i <2; i++){
          try {
            Query query = remoteQueryService.newQuery(qs[i]);
            SelectResults res = (SelectResults)query.execute();
            getLogWriter().info("PR NULL Pdxstring test size of resultset: "+ res.size() + " for query: " + qs[i]);;
            if(i == 0){
              for(Object o : res){
                if(o != null){
                  fail("Query : " + qs[i] + " should have returned null and not " + o);
                }
              }
            }else{
              checkForPdxString(res.asList(), qs[i]);
            }
          } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
      }
     });
 
    this.closeClient(server2);
    this.closeClient(client);
    this.closeClient(server1);
    this.closeClient(server0);
  }
  
 
    private void compareResultsOrder(SelectResults[][] r, boolean isPr){
    for (int j = 0; j < r.length; j++) {
      Object []r1 = (r[j][0]).toArray();
      Object []r2 = (r[j][1]).toArray();
      if(r1.length != r2.length){
        fail("Size of results not equal: " + r1.length + " vs " + r2.length);
      }
      for (int i = 0, k=0; i < r1.length && k < r2.length; i++,k++) {
        System.out.println("r1: " + r1[i] +  " r2: " + r2[k]);
        if(!r1[i].equals(r2[k])){
          fail("Order not equal: " + r1[i] + " : " +r2[k] + " isPR: " + isPr );
        }
      }
    }
  }
  
  private void checkForPdxString(List results, String query) {
    boolean isGroupByQuery = false;
    for (int i : groupByQueryIndex) {
      if (query.equals(queryString[i]) || query.equals(queryString2[i])) {
        isGroupByQuery = true;
        break;
      }
    }
    for (Object o : results) {
      if (o instanceof Struct) {
        if (!isGroupByQuery) {
          Object o1 = ((Struct) o).getFieldValues()[0];
          Object o2 = ((Struct) o).getFieldValues()[1];
          if (!(o1 instanceof String)) {
            fail("Returned instance of " + o1.getClass()
                + " instead of String for query: " + query);
          }

          if (!(o2 instanceof String)) {
            fail("Returned instance of " + o2.getClass()
                + " instead of String for query: " + query);
          }
        }
      } else {
        if (!isGroupByQuery) {
          if (!(o instanceof String)) {
            fail("Returned instance of " + o.getClass()
                + " instead of String for query: " + query);
          }
        }
      }
    }
  }

  public boolean compareResultsOfWithAndWithoutIndex(SelectResults[][] r ) { 
    boolean ok = true; 
    Set set1 = null; 
    Set set2 = null; 
    Iterator itert1 = null; 
    Iterator itert2 = null; 
    ObjectType type1, type2; 
    outer:  for (int j = 0; j < r.length; j++) { 
      CollectionType collType1 = r[j][0].getCollectionType(); 
      CollectionType collType2 = r[j][1].getCollectionType(); 
      type1 = collType1.getElementType(); 
      type2 = collType2.getElementType(); 
    
        if (r[j][0].size() == r[j][1].size()) { 
        System.out.println("Both SelectResults are of Same Size i.e.  Size= " 
            + r[j][1].size()); 
      } 
      else { 
        System.out.println("FAILED4: SelectResults size is different in both the cases. Size1="  + r[j][0].size() + " Size2 = " + r[j][1].size()); 
        ok = false; 
        break; 
      } 
      set2 = (((SelectResults)r[j][1]).asSet()); 
      set1 = (((SelectResults)r[j][0]).asSet()); 
      boolean pass = true; 
      itert1 = set1.iterator(); 
      while (itert1.hasNext()) { 
        Object p1 = itert1.next(); 
        itert2 = set2.iterator(); 

        boolean exactMatch = false; 
        while (itert2.hasNext()) { 
          Object p2 = itert2.next(); 
          if (p1 instanceof Struct) { 
            Object[] values1 = ((Struct)p1).getFieldValues(); 
            Object[] values2 = ((Struct)p2).getFieldValues(); 
            //test.assertEquals(values1.length, values2.length); 
            if(values1.length != values2.length) { 
              ok = false; 
              break outer; 
            } 
            boolean elementEqual = true; 
            for (int i = 0; i < values1.length; ++i) { 
              elementEqual = elementEqual 
              && ((values1[i] == values2[i]) || values1[i].equals(values2[i])); 
            } 
            exactMatch = elementEqual; 
          } 
          else { 
            exactMatch = (p2 == p1) || p2.equals(p1); 
          } 
          if (exactMatch) { 
            break; 
          } 
        } 
        if (!exactMatch) { 
          System.out.println("FAILED5: Atleast one element in the pair of SelectResults supposedly identical, is not equal "); 
          ok = false; 
          break outer; 
        } 
      } 
    } 
    return ok; 
  } 
 
 /**
   * Test to verify if duplicate results are not being accumulated when
   * PdxString is used in PR query
   * 
   * @throws CacheException
   */
  public void testPRQueryForDuplicates() throws CacheException {
    final String regionName = "exampleRegion";
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final String name = "/" + regionName;
    final String[] qs = { "select distinct pkid from " + name , "select distinct pkid, status from " + name } ;
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regionName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // create client load data and execute queries
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        cf.addPoolServer(getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        // Put Portfolios with 2 different pkids
        for (int set = 1; set <= 2; set++) {
          for (int current = 1; current <= 5; current++) {
            region.put("key-"+set+"_"+current, new PortfolioPdx(set,current));
          }
        }
    
        for(int i = 0 ; i < qs.length; i++){
          SelectResults sr = (SelectResults) cache.getQueryService().newQuery(qs[i]).execute();
          assertEquals("Did not get expected result from query: " + qs[i] + " ",2,sr.size());
        }
        
        return null;
      }
    });
    
    // execute query on server by setting DefaultQuery.setPdxReadSerialized
    // to simulate remote query
    vm0.invoke(new SerializableCallable("Create server") {
      @Override
      public Object call() throws Exception {
        DefaultQuery.setPdxReadSerialized(true);
        try{
	  for(int i = 0 ; i < qs.length; i++){
	    SelectResults sr = (SelectResults) getCache().getQueryService().newQuery(qs[i]).execute();
            assertEquals("Did not get expected result from query: " + qs[i] + " ",2,sr.size());
          }
	} finally{
          DefaultQuery.setPdxReadSerialized(false);
	}
        return null;
      }
    });
    
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }
   
  protected void configAndStartBridgeServer(boolean isPr, boolean isAccessor, boolean asyncIndex) {
    AttributesFactory factory = new AttributesFactory();
    if (isPr) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      //factory.setDataPolicy(DataPolicy.PARTITION);
      if (isAccessor){
        paf.setLocalMaxMemory(0);
      }
      PartitionAttributes prAttr = paf.setTotalNumBuckets(20).setRedundantCopies(0).create();
      factory.setPartitionAttributes(prAttr);
    } else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);      
    }
    if (asyncIndex) {
      factory.setIndexMaintenanceSynchronous(!asyncIndex);
    }
    createRegion(this.regionName, this.rootRegionName, factory.create());
    try {
      startBridgeServer(0, false);
    } catch (Exception ex) {
      fail("While starting CacheServer", ex);
    }
  }
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {

    Cache cache = getCache();
    CacheServer server = cache.addCacheServer();
    server.setPort(port);
    server.start();
    bridgeServerPort = server.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer server =
      (CacheServer) cache.getCacheServers().iterator().next();
    server.stop();
    assertFalse(server.isRunning());
  }

  /* Close Client */
  public void closeClient(VM client) {
    SerializableRunnable closeCache =
      new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        getLogWriter().info("### Close Client. ###");
        try {
          closeCache();
          disconnectFromDS();
        } catch (Exception ex) {
          getLogWriter().info("### Failed to get close client. ###");
        }
      }
    };
    
    client.invoke(closeCache);
  }
  
  public void createPool(VM vm, String poolName, String server, int port, boolean subscriptionEnabled) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, subscriptionEnabled);  
  }

  public void createPool(VM vm, String poolName, String server, int port) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, false);  
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled) {
    createPool(vm, poolName, servers, ports, subscriptionEnabled, 0);    
  }
  
  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled, final int redundancy) {
    vm.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
      public void run2() throws CacheException {
        // Create Cache.
        getLonerSystem();
        addExpectedException("Connection refused");
        getCache();        
        PoolFactory cpf = PoolManager.createFactory();
        cpf.setSubscriptionEnabled(subscriptionEnabled);
        cpf.setSubscriptionRedundancy(redundancy);
        for (int i=0; i < servers.length; i++){
          getLogWriter().info("### Adding to Pool. ### Server : " + servers[i] + " Port : " + ports[i]);
          cpf.addServer(servers[i], ports[i]);
        }
        cpf.create(poolName);
      }
    });   
  }
  private static int getCacheServerPort() {
    return bridgeServerPort;
  }

}
