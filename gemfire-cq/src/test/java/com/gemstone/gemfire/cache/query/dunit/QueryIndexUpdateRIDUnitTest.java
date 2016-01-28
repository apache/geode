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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryUsingPoolDUnitTest;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;

/**
 * This class tests register interest behavior on client at startup given that
 * client has already created a Index on region on which it registers interest.
 * Then client run a query on region in local cache (Not on server) using the
 * Index.
 * 
 * @author shoagarwal
 *
 */
public class QueryIndexUpdateRIDUnitTest extends CacheTestCase{

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;
  
  private String region = "regionA";
  private final int KEYS = 1;
  private final int REGEX = 2;

  private String rootQ = "SELECT ALL * FROM /root p where p.ID > 0";
  private String incompleteQ = "SELECT ALL * FROM /root/"+region+" p where "; //User needs to append where cond.
  
  static public final String KEY = "key-";
  static public final String REGULAR_EXPRESSION = ".*1+?.*";

  private static final String ROOT = "root";
  
  public QueryIndexUpdateRIDUnitTest(String name) {
    super(name);
  }

  /* Test creates 1 Client and 1 Server. Client and Server create same region in their cache.
   * Client creates index and registers interest in region on server and runs a query.
   * Query must fail as registerInterest does not update indexes on client.
   */

  public void testClientIndexUpdateWithRIOnKeys() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("CqRegisterInterestIndexUpdateDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, false);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Init values at server. 
    final int size = 10;
    this.createValues(server, cqDUnitTest.regions[0], size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root/regionA p");

    //Register Interest in all Keys on server 
    this.registerInterestList(client, cqDUnitTest.regions[0], 4, KEYS);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, cqDUnitTest.cqs[0], 4);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * Tests overlap keys between client region and server region to verify the server region values are synched
   * with client region on register interest.
   * @throws Exception
   */
  public void testClientIndexUpdateWithRIOnOverlapKeys() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("CqRegisterInterestIndexUpdateDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, false);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root/regionA p");

    final int size = 10;
    // Init values at client
    this.createValues(client, cqDUnitTest.regions[0], size, 1);

    //wait for index to get updated.
    pause(5 * 1000);
    //this.validateQueryOnIndex(client, incompleteQ+"p.getID() > 0", 10);
    
    this.validateQueryOnIndex(client, incompleteQ+"p.ID > 0", 10);

    // Init values at server.
    this.createValues(server, cqDUnitTest.regions[0], size, 4 /*start index*/);

    //Register Interest in all Keys on server 
    this.registerInterestList(client, cqDUnitTest.regions[0], size, KEYS, 4 /*start index*/);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, incompleteQ+"p.ID < "+ 4*4, 3);
    this.validateQueryOnIndex(client, incompleteQ+"p.ID >= 16", 7);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  public void testClientIndexUpdateWithRIOnRegion() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("CqRegisterInterestIndexUpdateDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, false);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    //Init values at server. 
    final int size = 10;
    this.createValues(server, cqDUnitTest.regions[0], size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root/regionA p");

    //Register Interest in all Keys on server 
    cqDUnitTest.registerInterestListCQ(client, cqDUnitTest.regions[0], size, true);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, cqDUnitTest.cqs[0], size);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  public void testClientIndexUpdateWithRIOnRegEx() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("QueryIndexUpdateRIDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, false);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    //Init values at server. 
    final int size = 10;
    this.createValues(server, cqDUnitTest.regions[0], size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root/regionA p");

    //Register Interest in all Keys on server 
    this.registerInterestList(client, cqDUnitTest.regions[0], 2, REGEX);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, cqDUnitTest.cqs[0], 2);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /**
   * This test tests the RegionClearedException Path in AbsractRegionMap while doing
   * initialImagePut() during registerInterest on client.
   * 
   * @throws Exception
   */
  public void testClientIndexUpdateWithRIOnClearedRegion() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("QueryIndexUpdateRIDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, false);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    //Init values at server. 
    final int size = 1000;
    this.createValues(server, cqDUnitTest.regions[0], size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    cqDUnitTest.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root/regionA p");

    //Create entries on client to clear region later
    this.createValues(client, cqDUnitTest.regions[0], size);

    //Register Interest in all Keys on server 
    //client.invoke(this.getSRRegisterInterestList(cqDUnitTest.regions[0], size, -1 /* Default ALL KEYS */, 0));
    //this.asyncRegisterInterestList(client, cqDUnitTest.regions[0], size, -1 /* Default ALL KEYS */, 0);
    registerInterestList(client, cqDUnitTest.regions[0], size, -1);
    
    //Wait for Index to get updated.
    //pause(500);
    
    //Start clearing region on client asynchronously.
    //this.asyncClearRegion(client, cqDUnitTest.regions[0]);
    client.invoke(this.getSRClearRegion(cqDUnitTest.regions[0]));
    
   //Let register interest finish during region clearance
    //pause(5*1000);
    
    //This query execution should fail as it will run on client index and region has been cleared.
    //Validate query results.
    this.validateQueryOnIndexWithRegion(client, cqDUnitTest.cqs[0], 0, region);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /* 
   * Same tests as above using Partitioned Regions. 
   */

  public void testClientIndexUpdateWithRIOnPRRegion() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("QueryIndexUpdateRIDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, true);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Init values at server. 
    final int size = 10;
    this.createValues(server, ROOT, size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    this.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root p");

    //Register Interest in all Keys on server 
    this.registerInterestList(client, ROOT, size, 0);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, this.rootQ, size);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  public void testClientIndexUpdateWithRIOnPRKeys() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("QueryIndexUpdateRIDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, true);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Init values at server. 
    final int size = 10;
    this.createValues(server, ROOT, size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    this.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root p");

    //Register Interest in all Keys on server 
    this.registerInterestList(client, ROOT, 4, KEYS);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, this.rootQ, 4);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  public void testClientIndexUpdateWithRIOnPRRegEx() throws Exception{
    
    CqQueryUsingPoolDUnitTest cqDUnitTest = new CqQueryUsingPoolDUnitTest("QueryIndexUpdateRIDunitTest");
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);

    this.createServer(server, 0, true);

    final int port = server.invokeInt(QueryIndexUpdateRIDUnitTest.class,
        "getCacheServerPort");
    final String host0 = getServerHostName(server.getHost());

    // Init values at server. 
    final int size = 10;
    this.createValues(server, ROOT, size);

    String poolName = "testClientIndexUpdateWithRegisterInterest";
    cqDUnitTest.createPool(client, poolName, host0, port);
    
    // Create client.
    this.createClient(client, port, host0);
    //Create Index on client
    cqDUnitTest.createFunctionalIndex(client, "IdIndex", "p.ID", "/root p");

    //Register Interest in all Keys on server 
    this.registerInterestList(client, "root", 2, REGEX);

    //Wait for Index to get updated.
    pause(5 * 1000);

    //This query execution should fail as it will run on client index and index are not updated just by registerInterest.
    //Validate query results.
    this.validateQueryOnIndex(client, this.rootQ, 2);

    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server);
  }

  /* Register Interest on data on server */
  public void registerInterestList(VM vm, final String regionName, final int keySize, final int policy) {
    registerInterestList(vm, regionName, keySize, policy, 0);    
  }

  /* Register Interest on data on server */
  public void registerInterestList(VM vm, final String regionName, final int keySize, final int policy, final int start) {
    vm.invoke(new CacheSerializableRunnable("Register InterestList") {
      public void run2() throws CacheException {
        
        // Get Query Service.
        Region region = null;
        try {
          if("root".equals(regionName)){
            region = getRootRegion();
          } else {
            region = getRootRegion().getSubregion(regionName);
          }
          region.getAttributesMutator().setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
        } catch (Exception cqe) {
          AssertionError err = new AssertionError("Failed to get Region.");
          err.initCause(cqe);
          throw err;
        }
        try {
          switch (policy) {
            case REGEX:
              region.registerInterestRegex(REGULAR_EXPRESSION);
              break;
            case KEYS:
              List list = new ArrayList();
              for (int i = start != 0 ? start : 1; i <= keySize; i++) {
                list.add(KEY+i);
              }
              region.registerInterest(list);
              break;
            default:
              region.registerInterest("ALL_KEYS");
          }
        } catch (Exception ex) {
          AssertionError err = new AssertionError("Failed to Register InterestList");
          err.initCause(ex);
          throw err;
        }
      }
    });   
  }

  /* Register Interest on data on server */
  public void asyncRegisterInterestList(VM vm, final String regionName, final int keySize, final int policy, final int start) {
    vm.invokeAsync(new CacheSerializableRunnable("Register InterestList") {
      public void run2() throws CacheException {
        
        // Get Query Service.
        Region region = null;
        try {
          if("root".equals(regionName)){
            region = getRootRegion();
          } else {
            region = getRootRegion().getSubregion(regionName);
          }
          region.getAttributesMutator().setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
        } catch (Exception cqe) {
          AssertionError err = new AssertionError("Failed to get Region.");
          err.initCause(cqe);
          throw err;
        }
        try {
          switch (policy) {
            case REGEX:
              region.registerInterestRegex(REGULAR_EXPRESSION);
              break;
            case KEYS:
              List list = new ArrayList();
              for (int i = start != 0 ? start : 1; i <= keySize; i++) {
                list.add(KEY+i);
              }
              region.registerInterest(list);
              break;
            default:
              region.registerInterest("ALL_KEYS");
          }
        } catch (Exception ex) {
          AssertionError err = new AssertionError("Failed to Register InterestList");
          err.initCause(ex);
          throw err;
        }
      }
    });   
  }

  public void createServer(VM server, final int thePort, final boolean partitioned)
  {
    SerializableRunnable createServer = new CacheSerializableRunnable(
        "Create Cache Server") {
      public void run2() throws CacheException
      {
        getLogWriter().info("### Create Cache Server. ###");
        AttributesFactory factory = new AttributesFactory();
        factory.setMirrorType(MirrorType.KEYS_VALUES);

        // setting the eviction attributes.
        if (partitioned) {
          factory.setDataPolicy(DataPolicy.PARTITION);
          createRootRegion(factory.createRegionAttributes());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          createRegion(region, factory.createRegionAttributes());
        }

        
        
        pause(2000);

        try {
          startBridgeServer(thePort, true);
        }

        catch (Exception ex) {
          fail("While starting CacheServer", ex);
        }
        pause(2000);
        
      }
    };

    server.invoke(createServer);
  }

  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   *
   * @since 6.6
   */
  public void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {
    
    Cache cache = getCache();
    CacheServer bridge = cache.addCacheServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /* Create Init values */
  public void createValues(VM vm, final String regionName, final int size) {
    createValues(vm, regionName, size, 0);
  }


  /**
   * Creates Init Values. start specifies the start index from which key no would start.
   * @param vm
   * @param regionName
   * @param size
   * @param start
   */
  public void createValues(VM vm, final String regionName, final int size, final int start) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1;
        if(!"root".equals(regionName)){
          region1 = getRootRegion().getSubregion(regionName);
        } else {
          region1 = getRootRegion();
        }
        for (int i = ((start != 0) ? start : 1); i <= size; i++) {
//          getLogWriter().info("### puting '"+KEY+i+"' in region " + region1);
          region1.put(KEY+i, new Portfolio((start != 0 ? start : 1) * i, i));
        }
        getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
      }
    });
  }

  /* Returns Cache Server Port */
  static int getCacheServerPort() {
    return bridgeServerPort;
  }

  /* Create Client */
  public void createClient(VM client, final int serverPort, final String serverHost) {
    int[] serverPorts = new int[] {serverPort};
    createClient(client, serverPorts, serverHost, null, null); 
  }

  /* Create Client */
  public void createClient(VM client, final int[] serverPorts, final String serverHost, final String redundancyLevel, 
      final String poolName) {
    SerializableRunnable createQService =
      new CacheSerializableRunnable("Create Client") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create Client. ###");
        //Region region1 = null;
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
        } catch (Exception cqe) {
          fail("Failed to getCQService.", cqe);
        }
        
        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        
        if (poolName != null) {
          regionFactory.setPoolName(poolName);
        } else {
          if (redundancyLevel != null){
            ClientServerTestCase.configureConnectionPool(regionFactory, serverHost, serverPorts, true, Integer.parseInt(redundancyLevel), -1, null);
          } else {
            ClientServerTestCase.configureConnectionPool(regionFactory, serverHost,serverPorts, true, -1, -1, null);
          }
        }
               
          createRootRegion(regionFactory.createRegionAttributes());
          getLogWriter().info("### Successfully Created Root Region on Client");
      }
    };
    
    client.invoke(createQService);
  }

  public void validateQueryOnIndex(VM vm, final String query, final int resultSize) {
    validateQueryOnIndexWithRegion(vm, query, resultSize, null);
  }

  /**
   * Validates a query result with client region values if region is not null, otherwise verifies the size only.
   * @param vm
   * @param query
   * @param resultSize
   * @param region
   */
  public void validateQueryOnIndexWithRegion(VM vm, final String query, final int resultSize, final String region) {
    vm.invoke(new CacheSerializableRunnable("Validate Query") {
      public void run2() throws CacheException {
        getLogWriter().info("### Validating Query. ###");
        QueryService qs = getCache().getQueryService();
        
        Query q = qs.newQuery(query);
        //Set the index observer
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        try {
          Object r = q.execute();
          if(r instanceof SelectResults){
            int rSize = ((SelectResults)r).asSet().size();
            getLogWriter().info("### Result Size is :" + rSize);
            
            if(region == null) {
              assertEquals(resultSize, rSize);
            } else {
              Region reg;
              if(region != null && (reg = getCache().getRegion("/root/"+region)) != null) {
                assertEquals(rSize, reg.size());
                for (Object value : reg.values()) {
                  if(!((SelectResults)r).asSet().contains((Portfolio)value)){
                    fail("Query resultset mismatch with region values for value: " + value);
                  }                
                }
              }
            }
          }
        }
        catch (Exception e) {
          fail("Failed to execute the query.", e);
        }
        if(!observer.isIndexesUsed) {
          fail("Index not used for query");
        }
      }
    });
  }

  public void asyncClearRegion(VM vm, final String regionName){
    vm.invokeAsync(new CacheSerializableRunnable("Destroy entries") {
      public void run2() throws CacheException {
        getLogWriter().info("### Clearing Region. ###");
        Region region1;
        if(!"root".equals(regionName)){
          region1 = getRootRegion().getSubregion(regionName);
        } else {
          region1 = getRootRegion();
        }
        region1.clear();
        getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
      }
    });
  }

  private SerializableRunnable getSRClearRegion(final String regionName) {
    SerializableRunnable sr = new CacheSerializableRunnable("Destroy entries") {
      public void run2() throws CacheException {
        getLogWriter().info("### Clearing Region. ###");
        Region region1;
        if(!"root".equals(regionName)){
          region1 = getRootRegion().getSubregion(regionName);
        } else {
          region1 = getRootRegion();
        }
        region1.clear();
        getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
      }
    };
    return sr;
  }

  private SerializableRunnable getSRRegisterInterestList(final String regionName, 
      final int keySize, final int policy, final int start) {
    SerializableRunnable sr = new CacheSerializableRunnable("Register InterestList") {
      public void run2() throws CacheException {
        
        // Get Query Service.
        Region region = null;
        try {
          if("root".equals(regionName)){
            region = getRootRegion();
          } else {
            region = getRootRegion().getSubregion(regionName);
          }
          region.getAttributesMutator().setCacheListener(new CertifiableTestCacheListener(getLogWriter()));
        } catch (Exception cqe) {
          AssertionError err = new AssertionError("Failed to get Region.");
          err.initCause(cqe);
          throw err;
        }
        try {
          switch (policy) {
            case REGEX:
              region.registerInterestRegex(REGULAR_EXPRESSION);
              break;
            case KEYS:
              List list = new ArrayList();
              for (int i = start != 0 ? start : 1; i <= keySize; i++) {
                list.add(KEY+i);
              }
              region.registerInterest(list);
              break;
            default:
              region.registerInterest("ALL_KEYS");
          }
        } catch (Exception ex) {
          AssertionError err = new AssertionError("Failed to Register InterestList");
          err.initCause(ex);
          throw err;
        }
      }
    };    
   return sr;
  }

  public static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }

}
