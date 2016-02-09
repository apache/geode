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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqAttributesMutator;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CertifiableTestCacheListener;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.InitialImageOperation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * This class tests the ContiunousQuery mechanism in GemFire.
 * It does so by creating a cache server with a cache and a pre-defined region and
 * a data loader. The client creates the same region and attaches the connection pool.
 * 
 *
 * @author anil
 */
public class CqQueryUsingPoolDUnitTest extends CacheTestCase {
  
  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;
  
  protected static int port = 0;
  protected static int port2 = 0;
  
  public static int noTest = -1;
  
  public final String[] regions = new String[] {
      "regionA",
      "regionB"
  };
  
  private final static int CREATE = 0;
  private final static int UPDATE = 1;
  private final static int DESTROY = 2;
  private final static int INVALIDATE = 3;
  private final static int CLOSE = 4;
  private final static int REGION_CLEAR = 5;
  private final static int REGION_INVALIDATE = 6;

  static public final String KEY = "key-";

  static private final String WAIT_PROPERTY = "CqQueryTest.maxWaitTime";

  static private final int WAIT_DEFAULT = (20 * 1000);
  
  public static final long MAX_TIME = Integer.getInteger(WAIT_PROPERTY, 
      WAIT_DEFAULT).intValue();

  public final String[] cqs = new String [] {
      //0 - Test for ">" 
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0",
      
      //1 -  Test for "=" and "and".
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID = 2 and p.status='active'",
      
      //2 -  Test for "<" and "and".
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID < 5 and p.status='active'",
      
      // FOLLOWING CQS ARE NOT TESTED WITH VALUES; THEY ARE USED TO TEST PARSING LOGIC WITHIN CQ.
      //3
      "SELECT * FROM /root/" + regions[0] + " ;",
      //4
      "SELECT ALL * FROM /root/" + regions[0],
      //5
      "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; " +
      "SELECT ALL * FROM /root/" + regions[0] +  " TYPE Portfolio",
      //6
      "import com.gemstone.gemfire.cache.\"query\".data.Portfolio; " +
      "SELECT ALL * FROM /root/" + regions[0] +  " p TYPE Portfolio",
      //7
      "SELECT ALL * FROM /root/" + regions[1] + " p where p.ID < 5 and p.status='active';",
      //8
      "SELECT ALL * FROM /root/" + regions[0] + "  ;",
      //9
      "SELECT ALL * FROM /root/" + regions[0] +" p where p.description = NULL",
      // 10
      "SELECT ALL * FROM /root/" + regions[0] + " p where p.ID > 0 and p.status='active'",
      //11 - Test for "No Alias" 
      "SELECT ALL * FROM /root/" + regions[0] + " where ID > 0",
      
  };
  
  private String[] invalidCQs = new String [] {
      // Test for ">"
      "SELECT ALL * FROM /root/invalidRegion p where p.ID > 0"
  };
  
  public CqQueryUsingPoolDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception {
    super.setUp();
    
    //We're seeing this on the server when the client
    //disconnects.
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("SocketTimeoutException");
    IgnoredException.addIgnoredException("ServerConnectivityException");
    IgnoredException.addIgnoredException("Socket Closed");
    IgnoredException.addIgnoredException("SocketException");
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating connection pools
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    
  }
  
  /* Returns Cache Server Port */
  static int getCacheServerPort() {
    return bridgeServerPort;
  }
  
  /* Create Cache Server */
  public void createServer(VM server)
  {
    createServer(server, 0);
  }

  public void createServer(VM server, final int p)
  {
    createServer(server, p, false);
  }

  public void createServer(VM server, final int thePort, final boolean eviction) {
    MirrorType mirrorType = MirrorType.KEYS_VALUES;
    createServer(server, thePort, eviction, mirrorType);
  }
  
  public void createServer(VM server, final int thePort, final boolean eviction, final MirrorType mirrorType)
  {
    SerializableRunnable createServer = new CacheSerializableRunnable(
        "Create Cache Server") {
      public void run2() throws CacheException
      {
        LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setMirrorType(mirrorType);

        // setting the eviction attributes.
        if (eviction) {
          EvictionAttributes evictAttrs = EvictionAttributes
            .createLRUEntryAttributes(100000, EvictionAction.OVERFLOW_TO_DISK);
          factory.setEvictionAttributes(evictAttrs);
        }

        for (int i = 0; i < regions.length; i++) {
          createRegion(regions[i], factory.createRegionAttributes());
        }
        
        try {
          startBridgeServer(thePort, true);
        }

        catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
        
      }
    };

    server.invoke(createServer);
  }
  
  /**
   * Create a bridge server with partitioned region.
   * @param server VM where to create the bridge server.
   * @param port bridge server port.
   * @param isAccessor if true the under lying partitioned region will not host data on this vm.
   * @param redundantCopies number of redundant copies for the primary bucket.
   */
  public void createServerWithPR(VM server, final int port, final boolean isAccessor, final int redundantCopies)
  {
    SerializableRunnable createServer = new CacheSerializableRunnable(
        "Create Cache Server") {
      public void run2() throws CacheException
      {
          LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
          //AttributesFactory factory = new AttributesFactory();
          //factory.setScope(Scope.DISTRIBUTED_ACK);
          //factory.setMirrorType(MirrorType.KEYS_VALUES);
          
          //int maxMem = 0;
          AttributesFactory attr = new AttributesFactory();
          //attr.setValueConstraint(valueConstraint);
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          if (isAccessor){
            paf.setLocalMaxMemory(0);
          }
          PartitionAttributes prAttr = paf.setTotalNumBuckets(197).setRedundantCopies(redundantCopies).create();
          attr.setPartitionAttributes(prAttr);
          
          assertFalse(getSystem().isLoner());
          //assertTrue(getSystem().getDistributionManager().getOtherDistributionManagerIds().size() > 0);
          for (int i = 0; i < regions.length; i++) {
            Region r = createRegion(regions[i], attr.create());
            LogWriterUtils.getLogWriter().info("Server created the region: "+r);
          }
          try {
            startBridgeServer(port, true);
          }
          catch (Exception ex) {
            Assert.fail("While starting CacheServer", ex);
          }
       
      }
    };

    server.invoke(createServer);
  }
  
  
  /* Close Cache Server */
  public void closeServer(VM server) {
    server.invoke(new SerializableRunnable("Close CacheServer") {
      public void run() {
        LogWriterUtils.getLogWriter().info("### Close CacheServer. ###");
        stopBridgeServer(getCache());
      }
    });
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
        LogWriterUtils.getLogWriter().info("### Create Client. ###");
        //Region region1 = null;
        // Initialize CQ Service.
        try {
          getCache().getQueryService();
          IgnoredException.addIgnoredException("java.net.ConnectException");
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
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
        for (int i=0; i < regions.length; i++) {        
          createRegion(regions[i], regionFactory.createRegionAttributes());
          LogWriterUtils.getLogWriter().info("### Successfully Created Region on Client :" + regions[i]);
          //region1.getAttributesMutator().setCacheListener(new CqListener());
        }
      }
    };
    
    client.invoke(createQService);
  }


  /* Close Client */
  public void closeClient(VM client) {
    SerializableRunnable closeCQService =
      new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Close Client. ###");
        try {
          ((DefaultQueryService)getCache().getQueryService()).closeCqService();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("### Failed to get CqService during ClientClose() ###");
        }
        
      }
    };
    
    client.invoke(closeCQService);
    Wait.pause(1000);
  }

  public void createFunctionalIndex(VM vm, final String indexName, final String indexedExpression, final String fromClause) {
    vm.invoke(new CacheSerializableRunnable("Create Functional Index") {
      public void run2() throws CacheException {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
        }catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("### Failed to get CqService during ClientClose() ###");
        }
        try {
          qs.createIndex(indexName, IndexType.FUNCTIONAL, indexedExpression, fromClause);
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("### Failed to create Index :" + indexName);
        }
      }
    });
  }

  /* Create/Init values */
  public void createValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.put(KEY+i, new Portfolio(i, i));
        }
        LogWriterUtils.getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
      }
    });
  }

  /* Create/Init values */
  public void createValuesWithTime(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          Portfolio portfolio = new Portfolio(i);
          portfolio.createTime = System.currentTimeMillis();
          region1.put(KEY+i, portfolio);
        }
        LogWriterUtils.getLogWriter().info("### Number of Entries in Region :" + region1.keys().size());
      }
    });
  }

  /* delete values */
  public void deleteValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.destroy(KEY+i);
        }
        LogWriterUtils.getLogWriter().info("### Number of Entries In Region after Delete :" + region1.keys().size());
      }
      
    });
  }
  
  /**
   * support for invalidating values.
   */  
  public void invalidateValues(VM vm, final String regionName, final int size) {
    vm.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regionName);
        for (int i = 1; i <= size; i++) {
          region1.invalidate(KEY+i);
        }
        LogWriterUtils.getLogWriter().info("### Number of Entries In Region after Delete :" + region1.keys().size());
      }
      
    });
  }

  public void createPool(VM vm, String poolName, String server, int port) {
    createPool(vm, poolName, new String[]{server}, new int[]{port});  
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports) {
    createPool(vm, poolName, servers, ports, null);
  }
  
  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports, final String redundancyLevel) {
    vm.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
      public void run2() throws CacheException {
        // Create Cache.
        getCache();
        IgnoredException.addIgnoredException("java.net.ConnectException");
        
        PoolFactory cpf = PoolManager.createFactory();
        cpf.setSubscriptionEnabled(true);

        if (redundancyLevel != null){
          int redundancy = Integer.parseInt(redundancyLevel);
          cpf.setSubscriptionRedundancy(redundancy);
        } 
        
        for (int i=0; i < servers.length; i++){
          LogWriterUtils.getLogWriter().info("### Adding to Pool. ### Server : " + servers[i] + " Port : " + ports[i]);
          cpf.addServer(servers[i], ports[i]);
        }
        
        cpf.create(poolName);
      }
    });   
  }

  /* Register CQs */
  public void createCQ(VM vm, final String poolName, final String cqName, final String queryStr) {
    vm.invoke(new CacheSerializableRunnable("Create CQ :" + cqName) {
      public void run2() throws CacheException {
        //pause(60 * 1000);
        //getLogWriter().info("### DEBUG CREATE CQ START ####");
        //pause(20 * 1000);
        
        LogWriterUtils.getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        CqAttributesFactory cqf = new CqAttributesFactory();
        CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
        ((CqQueryTestListener)cqListeners[0]).cqName = cqName;
        
        cqf.initCqListeners(cqListeners);
        CqAttributes cqa = cqf.create();
        
        // Create CQ.
        try {
          CqQuery cq1 = qService.newCq(cqName, queryStr, cqa);
          assertTrue("newCq() state mismatch", cq1.getState().isStopped());
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
          err.initCause(ex);
          LogWriterUtils.getLogWriter().info("QueryService is :" + qService, err);
          throw err;
        }
      }
    });   
  }

  // REMOVE..........
  public void createCQ(VM vm, final String cqName, final String queryStr) {
    vm.invoke(new CacheSerializableRunnable("Create CQ :" + cqName) {
      public void run2() throws CacheException {
        //pause(60 * 1000);
        //getLogWriter().info("### DEBUG CREATE CQ START ####");
        //pause(20 * 1000);
        
        LogWriterUtils.getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        CqAttributesFactory cqf = new CqAttributesFactory();
        CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
        ((CqQueryTestListener)cqListeners[0]).cqName = cqName;
        
        cqf.initCqListeners(cqListeners);
        CqAttributes cqa = cqf.create();
        
        // Create CQ.
        try {
          CqQuery cq1 = qService.newCq(cqName, queryStr, cqa);
          assertTrue("newCq() state mismatch", cq1.getState().isStopped());
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
          err.initCause(ex);
          LogWriterUtils.getLogWriter().info("QueryService is :" + qService, err);
          throw err;
        }
      }
    });   
  }

  /* Register CQs  with no name, execute, and close*/
  public void createAndExecCQNoName(VM vm, final String poolName,  final String queryStr) {
    vm.invoke(new CacheSerializableRunnable("Create CQ with no name:" ) {
      public void run2() throws CacheException {
        //pause(60 * 1000);
        LogWriterUtils.getLogWriter().info("### DEBUG CREATE CQ START ####");
        //pause(20 * 1000);
        
        LogWriterUtils.getLogWriter().info("### Create CQ with no name. ###");
        // Get CQ Service.
        QueryService qService = null;
        CqQuery cq1 = null;
        String cqName = null;

        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        SelectResults cqResults = null;
        for (int i = 0; i < 20; ++i) {
          // Create CQ Attributes.
          CqAttributesFactory cqf = new CqAttributesFactory();
          CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
          
          cqf.initCqListeners(cqListeners);
          CqAttributes cqa = cqf.create();
          
          // Create CQ with no name and execute with initial results.
          try {
            cq1 = qService.newCq(queryStr, cqa);
            ((CqQueryTestListener)cqListeners[0]).cqName = cq1.getName();
          } catch (Exception ex){
            LogWriterUtils.getLogWriter().info("CQService is :" + qService);
            Assert.fail("Failed to create CQ with no name" +  " . ", ex);
          }
          
          if (cq1 == null) {
            LogWriterUtils.getLogWriter().info("Failed to get CqQuery object for CQ with no name.");
          }
          else {
            cqName = cq1.getName();
            LogWriterUtils.getLogWriter().info("Created CQ with no name, generated CQ name: " + cqName + " CQ state:" + cq1.getState());
            assertTrue("Create CQ with no name illegal state", cq1.getState().isStopped());
          }
          if ( i%2 == 0) {
            try {
              cqResults = cq1.executeWithInitialResults();
            } catch (Exception ex){
              LogWriterUtils.getLogWriter().info("CqService is :" + qService);
              Assert.fail("Failed to execute CQ with initial results, cq name: " 
                  + cqName + " . ",  ex);
            }
            LogWriterUtils.getLogWriter().info("initial result size = " + cqResults.size());
            LogWriterUtils.getLogWriter().info("CQ state after execute with initial results = " + cq1.getState());
            assertTrue("executeWithInitialResults() state mismatch", cq1.getState().isRunning());
          }
          else {
            try {
              cq1.execute();
            } catch (Exception ex){
              LogWriterUtils.getLogWriter().info("CQService is :" + qService);
              Assert.fail("Failed to execute CQ " + cqName + " . ", ex);
            }
            LogWriterUtils.getLogWriter().info("CQ state after execute = " + cq1.getState());
            assertTrue("execute() state mismatch", cq1.getState().isRunning());
          }
          
          //Close the CQ
          try {
            cq1.close();
          } catch (Exception ex){
            LogWriterUtils.getLogWriter().info("CqService is :" + qService);
            Assert.fail("Failed to close CQ " + cqName + " . ", ex);
          }
          assertTrue("closeCq() state mismatch", cq1.getState().isClosed());
        }
      }
    });   
  }
  
  public void executeCQ(VM vm, final String cqName, final boolean initialResults,
      String expectedErr) {
    executeCQ(vm, cqName, initialResults, noTest, null, expectedErr);
  }    
  
  /**
   * Execute/register CQ as running.
   * @param initialResults true if initialResults are requested
   * @param expectedResultsSize if >= 0, validate results against this size
   * @param expectedErr if not null, an error we expect
   */    
  public void executeCQ(VM vm, final String cqName,
      final boolean initialResults,
      final int expectedResultsSize,
      final String[] expectedKeys,
      final String expectedErr) {
    vm.invoke(new CacheSerializableRunnable("Execute CQ :" + cqName) {

      private void work() throws CacheException {
      //pause(60 * 1000);
      LogWriterUtils.getLogWriter().info("### DEBUG EXECUTE CQ START ####");
      //pause(20 * 1000);
      
      // Get CQ Service.
      QueryService cqService = null;
      CqQuery cq1 = null;
//    try {
      cqService = getCache().getQueryService();
//    } catch (Exception cqe) {
//    getLogWriter().error(cqe);
//    AssertionError err = new AssertionError("Failed to get QueryService" + cqName);
//    err.initCause(ex);
//    throw err;
//    fail("Failed to getCQService.");
//    }
      
      // Get CqQuery object.
      try {
        cq1 = cqService.getCq(cqName);
        if (cq1 == null) {
          LogWriterUtils.getLogWriter().info("Failed to get CqQuery object for CQ name: " + cqName);
          fail("Failed to get CQ " + cqName);
        }
        else {
          LogWriterUtils.getLogWriter().info("Obtained CQ, CQ name: " + cq1.getName());
          assertTrue("newCq() state mismatch", cq1.getState().isStopped());
        }
      } catch (Exception ex){
        LogWriterUtils.getLogWriter().info("CqService is :" + cqService);
        LogWriterUtils.getLogWriter().error(ex);
        AssertionError err = new AssertionError("Failed to execute  CQ " + cqName);
        err.initCause(ex);
        throw err;
      }
      
      if (initialResults) {
        SelectResults cqResults = null;
        
        try {
          cqResults = cq1.executeWithInitialResults();
        } catch (Exception ex){
          LogWriterUtils.getLogWriter().info("CqService is :" + cqService);
          ex.printStackTrace();
          AssertionError err = new AssertionError("Failed to execute  CQ " + cqName);
          err.initCause(ex);
          throw err;
        }
        LogWriterUtils.getLogWriter().info("initial result size = " + cqResults.size());
        assertTrue("executeWithInitialResults() state mismatch", cq1.getState().isRunning());
        if (expectedResultsSize >= 0) {
          assertEquals("Unexpected results size for CQ: " + cqName + 
              " CQ Query :" + cq1.getQueryString(), 
              expectedResultsSize, cqResults.size());
        }
        
        if (expectedKeys != null) {
          HashSet resultKeys = new HashSet();
          for (Object o : cqResults.asList()) {
            Struct s = (Struct)o;
            resultKeys.add(s.get("key"));
          }
          for (int i =0; i < expectedKeys.length; i++){
            assertTrue("Expected key :" +  expectedKeys[i] + 
                " Not found in CqResults for CQ: " + cqName + 
              " CQ Query :" + cq1.getQueryString() + 
              " Keys in CqResults :" + resultKeys,
              resultKeys.contains(expectedKeys[i]));
          }
        }
      } 
      else {
        try {
          cq1.execute();
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to execute  CQ " + cqName);
          err.initCause(ex);
          if (expectedErr == null) {
            LogWriterUtils.getLogWriter().info("CqService is :" + cqService, err);
          }
          throw err;
        }
        assertTrue("execute() state mismatch", cq1.getState().isRunning());
      }
    }
      
      public void run2() throws CacheException {
        if (expectedErr != null) {
          getCache().getLogger().info("<ExpectedException action=add>"
                + expectedErr + "</ExpectedException>");
        }
        try {
          work();
        } 
        finally {
          if (expectedErr != null) {
            getCache().getLogger().info("<ExpectedException action=remove>"
              + expectedErr + "</ExpectedException>");
          }
        }
      }
      });   
  }
  
  /* Stop/pause CQ */
  public void stopCQ(VM vm, final String cqName) throws Exception {
    vm.invoke(new CacheSerializableRunnable("Stop CQ :" + cqName) {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Stop CQ. ###" + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        // Stop CQ.
        CqQuery cq1 = null;
        try {
          cq1 = cqService.getCq(cqName);
          cq1.stop();
        } catch (Exception ex){
          Assert.fail("Failed to stop CQ " + cqName + " . ", ex);
        }
        assertTrue("Stop CQ state mismatch", cq1.getState().isStopped());
      }
    });
  }
  
  // Stop and execute CQ repeatedly
  /* Stop/pause CQ */
  private void stopExecCQ(VM vm, final String cqName, final int count) throws Exception {
    vm.invoke(new CacheSerializableRunnable("Stop CQ :" + cqName) {
      public void run2() throws CacheException {
        CqQuery cq1 = null;
        LogWriterUtils.getLogWriter().info("### Stop and Exec CQ. ###" + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCqService.", cqe);
        }
        
        // Get CQ.
        try {
          cq1 = cqService.getCq(cqName);
        } catch (Exception ex){
          Assert.fail("Failed to get CQ " + cqName + " . ", ex);
        }
        
        for (int i = 0; i < count; ++i) {
          // Stop CQ.
          try {
            cq1.stop();
          } catch (Exception ex) {
            Assert.fail("Count = " + i + "Failed to stop CQ " + cqName + " . ", ex);
          }
          assertTrue("Stop CQ state mismatch, count = " + i, cq1.getState().isStopped());
          LogWriterUtils.getLogWriter().info("After stop in Stop and Execute loop, ran successfully, loop count: " + i);
          LogWriterUtils.getLogWriter().info("CQ state: " + cq1.getState());
          
          // Re-execute CQ
          try {
            cq1.execute();
          } catch (Exception ex) {
            Assert.fail("Count = " + i + "Failed to execute CQ " + cqName + " . ", ex);
          }
          assertTrue("Execute CQ state mismatch, count = " + i, cq1.getState().isRunning());
          LogWriterUtils.getLogWriter().info("After execute in Stop and Execute loop, ran successfully, loop count: " + i);
          LogWriterUtils.getLogWriter().info("CQ state: " + cq1.getState());
        }
      }
    });
  }
  
  
  /* UnRegister CQs */
  public void closeCQ(VM vm, final String cqName) throws Exception {
    vm.invoke(new CacheSerializableRunnable("Close CQ :" + cqName) {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Close CQ. ###" + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCqService.", cqe);
        }
        
        // Close CQ.
        CqQuery cq1 = null;
        try {
          cq1 = cqService.getCq(cqName);
          cq1.close();
        } catch (Exception ex) {
          Assert.fail("Failed to close CQ " + cqName + " . ", ex);
        }
        assertTrue("Close CQ state mismatch", cq1.getState().isClosed());
      }
    });
  }
  
  /* Register CQs */
  public void registerInterestListCQ(VM vm, final String regionName, final int keySize, final boolean all) {
    vm.invoke(new CacheSerializableRunnable("Register InterestList and CQ") {
      public void run2() throws CacheException {
        
        // Get CQ Service.
        Region region = null;
        try {
          region = getRootRegion().getSubregion(regionName);
          region.getAttributesMutator().setCacheListener(new CertifiableTestCacheListener(LogWriterUtils.getLogWriter()));
        } catch (Exception cqe) {
          AssertionError err = new AssertionError("Failed to get Region.");
          err.initCause(cqe);
          throw err;

        }
        
        try {
          if (all) {
            region.registerInterest("ALL_KEYS");
          } else {
            List list = new ArrayList();
            for (int i = 1; i <= keySize; i++) {
              list.add(KEY+i);
            }
            region.registerInterest(list);
          }
        } catch (Exception ex) {
          AssertionError err = new AssertionError("Failed to Register InterestList");
          err.initCause(ex);
          throw err;
        }
      }
    });   
  }
  
  /* Validate CQ Count */
  public void validateCQCount(VM vm, final int cqCnt) throws Exception {
    vm.invoke(new CacheSerializableRunnable("validate cq count") {
      public void run2() throws CacheException {
        // Get CQ Service.
        
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        int numCqs = 0;
        try {
          numCqs = cqService.getCqs().length;
        } catch (Exception ex) {
          Assert.fail ("Failed to get the CQ Count.", ex);
        }
        assertEquals("Number of cqs mismatch.", cqCnt, numCqs);
      }
    });
  }
  
  
  /** 
   * Throws AssertionError if the CQ can be found or if any other
   * error occurs
   */
  private void failIfCQExists(VM vm, final String cqName) {
    vm.invoke(new CacheSerializableRunnable("Fail if CQ exists") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Fail if CQ Exists. ### " + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {          
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery != null) {
          fail("Unexpectedly found CqQuery for CQ : " + cqName);
        }
      }
    });
  }
  
  private void validateCQError(VM vm, final String cqName,
      final int numError) {
    vm.invoke(new CacheSerializableRunnable("Validate CQs") {
      public void run2() throws CacheException {
        
        LogWriterUtils.getLogWriter().info("### Validating CQ. ### " + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {          
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery == null) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }
        
        CqAttributes cqAttr = cQuery.getCqAttributes();
        CqListener cqListener = cqAttr.getCqListener();
        CqQueryTestListener listener = (CqQueryTestListener) cqListener;
        listener.printInfo(false);
        
        // Check for totalEvents count.
        if (numError != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Total Event Count mismatch", numError, listener.getErrorEventCount());
        }
      }
    });
  }
  
  public void validateCQ(VM vm, final String cqName,
      final int resultSize,
      final int creates,
      final int updates,
      final int deletes) {
    validateCQ(vm, cqName, resultSize, creates, updates, deletes,
        noTest, noTest, noTest, noTest);
  }
  
  public void validateCQ(VM vm, final String cqName,
      final int resultSize,
      final int creates,
      final int updates,
      final int deletes,
      final int queryInserts,
      final int queryUpdates,
      final int queryDeletes,
      final int totalEvents) {
    vm.invoke(new CacheSerializableRunnable("Validate CQs") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Validating CQ. ### " + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {          
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery == null) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }
        
        CqAttributes cqAttr = cQuery.getCqAttributes();
        CqListener cqListeners[] = cqAttr.getCqListeners();
        CqQueryTestListener listener = (CqQueryTestListener) cqListeners[0];
        listener.printInfo(false);
        
        // Check for totalEvents count.
        if (totalEvents != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Total Event Count mismatch", totalEvents, listener.getTotalEventCount());
        }
        
        if (resultSize != noTest) {
          //SelectResults results = cQuery.getCqResults();
          //getLogWriter().info("### CQ Result Size is :" + results.size());
          // Result size validation.
          // Since ResultSet is not maintained for this release.
          // Instead of resultSize its been validated with total number of events.
          fail("test for event counts instead of results size");
//        assertEquals("Result Size mismatch", resultSize, listener.getTotalEventCount());
        }
        
        // Check for create count.
        if (creates != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Create Event mismatch", creates, listener.getCreateEventCount());
        }
        
        // Check for update count.
        if (updates != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Update Event mismatch", updates, listener.getUpdateEventCount());
        }
        
        // Check for delete count.
        if (deletes != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Delete Event mismatch", deletes, listener.getDeleteEventCount());
        }
        
        // Check for queryInsert count.
        if (queryInserts != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Query Insert Event mismatch", queryInserts, listener.getQueryInsertEventCount());
        }
        
        // Check for queryUpdate count.
        if (queryUpdates != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Query Update Event mismatch", queryUpdates, listener.getQueryUpdateEventCount());
        }
        
        // Check for queryDelete count.
        if (queryDeletes != noTest) {
          // Result size validation.
          listener.printInfo(true);
          assertEquals("Query Delete Event mismatch", queryDeletes, listener.getQueryDeleteEventCount());
        }        
      }
    });
  }
  
  public void waitForCreated(VM vm, final String cqName, final String key){
    waitForEvent(vm, 0, cqName, key);
  }
  
  public void waitForUpdated(VM vm, final String cqName, final String key){
    waitForEvent(vm, 1, cqName, key);
  }
  
  public void waitForDestroyed(VM vm, final String cqName, final String key){
    waitForEvent(vm, 2, cqName, key);
  }
  
  public void waitForInvalidated(VM vm, final String cqName, final String key){
    waitForEvent(vm, 3, cqName, key);
  }
  
  public void waitForClose(VM vm, final String cqName){
    waitForEvent(vm, 4, cqName, null);
  }
  
  public void waitForRegionClear(VM vm, final String cqName){
    waitForEvent(vm, 5, cqName, null);
  }

  public void waitForRegionInvalidate(VM vm, final String cqName){
    waitForEvent(vm, 6, cqName, null);
  }

  private void waitForEvent(VM vm, final int event, final String cqName, final String key) {
    vm.invoke(new CacheSerializableRunnable("validate cq count") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery == null) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }
        
        CqAttributes cqAttr = cQuery.getCqAttributes();
        CqListener[] cqListener = cqAttr.getCqListeners();
        CqQueryTestListener listener = (CqQueryTestListener) cqListener[0];
        
        switch (event) {
          case CREATE :
            listener.waitForCreated(key);
            break;
            
          case UPDATE :
            listener.waitForUpdated(key);
            break;
            
          case DESTROY :
            listener.waitForDestroyed(key);
            break;            
            
          case INVALIDATE :
            listener.waitForInvalidated(key);
            break;            
            
          case CLOSE :
            listener.waitForClose();
            break;            

          case REGION_CLEAR :
            listener.waitForRegionClear();
            break;            

          case REGION_INVALIDATE :
            listener.waitForRegionInvalidate();
            break;            

        }
      }
    });
  }
  
  /**
   * Waits till the CQ state is same as the expected.
   * Waits for max time, if the CQ state is not same as expected 
   * throws exception.
   */
  public void waitForCqState(VM vm, final String cqName, final int state) {
    vm.invoke(new CacheSerializableRunnable("Wait For cq State") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }

        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery == null) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }

        // Get CQ State.
        CqStateImpl cqState = (CqStateImpl)cQuery.getState();
        // Wait max time, till the CQ state is as expected.
        final long start = System.currentTimeMillis();
        while (cqState.getState() != state) {
          junit.framework.Assert.assertTrue("Waited over " + MAX_TIME 
              + "ms for Cq State to be changed to " + state
              + "; consider raising " + WAIT_PROPERTY, 
              (System.currentTimeMillis() - start) < MAX_TIME);
          Wait.pause(100);
        }
      }
    });
  }

  public void clearCQListenerEvents(VM vm, final String cqName) {
    vm.invoke(new CacheSerializableRunnable("validate cq count") {
      public void run2() throws CacheException {
        // Get CQ Service.
        
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery == null) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }
        
        CqAttributes cqAttr = cQuery.getCqAttributes();
        CqListener cqListener = cqAttr.getCqListener();
        CqQueryTestListener listener = (CqQueryTestListener) cqListener;
        listener.getEventHistory();        
      }
    });
  }
  
  public void validateQuery(VM vm, final String query, final int resultSize) {
    vm.invoke(new CacheSerializableRunnable("Validate Query") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Validating Query. ###");
        QueryService qs = getCache().getQueryService();
        
        Query q = qs.newQuery(query);
        try {
          Object r = q.execute();
          if(r instanceof Collection){
            int rSize = ((Collection)r).size();
            LogWriterUtils.getLogWriter().info("### Result Size is :" + rSize);
            assertEquals(rSize, resultSize);
          }
        }
        catch (Exception e) {
          Assert.fail("Failed to execute the query.", e);
        }
      }
    });
  }
  
  private Properties getConnectionProps(String[] hosts, int[] ports, Properties newProps) {
    
    Properties props = new Properties();
    String endPoints = "";
    String host = hosts[0];
    for (int i=0; i < ports.length; i++){
      if (hosts.length > 1)
      {
        host = hosts[i];
      }
      endPoints = endPoints + "server" + i + "=" + host + ":" + ports[i];
      if (ports.length > (i+1))
      {
        endPoints = endPoints + ",";
      }
    }
    
    props.setProperty("endpoints", endPoints);
    props.setProperty("retryAttempts", "1");
    //props.setProperty("establishCallbackConnection", "true");
    //props.setProperty("LBPolicy", "Sticky");
    //props.setProperty("readTimeout", "120000");
    
    // Add other property elements.
    if (newProps != null) {
      Enumeration e = newProps.keys();
      while(e.hasMoreElements()) {
        String key = (String)e.nextElement();
        props.setProperty(key, newProps.getProperty(key));
      }
    }
    return props;
  }
  
  
  // Exercise CQ attributes mutator functions
  private void mutateCQAttributes(VM vm, final String cqName, final int mutator_function) throws Exception {
    vm.invoke(new CacheSerializableRunnable("Stop CQ :" + cqName) {
      public void run2() throws CacheException {
        CqQuery cq1 = null;
        LogWriterUtils.getLogWriter().info("### CQ attributes mutator for ###" + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        // Get CQ.
        try {
          cq1 = cqService.getCq(cqName);
        } catch (Exception ex) {
          Assert.fail("Failed to get CQ " + cqName + " . ", ex);
        }
        CqAttributesMutator cqAttrMutator = cq1.getCqAttributesMutator();
        CqAttributes cqAttr = cq1.getCqAttributes();
        CqListener cqListeners[];
        switch (mutator_function) {
          case CREATE:
            // Reinitialize with 2 CQ Listeners
            CqListener cqListenersArray[] = {new CqQueryTestListener(getCache().getLogger()), 
              new CqQueryTestListener(getCache().getLogger())};
            cqAttrMutator.initCqListeners(cqListenersArray);
            cqListeners = cqAttr.getCqListeners();
            assertEquals("CqListener count mismatch", cqListeners.length, 2);
            break;
            
          case UPDATE:
            // Add 2 new CQ Listeners
            CqListener newListener1 = new CqQueryTestListener(getCache().getLogger());
            CqListener newListener2 = new CqQueryTestListener(getCache().getLogger());
            cqAttrMutator.addCqListener(newListener1);
            cqAttrMutator.addCqListener(newListener2);
            
            cqListeners = cqAttr.getCqListeners();
            assertEquals("CqListener count mismatch", cqListeners.length, 3);
            break;
            
          case DESTROY:
            cqListeners = cqAttr.getCqListeners();
            cqAttrMutator.removeCqListener(cqListeners[0]);
            cqListeners = cqAttr.getCqListeners();
            assertEquals("CqListener count mismatch", cqListeners.length, 2);
            
            // Remove a listener and validate
            cqAttrMutator.removeCqListener(cqListeners[0]);
            cqListeners = cqAttr.getCqListeners();
            assertEquals("CqListener count mismatch", cqListeners.length, 1);
            break;
        }
      }
    });
  }
  
  
  
  
  /**
   * Test for InterestList and CQ registered from same clients.
   * @throws Exception
   */
  public void testInterestListAndCQs() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testInterestListAndCQs";
    createPool(client, poolName, host0, thePort);
    
    createClient(client, thePort, host0);
    
    /* Create CQs. */
    createCQ(client, poolName, "testInterestListAndCQs_0", cqs[0]); 
    validateCQCount(client, 1);
    
    /* Init values at server. */
    final int size = 10;
    
    executeCQ(client, "testInterestListAndCQs_0", false, null); 
    registerInterestListCQ(client, regions[0], size, false);
    
    createValues(server, regions[0], size);
    // Wait for client to Synch.
    
    for (int i=1; i <=10; i++){
      waitForCreated(client, "testInterestListAndCQs_0", KEY + i);
    }
    Wait.pause(5 * 1000);
    
    // validate CQs.
    validateCQ(client, "testInterestListAndCQs_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ noTest,
        /* deletes; */ noTest,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    // Validate InterestList.
    // CREATE
    client.invoke(new CacheSerializableRunnable("validate updates") {
      public void run2() throws CacheException {
        final Region region = getRootRegion().getSubregion(regions[0]);
        assertNotNull(region);
        
//        Set keys = region.entrySet();
//        assertEquals("Mismatch, number of keys in local region is not equal to the interest list size", 
//            size, keys.size());
        // TODO does this WaitCriterion actually help?
        WaitCriterion wc = new WaitCriterion() {
          String excuse;
          public boolean done() {
            int sz = region.entrySet().size();
            if (sz == size) {
              return true;
            }
            excuse = "Mismatch, number of keys (" + sz + 
                ") in local region is not equal to the interest list size (" +
                size + ")";
            return false;
          }
          public String description() {
            return excuse;
          }
        };
        Wait.waitForCriterion(wc, 60 * 1000, 1000, true);
        
        CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForCreated(KEY+i);
          assertNotNull(region.getEntry(KEY+i));
        }
      }
    });
    
    // UPDATE
    createValues(server, regions[0], size);
    // Wait for client to Synch.
    for (int i=1; i <=10; i++){
      waitForUpdated(client, "testInterestListAndCQs_0", KEY + i);
    }
    
    
    client.invoke(new CacheSerializableRunnable("validate updates") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertNotNull(region);
        
        Set keys = region.entrySet();
        assertEquals("Mismatch, number of keys in local region is not equal to the interest list size", 
            size, keys.size());
        
        CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForUpdated(KEY+i);
          assertNotNull(region.getEntry(KEY+i));
        }
      }
    });
    
    // INVALIDATE
    server.invoke(new CacheSerializableRunnable("Invalidate values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = 1; i <= size; i++) {
          region1.invalidate(KEY+i);
        }
      }
    });
    
    
    waitForInvalidated(client, "testInterestListAndCQs_0", KEY + 10);
    
    
    client.invoke(new CacheSerializableRunnable("validate invalidates") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertNotNull(region);
        
        Set keys = region.entrySet();
        assertEquals("Mismatch, number of keys in local region is not equal to the interest list size", 
            size, keys.size());
        
        CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForInvalidated(KEY+i);
          assertNotNull(region.getEntry(KEY+i));
        }
      }
    });
    
    validateCQ(client, "testInterestListAndCQs_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ noTest,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ size,
        /* totalEvents: */ size * 3);
    
    // DESTROY - this should not have any effect on CQ, as the events are
    // already destroyed from invalidate events.
    server.invoke(new CacheSerializableRunnable("Invalidate values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = 1; i <= size; i++) {
          region1.destroy(KEY+i);
        }
      }
    });

    // Wait for destroyed.
    client.invoke(new CacheSerializableRunnable("validate destroys") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regions[0]);
        assertNotNull(region);
                
        CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForDestroyed(KEY+i);
        }
      }
    });

    validateCQ(client, "testInterestListAndCQs_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ size,
        /* deletes; */ noTest,
        /* queryInserts: */ size,
        /* queryUpdates: */ size,
        /* queryDeletes: */ size,
        /* totalEvents: */ size * 3);

    closeClient(client);
    closeServer(server);
  }
  
  
  /**
   * Test for CQ register and UnRegister.
   * @throws Exception
   */
  public void testCQStopExecute() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testCQStopExecute";
    createPool(client, poolName, host0, thePort);
    
    //createClient(client, thePort, host0);
    
    /* Create CQs. */
    createCQ(client, poolName, "testCQStopExecute_0", cqs[0]); 
    validateCQCount(client, 1);
    
    executeCQ(client, "testCQStopExecute_0", false, null); 
    
    /* Init values at server. */
    int size = 10;
    createValues(server, regions[0], size);
    // Wait for client to Synch.
    
    waitForCreated(client, "testCQStopExecute_0", KEY+size);
    
    
    // Check if Client and Server in sync.
    //validateServerClientRegionEntries(server, client, regions[0]);
    validateQuery(server, cqs[0], 10);    
    // validate CQs.
    //validateCQ(client, "testCQStopExecute_0", size, noTest, noTest, noTest);
    validateCQ(client, "testCQStopExecute_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Test  CQ stop
    stopCQ(client, "testCQStopExecute_0");
    
    // Test  CQ re-enable
    executeCQ(client, "testCQStopExecute_0", false, null);
    
    /* Init values at server. */
    createValues(server, regions[0], 20);
    // Wait for client to Synch.
    waitForCreated(client, "testCQStopExecute_0", KEY+20);
    size = 30;
    
    // Check if Client and Server in sync.
    //validateServerClientRegionEntries(server, client, regions[0]);
    validateQuery(server, cqs[0], 20);    
    // validate CQs.
    //validateCQ(client, "testCQStopExecute_0", size, noTest, noTest, noTest);
    validateCQ(client, "testCQStopExecute_0",
        /* resultSize: */ noTest,
        /* creates: */ 20,
        /* updates: */ 10,
        /* deletes; */ 0,
        /* queryInserts: */ 20,
        /* queryUpdates: */ 10,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    // Stop and execute CQ 20 times
    stopExecCQ(client, "testCQStopExecute_0", 20);
    
    // Test  CQ Close
    closeCQ(client, "testCQStopExecute_0");
    
    // Close.
    closeClient(client);
    closeServer(server);
  }
  
  /**
   * Test for CQ Attributes Mutator functions
   * @throws Exception
   */
  public void testCQAttributesMutator() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testCQAttributesMutator";
    createPool(client, poolName, host0, thePort);
    //createClient(client, thePort, host0);
    
    /* Create CQs. */
    String cqName = new String("testCQAttributesMutator_0");
    createCQ(client, poolName, cqName, cqs[0]); 
    validateCQCount(client, 1);    
    executeCQ(client,cqName, false, null); 
    
    /* Init values at server. */
    int size = 10;
    createValues(server, regions[0], size);
    // Wait for client to Synch.
    waitForCreated(client, cqName, KEY + size);
    
    // validate CQs.
    validateCQ(client, cqName,
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Add 2 new CQ Listeners
    mutateCQAttributes(client, cqName, UPDATE);
    
    /* Init values at server. */
    createValues(server, regions[0], size * 2);
    waitForCreated(client, cqName, KEY + (size * 2));
        
    validateCQ(client, cqName,
        /* resultSize: */ noTest,
        /* creates: */ 20,
        /* updates: */ 10,
        /* deletes; */ 0,
        /* queryInserts: */ 20,
        /* queryUpdates: */ 10,
        /* queryDeletes: */ 0,
        /* totalEvents: */ 30);
    
    // Remove 2 listeners and validate
    mutateCQAttributes(client, cqName, DESTROY);
    
    validateCQ(client, cqName,
        /* resultSize: */ noTest,
        /* creates: */ 10,
        /* updates: */ 10,
        /* deletes; */ 0,
        /* queryInserts: */ 10,
        /* queryUpdates: */ 10,
        /* queryDeletes: */ 0,
        /* totalEvents: */ 20);
    
    // Reinitialize with 2 CQ Listeners
    mutateCQAttributes(client, cqName, CREATE);  
    
    /* Delete values at server. */
    deleteValues(server, regions[0], 20);
    // Wait for client to Synch.
    waitForDestroyed(client, cqName, KEY + (size * 2));
    
    validateCQ(client, cqName,
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ 0,
        /* deletes; */ 20,
        /* queryInserts: */ 0,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 20,
        /* totalEvents: */ 20);
    
    // Close CQ
    closeCQ(client, cqName);
    
    // Close.
    closeClient(client);
    closeServer(server);
  }
  
  /**
   * Test for CQ register and UnRegister.
   * @throws Exception
   */
  public void testCQCreateClose() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testCQCreateClose";
    System.out.println("##### Pool Name :" + poolName + " host :" + host0 + " port :" + thePort);
    createPool(client, poolName, host0, thePort);

    // createClient(client, thePort, host0);
    
    /* debug */
    //getLogWriter().info("### DEBUG STOP ####");
    //pause(60 * 1000);
    //getLogWriter().info("### DEBUG START ####");
    
    /* Create CQs. */
    createCQ(client, poolName, "testCQCreateClose_0", cqs[0]); 
    validateCQCount(client, 1);
    
    executeCQ(client, "testCQCreateClose_0", false, null); 
    
    /* Init values at server. */
    int size = 10;
    createValues(server, regions[0], size);
    // Wait for client to Synch.
    waitForCreated(client, "testCQCreateClose_0", KEY+size);
    
    // Check if Client and Server in sync.
    //validateServerClientRegionEntries(server, client, regions[0]);
    validateQuery(server, cqs[0], 10);    
    // validate CQs.
    validateCQ(client, "testCQCreateClose_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Test  CQ stop
    stopCQ(client, "testCQCreateClose_0");
    
    // Test  CQ re-enable
    executeCQ(client, "testCQCreateClose_0", false, null);
    
    // Test  CQ Close
    closeCQ(client, "testCQCreateClose_0");
    
    //Create CQs with no name, execute, and close. 
    // UNCOMMENT....
    createAndExecCQNoName(client, poolName, cqs[0]); 
    
    // Accessing the closed CQ.
    failIfCQExists(client, "testCQCreateClose_0");
    
    // re-Create the cq which is closed.
    createCQ(client, poolName, "testCQCreateClose_0", cqs[0]);
    
    /* Test CQ Count */
    validateCQCount(client, 1);
    
    // Registering CQ with same name from same client.
    try {
      createCQ(client, poolName, "testCQCreateClose_0", cqs[0]);
      fail("Trying to create CQ with same name. Should have thrown CQExistsException");
    } catch (com.gemstone.gemfire.test.dunit.RMIException rmiExc) {
      Throwable cause = rmiExc.getCause();
      assertTrue("unexpected cause: " + cause.getClass().getName(), cause instanceof AssertionError);
      Throwable causeCause = cause.getCause(); // should be a CQExistsException
      assertTrue("Got wrong exception: " + causeCause.getClass().getName(),
          causeCause instanceof CqExistsException);
    }
    
    // Getting values from non-existent CQ.
    failIfCQExists(client, "testCQCreateClose_NO");
    
    // Server Registering CQ.
    try {
      createCQ(server, "testCQCreateClose_1", cqs[0]);
      fail("Trying to create CQ on Cache Server. Should have thrown Exception.");
    } catch (com.gemstone.gemfire.test.dunit.RMIException rmiExc) {
      Throwable cause = rmiExc.getCause();
      assertTrue("unexpected cause: " + cause.getClass().getName(), 
          cause instanceof AssertionError);
      Throwable causeCause = cause.getCause(); // should be a IllegalStateException
      assertTrue("Got wrong exception: " + causeCause.getClass().getName(),
          causeCause instanceof IllegalStateException);
    }
        
    validateCQCount(client, 1);
    
    createCQ(client, poolName, "testCQCreateClose_3", cqs[2]);
    
    validateCQCount(client, 2);
    
    /* Test for closeAllCQs() */
    
    client.invoke(new CacheSerializableRunnable("CloseAll CQ :") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Close All CQ. ###");
        // Get CQ Service.
        QueryService cqService = null;
        try {          
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          LogWriterUtils.getLogWriter().info("Failed to getCQService.", cqe);
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        // Close CQ.
        try {
          cqService.closeCqs();
        } catch (Exception ex) {
          LogWriterUtils.getLogWriter().info("Failed to close All CQ.", ex);
          Assert.fail("Failed to close All CQ. ", ex);
        }
      }
    });
    
    validateCQCount(client, 0);
    
    // Initialize.
    createCQ(client, poolName, "testCQCreateClose_2", cqs[1]);
    createCQ(client, poolName, "testCQCreateClose_4", cqs[1]);
    createCQ(client, poolName, "testCQCreateClose_5", cqs[1]);
    
    // Execute few of the initialized cqs
    executeCQ(client, "testCQCreateClose_4", false, null);
    executeCQ(client, "testCQCreateClose_5", false, null);
    
    // Call close all CQ.
    client.invoke(new CacheSerializableRunnable("CloseAll CQ 2 :") {
      public void run2() throws CacheException {
        LogWriterUtils.getLogWriter().info("### Close All CQ 2. ###");
        // Get CQ Service.
        QueryService cqService = null;
        try {          
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        
        // Close CQ.
        try {
          cqService.closeCqs();
        } catch (Exception ex) {
          Assert.fail("Failed to close All CQ  . ", ex);
        }
      }
    });
    
    // Close.
    closeClient(client);
    closeServer(server);
  }
  
  /**
   * This will test the events after region destory.
   * The CQs on the destroy region needs to be closed.
   *
   */
  public void testRegionDestroy() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    /* Init Server and Client */
    createServer(server);
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testRegionDestroy";
    createPool(client, poolName, host0, thePort);
    
    createClient(client, thePort, host0);
    
    /* Create CQs. */
    createCQ(client, poolName, "testRegionDestroy_0", cqs[0]); 
    createCQ(client, poolName, "testRegionDestroy_1", cqs[0]); 
    createCQ(client, poolName, "testRegionDestroy_2", cqs[0]); 
    
    executeCQ(client, "testRegionDestroy_0", false, null);
    executeCQ(client, "testRegionDestroy_1", false, null);
    executeCQ(client, "testRegionDestroy_2", false, null);
    
    /* Init values at server. */
    final int size = 10;
    registerInterestListCQ(client, regions[0], size, false);    
    createValues(server, regions[0], size);
    
    // Wait for client to Synch.
    
    waitForCreated(client, "testRegionDestroy_0", KEY + 10);
    
    
    // validate CQs.
    validateCQ(client, "testRegionDestroy_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ noTest,
        /* deletes; */ noTest,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Validate InterestList.
    // CREATE
    client.invoke(new CacheSerializableRunnable("validate updates") {
      public void run2() throws CacheException {
        // Wait for the region to become the correct size
        WaitCriterion wc = new WaitCriterion() {
          String excuse;
          public boolean done() {
            Region region = getRootRegion().getSubregion(regions[0]);
            if (region == null) {
              excuse = "Can't find region";
              return false;
            }
            int sz = region.entrySet().size();
            if (sz != size) {
              excuse = "Region is of size " + sz + ", expected " + size;
              return false;
            }
            return true;
          }
          public String description() {
            return excuse;
          }
        };
        Wait.waitForCriterion(wc, 30 * 1000, 250, true);
        
        Region region = getRootRegion().getSubregion(regions[0]);
        assertNotNull(region);
        
        CertifiableTestCacheListener ctl = (CertifiableTestCacheListener) 
            region.getAttributes().getCacheListener();
        for (int i = 1; i <= 10; i++) {
          ctl.waitForCreated(KEY+i);
          assertNotNull(region.getEntry(KEY+i));
        }
      }
    });
    
    // Destroy Region.
    server.invoke(new CacheSerializableRunnable("Destroy Region") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        region1.destroyRegion();
      }
    });
    
    Wait.pause(2 * 1000);
    validateCQCount(client, 0);
    
    closeClient(client);
    closeServer(server);
    
  }
  
  /**
   * Test for CQ with multiple clients.
   */
  public void testCQWithMultipleClients() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client1 = host.getVM(1);
    VM client2 = host.getVM(2);
    VM client3 = host.getVM(3);
    
    /* Create Server and Client */
    createServer(server);
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName1 = "testCQWithMultipleClients1";
    String poolName2 = "testCQWithMultipleClients2";
    String poolName3 = "testCQWithMultipleClients3";
    
    createPool(client1, poolName1, host0, thePort);
    createPool(client2, poolName2, host0, thePort);
    
    /* Create CQs. and initialize the region */
    createCQ(client1, poolName1, "testCQWithMultipleClients_0", cqs[0]);
    executeCQ(client1, "testCQWithMultipleClients_0", false, null);

    createCQ(client2, poolName2, "testCQWithMultipleClients_0", cqs[0]);
    executeCQ(client2, "testCQWithMultipleClients_0", false, null);
    
    int size = 10;
    
    // Create Values on Server.
    createValues(server, regions[0], size);
    
    waitForCreated(client1, "testCQWithMultipleClients_0", KEY + 10);
    
    /* Validate the CQs */
    validateCQ(client1, "testCQWithMultipleClients_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    waitForCreated(client2, "testCQWithMultipleClients_0", KEY + 10);
    
    validateCQ(client2, "testCQWithMultipleClients_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    
    /* Close test */
    closeCQ(client1, "testCQWithMultipleClients_0");
    
    validateCQ(client2, "testCQWithMultipleClients_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    /* Init new client and create cq */
    createPool(client3, poolName3, host0, thePort);
    
    createCQ(client3, poolName3, "testCQWithMultipleClients_0", cqs[0]);
    createCQ(client3, poolName3, "testCQWithMultipleClients_1", cqs[1]);
    executeCQ(client3, "testCQWithMultipleClients_0", false, null);
    executeCQ(client3, "testCQWithMultipleClients_1", false, null);
    
    // Update values on Server. This will be updated on new Client CQs.
    createValues(server, regions[0], size);
    
    
    waitForUpdated(client3, "testCQWithMultipleClients_0", KEY + 10);
    
    
    validateCQ(client3, "testCQWithMultipleClients_0",
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ 0,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    validateCQ(client3, "testCQWithMultipleClients_1",
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ 1,
        /* deletes; */ 0,
        /* queryInserts: */ 0,
        /* queryUpdates: */ 1,
        /* queryDeletes: */ 0,
        /* totalEvents: */ 1);    
    
    /* Validate the CQ count */
    validateCQCount(client1, 0);
    validateCQCount(client2, 1);
    validateCQCount(client3, 2);
    
    /* Close Client Test */
    closeClient(client1);
    
    clearCQListenerEvents(client2, "testCQWithMultipleClients_0");
    clearCQListenerEvents(client3, "testCQWithMultipleClients_1");
    
    // Update values on server, update again.
    createValues(server, regions[0], size);
    
    
    waitForUpdated(client2, "testCQWithMultipleClients_0", KEY + 10);
    
    
    validateCQ(client2, "testCQWithMultipleClients_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ size * 2,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ size * 2,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size * 3);
    
    waitForUpdated(client3, "testCQWithMultipleClients_1", KEY + 2);
    
    validateCQ(client3, "testCQWithMultipleClients_1",
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ 2,
        /* deletes; */ 0,
        /* queryInserts: */ 0,
        /* queryUpdates: */ 2,
        /* queryDeletes: */ 0,
        /* totalEvents: */ 2);    
    
    /* Close Server and Client */
    closeClient(client2);
    closeClient(client3);
    closeServer(server);
  }
  
  /**
   * Test for CQ ResultSet.
   */
  public void testCQResultSet() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    createServer(server);
    
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testCQResultSet";
    createPool(client, poolName, host0, thePort);
    
    // Create client.
    // createClient(client, thePort, host0);
    
    /* CQ Test with initial Values. */
    int size = 10;
    createValues(server, regions[0], size);
    Wait.pause(1*500);
    
    // Create CQs.
    createCQ(client, poolName, "testCQResultSet_0", cqs[0]);    
    
    // Check resultSet Size.
    executeCQ(client, "testCQResultSet_0", true, 10, null, null);
    
    /* CQ Test with no Values on Region */
    createCQ(client, poolName, "testCQResultSet_1", cqs[2]);
    // Check resultSet Size.
    executeCQ(client, "testCQResultSet_1", true, 0, null, null);
    stopCQ(client, "testCQResultSet_1");
    
    // Init values.
    createValues(server, regions[1], 5);    
    validateQuery(server, cqs[2], 2);
    
    executeCQ(client, "testCQResultSet_1", true, 2, null, null);
    
    /* compare values...
     Disabled since we don't currently maintain results on the client
     
     validateCQ(client, "testCQResultSet_1", 2, noTest, noTest, noTest);
     Portfolio[] values = new Portfolio[] {new Portfolio(2), new Portfolio(4)}; 
     Hashtable t = new Hashtable();
     String[] keys = new String[] {"key-2", "key-4"};
     t.put(keys[0], values[0]);
     t.put(keys[1], values[1]);
     
     compareValues(client, "testCQResultSet_1", t);
     
     deleteValues(server, regions[1], 3);
     t.remove("key-4");
     pause(2 * 1000);
     
     try {
     compareValues(client, "testCQResultSet_1", t);
     fail("Should have thrown Exception. The value should not be present in cq results region");
     }
     catch (Exception ex) { // @todo check for specific exception type
     }
     
     */
    
    // Close.
    closeClient(client);    
    closeServer(server);
  }
  
  /**
   * Test for CQ Listener events.
   *
   */
  public void testCQEvents() throws Exception {
    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    createServer(server);
    
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCQEvents";
    createPool(client, poolName, host0, thePort);

    // Create client.
    //createClient(client, thePort, host0);
    
    // Create CQs.
    createCQ(client, poolName, "testCQEvents_0", cqs[0]);
    
    executeCQ(client, "testCQEvents_0", false, null); 
    
    // Init values at server.
    int size = 10;
    createValues(server, regions[0], size);
    
    waitForCreated(client, "testCQEvents_0", KEY+size);
    
    // validate Create events.
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Update values.
    createValues(server, regions[0], 5);
    createValues(server, regions[0], 10);
    
    waitForUpdated(client, "testCQEvents_0", KEY+size);
    
    
    // validate Update events.
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 15,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 15,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size + 15);
    
    // Validate delete events.
    deleteValues(server, regions[0], 5);
    waitForDestroyed(client, "testCQEvents_0", KEY+5);
    
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 15,
        /* deletes; */5,
        /* queryInserts: */ size,
        /* queryUpdates: */ 15,
        /* queryDeletes: */ 5,
        /* totalEvents: */ size + 15 + 5);
    
    // Insert invalid Events.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = -1; i >= -5; i--) {
          //change : new Portfolio(i) rdubey ( for Suspect strings problem).
          //region1.put(KEY+i, new Portfolio(i) ); 
          region1.put(KEY+i, KEY+i);
        }
      }
    });
    
    Wait.pause(1 * 1000);
    // cqs should not get any creates, deletes or updates. rdubey.
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 15,
        /* deletes; */5,
        /* queryInserts: */ size,
        /* queryUpdates: */ 15,
        /* queryDeletes: */ 5,
        /* totalEvents: */ size + 15 + 5);
       
    // Close.
    closeClient(client);
    closeServer(server);
  }
  
  /**
   * Test query execution multiple times on server without ALIAS.
   * @throws Exception
   */
  public void testCqEventsWithoutAlias() throws Exception {

    
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    createServer(server);
    
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testCQEvents";
    createPool(client, poolName, host0, thePort);

    // Create client.
    //createClient(client, thePort, host0);
    
    // Create CQs.
    createCQ(client, poolName, "testCQEvents_0", cqs[11]);
    
    executeCQ(client, "testCQEvents_0", false, null); 
    
    // Init values at server.
    int size = 10;
    createValues(server, regions[0], size);
    
    waitForCreated(client, "testCQEvents_0", KEY+size);
    
    // validate Create events.
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    // Update values.
    createValues(server, regions[0], 5);
    createValues(server, regions[0], 10);
    
    waitForUpdated(client, "testCQEvents_0", KEY+size);
    
    
    // validate Update events.
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 15,
        /* deletes; */ 0,
        /* queryInserts: */ size,
        /* queryUpdates: */ 15,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size + 15);
    
    // Validate delete events.
    deleteValues(server, regions[0], 5);
    waitForDestroyed(client, "testCQEvents_0", KEY+5);
    
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 15,
        /* deletes; */5,
        /* queryInserts: */ size,
        /* queryUpdates: */ 15,
        /* queryDeletes: */ 5,
        /* totalEvents: */ size + 15 + 5);
    
    // Insert invalid Events.
    server.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = -1; i >= -5; i--) {
          //change : new Portfolio(i) rdubey ( for Suspect strings problem).
          //region1.put(KEY+i, new Portfolio(i) ); 
          region1.put(KEY+i, KEY+i);
        }
      }
    });
    
    Wait.pause(1 * 1000);
    // cqs should not get any creates, deletes or updates. rdubey.
    validateCQ(client, "testCQEvents_0",
        /* resultSize: */ noTest,
        /* creates: */ size,
        /* updates: */ 15,
        /* deletes; */5,
        /* queryInserts: */ size,
        /* queryUpdates: */ 15,
        /* queryDeletes: */ 5,
        /* totalEvents: */ size + 15 + 5);
       
    // Close.
    closeClient(client);
    closeServer(server);
  }
  /**
   * Test for stopping and restarting CQs.
   * @throws Exception
   */
  public void testEnableDisableCQ() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    createServer(server);
    
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testEnableDisableCQ";
    createPool(client, poolName, host0, thePort);

    // Create client.
    //createClient(client, thePort, host0);
    
    // Create CQs.
    createCQ(client, poolName, "testEnableDisable_0", cqs[0]);
    executeCQ(client, "testEnableDisable_0", false, null);
    
    /* Test for disableCQ */
    client.invoke(new CacheSerializableRunnable("Client disableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
          cqService.stopCqs();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }                
      }      
    });
    
    Wait.pause(1 * 1000);
    // Init values at server.
    int size = 10;
    createValues(server, regions[0], size);
    Wait.pause(1 * 500);
    // There should not be any creates.
    validateCQ(client, "testEnableDisable_0",
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ 0,
        /* deletes; */ 0,
        /* queryInserts: */ 0,
        /* queryUpdates: */ 0,
        /* queryDeletes: */ 0,
        /* totalEvents: */ 0);
    
    /* Test for enable CQ */
    client.invoke(new CacheSerializableRunnable("Client enableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
          cqService.executeCqs();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }                
      }
    });
    Wait.pause(1 * 1000);
    createValues(server, regions[0], size);    
    waitForUpdated(client, "testEnableDisable_0", KEY+size);
    // It gets created on the CQs
    validateCQ(client, "testEnableDisable_0",
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ 0,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    /* Test for disableCQ on Region*/
    client.invoke(new CacheSerializableRunnable("Client disableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
          cqService.stopCqs("/root/" + regions[0]);
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }                
      }
    });
    
    Wait.pause(2 * 1000);
    deleteValues(server, regions[0], size / 2);
    Wait.pause(1 * 500);    
    // There should not be any deletes.
    validateCQ(client, "testEnableDisable_0",
        /* resultSize: */ noTest,
        /* creates: */ 0,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ 0,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size);
    
    /* Test for enable CQ on region */
    client.invoke(new CacheSerializableRunnable("Client enableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
          cqService.executeCqs("/root/" + regions[0]);
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }                
      }
    });
    Wait.pause(1 * 1000);
    createValues(server, regions[0], size / 2);    
    waitForCreated(client, "testEnableDisable_0", KEY+(size / 2));
    // Gets updated on the CQ.
    validateCQ(client, "testEnableDisable_0",
        /* resultSize: */ noTest,
        /* creates: */ size / 2,
        /* updates: */ size,
        /* deletes; */ 0,
        /* queryInserts: */ size / 2,
        /* queryUpdates: */ size,
        /* queryDeletes: */ 0,
        /* totalEvents: */ size * 3 / 2);
    
    // Close.
    closeClient(client);
    closeServer(server);    
  }
  
  /**
   * Test for Complex queries.
   * @throws Exception
   */
  public void testQuery() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    createServer(server);
    
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    String poolName = "testQuery";
    createPool(client, poolName, host0, thePort);
    
    // Create client.
    createClient(client, thePort, host0);
    
    // Create CQs.
    createCQ(client, poolName, "testQuery_3", cqs[3]);
    executeCQ(client, "testQuery_3", true, null);
    
    createCQ(client, poolName, "testQuery_4", cqs[4]);
    executeCQ(client, "testQuery_4", true, null);
    
    createCQ(client, poolName, "testQuery_5", cqs[5]);
    executeCQ(client, "testQuery_5", true, null);
    
    createCQ(client, poolName, "testQuery_6", cqs[6]);
    executeCQ(client, "testQuery_6", true, null);
    
    createCQ(client, poolName, "testQuery_7", cqs[7]);
    executeCQ(client, "testQuery_7", true, null);
    
    createCQ(client, poolName, "testQuery_8", cqs[8]);
    executeCQ(client, "testQuery_8", true, null);
    
    // Close.
    closeClient(client);
    closeServer(server);
  }
  
  /**
   * Test for CQ Fail over.
   * @throws Exception
   */
  public void testCQFailOver() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    createServer(server1);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    // Create client.
//    Properties props = new Properties();
    // Create client with redundancyLevel -1
    
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    
    //createClient(client, new int[] {port1, ports[0]}, host0, "-1");

    String poolName = "testCQFailOver";
    createPool(client, poolName, new String[] {host0, host0}, new int[] {port1, ports[0]});

    int numCQs = 1;
    for (int i=0; i < numCQs; i++) {
      // Create CQs.
      createCQ(client, poolName, "testCQFailOver_" + i, cqs[i]);
      executeCQ(client, "testCQFailOver_" + i, false, null);
    }
    Wait.pause(1 * 1000);
    
    // CREATE.
    createValues(server1, regions[0], 10);
    createValues(server1, regions[1], 10);
    waitForCreated(client, "testCQFailOver_0", KEY+10);

    Wait.pause(1 * 1000);
    
    createServer(server2, ports[0]);
    final int thePort2 = server2.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    System.out.println("### Port on which server1 running : " + port1 + 
        " Server2 running : " + thePort2);
    Wait.pause(3 * 1000);

    // Extra pause - added after downmerging trunk r17050
    Wait.pause(5 * 1000);
    
    // UPDATE - 1.
    createValues(server1, regions[0], 10);    
    createValues(server1, regions[1], 10);
    
    waitForUpdated(client, "testCQFailOver_0", KEY+10);
    
    int[] resultsCnt = new int[] {10, 1, 2};
    
    for (int i=0; i < numCQs; i++) {
      validateCQ(client, "testCQFailOver_" + i, noTest, resultsCnt[i], resultsCnt[i], noTest);
    }    
    
    // Close server1.
    closeServer(server1);
    
    // Fail over should happen.
    Wait.pause(3 * 1000);
    
    for (int i=0; i < numCQs; i++) {
      validateCQ(client, "testCQFailOver_" + i, noTest, resultsCnt[i], resultsCnt[i], noTest);
    }    
    
    // UPDATE - 2
    this.clearCQListenerEvents(client, "testCQFailOver_0");
    createValues(server2, regions[0], 10);
    createValues(server2, regions[1], 10);
    
    for (int i=1; i <= 10; i++) {
      waitForUpdated(client, "testCQFailOver_0", KEY+i);
    }
    
    for (int i=0; i < numCQs; i++) {
      validateCQ(client, "testCQFailOver_" + i, noTest, resultsCnt[i], resultsCnt[i] * 2, noTest);
    }    
    
    // Close.
    closeClient(client);
    closeServer(server2);
  }
  
  /**
   * Test for CQ Fail over/HA with redundancy level set.
   * @throws Exception
   */
  public void testCQHA() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM server3 = host.getVM(2);
    
    VM client = host.getVM(3);
    
    //Killing servers can cause this message on the client side.
    IgnoredException.addIgnoredException("Could not find any server");
    
    createServer(server1);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    
    createServer(server2, ports[0]);
    final int thePort2 = server2.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    
    createServer(server3, ports[1]);
    final int port3 = server3.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    System.out.println("### Port on which server1 running : " + port1 + 
        " server2 running : " + thePort2 + 
        " Server3 running : " + port3);
    
    
    // Create client - With 3 server endpoints and redundancy level set to 2.
    
    String poolName = "testCQStopExecute";
    createPool(client, poolName, new String[] {host0, host0, host0}, new int[] {port1, thePort2, port3});
    
    // Create client with redundancyLevel 1
    //createClient(client, new int[] {port1, thePort2, port3}, host0, "1");
    
    // Create CQs.
    int numCQs = 1;
    for (int i=0; i < numCQs; i++) {
      // Create CQs.
      createCQ(client, poolName, "testCQHA_" + i, cqs[i]);
      executeCQ(client, "testCQHA_" + i, false, null);
    }
    
    Wait.pause(1 * 1000);
    
    // CREATE.
    createValues(server1, regions[0], 10);
    createValues(server1, regions[1], 10);
    
    
    waitForCreated(client, "testCQHA_0", KEY + 10);
    
    
    // Clients expected initial result.
    int[] resultsCnt = new int[] {10, 1, 2};
    
    // Close server1.
    // To maintain the redundancy; it will make connection to endpoint-3.
    closeServer(server1);
    Wait.pause(3 * 1000);
    
    // UPDATE-1.
    createValues(server2, regions[0], 10);
    createValues(server2, regions[1], 10);
    
    
    waitForUpdated(client, "testCQHA_0", KEY + 10);
    
    
    // Validate CQ.
    for (int i=0; i < numCQs; i++) {
      validateCQ(client, "testCQHA_" + i, noTest, resultsCnt[i], resultsCnt[i], noTest);
    }    
    
    // Close server-2
    closeServer(server2);
    Wait.pause(2 * 1000);
    
    // UPDATE - 2.
    clearCQListenerEvents(client, "testCQHA_0");
    
    createValues(server3, regions[0], 10);    
    createValues(server3, regions[1], 10);
    
    // Wait for events at client.
    
    waitForUpdated(client, "testCQHA_0", KEY + 10);
    
    
    for (int i=0; i < numCQs; i++) {
      validateCQ(client, "testCQHA_" + i, noTest, resultsCnt[i], resultsCnt[i] * 2, noTest);
    }    
    
    // Close.
    closeClient(client);
    closeServer(server3);
  }

  /**
   * Test Filter registration during GII. 
   * Bug fix 39014
   * @throws Exception
   */
  public void testFilterRegistrationDuringGII() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client1 = host.getVM(2);
    VM client2 = host.getVM(3);
    
    createServer(server1);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    
    String poolName = "testFilterRegistrationDuringGII";
    createPool(client1, poolName, new String[] {host0, host0}, new int[] {port1, ports[0]}, "-1");
    createPool(client2, poolName, new String[] {host0, host0}, new int[] {port1, ports[0]}, "-1");
        
    createClient(client1, new int[] {port1, ports[0]}, host0, "-1", poolName);
    createClient(client2, new int[] {port1, ports[0]}, host0, "-1", poolName);
    
    // Create CQs.
    final int numCQs = 2;
    for (int i=0; i < numCQs; i++) {
      // Create CQs.
      createCQ(client1, poolName, "client1_" + i, cqs[i]);
      executeCQ(client1, "client1_" + i, false, null);
      createCQ(client2, poolName, "client2_" + i, cqs[i]);
      executeCQ(client2, "client2_" + i, false, null);
    }
    
    final int interestSize = 20;
    registerInterestListCQ(client1, regions[0], interestSize, false);
    registerInterestListCQ(client2, regions[0], 0, true);
    
    Wait.pause(1 * 1000);
    
    // CREATE.
    createValues(server1, regions[0], 100);
    createValues(server1, regions[1], 10);
    
    waitForCreated(client1, "client1_0", KEY + 10);

    // Create server2.
    server2.invoke(new CacheSerializableRunnable("Create Cache Server") {
      public void run2() throws CacheException
      {
        LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.DISTRIBUTED_ACK);
        factory.setMirrorType(MirrorType.KEYS_VALUES);

        for (int i = 0; i < regions.length; i++) {
          createRegion(regions[i], factory.createRegionAttributes());
        }

        InitialImageOperation.slowImageProcessing = 100;
        try {
          try {
            startBridgeServer(ports[0], true);
          } catch (Exception ex) {
            Assert.fail("While starting CacheServer", ex);
          }

          while(true) {
            if (InitialImageOperation.slowImageSleeps > 0) {
              // Create events while GII for HARegion is in progress.
              LocalRegion region1 = (LocalRegion)getRootRegion().getSubregion(regions[0]);
              for (int i = 90; i <= 120; i++) {
                region1.put(KEY+i, new Portfolio(i, i));
              }
              break;
            }
            Wait.pause(20);
          }
        } finally {
          InitialImageOperation.slowImageProcessing = 0;
        }
      }
    }
    );

    Wait.pause(3 * 1000);
    
    
    // Check if CQs are registered as part of GII.
    server2.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        DefaultQueryService qs = (DefaultQueryService)getCache().getQueryService();
        Collection<CacheClientProxy> proxies = CacheClientNotifier.getInstance().getClientProxies();
        Iterator iter = proxies.iterator();
        try {
          for (CacheClientProxy p : proxies){
            ClientProxyMembershipID clientId = p.getProxyID(); 
            List cqs = qs.getCqService().getAllClientCqs(clientId);
            getCache().getLogger().fine("Number of CQs found for client :" + clientId + " are :" + cqs.size());
            if (cqs.size() != numCQs) {
              fail("Number of CQs registerted by the client is :" + cqs.size() + 
                  " less than expected : " + numCQs);
            }
            CqQuery cq = (CqQuery)cqs.get(0);
            LocalRegion region1 = (LocalRegion)getRootRegion().getSubregion(regions[0]);
            if (cq.getName().startsWith("client1_")) {
              if (region1.getFilterProfile().getKeysOfInterest(clientId) == null ||
                  region1.getFilterProfile().getKeysOfInterest(clientId).size() != interestSize) {
                fail("Interest registartion during Secondary HARegion creation has failed.");
              }
            } else {
              if (!region1.getFilterProfile().isInterestedInAllKeys(clientId)) {
                fail("Interest registartion during Secondary HARegion creation has failed.");
              }
            }
          }
        } catch (Exception ex) {
          fail("Exception while validating filter count. " + ex.getMessage());
        }
      }
    });

    // Close.
    closeClient(client1);
    closeClient(client2);
    closeServer(server1);
    closeServer(server2);
  }

  /**
   * Test without CQs.
   * This was added after an exception encountered with CQService, when there was
   * no CQService intiated.
   * @throws Exception
   */
  public void testWithoutCQs() throws Exception {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    createServer(server1);
    createServer(server2);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server1.getHost());
    
    final int thePort2 = server2.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    
    SerializableRunnable createConnectionPool =
      new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        getCache();
        IgnoredException.addIgnoredException("java.net.ConnectException||java.net.SocketException");
        AttributesFactory regionFactory = new AttributesFactory();
        regionFactory.setScope(Scope.LOCAL);
        
        ClientServerTestCase.configureConnectionPool(regionFactory, host0, port1, thePort2, true, -1, -1, null);
        
        createRegion(regions[0], regionFactory.createRegionAttributes());
      }
    };
    
    
    // Create client.
    client.invoke(createConnectionPool);
    
    server1.invoke(new CacheSerializableRunnable("Create values") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        for (int i = 0; i < 20; i++) {
          region1.put("key-string-"+i, "value-"+i);
        }
      }
    });
    
    // Put some values on the client.
    client.invoke(new CacheSerializableRunnable("Put values client") {
      public void run2() throws CacheException {
        Region region1 = getRootRegion().getSubregion(regions[0]);
        
        for (int i = 0; i < 10; i++) {
          region1.put("key-string-"+i, "client-value-"+i);
        }
      }
    });
    
    
    Wait.pause(2 * 1000);
    closeServer(server1);
    closeServer(server2);
  }
  
  
  /**
   * Test getCQs for a regions
   */
  public void testGetCQsForARegionName() throws Exception {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    
    createServer(server);
    
    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());
    
    // Create client.
    // createClient(client, thePort, host0);
    
    String poolName = "testGetCQsForARegionName";
    createPool(client, poolName, host0, thePort);
    
    // Create CQs.
    createCQ(client, poolName, "testQuery_3", cqs[3]);
    executeCQ(client, "testQuery_3", true, null);
    
    createCQ(client, poolName, "testQuery_4", cqs[4]);
    executeCQ(client, "testQuery_4", true, null);
    
    createCQ(client, poolName, "testQuery_5", cqs[5]);
    executeCQ(client, "testQuery_5", true, null);
    
    createCQ(client, poolName, "testQuery_6", cqs[6]);
    executeCQ(client, "testQuery_6", true, null);
    //with regions[1]
    createCQ(client, poolName, "testQuery_7", cqs[7]);
    executeCQ(client, "testQuery_7", true, null);
    
    createCQ(client, poolName, "testQuery_8", cqs[8]);
    executeCQ(client, "testQuery_8", true, null);
    
    client.invoke(new CacheSerializableRunnable("Client disableCQs()") {
      public void run2() throws CacheException {
        // Get CQ Service.
        QueryService cqService = null;
        try {
          cqService = getCache().getQueryService();
          CqQuery[] cq= cqService.getCqs("/root/"+regions[0]);
          assertNotNull("CQservice should not return null for cqs on this region : /root/"+regions[0], cq);
          getCache().getLogger().info("cqs for region: /root/"+regions[0]+" : "+cq.length);
          // closing on of the cqs.
          
          cq[0].close();
          cq = cqService.getCqs("/root/"+regions[0]);
          assertNotNull("CQservice should not return null for cqs on this region : /root/"+regions[0], cq);
          getCache().getLogger().info("cqs for region: /root/"+regions[0]+" after closeing one of the cqs : "+cq.length);
          
          cq = cqService.getCqs("/root/"+regions[1]);
          getCache().getLogger().info("cqs for region: /root/"+regions[1]+" : "+cq.length);
          assertNotNull("CQservice should not return null for cqs on this region : /root/"+regions[1], cq);
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService", cqe);
        }                
      }
    });
    
    // Close.
    closeClient(client);
    closeServer(server);
    
  }
  
  /**
   * Tests execution of queries with NULL in where clause like where ID = NULL
   * etc.
   * 
   * @throws Exception
   */
  public void testQueryWithNULLInWhereClause() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server = host.getVM(0);
    VM client = host.getVM(1);
    VM producer = host.getVM(2);

    createServer(server);

    final int thePort = server.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");
    final String host0 = NetworkUtils.getServerHostName(server.getHost());

    String poolName = "testQueryWithNULLInWhereClause";
    createPool(client, poolName, host0, thePort);
    
    // Create client.
    // createClient(client, thePort, host0);
    
    // producer is not doing any thing.
    createClient(producer, thePort, host0);

    final int size = 50;
    createValues(producer, regions[0], size);

    createCQ(client, poolName, "testQuery_9", cqs[9]);
    executeCQ(client, "testQuery_9", true, null);

    createValues(producer, regions[0], (2 * size));

    for (int i = 1; i <= size; i++) {
      if (i%2 == 0 )
        waitForUpdated(client, "testQuery_9", KEY + i);
    }

    for (int i = (size + 1); i <= 2 * size; i++) {
      if (i%2 == 0 )
        waitForCreated(client, "testQuery_9", KEY + i);
    }

    validateCQ(client, "testQuery_9", noTest, 25, 25, noTest);

    // Close.
    closeClient(client);
    closeServer(server);
    
  }
  
  /**
   * Tests execution of queries with NULL in where clause like where ID = NULL
   * etc.
   * 
   * @throws Exception
   */
  public void testForSupportedRegionAttributes() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    // Create server with Global scope.
    SerializableRunnable createServer = new CacheSerializableRunnable(
    "Create Cache Server") {
      public void run2() throws CacheException
      {
        LogWriterUtils.getLogWriter().info("### Create Cache Server. ###");
        
        // Create region with Global scope
        AttributesFactory factory1 = new AttributesFactory();
        factory1.setScope(Scope.GLOBAL);
        factory1.setMirrorType(MirrorType.KEYS_VALUES);
        createRegion(regions[0], factory1.createRegionAttributes());
        
        // Create region with non Global, distributed_ack scope
        AttributesFactory factory2 = new AttributesFactory();
        factory2.setScope(Scope.DISTRIBUTED_NO_ACK);
        factory2.setMirrorType(MirrorType.KEYS_VALUES);
        createRegion(regions[1], factory2.createRegionAttributes());
        
        Wait.pause(2000);

        try {
          startBridgeServer(port, true);
        }

        catch (Exception ex) {
          Assert.fail("While starting CacheServer", ex);
        }
        Wait.pause(2000);

      }
    };

    server1.invoke(createServer);
    server2.invoke(createServer);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class,
        "getCacheServerPort");

    final String host0 = NetworkUtils.getServerHostName(server1.getHost());

    final int thePort2 = server2.invokeInt(CqQueryUsingPoolDUnitTest.class,
    "getCacheServerPort");

    String poolName = "testForSupportedRegionAttributes";
    createPool(client, poolName, new String[] {host0, host0}, new int[] {port1, thePort2});
    
    // Create client.
    //createClient(client, new int[] {port1, thePort2}, host0, "-1");

    // Create CQ on region with GLOBAL SCOPE.
    createCQ(client, poolName, "testForSupportedRegionAttributes_0", cqs[0]);
    executeCQ(client, "testForSupportedRegionAttributes_0", false, null);

    int size = 5;
    
    createValues(server1, regions[0], size);

    for (int i = 1; i <= size; i++) {
      waitForCreated(client, "testForSupportedRegionAttributes_0", KEY + i);
    }

    // Create CQ on region with non GLOBAL, DISTRIBUTED_ACK SCOPE.
    createCQ(client, poolName, "testForSupportedRegionAttributes_1", cqs[2]);
    
    String errMsg = "The replicated region " +
       " specified in CQ creation does not have scope supported by CQ." +
       " The CQ supported scopes are DISTRIBUTED_ACK and GLOBAL.";
    final String expectedErr = "Cq not registered on primary";
    client.invoke(new CacheSerializableRunnable("Set expect") {
      public void run2() {
        getCache().getLogger().info("<ExpectedException action=add>"
            + expectedErr + "</ExpectedException>");
      }
    });

    try {
      executeCQ(client, "testForSupportedRegionAttributes_1", false, "CqException");
      fail("The test should have failed with exception, " + errMsg);
    } catch (Exception ex){
      // Expected.
    } finally {
      client.invoke(new CacheSerializableRunnable("Remove expect") {
        public void run2() {
          getCache().getLogger().info("<ExpectedException action=remove>"
              + expectedErr + "</ExpectedException>");
        }
      });
    }
    
    // Close.
    closeClient(client);
    closeServer(server1);
    closeServer(server2);

  }
    
  
  // HELPER METHODS....
  
  /* For debug purpose - Compares entries in the region */
  private void validateServerClientRegionEntries(VM server, VM client, final String regionName) {
    
    server.invoke(new CacheSerializableRunnable("Server Region Entries") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        LogWriterUtils.getLogWriter().info("### Entries in Server :" + region.keys().size());
      }
    });
    
    client.invoke(new CacheSerializableRunnable("Client Region Entries") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        LogWriterUtils.getLogWriter().info("### Entries in Client :" + region.keys().size()); 
      }
    });
  }
  
  /**
   * Starts a bridge server on the given port to serve up the given
   * region.
   *
   * @since 4.0
   */
  public void startBridgeServer(int port) throws IOException {
    startBridgeServer(port, CacheServer.DEFAULT_NOTIFY_BY_SUBSCRIPTION);
  }
  
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   *
   * @since 4.0
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
  
  /**
   * Stops the bridge server that serves up the given cache.
   *
   * @since 4.0
   */
  protected void stopBridgeServer(Cache cache) {
    CacheServer bridge =
      (CacheServer) cache.getCacheServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }
  
  private void stopBridgeServers(Cache cache) {
    CacheServer bridge = null;
    for (Iterator bsI = cache.getCacheServers().iterator();bsI.hasNext(); ) {
      bridge = (CacheServer) bsI.next();
      bridge.stop();
      assertFalse(bridge.isRunning());
    }
  }
  
  private void restartBridgeServers(Cache cache) throws IOException
  {
    CacheServer bridge = null;
    for (Iterator bsI = cache.getCacheServers().iterator();bsI.hasNext(); ) {
      bridge = (CacheServer) bsI.next();
      bridge.start();
      assertTrue(bridge.isRunning());
    }
  }
  
  private InternalDistributedSystem createLonerDS() {
    disconnectFromDS();
    Properties lonerProps = new Properties();
    lonerProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    lonerProps.setProperty(DistributionConfig.LOCATORS_NAME, "");
    InternalDistributedSystem ds = getSystem(lonerProps);
    assertEquals(0, ds.getDistributionManager().getOtherDistributionManagerIds().size());
    return ds;
  }
  
  /**
   * Returns region attributes for a <code>LOCAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    return factory.createRegionAttributes();
  }
  
  
}
