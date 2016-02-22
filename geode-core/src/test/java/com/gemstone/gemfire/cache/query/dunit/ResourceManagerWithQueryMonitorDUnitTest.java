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

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import util.TestException;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.QueryExecutionLowMemoryException;
import com.gemstone.gemfire.cache.query.QueryExecutionTimeoutException;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.QueryMonitor;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.MemoryEvent;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;
import com.gemstone.gemfire.internal.cache.control.TestMemoryThresholdListener;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestUtils;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.VM;

public class ResourceManagerWithQueryMonitorDUnitTest extends ClientServerTestCase {
  
  private static int MAX_TEST_QUERY_TIMEOUT = 4000;
  private static int TEST_QUERY_TIMEOUT = 1000;
  private final static int CRITICAL_HEAP_USED = 950;
  private final static int NORMAL_HEAP_USED = 500;
  public ResourceManagerWithQueryMonitorDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    Invoke.invokeInEveryVM(this.setHeapMemoryMonitorTestMode);
    IgnoredException.addIgnoredException("above heap critical threshold");
    IgnoredException.addIgnoredException("below heap critical threshold");
  }
  
  @Override
  protected void preTearDownClientServerTestCase() throws Exception {
    Invoke.invokeInEveryVM(resetQueryMonitor);
    Invoke.invokeInEveryVM(resetResourceManager);
  }

  private SerializableCallable setHeapMemoryMonitorTestMode = new SerializableCallable() {
    public Object call() throws Exception {
      HeapMemoryMonitor.setTestDisableMemoryUpdates(true);
      return null;
    }
  };
  
  private SerializableCallable resetResourceManager = new SerializableCallable() {
    public Object call() throws Exception {
      InternalResourceManager irm = ((GemFireCacheImpl)getCache()).getResourceManager();
      // Reset CRITICAL_UP by informing all that heap usage is now 1 byte (0 would disable).
      irm.getHeapMonitor().updateStateAndSendEvent(NORMAL_HEAP_USED);
      Set<ResourceListener> listeners = irm.getResourceListeners(ResourceType.HEAP_MEMORY);
      Iterator<ResourceListener> it = listeners.iterator();
      while (it.hasNext()) {
        ResourceListener<MemoryEvent> l = it.next();
        if (l instanceof TestMemoryThresholdListener) {
          ((TestMemoryThresholdListener)l).resetThresholdCalls();
        }
      }
      irm.setCriticalHeapPercentage(0f);
      irm.setEvictionHeapPercentage(0f);
      irm.getHeapMonitor().setTestMaxMemoryBytes(0);
      HeapMemoryMonitor.setTestDisableMemoryUpdates(false);
      return null;
    }
  };
  
  private SerializableCallable resetQueryMonitor = new SerializableCallable() {
    public Object call() throws Exception {
      QueryMonitor.setLowMemory(false, 0);
      DefaultQuery.testHook = null;
      return null;
    }
  };
 
  public void testRMAndNoTimeoutSet() throws Exception {
    doCriticalMemoryHitTest("portfolios", false, 85/*crit threshold*/, false, -1, true);
  }
  
  public void testRMAndNoTimeoutSetParReg() throws Exception {
    doCriticalMemoryHitTest("portfolios", true, 85/*crit threshold*/, false, -1, true);
  }
  
  public void testRMButDisabledQueryMonitorForLowMemAndNoTimeoutSet() throws Exception {
    //verify that timeout is not set and that a query can execute properly
    doCriticalMemoryHitTest("portfolios", false, 85/*crit threshold*/, true, -1, true);
  }
  
  public void testRMAndTimeoutSet() throws Exception {
    //verify that we still receive critical heap cancelation
    doCriticalMemoryHitTest("portfolios", false, 85/*crit threshold*/, true, TEST_QUERY_TIMEOUT, true);
  }
  
  public void testRMAndTimeoutSetAndQueryTimesoutInstead() throws Exception {
    //verify that timeout is set correctly and cancel query 
    doCriticalMemoryHitTest("portfolios", false, 85/*crit threshold*/, true, TEST_QUERY_TIMEOUT, false);
  }
  
  public void testRMButDisabledQueryMonitorForLowMemAndTimeoutSet()  throws Exception {
    //verify that timeout is still working properly
    doCriticalMemoryHitTest("portfolios", false, 85/*crit threshold*/, true, TEST_QUERY_TIMEOUT, true);
  }
  
  
  //Query directly on member with RM and QM set
  public void testRMAndNoTimeoutSetOnServer() throws Exception {
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/*crit threshold*/, false, -1, true);
  }
  
  public void testRMAndNoTimeoutSetParRegOnServer() throws Exception {
    doCriticalMemoryHitTestOnServer("portfolios", true, 85/*crit threshold*/, false, -1, true);
  }
  
  public void testRMButDisabledQueryMonitorForLowMemAndNoTimeoutSetOnServer() throws Exception {
    //verify that timeout is not set and that a query can execute properly
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/*crit threshold*/, true, -1, true);
  }
  
  public void testRMAndTimeoutSetOnServer() throws Exception {
    //verify that we still receive critical heap cancelation
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/*crit threshold*/, true, TEST_QUERY_TIMEOUT, true);
  }
  
  public void testRMAndTimeoutSetAndQueryTimesoutInsteadOnServer() throws Exception {
    //verify that timeout is set correctly and cancel query 
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/*crit threshold*/, true, TEST_QUERY_TIMEOUT, false);
  }
  
  public void testRMButDisabledQueryMonitorForLowMemAndTimeoutSetOnServer()  throws Exception {
    //verify that timeout is still working properly
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/*crit threshold*/, true, TEST_QUERY_TIMEOUT, true);
  }
  
  public void testPRGatherCancellation() throws Exception {
    doCriticalMemoryHitTestWithMultipleServers("portfolios", true, 85/*crit threshold*/, false, -1, true);
  }

  public void testPRGatherCancellationWhileGatheringResults() throws Exception {
    doCriticalMemoryHitDuringGatherTestWithMultipleServers("portfolios", true, 85/*crit threshold*/, false, -1, true);
  }
  
  public void testPRGatherCancellationWhileAddingResults() throws Exception {
    doCriticalMemoryHitAddResultsTestWithMultipleServers("portfolios", true, 85/*crit threshold*/, false, -1, true);
  }
  
  public void testIndexCreationCancellationPR() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/*crit threshold*/, false, -1, true, "compact");
  }
  
  public void testIndexCreationCancellation() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", false, 85/*crit threshold*/, false, -1, true, "compact");
  }
  
  public void testIndexCreationNoCancellationPR() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/*crit threshold*/, true, -1, true, "compact");
  }
  
  public void testHashIndexCreationCancellationPR() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/*crit threshold*/, false, -1, true, "hash");
  }
  
  public void testHashIndexCreationCancellation() throws Exception {
    //need to add hook to canceled result set and very it is triggered for multiple servers
    doCriticalMemoryHitWithIndexTest("portfolios", false, 85/*crit threshold*/, false, -1, true, "hash");
  }
  
  public void testHashIndexCreationNoCancellationPR() throws Exception {
    //need to add hook to canceled result set and very it is triggered for multiple servers
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/*crit threshold*/, true, -1, true, "hash");
  }

  private void doCriticalMemoryHitTest(final String regionName, boolean createPR, final int criticalThreshold, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      startCacheServer(server, port, 
          criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      
      startClient(client, server, port, regionName);
      populateData(server, regionName, numObjects);
      
      doTestCriticalHeapAndQueryTimeout(server, client, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      
      //Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }
      
      //Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults)query.execute();
            assertEquals(numObjects, results.size());
          }
          catch (QueryInvocationTargetException e) {
            assertFalse(true);
          }
          catch (NameResolutionException e) {
            assertFalse(true);
          }
          catch (TypeMismatchException e) {
            assertFalse(true);
          }
          catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });
      
      //Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server, client, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      } 
    }
    finally {
      stopServer(server);
    } 
  }
  
  //test to verify what happens during index creation if memory threshold is hit
  private void doCriticalMemoryHitWithIndexTest(final String regionName, boolean createPR, final int criticalThreshold, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold, final String indexType)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(2);
    final VM client = host.getVM(1);
    final int numObjects = 200;
    try  {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0],  
          criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      startCacheServer(server2, port[1],  
          criticalThreshold, true, -1,
          regionName, createPR, 0);
      
      startClient(client, server1, port[0], regionName);
      populateData(server1, regionName, numObjects);
     
      createCancelDuringGatherTestHook(server1);
      server1.invoke(new SerializableCallable("create index") {
          public Object call() {
            QueryService qs = null;
            try {
              qs = getCache().getQueryService();
              Index index = null;
              if (indexType.equals("compact")) {
                index = qs.createIndex("newIndex", "ID", "/" + regionName);
              }
              else if (indexType.equals("hash")){
                index = qs.createHashIndex("newIndex", "ID", "/" + regionName);
              }
              assertNotNull(index);
              assertTrue(((CancelDuringGatherHook)DefaultQuery.testHook).triggeredOOME);
              
              if (hitCriticalThreshold && !disabledQueryMonitorForLowMem) {
                throw new CacheException("Should have hit low memory"){};
              }
              assertEquals(1, qs.getIndexes().size());
            }
            catch (Exception e) {
              if (e instanceof IndexInvalidException) {
                if (!hitCriticalThreshold || disabledQueryMonitorForLowMem) {
                  throw new CacheException("Should not have run into low memory exception"){};
                }
              }
              else {
                throw new CacheException(e){};
              }
            }
            return 0;
          }
      });
    }
    finally {
      stopServer(server1);
      stopServer(server2);
    }
  }
  
  private void doCriticalMemoryHitAddResultsTestWithMultipleServers(final String regionName, boolean createPR, final int criticalThreshold, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0],  
          criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      startCacheServer(server2, port[1],  
          criticalThreshold, true, -1,
          regionName, createPR, 0);
      
      startClient(client, server1, port[0], regionName);
      populateData(server2, regionName, numObjects);
      
      createCancelDuringAddResultsTestHook(server1);
      client.invoke(new SerializableCallable("executing query to be canceled during add results") {
        public Object call() {
          QueryService qs = null;
          try {
            qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults) query.execute();
            if (hitCriticalThreshold && disabledQueryMonitorForLowMem == false) {
              throw new CacheException("should have hit low memory"){};
            }
          }
          catch (Exception e) {
            handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
          }
          return 0;
        }
      });
      
      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      //Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
      
      //Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults)query.execute();
            assertEquals(numObjects, results.size());
          }
          catch (QueryInvocationTargetException e) {
            assertFalse(true);
          }
          catch (NameResolutionException e) {
            assertFalse(true);
          }
          catch (TypeMismatchException e) {
            assertFalse(true);
          }
          catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }    
    }
    finally {
      stopServer(server1);
      stopServer(server2);
    }
  }
  
  //tests low memory hit while gathering partition region results
  private void doCriticalMemoryHitDuringGatherTestWithMultipleServers(final String regionName, boolean createPR, final int criticalThreshold, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0],  
          criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      startCacheServer(server2, port[1],  
          criticalThreshold, true, -1,
          regionName, createPR, 0);
      
      startClient(client, server1, port[0], regionName);
      populateData(server2, regionName, numObjects);
      
      createCancelDuringGatherTestHook(server1);
      client.invoke(new SerializableCallable("executing query to be canceled by gather") {
        public Object call() {
          QueryService qs = null;
          try {
            qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            query.execute();
          }
          catch (ServerOperationException soe) {
            if (soe.getRootCause() instanceof QueryException) {
              QueryException e = (QueryException) soe.getRootCause();
              if (!isExceptionDueToLowMemory(e, CRITICAL_HEAP_USED)) {
                throw new CacheException(soe){};
              }
              else {
                return 0;
              }
            }
          }
          catch (Exception e) {
            throw new CacheException(e){};
          }
          //assertTrue(((CancelDuringGatherHook)DefaultQuery.testHook).triggeredOOME);
          throw new CacheException("should have hit low memory"){};
        }
      });
      
      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      //Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
      
      //Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults)query.execute();
            assertEquals(numObjects, results.size());
          }
          catch (QueryInvocationTargetException e) {
            assertFalse(true);
          }
          catch (NameResolutionException e) {
            assertFalse(true);
          }
          catch (TypeMismatchException e) {
            assertFalse(true);
          }
          catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }    
    }
    finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  //Executes on client cache with multiple configured servers
  private void doCriticalMemoryHitTestWithMultipleServers(final String regionName, boolean createPR, final int criticalThreshold, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final int numObjects = 200;

    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0],  
          criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      startCacheServer(server2, port[1],  
          criticalThreshold, true, -1,
          regionName, createPR, 0);
      
      startClient(client, server1, port[0], regionName);
      populateData(server2, regionName, numObjects);
      
      doTestCriticalHeapAndQueryTimeout(server1, client, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      //Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
      
      //Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults)query.execute();
            assertEquals(numObjects, results.size());
          }
          catch (QueryInvocationTargetException e) {
            assertFalse(true);
          }
          catch (NameResolutionException e) {
            assertFalse(true);
          }
          catch (TypeMismatchException e) {
            assertFalse(true);
          }
          catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });
      
      //Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server1, client, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
     //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
    }
    finally {
      stopServer(server1);
      stopServer(server2);
    }
  }
  
  //Executes the query on the server with the RM and QM configured
  private void doCriticalMemoryHitTestOnServer(final String regionName, boolean createPR, final int criticalThreshold, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold)
      throws Exception {
    //create region on the server
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final int numObjects = 200;
      try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      startCacheServer(server, port,  
          criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      
      //startPeerClient(client, server, port, regionName);
      populateData(server, regionName, numObjects);
      
      doTestCriticalHeapAndQueryTimeout(server, server, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
      
      //Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }
      
      //Check to see if query execution is ok under "normal" or "healthy" conditions
      server.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults)query.execute();
            assertEquals(numObjects, results.size());
          }
          catch (QueryInvocationTargetException e) {
            assertFalse(true);
          }
          catch (NameResolutionException e) {
            assertFalse(true);
          }
          catch (TypeMismatchException e) {
            assertFalse(true);
          }
          catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });
      
      //Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server, server, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
     
      //Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }
    }
    finally {
      stopServer(server);
    }
  }
  
  
  //This helper method will set up a test hook
  //Execute a query on the server, pause due to the test hook
  //Execute a critical heap event
  //release the test hook
  //Check to see that the query either failed due to critical heap if query monitor is not disabled
  //or it will fail due to time out, due to the sleeps we put in
  //If timeout is disabled/not set, then the query should execute just fine
  //The last part of the test is to execute another query with the system under duress and have it be rejected/cancelled if rm and qm are in use
  private void doTestCriticalHeapAndQueryTimeout(VM server, VM client, final String regionName, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold) {
    createLatchTestHook(server);
    
    AsyncInvocation queryExecution = invokeClientQuery(client, regionName, disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);
    
    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    //We simulate a low memory/critical heap percentage hit
    if (hitCriticalThreshold) {
      vmHitsCriticalHeap(server);
    }
    
    //Pause until query would time out if low memory was ignored
    try {
      Thread.sleep(MAX_TEST_QUERY_TIMEOUT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    
    //release the hook to have the query throw either a low memory or query timeout
    //unless otherwise configured
    releaseHook(server);
    
    ThreadUtils.join(queryExecution, 60000);
    //Make sure no exceptions were thrown during query testing
    try {
      assertEquals(0, queryExecution.getResult());
    }
    catch (Throwable e) {
      e.printStackTrace();
      fail();
    }
  }
    
  private AsyncInvocation invokeClientQuery(VM client, final String regionName, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold) {
    return client.invokeAsync(new SerializableCallable("execute query from client") {
      public Object call() throws CacheException {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          Query query = qs.newQuery("Select * From /" + regionName);
          query.execute();

          if (disabledQueryMonitorForLowMem) {
            if (queryTimeout != -1) {
              //we should have timed out due to the way the test is written
              //the query should have hit the configured timeouts
              throw new CacheException("Should have reached the query timeout") {};
            }
          }
          else {
            if (hitCriticalThreshold) {
              throw new CacheException("Exception should have been thrown due to low memory"){};
            }
          }
        }
        catch (Exception e) {
          handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
        }
        
        try {
          Query query = qs.newQuery("Select * From /" + regionName);
          query.execute();
          if (hitCriticalThreshold && disabledQueryMonitorForLowMem == false) {
            throw new CacheException("Low memory should still be cancelling queries"){};
          }
        }
        catch (Exception e) {
          handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
        }
        return 0;
      }
    });
    
  }
  
  private void handleException(Exception e, boolean hitCriticalThreshold, boolean disabledQueryMonitorForLowMem, long queryTimeout) throws CacheException {
    if (e instanceof QueryExecutionLowMemoryException) {
      if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
        //meaning the query should not be canceled due to low memory
         throw new CacheException("Query should not have been canceled due to memory"){};
      }
    }
    else if (e instanceof QueryExecutionTimeoutException) {
        //if we have a queryTimeout set
        if (queryTimeout == -1) {
          //no time out set, this should not be thrown
          throw new CacheException("Query failed due to unexplained reason, should not have been a time out or low memory " + DefaultQuery.testHook.getClass().getName() + " " + e ){};
        }
    }
    else if (e instanceof QueryException) {
      if (isExceptionDueToLowMemory((QueryException)e, CRITICAL_HEAP_USED)) {
        if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
          //meaning the query should not be canceled due to low memory
           throw new CacheException("Query should not have been canceled due to memory"){};
        }
      }
      else if (isExceptionDueToTimeout((QueryException)e, queryTimeout)) {

        if (queryTimeout == -1) {
          //no time out set, this should not be thrown
          throw new CacheException("Query failed due to unexplained reason, should not have been a time out or low memory"){};
        }
      }
      else {
        throw new CacheException(e){};
      }
    }
    else if (e instanceof ServerOperationException) {
      ServerOperationException soe = (ServerOperationException) e;
      if (soe.getRootCause() instanceof QueryExecutionLowMemoryException) {
        if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
          //meaning the query should not be canceled due to low memory
           throw new CacheException("Query should not have been canceled due to memory"){};
        }
      }
      else if (soe.getRootCause() instanceof QueryException) {
        QueryException qe = (QueryException) soe.getRootCause();
        if (isExceptionDueToLowMemory(qe, CRITICAL_HEAP_USED)) {
          if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
            //meaning the query should not be canceled due to low memory
             throw new CacheException("Query should not have been canceled due to memory"){};
          }
        }
        else if (isExceptionDueToTimeout(qe, queryTimeout)) {
          if (queryTimeout == -1) {
            e.printStackTrace();
            //no time out set, this should not be thrown
            throw new CacheException("Query failed due to unexplained reason, should not have been a time out or low memory"){};
          }
        }
        else {
          throw new CacheException(soe){};
        }
      }
      else if (soe.getRootCause() instanceof QueryExecutionTimeoutException) {
        //if we have a queryTimeout set
        if (queryTimeout == -1) {
          //no time out set, this should not be thrown
          throw new CacheException("Query failed due to unexplained reason, should not have been a time out or low memory " + DefaultQuery.testHook.getClass().getName() + " " + soe.getRootCause() ){};
        }
      }
      else {
        throw new CacheException(soe){};
      }
    }
    else {
      throw new CacheException(e){};
    }
  }

  
  private void vmHitsCriticalHeap(VM vm) {
    vm.invoke(new CacheSerializableRunnable("vm hits critical heap") {
      public void run2() {
        InternalResourceManager resourceManager = (InternalResourceManager) getCache().getResourceManager();
        resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED);
      }
    });
  }
  
  private void vmRecoversFromCriticalHeap(VM vm) {
    vm.invoke(new CacheSerializableRunnable("vm hits critical heap") {
      public void run2() {
        InternalResourceManager resourceManager = (InternalResourceManager) getCache().getResourceManager();
        resourceManager.getHeapMonitor().updateStateAndSendEvent(NORMAL_HEAP_USED);
      }
    });
  }
  
  private void createLatchTestHook(VM vm) {
    vm.invoke(new CacheSerializableRunnable("create latch test Hook") {
      public void run2() {
        DefaultQuery.TestHook hook = getPauseHook();
        DefaultQuery.testHook = hook;
      }
    });
  }
  
  private void createCancelDuringGatherTestHook(VM vm) {
    vm.invoke(new CacheSerializableRunnable("create cancel during gather test Hook") {
      public void run2() {
        DefaultQuery.TestHook hook = getCancelDuringGatherHook();
        DefaultQuery.testHook = hook;
      }
    });
  }
  
  private void createCancelDuringAddResultsTestHook(VM vm) {
    vm.invoke(new CacheSerializableRunnable("create cancel during gather test Hook") {
      public void run2() {
        DefaultQuery.TestHook hook = getCancelDuringAddResultsHook();
        DefaultQuery.testHook = hook;
      }
    });
  }
  
  
  private void releaseHook(VM vm) {
    vm.invoke(new CacheSerializableRunnable("release latch Hook") {
      public void run2() {
        PauseTestHook hook = (PauseTestHook) DefaultQuery.testHook;
        hook.countDown();
      }   
    });
  }
  
  //Verify that PRQueryEvaluator dropped objects if low memory
  private void verifyRejectedObjects(VM vm, final boolean disabledQueryMonitorForLowMem, final int queryTimeout, final boolean hitCriticalThreshold) {
    vm.invoke(new CacheSerializableRunnable("verify dropped objects") {
      public void run2() {
        if ((disabledQueryMonitorForLowMem == false && hitCriticalThreshold)) {
          if (DefaultQuery.testHook instanceof PauseTestHook) {
            PauseTestHook hook = (PauseTestHook) DefaultQuery.testHook;
            assertTrue(hook.rejectedObjects);
          }
          else if (DefaultQuery.testHook instanceof CancelDuringGatherHook) {
            CancelDuringGatherHook hook = (CancelDuringGatherHook) DefaultQuery.testHook;
            assertTrue(hook.rejectedObjects);
          }
          else if (DefaultQuery.testHook instanceof CancelDuringAddResultsHook) {
            CancelDuringAddResultsHook hook = (CancelDuringAddResultsHook) DefaultQuery.testHook;
            assertTrue(hook.rejectedObjects);
          }
        }
      }
      
    });
  }
  
  private void populateData(VM vm, final String regionName, final int numObjects) {
    vm.invoke(new CacheSerializableRunnable("populate data for " + regionName) {
      public void run2() {
        Region region = getCache().getRegion(regionName);
        for (int i = 0; i < numObjects; i++) {
          region.put("key_" + i, new Portfolio(i));
        }
      }
    });
  }
  
  private void stopServer(VM server) {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.TEST_MAX_QUERY_EXECUTION_TIME_OVERRIDE_EXCEPTION = false;
        cache.TEST_MAX_QUERY_EXECUTION_TIME = -1;
       return null;
      }
    });
  }

  private void startCacheServer(VM server, final int port,
      final int criticalThreshold, final boolean disableQueryMonitorForLowMemory,
      final int queryTimeout, final String regionName,
      final boolean createPR, final int prRedundancy) throws Exception {

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getServerProperties(disableQueryMonitorForLowMemory, queryTimeout));
        if (disableQueryMonitorForLowMemory == true) {
          System.setProperty("gemfire.Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY", "true");
        }
        else {
          System.clearProperty("gemfire.Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY");
        }
        
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
    
        if (queryTimeout != -1) {
          cache.TEST_MAX_QUERY_EXECUTION_TIME_OVERRIDE_EXCEPTION = true;
          cache.TEST_MAX_QUERY_EXECUTION_TIME = queryTimeout;
        }
        else {
          cache.TEST_MAX_QUERY_EXECUTION_TIME_OVERRIDE_EXCEPTION = false;
          cache.TEST_MAX_QUERY_EXECUTION_TIME = -1;
        }
                
        if (criticalThreshold != 0) {
          InternalResourceManager resourceManager = (InternalResourceManager) cache.getResourceManager();
          HeapMemoryMonitor heapMonitor = resourceManager.getHeapMonitor();
          heapMonitor.setTestMaxMemoryBytes(1000);
          HeapMemoryMonitor.setTestBytesUsedForThresholdSet(NORMAL_HEAP_USED);
          resourceManager.setCriticalHeapPercentage(criticalThreshold);
        }
        
        AttributesFactory factory = new AttributesFactory();
        if (createPR) {
          PartitionAttributesFactory paf = new PartitionAttributesFactory();
          paf.setRedundantCopies(prRedundancy);
          paf.setTotalNumBuckets(11);
          factory.setPartitionAttributes(paf.create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        } else {
          assertTrue(region instanceof DistributedRegion);
        }
        CacheServer cacheServer = getCache().addCacheServer();
        cacheServer.setPort(port);
        cacheServer.start();
       
        return null;
      }
    });
  }

  private void startClient(VM client, final VM server, final int port,
      final String regionName) {

    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        Properties props = getClientProps();
        getSystem(props);
        
        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(NetworkUtils.getServerHostName(server.getHost()), port);
        ClientCache cache = (ClientCache)getClientCache(ccf);
      }
    });
  }
  
  private void startPeerClient(VM client, final VM server, final int port,
      final String regionName) {

    client.invoke(new CacheSerializableRunnable("Start peer client") {
      public void run2() throws CacheException {
        Properties props = getClientProps();
        getSystem(props);
        
        PoolFactory pf = PoolManager.createFactory();
        pf.addServer(NetworkUtils.getServerHostName(server.getHost()), port);
        pf.create("pool1");
        
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.LOCAL);
        af.setPoolName("pool1");  
        Region region = createRootRegion(regionName, af.create());
        
        getCache();
      }
    });
  }
  
  protected Properties getClientProps() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }

  protected Properties getServerProperties(boolean disableQueryMonitorForMemory, int queryTimeout) {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.LOCATORS_NAME, "localhost["+DistributedTestUtils.getDUnitLocatorPort()+"]");
    return p;
  }
  
  private boolean isExceptionDueToLowMemory(QueryException e, int HEAP_USED) {
    String message = e.getMessage();
    return (message.contains(LocalizedStrings.QueryMonitor_LOW_MEMORY_CANCELED_QUERY.toLocalizedString(HEAP_USED)) || message.contains(LocalizedStrings.QueryMonitor_LOW_MEMORY_WHILE_GATHERING_RESULTS_FROM_PARTITION_REGION.toLocalizedString()));
  }
  
  private boolean isExceptionDueToTimeout(QueryException e, long queryTimeout) {
    String message = e.getMessage();
    //-1 needs to be matched due to client/server set up, BaseCommand uses the MAX_QUERY_EXECUTION_TIME and not the TEST_MAX_QUERY_EXECUTION_TIME
    return (message.contains("The QueryMonitor thread may be sleeping longer than") || message.contains(LocalizedStrings.QueryMonitor_LONG_RUNNING_QUERY_CANCELED.toLocalizedString(queryTimeout)) || message.contains(LocalizedStrings.QueryMonitor_LONG_RUNNING_QUERY_CANCELED.toLocalizedString(-1)));
  }
  
  private DefaultQuery.TestHook getPauseHook() {
    return new PauseTestHook();
  }
  
  private DefaultQuery.TestHook getCancelDuringGatherHook() {
    return new CancelDuringGatherHook();
  }
  
  private DefaultQuery.TestHook getCancelDuringAddResultsHook() {
    return new CancelDuringAddResultsHook();
  }
  
  private class PauseTestHook implements DefaultQuery.TestHook {
    private CountDownLatch latch = new CountDownLatch(1);
    public boolean rejectedObjects = false;

    public void doTestHook(int spot) {
      if (spot == 1) {
        try {
          if (!latch.await(8, TimeUnit.SECONDS)) {
            throw new TestException("query was never unlatched");
          }
        } catch (InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      } else if (spot == 2) {
        rejectedObjects = true;
      }
    }
    
    public void doTestHook(String description) {
      
    }

    public void countDown() {
      latch.countDown();
    }
  }
  
  private class CancelDuringGatherHook implements DefaultQuery.TestHook {
    public boolean rejectedObjects = false;
    public boolean triggeredOOME = false;
    private int count = 0;
    private int numObjectsBeforeCancel = 5;
    public void doTestHook(int spot) {
      if (spot == 2) {
        rejectedObjects = true;
      }
      else if (spot == 3) {
        if (count++ == numObjectsBeforeCancel) {
          InternalResourceManager resourceManager = (InternalResourceManager) getCache().getResourceManager();
          resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED);
          triggeredOOME = true;
        }
      }
    }
    public void doTestHook(String description) {
      
    }
  }
  
  private class CancelDuringAddResultsHook implements DefaultQuery.TestHook {
    public boolean triggeredOOME = false;
    public boolean rejectedObjects = false;
    public void doTestHook(int spot) {
      if (spot == 4) {
        if (triggeredOOME == false) {
          InternalResourceManager resourceManager = (InternalResourceManager) getCache().getResourceManager();
          resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED);
          triggeredOOME = true;
          try {
            Thread.sleep(1000);
          }
          catch(InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      else if (spot == 5) {
        rejectedObjects = true;
      }
    }
    
    public void doTestHook(String description) {
      
    }
  }
}
