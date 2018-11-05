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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryExecutionLowMemoryException;
import org.apache.geode.cache.query.QueryExecutionTimeoutException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.ClientServerTestCase;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.control.ResourceListener;
import org.apache.geode.internal.cache.control.TestMemoryThresholdListener;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class ResourceManagerWithQueryMonitorDUnitTest extends ClientServerTestCase {

  private static final Logger logger = LogService.getLogger();

  private static int MAX_TEST_QUERY_TIMEOUT = 4000;
  private static int TEST_QUERY_TIMEOUT = 1000;
  private static final int CRITICAL_HEAP_USED = 950;
  private static final int NORMAL_HEAP_USED = 500;

  @Override
  public final void postSetUpClientServerTestCase() throws Exception {
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
      InternalResourceManager irm = ((GemFireCacheImpl) getCache()).getInternalResourceManager();
      // Reset CRITICAL_UP by informing all that heap usage is now 1 byte (0 would disable).
      irm.getHeapMonitor().updateStateAndSendEvent(NORMAL_HEAP_USED);
      Set<ResourceListener> listeners = irm.getResourceListeners(ResourceType.HEAP_MEMORY);
      Iterator<ResourceListener> it = listeners.iterator();
      while (it.hasNext()) {
        ResourceListener<MemoryEvent> l = it.next();
        if (l instanceof TestMemoryThresholdListener) {
          ((TestMemoryThresholdListener) l).resetThresholdCalls();
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
      InternalCache cache = getCache();
      if (cache.getQueryMonitor() != null) {
        cache.getQueryMonitor().setLowMemory(false, 0);
      }
      DefaultQuery.testHook = null;
      return null;
    }
  };

  @Test
  public void testRMAndNoTimeoutSet() throws Exception {
    doCriticalMemoryHitTest("portfolios", false, 85/* crit threshold */, false, -1, true);
  }

  @Test
  public void testRMAndNoTimeoutSetParReg() throws Exception {
    doCriticalMemoryHitTest("portfolios", true, 85/* crit threshold */, false, -1, true);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndNoTimeoutSet() throws Exception {
    // verify that timeout is not set and that a query can execute properly
    doCriticalMemoryHitTest("portfolios", false, 85/* crit threshold */, true, -1, true);
  }

  @Test
  public void testRMAndTimeoutSet() throws Exception {
    // verify that we still receive critical heap cancelation
    doCriticalMemoryHitTest("portfolios", false, 85/* crit threshold */, true, TEST_QUERY_TIMEOUT,
        true);
  }

  @Test
  public void testRMAndTimeoutSetAndQueryTimesoutInstead() throws Exception {
    // verify that timeout is set correctly and cancel query
    doCriticalMemoryHitTest("portfolios", false, 85/* crit threshold */, true, TEST_QUERY_TIMEOUT,
        false);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndTimeoutSet() throws Exception {
    // verify that timeout is still working properly
    doCriticalMemoryHitTest("portfolios", false, 85/* crit threshold */, true, TEST_QUERY_TIMEOUT,
        true);
  }

  // Query directly on member with RM and QM set
  @Test
  public void testRMAndNoTimeoutSetOnServer() throws Exception {
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/* crit threshold */, false, -1, true);
  }

  // Query directly on member with RM and QM set
  @Test
  public void whenTimeoutIsSetAndAQueryIsExecutedThenTimeoutMustStopTheQueryBeforeCriticalMemory()
      throws Exception {
    // Timeout is set along with critical heap but it be called after the timeout expires
    // Timeout is set to 1ms which is very unrealistic time period for a query to be able to fetch
    // 200 entries from the region successfully, hence a timeout is expected.
    executeQueryWithTimeoutSetAndCriticalThreshold("portfolios", false, 85/* crit threshold */,
        false, 1, true);
  }

  @Test
  public void whenTimeoutIsSetAndAQueryIsExecutedFromClientThenTimeoutMustStopTheQueryBeforeCriticalMemory()
      throws Exception {
    // Timeout is set along with critical heap but it be called after the timeout expires
    // Timeout is set to 1ms which is very unrealistic time period for a query to be able to fetch
    // 200 entries from the region successfully, hence a timeout is expected.
    executeQueryFromClientWithTimeoutSetAndCriticalThreshold("portfolios", false,
        85/* crit threshold */, false, 1, true);
  }

  @Test
  public void testRMAndNoTimeoutSetParRegOnServer() throws Exception {
    doCriticalMemoryHitTestOnServer("portfolios", true, 85/* crit threshold */, false, -1, true);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndNoTimeoutSetOnServer() throws Exception {
    // verify that timeout is not set and that a query can execute properly
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/* crit threshold */, true, -1, true);
  }

  @Test
  public void testRMAndTimeoutSetOnServer() throws Exception {
    // verify that we still receive critical heap cancelation
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/* crit threshold */, true,
        TEST_QUERY_TIMEOUT, true);
  }

  @Test
  public void testRMAndTimeoutSetAndQueryTimesoutInsteadOnServer() throws Exception {
    // verify that timeout is set correctly and cancel query
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/* crit threshold */, true,
        TEST_QUERY_TIMEOUT, false);
  }

  @Test
  public void testRMButDisabledQueryMonitorForLowMemAndTimeoutSetOnServer() throws Exception {
    // verify that timeout is still working properly
    doCriticalMemoryHitTestOnServer("portfolios", false, 85/* crit threshold */, true,
        TEST_QUERY_TIMEOUT, true);
  }

  @Test
  public void testPRGatherCancellation() throws Exception {
    doCriticalMemoryHitTestWithMultipleServers("portfolios", true, 85/* crit threshold */, false,
        -1, true);
  }

  @Test
  public void testPRGatherCancellationWhileGatheringResults() throws Exception {
    doCriticalMemoryHitDuringGatherTestWithMultipleServers("portfolios", true,
        85/* crit threshold */, false, -1, true);
  }

  @Test
  public void testPRGatherCancellationWhileAddingResults() throws Exception {
    doCriticalMemoryHitAddResultsTestWithMultipleServers("portfolios", true, 85/* crit threshold */,
        false, -1, true);
  }

  @Test
  public void testIndexCreationCancellationPR() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/* crit threshold */, false, -1, true,
        "compact");
  }

  @Test
  public void testIndexCreationCancellation() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", false, 85/* crit threshold */, false, -1, true,
        "compact");
  }

  @Test
  public void testIndexCreationNoCancellationPR() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/* crit threshold */, true, -1, true,
        "compact");
  }

  @Test
  public void testHashIndexCreationCancellationPR() throws Exception {
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/* crit threshold */, false, -1, true,
        "hash");
  }

  @Test
  public void testHashIndexCreationCancellation() throws Exception {
    // need to add hook to canceled result set and very it is triggered for multiple servers
    doCriticalMemoryHitWithIndexTest("portfolios", false, 85/* crit threshold */, false, -1, true,
        "hash");
  }

  @Test
  public void testHashIndexCreationNoCancellationPR() throws Exception {
    // need to add hook to canceled result set and very it is triggered for multiple servers
    doCriticalMemoryHitWithIndexTest("portfolios", true, 85/* crit threshold */, true, -1, true,
        "hash");
  }

  private void doCriticalMemoryHitTest(final String regionName, boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout, final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server = VM.getVM(0);
    final VM client = VM.getVM(1);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      startCacheServer(server, port, criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);

      startClient(client, server, port, regionName);
      populateData(server, regionName, numObjects);

      doTestCriticalHeapAndQueryTimeout(server, client, regionName, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);

      // Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults) query.execute();
            assertEquals(numObjects, results.size());
          } catch (QueryInvocationTargetException e) {
            assertFalse(true);
          } catch (NameResolutionException e) {
            assertFalse(true);
          } catch (TypeMismatchException e) {
            assertFalse(true);
          } catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });

      // Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server, client, regionName, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);
      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }
    } finally {
      stopServer(server);
    }
  }

  // test to verify what happens during index creation if memory threshold is hit
  private void doCriticalMemoryHitWithIndexTest(final String regionName, boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold,
      final String indexType)
      throws Exception {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(2);
    final VM client = VM.getVM(1);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0], criticalThreshold, disabledQueryMonitorForLowMem,
          queryTimeout, regionName, createPR, 0);
      startCacheServer(server2, port[1], criticalThreshold, true, -1, regionName, createPR, 0);

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
            } else if (indexType.equals("hash")) {
              index = qs.createHashIndex("newIndex", "ID", "/" + regionName);
            }
            assertNotNull(index);
            assertTrue(((CancelDuringGatherHook) DefaultQuery.testHook).triggeredOOME);

            if (hitCriticalThreshold && !disabledQueryMonitorForLowMem) {
              throw new CacheException("Should have hit low memory") {};
            }
            assertEquals(1, qs.getIndexes().size());
          } catch (Exception e) {
            if (e instanceof IndexInvalidException) {
              if (!hitCriticalThreshold || disabledQueryMonitorForLowMem) {
                throw new CacheException("Should not have run into low memory exception") {};
              }
            } else {
              throw new CacheException(e) {};
            }
          }
          return 0;
        }
      });
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  private void doCriticalMemoryHitAddResultsTestWithMultipleServers(final String regionName,
      boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client = VM.getVM(2);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0], criticalThreshold, disabledQueryMonitorForLowMem,
          queryTimeout, regionName, createPR, 0);
      startCacheServer(server2, port[1], criticalThreshold, true, -1, regionName, createPR, 0);

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
              throw new CacheException("should have hit low memory") {};
            }
          } catch (Exception e) {
            handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
          }
          return 0;
        }
      });

      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout,
          hitCriticalThreshold);
      // Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults) query.execute();
            assertEquals(numObjects, results.size());
          } catch (QueryInvocationTargetException e) {
            assertFalse(true);
          } catch (NameResolutionException e) {
            assertFalse(true);
          } catch (TypeMismatchException e) {
            assertFalse(true);
          } catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  // tests low memory hit while gathering partition region results
  private void doCriticalMemoryHitDuringGatherTestWithMultipleServers(final String regionName,
      boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client = VM.getVM(2);
    final int numObjects = 200;
    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0], criticalThreshold, disabledQueryMonitorForLowMem,
          queryTimeout, regionName, createPR, 0);
      startCacheServer(server2, port[1], criticalThreshold, true, -1, regionName, createPR, 0);

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
          } catch (ServerOperationException soe) {
            if (soe.getRootCause() instanceof QueryException) {
              QueryException e = (QueryException) soe.getRootCause();
              if (!isExceptionDueToLowMemory(e, CRITICAL_HEAP_USED)) {
                throw new CacheException(soe) {};
              } else {
                return 0;
              }
            }
          } catch (Exception e) {
            throw new CacheException(e) {};
          }
          // assertTrue(((CancelDuringGatherHook)DefaultQuery.testHook).triggeredOOME);
          throw new CacheException("should have hit low memory") {};
        }
      });

      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout,
          hitCriticalThreshold);
      // Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults) query.execute();
            assertEquals(numObjects, results.size());
          } catch (QueryInvocationTargetException e) {
            assertFalse(true);
          } catch (NameResolutionException e) {
            assertFalse(true);
          } catch (TypeMismatchException e) {
            assertFalse(true);
          } catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  // Executes on client cache with multiple configured servers
  private void doCriticalMemoryHitTestWithMultipleServers(final String regionName, boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server1 = VM.getVM(0);
    final VM server2 = VM.getVM(1);
    final VM client = VM.getVM(2);
    final int numObjects = 200;

    try {
      final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
      startCacheServer(server1, port[0], criticalThreshold, disabledQueryMonitorForLowMem,
          queryTimeout, regionName, createPR, 0);
      startCacheServer(server2, port[1], criticalThreshold, true, -1, regionName, createPR, 0);

      startClient(client, server1, port[0], regionName);
      populateData(server2, regionName, numObjects);

      doTestCriticalHeapAndQueryTimeout(server1, client, regionName, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);
      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout,
          hitCriticalThreshold);
      // Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      client.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults) query.execute();
            assertEquals(numObjects, results.size());
          } catch (QueryInvocationTargetException e) {
            assertFalse(true);
          } catch (NameResolutionException e) {
            assertFalse(true);
          } catch (TypeMismatchException e) {
            assertFalse(true);
          } catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });

      // Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server1, client, regionName, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);
      verifyRejectedObjects(server1, disabledQueryMonitorForLowMem, queryTimeout,
          hitCriticalThreshold);
      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server1);
      }
    } finally {
      stopServer(server1);
      stopServer(server2);
    }
  }

  // Executes the query on the server with the RM and QM configured
  private void doCriticalMemoryHitTestOnServer(final String regionName, boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server = VM.getVM(0);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      startCacheServer(server, port, criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);

      // startPeerClient(client, server, port, regionName);
      populateData(server, regionName, numObjects);

      doTestCriticalHeapAndQueryTimeout(server, server, regionName, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);

      // Pause for a second and then let's recover
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }

      // Check to see if query execution is ok under "normal" or "healthy" conditions
      server.invoke(new CacheSerializableRunnable("Executing query when system is 'Normal'") {
        public void run2() {
          try {
            QueryService qs = getCache().getQueryService();
            Query query = qs.newQuery("Select * From /" + regionName);
            SelectResults results = (SelectResults) query.execute();
            assertEquals(numObjects, results.size());
          } catch (QueryInvocationTargetException e) {
            assertFalse(true);
          } catch (NameResolutionException e) {
            assertFalse(true);
          } catch (TypeMismatchException e) {
            assertFalse(true);
          } catch (FunctionDomainException e) {
            assertFalse(true);
          }
        }
      });

      // Execute a critical heap event/ query timeout test again
      doTestCriticalHeapAndQueryTimeout(server, server, regionName, disabledQueryMonitorForLowMem,
          queryTimeout, hitCriticalThreshold);

      // Recover from critical heap
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }
    } finally {
      stopServer(server);
    }
  }


  private void executeQueryFromClientWithTimeoutSetAndCriticalThreshold(final String regionName,
      boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server = VM.getVM(0);
    final VM client = VM.getVM(1);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      startCacheServer(server, port, criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      startClient(client, server, port, regionName);
      populateData(server, regionName, numObjects);
      executeQueryWithCriticalHeapCalledAfterTimeout(server, client, regionName, queryTimeout,
          hitCriticalThreshold);
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }

    } finally {
      stopServer(server);
    }
  }

  private void executeQueryWithTimeoutSetAndCriticalThreshold(final String regionName,
      boolean createPR,
      final int criticalThreshold,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold)
      throws Exception {
    // create region on the server
    final VM server = VM.getVM(0);
    final int numObjects = 200;
    try {
      final int port = AvailablePortHelper.getRandomAvailableTCPPort();
      startCacheServer(server, port, criticalThreshold, disabledQueryMonitorForLowMem, queryTimeout,
          regionName, createPR, 0);
      populateData(server, regionName, numObjects);
      executeQueryWithCriticalHeapCalledAfterTimeout(server, server, regionName, queryTimeout,
          hitCriticalThreshold);
      if (hitCriticalThreshold) {
        vmRecoversFromCriticalHeap(server);
      }

    } finally {
      stopServer(server);
    }
  }

  // This helper method will set up a test hook
  // Execute a query on the server, pause due to the test hook
  // Execute a critical heap event
  // release the test hook
  // Check to see that the query either failed due to critical heap if query monitor is not disabled
  // or it will fail due to time out, due to the sleeps we put in
  // If timeout is disabled/not set, then the query should execute just fine
  // The last part of the test is to execute another query with the system under duress and have it
  // be rejected/cancelled if rm and qm are in use
  private void doTestCriticalHeapAndQueryTimeout(VM server, VM client, final String regionName,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold) {
    createLatchTestHook(server);

    AsyncInvocation queryExecution = invokeClientQuery(client, regionName,
        disabledQueryMonitorForLowMem, queryTimeout, hitCriticalThreshold);

    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    // We simulate a low memory/critical heap percentage hit
    if (hitCriticalThreshold) {
      vmHitsCriticalHeap(server);
    }

    // Pause until query would time out if low memory was ignored
    try {
      Thread.sleep(MAX_TEST_QUERY_TIMEOUT);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // release the hook to have the query throw either a low memory or query timeout
    // unless otherwise configured
    releaseHook(server);

    ThreadUtils.join(queryExecution, 60000);
    // Make sure no exceptions were thrown during query testing
    try {
      assertEquals(0, queryExecution.getResult());
    } catch (Throwable e) {
      e.printStackTrace();
      fail("queryExecution.getResult() threw Exception " + e.toString());
    }
  }

  private void executeQueryWithCriticalHeapCalledAfterTimeout(VM server, VM client,
      final String regionName,
      final int queryTimeout,
      final boolean hitCriticalThreshold) {
    createLatchTestHook(server);
    AsyncInvocation queryExecution = executeQueryWithTimeout(client, regionName, queryTimeout);

    // Wait till the timeout expires on the query
    try {
      Thread.sleep(queryTimeout + 1000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    // We simulate a low memory/critical heap percentage hit
    // But by design of this test the query must have been already terminated because of a 1ms
    // timeout
    if (hitCriticalThreshold) {
      vmHitsCriticalHeap(server);
    }

    releaseHook(server);

    ThreadUtils.join(queryExecution, 60000);
    // Make sure no exceptions were thrown during query testing
    try {
      assertEquals(0, queryExecution.getResult());
    } catch (Throwable e) {
      e.printStackTrace();
      fail("queryExecution.getResult() threw Exception " + e.toString());
    }
  }

  private AsyncInvocation executeQueryWithTimeout(VM client, final String regionName,
      final int queryTimeout) {
    return client.invokeAsync(new SerializableCallable("execute query from client") {
      public Object call() throws CacheException {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          Query query = qs.newQuery("Select * From /" + regionName);
          query.execute();

        } catch (Exception e) {
          e.printStackTrace();
          if (e instanceof QueryExecutionTimeoutException) {
            logger.info("Query Execution must be terminated by a timeout.");
            return 0;
          }
          if (e instanceof ServerOperationException) {
            ServerOperationException soe = (ServerOperationException) e;
            if (soe.getRootCause() instanceof QueryException) {
              QueryException qe = (QueryException) soe.getRootCause();
              if (isExceptionDueToTimeout(qe)) {
                logger.info("Query Execution must be terminated by a timeout. Expected behavior");
                return 0;
              }
            } else if (soe.getRootCause() instanceof QueryExecutionTimeoutException) {
              logger.info("Query Execution must be terminated by a timeout.");
              return 0;
            }
          }
          e.printStackTrace();
          throw new CacheException(
              "The query should have been terminated by a timeout exception but instead hit a different exception :"
                  + e) {};
        }
        return -1;
      }
    });

  }

  private AsyncInvocation invokeClientQuery(VM client, final String regionName,
      final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout,
      final boolean hitCriticalThreshold) {
    return client.invokeAsync(new SerializableCallable("execute query from client") {
      public Object call() throws CacheException {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          Query query = qs.newQuery("Select * From /" + regionName);
          query.execute();

          if (disabledQueryMonitorForLowMem) {
            if (queryTimeout != -1) {
              // we should have timed out due to the way the test is written
              // the query should have hit the configured timeouts
              throw new CacheException("Should have reached the query timeout") {};
            }
          } else {
            if (hitCriticalThreshold) {
              throw new CacheException("Exception should have been thrown due to low memory") {};
            }
          }
        } catch (Exception e) {
          handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
        }

        try {
          Query query = qs.newQuery("Select * From /" + regionName);
          query.execute();
          if (hitCriticalThreshold && disabledQueryMonitorForLowMem == false) {
            throw new CacheException("Low memory should still be cancelling queries") {};
          }
        } catch (Exception e) {
          handleException(e, hitCriticalThreshold, disabledQueryMonitorForLowMem, queryTimeout);
        }
        return 0;
      }
    });

  }

  private void handleException(Exception e, boolean hitCriticalThreshold,
      boolean disabledQueryMonitorForLowMem, long queryTimeout)
      throws CacheException {
    if (e instanceof QueryExecutionLowMemoryException) {
      if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
        // meaning the query should not be canceled due to low memory
        throw new CacheException("Query should not have been canceled due to memory") {};
      }
    } else if (e instanceof QueryExecutionTimeoutException) {
      // if we have a queryTimeout set
      if (queryTimeout == -1) {
        // no time out set, this should not be thrown
        throw new CacheException(
            "Query failed due to unexplained reason, should not have been a time out or low memory "
                + DefaultQuery.testHook.getClass().getName() + " " + e) {};
      }
    } else if (e instanceof QueryException) {
      if (isExceptionDueToLowMemory((QueryException) e, CRITICAL_HEAP_USED)) {
        if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
          // meaning the query should not be canceled due to low memory
          throw new CacheException("Query should not have been canceled due to memory") {};
        }
      } else if (isExceptionDueToTimeout((QueryException) e)) {

        if (queryTimeout == -1) {
          // no time out set, this should not be thrown
          throw new CacheException(
              "Query failed due to unexplained reason, should not have been a time out or low memory") {};
        }
      } else {
        throw new CacheException(e) {};
      }
    } else if (e instanceof ServerOperationException) {
      ServerOperationException soe = (ServerOperationException) e;
      if (soe.getRootCause() instanceof QueryExecutionLowMemoryException) {
        if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
          // meaning the query should not be canceled due to low memory
          throw new CacheException("Query should not have been canceled due to memory") {};
        }
      } else if (soe.getRootCause() instanceof QueryException) {
        QueryException qe = (QueryException) soe.getRootCause();
        if (isExceptionDueToLowMemory(qe, CRITICAL_HEAP_USED)) {
          if (!(hitCriticalThreshold && disabledQueryMonitorForLowMem == false)) {
            // meaning the query should not be canceled due to low memory
            throw new CacheException("Query should not have been canceled due to memory") {};
          }
        } else if (isExceptionDueToTimeout(qe)) {
          if (queryTimeout == -1) {
            e.printStackTrace();
            // no time out set, this should not be thrown
            throw new CacheException(
                "Query failed due to unexplained reason, should not have been a time out or low memory") {};
          }
        } else {
          throw new CacheException(soe) {};
        }
      } else if (soe.getRootCause() instanceof QueryExecutionTimeoutException) {
        // if we have a queryTimeout set
        if (queryTimeout == -1) {
          // no time out set, this should not be thrown
          throw new CacheException(
              "Query failed due to unexplained reason, should not have been a time out or low memory "
                  + DefaultQuery.testHook.getClass().getName() + " " + soe.getRootCause()) {};
        }
      } else {
        throw new CacheException(soe) {};
      }
    } else {
      throw new CacheException(e) {};
    }
  }


  private void vmHitsCriticalHeap(VM vm) {
    vm.invoke(new CacheSerializableRunnable("vm hits critical heap") {
      public void run2() {
        InternalResourceManager resourceManager =
            (InternalResourceManager) getCache().getResourceManager();
        resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED);
      }
    });
  }

  private void vmRecoversFromCriticalHeap(VM vm) {
    vm.invoke(new CacheSerializableRunnable("vm hits critical heap") {
      public void run2() {
        InternalResourceManager resourceManager =
            (InternalResourceManager) getCache().getResourceManager();
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

  // Verify that PRQueryEvaluator dropped objects if low memory
  private void verifyRejectedObjects(VM vm, final boolean disabledQueryMonitorForLowMem,
      final int queryTimeout, final boolean hitCriticalThreshold) {
    vm.invoke(new CacheSerializableRunnable("verify dropped objects") {
      public void run2() {
        if ((disabledQueryMonitorForLowMem == false && hitCriticalThreshold)) {
          if (DefaultQuery.testHook instanceof PauseTestHook) {
            PauseTestHook hook = (PauseTestHook) DefaultQuery.testHook;
            assertTrue(hook.rejectedObjects);
          } else if (DefaultQuery.testHook instanceof CancelDuringGatherHook) {
            CancelDuringGatherHook hook = (CancelDuringGatherHook) DefaultQuery.testHook;
            assertTrue(hook.rejectedObjects);
          } else if (DefaultQuery.testHook instanceof CancelDuringAddResultsHook) {
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
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        cache.MAX_QUERY_EXECUTION_TIME = -1;
        return null;
      }
    });
  }

  private void startCacheServer(VM server, final int port, final int criticalThreshold,
      final boolean disableQueryMonitorForLowMemory,
      final int queryTimeout,
      final String regionName, final boolean createPR,
      final int prRedundancy) throws Exception {

    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getServerProperties(disableQueryMonitorForLowMemory, queryTimeout));
        if (disableQueryMonitorForLowMemory == true) {
          System.setProperty(
              DistributionConfig.GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY",
              "true");
        } else {
          System.clearProperty(
              DistributionConfig.GEMFIRE_PREFIX + "Cache.DISABLE_QUERY_MONITOR_FOR_LOW_MEMORY");
        }

        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();

        if (queryTimeout != -1) {
          cache.MAX_QUERY_EXECUTION_TIME = queryTimeout;
        } else {
          cache.MAX_QUERY_EXECUTION_TIME = -1;
        }

        if (criticalThreshold != 0) {
          InternalResourceManager resourceManager =
              (InternalResourceManager) cache.getResourceManager();
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

  private void startClient(VM client, final VM server, final int port, final String regionName) {

    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        Properties props = getClientProps();
        getSystem(props);

        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(NetworkUtils.getServerHostName(server.getHost()), port);
        ClientCache cache = (ClientCache) getClientCache(ccf);
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
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    return p;
  }

  protected Properties getServerProperties(boolean disableQueryMonitorForMemory, int queryTimeout) {
    Properties p = new Properties();
    p.setProperty(LOCATORS, "localhost[" + DistributedTestUtils.getDUnitLocatorPort() + "]");
    return p;
  }

  private boolean isExceptionDueToLowMemory(QueryException e, int HEAP_USED) {
    String message = e.getMessage();
    return (message.contains(
        String.format(
            "Query execution canceled due to memory threshold crossed in system, memory used: %s bytes.",
            HEAP_USED))
        || message.contains(
            "Query execution canceled due to low memory while gathering results from partitioned regions"));
  }

  private boolean isExceptionDueToTimeout(QueryException e) {
    String message = e.getMessage();
    // -1 needs to be matched due to client/server set up, BaseCommand uses the
    // MAX_QUERY_EXECUTION_TIME and not the testMaxQueryExecutionTime
    return (message.contains("The QueryMonitor thread may be sleeping longer than")
        || message.contains("Query execution canceled after exceeding max execution time")
        || message.contains(
            String.format("Query execution canceled after exceeding max execution time %sms.",
                -1)));
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

  private static class PauseTestHook implements DefaultQuery.TestHook {
    private CountDownLatch latch = new CountDownLatch(1);
    public boolean rejectedObjects = false;

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored) {
      switch (spot) {
        case BEFORE_QUERY_EXECUTION:
          try {
            if (!latch.await(8, TimeUnit.SECONDS)) {
              fail("query was never unlatched");
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
          }
          break;
        case LOW_MEMORY_WHEN_DESERIALIZING_STREAMINGOPERATION:
          rejectedObjects = true;
          break;
      }
    }

    public void countDown() {
      latch.countDown();
    }
  }

  // non-static class because it needs to call getCache()
  private class CancelDuringGatherHook implements DefaultQuery.TestHook {
    public boolean rejectedObjects = false;
    public boolean triggeredOOME = false;
    private int count = 0;
    private int numObjectsBeforeCancel = 5;

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored) {
      switch (spot) {
        case LOW_MEMORY_WHEN_DESERIALIZING_STREAMINGOPERATION:
          rejectedObjects = true;
          break;
        case BEFORE_ADD_OR_UPDATE_MAPPING_OR_DESERIALIZING_NTH_STREAMINGOPERATION:
          if (count++ == numObjectsBeforeCancel) {
            InternalResourceManager resourceManager =
                (InternalResourceManager) getCache().getResourceManager();
            resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED);
            triggeredOOME = true;
          }
          break;
      }
    }
  }

  // non-static class because it needs to call getCache()
  private class CancelDuringAddResultsHook implements DefaultQuery.TestHook {
    public boolean triggeredOOME = false;
    public boolean rejectedObjects = false;

    @Override
    public void doTestHook(final SPOTS spot, final DefaultQuery _ignored) {
      switch (spot) {
        case BEFORE_BUILD_CUMULATIVE_RESULT:
          if (triggeredOOME == false) {
            InternalResourceManager resourceManager =
                (InternalResourceManager) getCache().getResourceManager();
            resourceManager.getHeapMonitor().updateStateAndSendEvent(CRITICAL_HEAP_USED);
            triggeredOOME = true;
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
          break;
        case BEFORE_THROW_QUERY_CANCELED_EXCEPTION:
          rejectedObjects = true;
          break;
      }
    }
  }
}
