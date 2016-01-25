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
package com.gemstone.gemfire.internal.cache;

import java.io.Serializable;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.control.RebalanceResults;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.TypeMismatchException;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.PartitionedIndex;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionMessageObserver;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.partitioned.QueryMessage;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author dsmith
 *
 */
public class PartitionedRegionQueryDUnitTest extends CacheTestCase {
  
  /**
   * @param name
   */
  public PartitionedRegionQueryDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }
  
  private static final AtomicReference<RebalanceResults> rebalanceResults = new AtomicReference<RebalanceResults>();

  public void testReevaluationDueToUpdateInProgress() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    //VM vm2 = host.getVM(2);
        
    createPR(vm0);    
    createPR(vm1);
    createIndex(vm0, "compactRangeIndex", "entry.value", "/region.entrySet entry");

    //Do Puts
    vm0.invoke(new SerializableRunnable("putting data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        for(int i =0; i < 100; i++) {
          region.put(i, new TestObject(i));
        }
      }
    });
    
    vm0.invoke(new SerializableRunnable("resetting sqt") {
      public void run() {        
        IndexManager.setIndexBufferTime(Long.MAX_VALUE, Long.MAX_VALUE);
      }
    });
    
    vm1.invoke(new SerializableRunnable("resetting sqt") {
      public void run() {
        IndexManager.setIndexBufferTime(Long.MAX_VALUE, Long.MAX_VALUE);
      }
    });
    
    vm0.invoke(new SerializableRunnable("query") {
      public void run() {
        try {
          QueryService qs = getCache().getQueryService();
          qs.newQuery("SELECT DISTINCT entry.key, entry.value FROM /region.entrySet entry WHERE entry.value.score >= 5 AND entry.value.score <= 10 ORDER BY value asc").execute();
        }
        catch (QueryInvocationTargetException e) {
          e.printStackTrace();
          fail (e.toString());
        }
        catch (NameResolutionException e) {
          fail (e.toString());

        }
        catch (TypeMismatchException e) {
          fail (e.toString());

        }
        catch (FunctionDomainException e) {
          fail (e.toString());

        }
        
      }
    });    
  }
  
  /**
   * Test of bug 43102.
   * 1. Buckets are created on several nodes
   * 2. A query is started
   * 3. While the query is executing, several buckets are
   * moved.
   */
  public void testRebalanceDuringQueryEvaluation() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    createAccessor(vm0);
    
    createPR(vm1);
    
    createBuckets(vm1);
    
    createPR(vm2);
    
    //Add a listener that will trigger a rebalance
    //as soon as the query arrives on this node.
    vm1.invoke(new SerializableRunnable("add listener") {
      
      public void run() {
        DistributionMessageObserver.setInstance(new DistributionMessageObserver() {

          @Override
          public void beforeProcessMessage(DistributionManager dm,
              DistributionMessage message) {
            if(message instanceof QueryMessage) {
              RebalanceOperation rebalance = getCache().getResourceManager().createRebalanceFactory().start();
              //wait for the rebalance
              try {
                rebalanceResults.compareAndSet(null, rebalance.getResults());
              } catch (CancellationException e) {
                //ignore
              } catch (InterruptedException e) {
              //ignore
              }
            }
          }
        });
        
      }
    });
    
    executeQuery(vm0);
    
    vm1.invoke(new SerializableRunnable("check rebalance happened") {
      
      public void run() {
        assertNotNull(rebalanceResults.get());
        assertEquals(5, rebalanceResults.get().getTotalBucketTransfersCompleted());
      }
    });
  }
  
  /**
   * Test of bug 50749
   * 1. Indexes and Buckets are created on several nodes
   * 2. Buckets are moved
   * 3. Check to make sure we don't have lingering bucket indexes with bucket regions already destroyed
   */
  public void testRebalanceWithIndex() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    createAccessor(vm0);
    
    createPR(vm1);
    createPR(vm2);
    createIndex(vm1, "prIndex", "r.score", "/region r");
    
    //Do Puts
    vm1.invoke(new SerializableRunnable("putting data") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        for(int i =0; i < 2000; i++) {
          region.put(i, new TestObject(i));
        }
      }
    });
    
    createPR(vm3);
    
    //Rebalance
    vm1.invoke(new SerializableRunnable("rebalance") {
      public void run() {
              RebalanceOperation rebalance = getCache().getResourceManager().createRebalanceFactory().start();
              //wait for the rebalance
              try {
                rebalance.getResults();
              } catch (CancellationException e) {
                //ignore
              } catch (InterruptedException e) {
              //ignore
              }
      }
    });
       
    checkForLingeringBucketIndexes(vm1, "prIndex");
    checkForLingeringBucketIndexes(vm2, "prIndex");
  
    closeCache(vm1, vm2, vm3, vm0);
  }
  
  /**
   * tests trace for pr queries when <trace> is used and query verbose is set to true on local and remote servers
   */
  public void testPartitionRegionDebugMessageQueryTraceOnBothServers() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
    
    try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "<trace> select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      //verify hooks
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;          
          assertTrue(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertTrue(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  
  
  /**
   * tests trace for pr queries when <trace> is used and query verbose is set to true on local but false on remote servers
   * All flags should be true still as the <trace> is OR'd with query verbose flag
   */
  public void testPartitionRegionDebugMessageQueryTraceOnLocalServerOnly() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
      try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "<trace> select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;        
          assertTrue(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertTrue(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  
  
  /**
   * tests trace for pr queries when <trace> is NOT used and query verbose is set to true on local but false on remote
   * The remote should not send a pr query trace info back because trace was not requested
   */
  public void testPartitionRegionDebugMessageQueryTraceOffLocalServerVerboseOn() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
    try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertNull(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertNull(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertNull(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  
  
  /**
   * tests trace for pr queries when <trace> is NOT used and query verbose is set to false on local but true on remote servers
   * We don't output the string or do anything on the local side, but we still pull off the object due to the remote server
   * generating and sending it over
   */
  public void testPartitionRegionDebugMessageQueryTraceOffRemoteServerOnly() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
    try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      //verify hooks
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertNull(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertNull(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertTrue(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  
  
  /**
   * tests trace for pr queries when <trace> is used and query verbose is set to false on local and remote servers
   * trace is OR'd so the entire trace process should be invoked
   */
  public void testPartitionRegionDebugMessageQueryTraceOnRemoteServerOnly() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
      try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "<trace> select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      //verify hooks
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertTrue(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
	   
  
  /**
   * tests trace for pr queries when <trace> is NOT used and query verbose is set to false on local but true remote servers
   * The local node still receives the pr trace info from the remote node due to query verbose being on
   * however nothing is used on the local side
   */
  public void testPartitionRegionDebugMessageQueryTraceOffRemoteServerOn() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
    try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = true;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      //verify hooks
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertNull(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertNull(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertTrue(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  
  
  /**
   * tests trace for pr queries when <trace> is NOT used and query verbose is set to false on local and remote servers
   * None of our hooks should have triggered
   */
  public void testPartitionRegionDebugMessageQueryTraceOffQueryVerboseOff() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
    try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      //verify hooks
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertNull(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertNull(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertNull(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertNull(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertNull(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  
  /**
   * tests trace for pr queries when <trace> is used and query verbose is set to false on local and remote servers
   * All hooks should have triggered due to trace being used
   */
  public void testPartitionRegionDebugMessageQueryTraceOnQueryVerboseOff() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    createAccessor(vm0);
    createPR(vm1);
    createPR(vm2);
    createBuckets(vm1);
    
    final PRQueryTraceTestHook server1TestHook = new PRQueryTraceTestHook();
    final PRQueryTraceTestHook server2TestHook = new PRQueryTraceTestHook();
    try {
      vm1.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server1TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm2.invoke(new SerializableRunnable() {
        public void run() {
           DefaultQuery.testHook = server2TestHook;
           DefaultQuery.QUERY_VERBOSE = false;
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          Query query = cache.getQueryService().newQuery(
              "<trace> select * from /region r where r > 0");
          try {
            SelectResults results = (SelectResults) query.execute();
            assertEquals(
                new HashSet(Arrays.asList(new Integer[] { 1, 2, 3, 4, 5, 6, 7, 8,
                    9 })), results.asSet());
  
          } catch (Exception e) {
            fail("Bad query", e);
          }
        }
      });
      
      vm1.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server1TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server1TestHook.getHooks().get("Pull off PR Query Trace Info"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace String"));
          assertTrue(server1TestHook.getHooks().get("Create PR Query Trace Info From Local Node"));
        }
      });
      vm2.invoke(new SerializableRunnable() {
        public void run() {
          PRQueryTraceTestHook server2TestHook = (PRQueryTraceTestHook) DefaultQuery.testHook;
          assertTrue(server2TestHook.getHooks().get("Populating Trace Info for Remote Query"));
          assertTrue(server2TestHook.getHooks().get("Create PR Query Trace Info for Remote Query"));
        }
      });
    }
    finally {
      setQueryVerbose(false, vm1, vm2);
    }
  }
  

  public void testOrderByOnPRWithReservedKeywords() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final String regionName = "region1";

    final String[] queries = { 
        "select distinct * from /" + regionName + " order by \"date\"",
        "select distinct \"date\" from /" + regionName + " order by \"date\"",
        "select distinct * from /" + regionName + " order by \"time\"",
        "select distinct \"time\" from /" + regionName + " order by \"time\"",
        "select distinct * from /" + regionName + " order by \"timestamp\"",
        "select distinct \"timestamp\" from /" + regionName + " order by \"timestamp\"",
        "select distinct \"date\" from /" + regionName + " order by \"date\".\"score\"",
        "select distinct * from /" + regionName + " order by nested.\"date\"",
        "select distinct * from /" + regionName + " order by nested.\"date\".nonKeyword",
        "select distinct * from /" + regionName + " order by nested.\"date\".\"date\"",
        "select distinct * from /" + regionName + " order by nested.\"date\".\"date\".score",
        };

    // Start server1
    final Integer port1 = (Integer) server1.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        String jsonCustomer = "{"
            + "\"firstName\": \"John\","
            + "\"lastName\": \"Smith\","
            + " \"age\": 25,"
            + " \"date\":"+" \""+ new java.util.Date()+"\","
            + " \"time\":"+" \""+ new java.sql.Time(1000)+"\","
            + " \"timestamp\":"+" \""+ new java.sql.Timestamp(1000)+"\""
            + "}";
         
         String jsonCustomer1 = "{"
            + "\"firstName\": \"John1\","
            + "\"lastName\": \"Smith1\","
            + " \"age\": 25,"
            + " \"date\":"+" \""+ new java.util.Date()+"\","
            + " \"time\":"+" \""+ new java.sql.Time(1000)+"\","
            + " \"timestamp\":"+" \""+ new java.sql.Timestamp(1000)+"\""
            + "}";
         
          String jsonCustomer2 = "{"
            + "\"firstName\": \"John2\","
            + "\"lastName\": \"Smith2\","
            + " \"age\": 25,"
            + " \"date\":"+" \""+ new java.util.Date()+"\","
            + " \"time\":"+" \""+ new java.sql.Time(1000)+"\","
            + " \"timestamp\":"+" \""+ new java.sql.Timestamp(1000)+"\""
            + "}";
          String jsonCustomer3 = "{"
              + "\"firstName\": \"John3\","
              + "\"lastName\": \"Smith3\","
              + " \"age\": 25,"
              + " \"date\":"+" \""+ new TestObject(1) +"\","
              + " \"time\":"+" \""+ new java.sql.Time(1000)+"\","
              + " \"timestamp\":"+" \""+ new java.sql.Timestamp(1000)+"\""
              + "}";
          String jsonCustomer4 = "{"
              + "\"firstName\": \"John4\","
              + "\"lastName\": \"Smith4\","
              + " \"age\": 25,"
              + " \"date\":"+" \""+ new TestObject(1) +"\","
              + " \"time\":"+" \""+ new java.sql.Time(1000)+"\","
              + " \"timestamp\":"+" \""+ new java.sql.Timestamp(1000)+"\","
              + " \"nested\":"+" \""+ new NestedKeywordObject(1) +"\""
              + "}";
          String jsonCustomer5 = "{"
              + "\"firstName\": \"John5\","
              + "\"lastName\": \"Smith5\","
              + " \"age\": 25,"
              + " \"date\":"+" \""+ new TestObject(1) +"\","
              + " \"time\":"+" \""+ new java.sql.Time(1000)+"\","
              + " \"timestamp\":"+" \""+ new java.sql.Timestamp(1000)+"\","
              + " \"nested\":"+" \""+ new NestedKeywordObject(new NestedKeywordObject(new TestObject(1))) +"\""
              + "}";

        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regionName);

        r1.put("jsondoc", JSONFormatter.fromJSON(jsonCustomer));
        r1.put("jsondoc1", JSONFormatter.fromJSON(jsonCustomer1));
        r1.put("jsondoc2", JSONFormatter.fromJSON(jsonCustomer2));
        r1.put("jsondoc3", JSONFormatter.fromJSON(jsonCustomer3));
        r1.put("jsondoc4", JSONFormatter.fromJSON(jsonCustomer4));
        r1.put("jsondoc5", JSONFormatter.fromJSON(jsonCustomer5));

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final Integer port2 = (Integer) server2.invoke(new SerializableCallable(
        "Create Server2") {
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

    client.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(server1.getHost()), port1);
        cf.addPoolServer(getServerHostName(server2.getHost()), port2);
        ClientCache cache = getClientCache(cf);

        Region region = cache.createClientRegionFactory(
            ClientRegionShortcut.CACHING_PROXY).create(regionName);
        QueryService qs = null;
        SelectResults sr = null;

        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }

        for (int i = 0; i < queries.length; i++) {
          try {
            sr = (SelectResults) qs.newQuery(queries[i]).execute();
            assertTrue("Size of resultset should be greater than 0 for query: "
             + queries[i], sr.size() > 0);
          } catch (Exception e) {
            fail("Failed executing query ", e);
          }
        }
        return null;
      }
    });

    this.closeClient(server1);
    this.closeClient(server2);
    this.closeClient(client);

  }
  
  /* Close Client */
  public void closeClient(VM client) {
    SerializableRunnable closeCache = new CacheSerializableRunnable(
        "Close Client") {
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
  
  private void executeQuery(VM vm0) {
    vm0.invoke(new SerializableRunnable() {
      
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        Query query = cache.getQueryService().newQuery("select * from /region r where r > 0");
        try {
          SelectResults results = (SelectResults) query.execute();
          assertEquals(new HashSet(Arrays.asList(new Integer[] { 1, 2, 3 ,4, 5, 6, 7, 8, 9 })), results.asSet());
        } catch (Exception e) {
          fail("Bad query", e);
        }
      }
    });
  }
  
  private void checkForLingeringBucketIndexes(VM vm, final String indexName) {
    vm.invoke(new SerializableRunnable() {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        QueryService qs = cache.getQueryService();
        PartitionedIndex index = (PartitionedIndex) qs.getIndex(region,
            indexName);
        Iterator iterator = index.getBucketIndexes().iterator();
        int numBucketIndexes = index.getBucketIndexes().size();
        while (iterator.hasNext()) {
          Index bucketIndex = (Index) iterator.next();
          assertFalse(((LocalRegion) bucketIndex.getRegion()).isDestroyed());
        }
      }
    });
  }

  private void createBuckets(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion("region");
        for(int i =0; i < 10; i++) {
          region.put(i, i);
        }
      }
    });
  }
  
  private void createPR(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(paf.create()).create("region");
      }
    });
  }

  private void createAccessor(VM vm) {
    vm.invoke(new SerializableRunnable("create accessor") {
      
      public void run() {
        Cache cache = getCache();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(10);
        paf.setLocalMaxMemory(0);
        cache.createRegionFactory(RegionShortcut.PARTITION_PROXY)
          .setPartitionAttributes(paf.create())
          .create("region");
      }
    });
  }
  
  private void createIndex(VM vm, final String indexName, final String indexedExpression, final String regionPath) {
    vm.invoke(new SerializableRunnable("create index") {
      public void run() {
        try {
          Cache cache = getCache();
          cache.getQueryService().createIndex(indexName, indexedExpression, regionPath);
        }
        catch (RegionNotFoundException e) {
          fail(e.toString());
        }
        catch (IndexExistsException e) {
          fail(e.toString());
        }
        catch (IndexNameConflictException e) {
          fail(e.toString());
        }
      }
    });
  }
  
  private void closeCache(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable() {
        public void run() {
          getCache().close();
        }
      });
    }
  }
  
  private void setQueryVerbose(final boolean value, VM... vms) {
    for(VM vm: vms) {
      vm.invoke(new SerializableRunnable() {
        public void run() {
          DefaultQuery.QUERY_VERBOSE = value;
       }
     });
    }
  }
  
  
  private class TestObject implements Serializable, Comparable {
    @Override
    public int compareTo(Object o) {
      if (o instanceof TestObject) {
        return score.compareTo(((TestObject)o).score);
      }
      return 1;
    }

    public Double score;
    
    public TestObject(double score) {
      this.score = score;
    }
    
  }
 
  private class NestedKeywordObject implements Serializable {
    
    public Object date;
    public Object nonKeyword;
 
    public NestedKeywordObject(Object object) {
      this.date = object;
    }
    public NestedKeywordObject(Object keywordObject, Object nonKeywordObject) {
      this.date = keywordObject;
      this.nonKeyword = nonKeywordObject;
    }
  }
  
  private class PRQueryTraceTestHook implements DefaultQuery.TestHook, Serializable {
      private HashMap<String, Boolean> hooks = new HashMap<String, Boolean>();
      public HashMap<String, Boolean> getHooks() {
        return hooks;
      }
      
      @Override
      public void doTestHook(int spot) {
        
      }

      @Override
      public void doTestHook(String spot) {
        hooks.put(spot, Boolean.TRUE);
      }
  }
}
