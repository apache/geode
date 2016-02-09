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

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.query.dunit.PdxQueryCQTestBase.TestObject;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;


public class PdxQueryCQDUnitTest extends PdxQueryCQTestBase {

  public PdxQueryCQDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Tests client-server query on PdxInstance.
   */
  public void testCq() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final int queryLimit = 6;  // where id > 5 (0-5)
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testCqPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);
    final String cqName = "testCq";
    
    // Execute CQ
    SerializableRunnable executeCq = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
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
          CqQuery cq = qService.newCq(cqName, queryString[3], cqa);
          SelectResults sr = cq.executeWithInitialResults();
          for (Object o: sr.asSet()) {
            Struct s = (Struct)o;
            Object value = s.get("value");
            if (!(value instanceof TestObject)) {
              fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } 
          }
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
          err.initCause(ex);
          LogWriterUtils.getLogWriter().info("QueryService is :" + qService, err);
          throw err;
        }
      }
    };

    vm2.invoke(executeCq);
    vm3.invoke(executeCq);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    // update
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
        // Check for TestObject instances.        
        assertEquals(numberOfEntries * 3, TestObject.numInstance);
      }
    });

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    SerializableRunnable validateCq = new CacheSerializableRunnable("Validate CQs") {
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
        final CqQueryTestListener listener = (CqQueryTestListener) cqListeners[0];
        
        //Wait for the events to show up on the client.
        Wait.waitForCriterion(new WaitCriterion() {
          
          public boolean done() {
            return listener.getTotalEventCount() >= (numberOfEntries * 2 - queryLimit);
          }
          
          public String description() {
            return null;
          }
        }, 30000, 100, false);
        
        listener.printInfo(false);
    
        // Check for event type.
        Object[] cqEvents = listener.getEvents();
        for (Object o: cqEvents) {
          CqEvent cqEvent = (CqEvent)o;
          Object value = cqEvent.getNewValue();
          if (!(value instanceof TestObject)) {
            fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
          } 
        }
        
        // Check for totalEvents count.
        assertEquals("Total Event Count mismatch", (numberOfEntries * 2 - queryLimit), listener.getTotalEventCount());
                
        // Check for create count.
        assertEquals("Create Event mismatch", numberOfEntries, listener.getCreateEventCount());
        
        // Check for update count.
        assertEquals("Update Event mismatch", numberOfEntries  - queryLimit, listener.getUpdateEventCount());      
      }
    };
    
    vm2.invoke(validateCq);
    vm3.invoke(validateCq);
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests client-server query on PdxInstance.
   */
  
  public void testCqAndInterestRegistrations() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final int queryLimit = 6;  // where id > 5 (0-5)
    
    final String[] queries = new String[] {
        "SELECT * FROM " + regName + " p WHERE p.ticker = 'vmware'",
        "SELECT * FROM " + regName + " WHERE id > 5", 
      };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");

    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testCqPool"; 
    
    createPool(vm2, poolName, new String[]{host0, host0}, new int[]{port0, port1}, true);
    createPool(vm3, poolName, new String[]{host0, host0}, new int[]{port1, port0}, true);
    
    final String cqName = "testCq";

    vm3.invoke(new CacheSerializableRunnable("init region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());

        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }   
      }
    });

    vm2.invoke(new CacheSerializableRunnable("init region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port0,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
      }
    });
    
    SerializableRunnable subscribe = new CacheSerializableRunnable("subscribe") {
      public void run2() throws CacheException {
        
        // Register interest
        Region region = getRootRegion().getSubregion(regionName);
        List list = new ArrayList();
        for (int i = 1; i <= numberOfEntries * 3; i++) {
          if (i % 4 == 0) {
            list.add("key-"+i);
          }
        }
        region.registerInterest(list);
        
        LogWriterUtils.getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        for (int i=0; i < queries.length; i++) {
          CqAttributesFactory cqf = new CqAttributesFactory();
          CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
          ((CqQueryTestListener)cqListeners[0]).cqName = (cqName + i);

          cqf.initCqListeners(cqListeners);
          CqAttributes cqa = cqf.create();

          // Create CQ.
          try {
            CqQuery cq = qService.newCq(cqName + i, queries[i], cqa);
            SelectResults sr = cq.executeWithInitialResults();
            for (Object o: sr.asSet()) {
              Struct s = (Struct)o;
              Object value = s.get("value");
              if (!(value instanceof TestObject)) {
                fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
              } 
            }
          } catch (Exception ex){
            AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
            err.initCause(ex);
            LogWriterUtils.getLogWriter().info("QueryService is :" + qService, err);
            throw err;
          }
        }
      }
    };

    vm2.invoke(subscribe);
    vm3.invoke(subscribe);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        assertEquals(0, TestObject.numInstance);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Validate CQs.
    for (int i=0; i < queries.length; i++) {
      int expectedEvent = 0;
      int updateEvents = 0;

      if (i != 0) {
        expectedEvent = numberOfEntries * 2 - queryLimit;
        updateEvents = numberOfEntries - queryLimit;
      } else {
        expectedEvent = numberOfEntries * 2;
        updateEvents = numberOfEntries;
      }

      validateCq (vm2, cqName + i, expectedEvent, numberOfEntries, updateEvents);
      validateCq (vm3, cqName + i, expectedEvent, numberOfEntries, updateEvents);
    }
    

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests client-server query on PdxInstance.
   */
  public void testCqAndInterestRegistrationsWithFailOver() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final int queryLimit = 6;  // where id > 5 (0-5)
    
    final String[] queries = new String[] {
        "SELECT * FROM " + regName + " p WHERE p.ticker = 'vmware'",
        "SELECT * FROM " + regName + " WHERE id > 5", 
      };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server3
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");
    final int port2 = vm2.invokeInt(PdxQueryCQTestBase.class, "getCacheServerPort");
    
    final String host0 = NetworkUtils.getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testCqPool";     
    createPool(vm3, poolName, new String[]{host0, host0, host0}, new int[]{port1, port0, port2}, true, 1);
    
    final String cqName = "testCq";

    vm3.invoke(new CacheSerializableRunnable("init region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        ClientServerTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());

        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }   
      }
    });

    SerializableRunnable subscribe = new CacheSerializableRunnable("subscribe") {
      public void run2() throws CacheException {
        
        // Register interest
        Region region = getRootRegion().getSubregion(regionName);
        List list = new ArrayList();
        for (int i = 1; i <= numberOfEntries * 3; i++) {
          if (i % 4 == 0) {
            list.add("key-"+i);
          }
        }
        region.registerInterest(list);
        
        LogWriterUtils.getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          Assert.fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        for (int i=0; i < queries.length; i++) {
          CqAttributesFactory cqf = new CqAttributesFactory();
          CqListener[] cqListeners = {new CqQueryTestListener(LogWriterUtils.getLogWriter())};
          ((CqQueryTestListener)cqListeners[0]).cqName = (cqName + i);

          cqf.initCqListeners(cqListeners);
          CqAttributes cqa = cqf.create();

          // Create CQ.
          try {
            CqQuery cq = qService.newCq(cqName + i, queries[i], cqa);
            SelectResults sr = cq.executeWithInitialResults();
            for (Object o: sr.asSet()) {
              Struct s = (Struct)o;
              Object value = s.get("value");
              if (!(value instanceof TestObject)) {
                fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
              } 
            }
          } catch (Exception ex){
            AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
            err.initCause(ex);
            LogWriterUtils.getLogWriter().info("QueryService is :" + qService, err);
            throw err;
          }
        }
      }
    };

    vm3.invoke(subscribe);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        assertEquals(0, TestObject.numInstance);
      }
    });

    // update
    vm3.invoke(new CacheSerializableRunnable("Update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Validate CQs.
    for (int i=0; i < queries.length; i++) {
      int expectedEvent = 0;
      int updateEvents = 0;

      if (i != 0) {
        expectedEvent = (numberOfEntries * 2) - queryLimit;
        updateEvents = numberOfEntries - queryLimit;
      } else {
        expectedEvent = numberOfEntries * 2;
        updateEvents = numberOfEntries;
      }

      validateCq (vm3, cqName + i, expectedEvent, numberOfEntries, updateEvents);
    }
    

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        assertEquals(0, TestObject.numInstance);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    

    // Update
    vm3.invokeAsync(new CacheSerializableRunnable("Update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Kill server
    this.closeClient(vm0);
    
    // validate cq
    for (int i=0; i < queries.length; i++) {
      int expectedEvent = 0;
      int updateEvents = 0;

      if (i != 0) {
        expectedEvent = (numberOfEntries * 4) - (queryLimit * 2); // Double the previous time
        updateEvents = (numberOfEntries * 3) - (queryLimit * 2); 
      } else {
        expectedEvent = numberOfEntries * 4;
        updateEvents = numberOfEntries * 3;
      }

      validateCq (vm3, cqName + i, expectedEvent, numberOfEntries, updateEvents);
    }
    
    this.closeClient(vm1);
    
    // Check for TestObject instances on Server3.
    // It should be 0
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    
  }
  
  public void validateCq(VM vm, final String cqName, final int expectedEvents, final int createEvents,
      final int updateEvents) {
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
      
              // Check for event type.
              Object[] cqEvents = listener.getEvents();
              for (Object o: cqEvents) {
                CqEvent cqEvent = (CqEvent)o;
                Object value = cqEvent.getNewValue();
                if (!(value instanceof TestObject)) {
                  fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
                } 
              }
      
              // Check for totalEvents count.
              if (listener.getTotalEventCount() != expectedEvents) {
                listener.waitForTotalEvents(expectedEvents);
              }
              
              assertEquals("Total Event Count mismatch", (expectedEvents), listener.getTotalEventCount());
      
              // Check for create count.
              assertEquals("Create Event mismatch", createEvents, listener.getCreateEventCount());
      
              // Check for update count.
              assertEquals("Update Event mismatch", updateEvents, listener.getUpdateEventCount());      
            }
        });
      }

}
