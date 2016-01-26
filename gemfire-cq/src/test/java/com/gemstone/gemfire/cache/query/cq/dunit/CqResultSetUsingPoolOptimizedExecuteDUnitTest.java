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
import com.gemstone.gemfire.cache.query.internal.cq.CqServiceImpl;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class CqResultSetUsingPoolOptimizedExecuteDUnitTest extends CqResultSetUsingPoolDUnitTest{

  public CqResultSetUsingPoolOptimizedExecuteDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = false;
      }
    });
  }
  
  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = true;
      }
    });
    super.tearDown2();
  }
  
  /**
   * Tests CQ Result Caching with CQ Failover.
   * When EXECUTE_QUERY_DURING_INIT is false and new server calls 
   * execute during HA the results cache is not initialized.
   * @throws Exception
   */
  @Override
  public void testCqResultsCachingWithFailOver() throws Exception
  {
    final Host host = Host.getHost(0);
    VM server1 = host.getVM(0);
    VM server2 = host.getVM(1);
    VM client = host.getVM(2);
    
    cqDUnitTest.createServer(server1);
    
    final int port1 = server1.invokeInt(CqQueryUsingPoolDUnitTest.class, "getCacheServerPort");
    final String host0 = getServerHostName(server1.getHost());
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
        CqServiceImpl CqServiceImpl = null;
        try {
          CqServiceImpl = (com.gemstone.gemfire.cache.query.internal.cq.CqServiceImpl) ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          getLogWriter().info("Failed to get the internal CqServiceImpl.", ex);
          fail ("Failed to get the internal CqServiceImpl.", ex);
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
        Collection<? extends InternalCqQuery> cqs = CqServiceImpl.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            if (size != totalObjects) {
              getLogWriter().info("The number of Cached events " + size + 
                  " is not equal to the expected size " + totalObjects);
              HashSet expectedKeys = new HashSet();
              for (int i = 1; i < totalObjects; i++) {
                expectedKeys.add("" + i);
              }
              Set cachedKeys = cqQuery.getCqResultKeyCache();
              expectedKeys.removeAll(cachedKeys);
              getLogWriter().info("Missing keys from the Cache : " + expectedKeys);
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
    pause(3 * 1000);
    
    // Close server1 for CQ fail over to server2.
    cqDUnitTest.closeServer(server1); 
    pause(3 * 1000);
    
    // Verify CQ Cache results.
    server2.invoke(new CacheSerializableRunnable("Verify CQ Cache results"){
      public void run2()throws CacheException {
        CqServiceImpl CqServiceImpl = null;
        try {
          CqServiceImpl = (CqServiceImpl) ((DefaultQueryService)getCache().getQueryService()).getCqService();
        } catch (Exception ex) {
          getLogWriter().info("Failed to get the internal CqServiceImpl.", ex);
          fail ("Failed to get the internal CqServiceImpl.", ex);
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
        Collection<? extends InternalCqQuery> cqs = CqServiceImpl.getAllCqs();
        for (InternalCqQuery cq: cqs){
          ServerCQImpl cqQuery = (ServerCQImpl)cq;
          if (cqQuery.getName().equals(cqName)) {
            int size = cqQuery.getCqResultKeysSize();
            assertEquals("The number of keys cached for cq " + cqName + " is wrong.", 
                0, size);              
          }
        }
      }
    });    
        
    // Close.
    cqDUnitTest.closeClient(client);
    cqDUnitTest.closeServer(server2); 
  }

}
