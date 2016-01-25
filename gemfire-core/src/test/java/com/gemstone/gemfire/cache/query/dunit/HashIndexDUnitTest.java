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

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

public class HashIndexDUnitTest extends DistributedTestCase{

  QueryTestUtils utils;
  VM vm0;
  
  public HashIndexDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    utils = new QueryTestUtils();
    utils.createServer(vm0, getAllDistributedSystemProperties(new Properties()));
    utils.createReplicateRegion("exampleRegion", vm0);
    utils.createHashIndex(vm0,"ID", "r.ID", "/exampleRegion r");
  }
  

  public void testHashIndexForIndexElemArray() throws Exception{
    doPut(200);// around 66 entries for a key in the index (< 100 so does not create a ConcurrentHashSet)
    doQuery();
    doUpdate(200);
    doQuery();
    doDestroy(200);
    doQuery();
    Thread.sleep(5000);
  }
  
  public void testHashIndexForConcurrentHashSet() throws Exception{
    doPut(333); //111 entries for a key in the index (> 100 so creates a ConcurrentHashSet)
    doQuery();
    doUpdate(333);
    doQuery();
    doDestroy(200);
    doQuery();
  }

  public void doPut(final int entries) {
     vm0.invokeAsync(new CacheSerializableRunnable("Putting values") {
      public void run2() {
        utils.createValuesStringKeys("exampleRegion", entries);
      }
    });
  }

  public void doUpdate(final int entries) {
    vm0.invokeAsync(new CacheSerializableRunnable("Updating values") {
     public void run2() {
       utils.createDiffValuesStringKeys("exampleRegion", entries);
     }
   });
 }

  
  public void doQuery() throws Exception{
    final String[] qarr = {"173", "174", "176", "180"};
    vm0.invokeAsync(new CacheSerializableRunnable("Executing query") {
      public void run2() throws CacheException {
        try {
          for (int i = 0; i < 50; i++) {
            utils.executeQueries(qarr);
          }
        }
        catch (Exception e) {
          throw new CacheException(e){};
        }
      }
    });
  }

  public void doDestroy(final int entries) {
    vm0.invokeAsync(new CacheSerializableRunnable("Destroying values") {
      public void run2() throws CacheException {
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        try {
         utils.destroyRegion("exampleRegion", entries);
        }
        catch (Exception e) {
          throw new CacheException(e){};
        }
      }
    });
  }
  
  public void tearDown2() throws Exception{
    Thread.sleep(5000);
    utils.closeServer(vm0);
  }

}
