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
package org.apache.geode.cache.query.dunit;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.QueryTestUtils;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class HashIndexDUnitTest extends JUnit4DistributedTestCase {

  QueryTestUtils utils;
  VM vm0;
  
  @Override
  public final void postSetUp() throws Exception {
    getSystem();
    Invoke.invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    utils = new QueryTestUtils();
    utils.initializeQueryMap();
    utils.createServer(vm0, DistributedTestUtils.getAllDistributedSystemProperties(new Properties()));
    utils.createReplicateRegion("exampleRegion", vm0);
    utils.createHashIndex(vm0,"ID", "r.ID", "/exampleRegion r");
  }
  

  @Test
  public void testHashIndexForIndexElemArray() throws Exception{
    doPut(200);// around 66 entries for a key in the index (< 100 so does not create a ConcurrentHashSet)
    doQuery();
    doUpdate(200);
    doQuery();
    doDestroy(200);
    doQuery();
    Thread.sleep(5000);
  }
  
  @Test
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
        putPortfolios("exampleRegion", entries);
      }
    });
  }

  public void doUpdate(final int entries) {
    vm0.invokeAsync(new CacheSerializableRunnable("Updating values") {
     public void run2() {
       putOffsetPortfolios("exampleRegion", entries);
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
         Region region = utils.getRegion("exampleRegion");
         for (int i = 1; i <= entries; i++) {
           try {
             region.destroy("KEY-"+ i);
           } catch (Exception e) {
             throw new Exception(e);
           }
         }
        }
        catch (Exception e) {
          throw new CacheException(e){};
        }
      }
    });
  }
  
  @Override
  public final void preTearDown() throws Exception {
    Thread.sleep(5000);
    utils.closeServer(vm0);
  }
  
  private void putPortfolios(String regionName, int size) {
    Region region = utils.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-"+ i, new Portfolio(i));
    }
  }
  
  private void putOffsetPortfolios(String regionName, int size) {
    Region region = utils.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-"+ i, new Portfolio(i + 1));
    }
  }
}
