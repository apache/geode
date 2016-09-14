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
package org.apache.geode.internal.cache;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 *
 */
@Category(DistributedTest.class)
public class ConcurrentDestroySubRegionDUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void postSetUp() throws Exception {
    // this test expects to be able to create a region named "region"
    // but other tests seem to leave regions around, so we need
    // to create a new cache
    disconnectAllFromDS();
  }
  
  /**
   * @param name
   */
  public ConcurrentDestroySubRegionDUnitTest() {
    super();
  }

  @Test
  public void test() throws Throwable {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);


    for(int i =0; i < 200; i++) {
      final SerializableRunnable createParent = new SerializableRunnable() {

        public void run() {
          Cache cache = getCache();
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setDataPolicy(DataPolicy.REPLICATE);
          Region region = cache.createRegion("region", af.create());
        }
      };

      vm0.invoke(createParent);
      vm1.invoke(createParent);

      final SerializableRunnable createChild = new SerializableRunnable() {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          if(region != null) {
            AttributesFactory af = new AttributesFactory();
            af.setScope(Scope.DISTRIBUTED_ACK);
            af.setDataPolicy(DataPolicy.REPLICATE);
            Region subregion = region.createSubregion("subregion", af.create());
          }

        }
      };
      vm0.invoke(createChild);

      final SerializableRunnable destroyParent = new SerializableRunnable() {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("/region");
          region.destroyRegion();
        }
      };

      AsyncInvocation future = vm0.invokeAsync(destroyParent);
      try {
        vm1.invoke(createChild);
      } catch(Exception e) {
        if(!(e.getCause() instanceof RegionDestroyedException)) {
          Assert.fail("Wrong exception", e);
        }
        RegionDestroyedException rde = (RegionDestroyedException) e.getCause();
        assertEquals("Error on loop " + i, "/region", rde.getRegionFullPath());
      }
      future.getResult(60 * 1000);
    }
  }
  
  @Test
  public void testPartitionedRegion() throws Throwable {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);


    for(int i =0; i < 50; i++) {
      final SerializableRunnable createParent = new SerializableRunnable() {

        public void run() {
          Cache cache = getCache();
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.DISTRIBUTED_ACK);
          af.setDataPolicy(DataPolicy.REPLICATE);
          Region region = cache.createRegion("region", af.create());
        }
      };

      vm0.invoke(createParent);
      vm1.invoke(createParent);

      final SerializableRunnable createChild = new SerializableRunnable() {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("region");
          if(region != null) {
            AttributesFactory af = new AttributesFactory();
            af.setDataPolicy(DataPolicy.PARTITION);
            Region subregion = region.createSubregion("subregion", af.create());
          }

        }
      };
      vm0.invoke(createChild);

      final SerializableRunnable destroyParent = new SerializableRunnable() {

        public void run() {
          Cache cache = getCache();
          Region region = cache.getRegion("/region");
          region.destroyRegion();
        }
      };

      AsyncInvocation future = vm0.invokeAsync(destroyParent);
      try {
        vm1.invoke(createChild);
      } catch(Exception e) {
        if(!(e.getCause() instanceof RegionDestroyedException)) {
          Assert.fail("Wrong exception", e);
        }
        RegionDestroyedException rde = (RegionDestroyedException) e.getCause();
        assertEquals("Error on loop " + i, "/region", rde.getRegionFullPath());
      }
      future.getResult(60 * 1000);
    }
  }

}
