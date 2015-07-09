/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache30.CacheTestCase;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class ConcurrentDestroySubRegionDUnitTest extends CacheTestCase {

  @Override
  public void setUp() throws Exception {
    super.setUp();
    // this test expects to be able to create a region named "region"
    // but other tests seem to leave regions around, so we need
    // to create a new cache
    disconnectAllFromDS();
  }
  
  /**
   * @param name
   */
  public ConcurrentDestroySubRegionDUnitTest(String name) {
    super(name);
  }

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
          fail("Wrong exception", e);
        }
        RegionDestroyedException rde = (RegionDestroyedException) e.getCause();
        assertEquals("Error on loop " + i, "/region", rde.getRegionFullPath());
      }
      future.getResult(60 * 1000);
    }
  }
  
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
          fail("Wrong exception", e);
        }
        RegionDestroyedException rde = (RegionDestroyedException) e.getCause();
        assertEquals("Error on loop " + i, "/region", rde.getRegionFullPath());
      }
      future.getResult(60 * 1000);
    }
  }

}
