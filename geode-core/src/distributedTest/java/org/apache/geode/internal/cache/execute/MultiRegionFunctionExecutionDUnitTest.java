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
package org.apache.geode.internal.cache.execute;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

@SuppressWarnings("serial")
@Category({FunctionServiceTest.class})
public class MultiRegionFunctionExecutionDUnitTest extends JUnit4CacheTestCase {

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;

  static Region PR1 = null;
  static Region PR2 = null;
  static Region RR1 = null;
  static Region RR2 = null;
  static Region LR1 = null;
  static Cache cache = null;

  public MultiRegionFunctionExecutionDUnitTest() {
    super();
  }

  @Override
  public final void postSetUp() throws Exception {
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        cache = null;
      }
    });
  }

  @Test
  public void testMultiRegionFunctionExecution() {
    vm0.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm0());
    vm1.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm1());
    vm2.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm2());
    vm3.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm3());
    createRegionsOnUnitControllerVm();
    Set<Region> regions = new HashSet<>();
    regions.add(PR1);
    InternalFunctionService.onRegions(regions).execute(new FunctionAdapter() {

      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext) context;
        Set<Region> regions = mrContext.getRegions();
        Assert.assertTrue(1 == regions.size());
        context.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public String getId() {
        // TODO Auto-generated method stub
        return getClass().getName();
      }

    }).getResult();
    regions.add(PR2);
    InternalFunctionService.onRegions(regions).execute(new FunctionAdapter() {

      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext) context;
        Set<Region> regions = mrContext.getRegions();
        Assert.assertTrue(0 != regions.size());
        context.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public String getId() {
        // TODO Auto-generated method stub
        return getClass().getName();
      }

    }).getResult();

    regions.add(PR2);
    regions.add(RR1);
    regions.add(RR2);
    regions.add(LR1);
    InternalFunctionService.onRegions(regions).execute(new FunctionAdapter() {

      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext) context;
        Set<Region> regions = mrContext.getRegions();
        Assert.assertTrue(0 != regions.size());
        context.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public String getId() {
        // TODO Auto-generated method stub
        return getClass().getName();
      }

    }).getResult();
  }

  @Test
  public void testMultiRegionFunctionExecution_NotTimedOut() {
    vm0.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm0());
    vm1.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm1());
    vm2.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm2());
    vm3.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm3());
    createRegionsOnUnitControllerVm();
    Set<Region> regions = new HashSet<>();
    regions.add(PR1);
    regions.add(PR2);
    regions.add(RR1);
    regions.add(RR2);
    regions.add(LR1);
    long timeout = 8000;
    TimeUnit unit = TimeUnit.MILLISECONDS;

    Object result = InternalFunctionService.onRegions(regions).execute(new FunctionAdapter() {
      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext) context;
        Set<Region> regions = mrContext.getRegions();
        Assert.assertTrue(0 != regions.size());
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        context.getResultSender().lastResult(Boolean.TRUE);
      }

      @Override
      public String getId() {
        return getClass().getName();
      }
    }, timeout, unit).getResult();
    List li = (ArrayList) result;
    assertEquals(Boolean.TRUE, li.get(0));
  }

  @Test
  public void testMultiRegionFunctionExecution_TimedOut() {
    vm0.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm0());
    vm1.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm1());
    vm2.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm2());
    vm3.invoke(() -> MultiRegionFunctionExecutionDUnitTest.createRegionsOnVm3());
    createRegionsOnUnitControllerVm();
    Set<Region> regions = new HashSet<>();
    regions.add(PR1);
    regions.add(PR2);
    regions.add(RR1);
    regions.add(RR2);
    regions.add(LR1);
    long timeout = 1000;
    TimeUnit unit = TimeUnit.MILLISECONDS;
    assertThatThrownBy(() -> {
      InternalFunctionService.onRegions(regions).execute(new FunctionAdapter() {
        @Override
        public void execute(FunctionContext context) {
          MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext) context;
          Set<Region> regions = mrContext.getRegions();
          Assert.assertTrue(0 != regions.size());
          try {
            Thread.sleep(2000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          context.getResultSender().lastResult(Boolean.TRUE);
        }

        @Override
        public String getId() {
          return getClass().getName();
        }

      }, timeout, unit);
    }).hasCauseInstanceOf(FunctionException.class)
        .hasMessageContaining("All results not received in time provided");
  }



  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.MultiRegionFunctionExecutionDUnitTest*"
            + ";org.apache.geode.test.dunit.**" + ";org.apache.geode.test.junit.rules.**"
            + ";com.sun.proxy.**");
    return props;
  }

  public void createCache() {
    try {
      Properties props = getDistributedSystemProperties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    } catch (Exception e) {
      org.apache.geode.test.dunit.Assert.fail("Failed while creating the cache", e);
    }
  }

  @SuppressWarnings("unchecked")
  public static void createRegionsOnVm0() {
    new MultiRegionFunctionExecutionDUnitTest().createCache();

    PartitionAttributesFactory pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    cache.createRegion("PR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    cache.createRegion("RR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    cache.createRegion("RR2", factory.create());
  }

  @SuppressWarnings("unchecked")
  public static void createRegionsOnVm1() {
    new MultiRegionFunctionExecutionDUnitTest().createCache();

    PartitionAttributesFactory pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    cache.createRegion("PR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.NORMAL);
    cache.createRegion("RR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    cache.createRegion("RR2", factory.create());
  }

  @SuppressWarnings("unchecked")
  public static void createRegionsOnVm2() {
    new MultiRegionFunctionExecutionDUnitTest().createCache();

    PartitionAttributesFactory pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    pf.setLocalMaxMemory(0);
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    cache.createRegion("PR1", factory.create());


    pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    cache.createRegion("PR2", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    cache.createRegion("RR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.NORMAL);
    cache.createRegion("RR2", factory.create());
  }

  @SuppressWarnings("unchecked")
  public static void createRegionsOnVm3() {
    new MultiRegionFunctionExecutionDUnitTest().createCache();

    PartitionAttributesFactory pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    cache.createRegion("PR1", factory.create());


    pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    cache.createRegion("PR2", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    cache.createRegion("RR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    cache.createRegion("RR2", factory.create());
  }

  @SuppressWarnings("unchecked")
  public static void createRegionsOnUnitControllerVm() {
    new MultiRegionFunctionExecutionDUnitTest().createCache();

    PartitionAttributesFactory pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    PR1 = cache.createRegion("PR1", factory.create());


    pf = new PartitionAttributesFactory();
    pf.setTotalNumBuckets(12);
    pf.setRedundantCopies(1);
    pf.setLocalMaxMemory(0);
    factory = new AttributesFactory();
    factory.setPartitionAttributes(pf.create());
    PR2 = cache.createRegion("PR2", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RR1 = cache.createRegion("RR1", factory.create());

    factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.EMPTY);
    RR2 = cache.createRegion("RR2", factory.create());

    factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    LR1 = cache.createRegion("LR1", factory.create());

    for (int i = 0; i < 24; i++) {
      PR1.put(new Integer(i), new Integer(i));
      PR2.put(new Integer(i), new Integer(i));
      RR1.put(new Integer(i), new Integer(i));
      RR2.put(new Integer(i), new Integer(i));
      LR1.put(new Integer(i), new Integer(i));
    }
  }

}
