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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

@SuppressWarnings("serial")
public class MultiRegionFunctionExecutionDUnitTest extends CacheTestCase {

  VM vm0 = null;

  VM vm1 = null;

  VM vm2 = null;

  VM vm3 = null;
  
  static Region PR1 = null ;
  static Region PR2 = null ;
  static Region RR1 = null ;
  static Region RR2 = null ;
  static Region LR1 = null ;
  static Cache cache = null ;

  public MultiRegionFunctionExecutionDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {

    super.setUp();
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    cache = null;
    Invoke.invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }
  
  public void testMultiRegionFunctionExecution(){
    vm0.invoke(MultiRegionFunctionExecutionDUnitTest.class, "createRegionsOnVm0");
    vm1.invoke(MultiRegionFunctionExecutionDUnitTest.class, "createRegionsOnVm1");
    vm2.invoke(MultiRegionFunctionExecutionDUnitTest.class, "createRegionsOnVm2");
    vm3.invoke(MultiRegionFunctionExecutionDUnitTest.class, "createRegionsOnVm3");
    createRegionsOnUnitControllerVm();
    Set<Region> regions = new HashSet<Region>();
    regions.add(PR1);
    InternalFunctionService.onRegions(regions).execute(new FunctionAdapter(){

      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext)context;
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
    InternalFunctionService.onRegions(regions).execute(new FunctionAdapter(){

      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext)context;
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
    InternalFunctionService.onRegions(regions).execute(new FunctionAdapter(){

      @Override
      public void execute(FunctionContext context) {
        MultiRegionFunctionContext mrContext = (MultiRegionFunctionContext)context;
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
  public void createCache() {
    try {
      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(props);
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
    }
    catch (Exception e) {
      com.gemstone.gemfire.test.dunit.Assert.fail("Failed while creating the cache", e);
    }
  }
  @SuppressWarnings("unchecked")
  public static void createRegionsOnVm0() {
    new MultiRegionFunctionExecutionDUnitTest("temp").createCache();  

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
    new MultiRegionFunctionExecutionDUnitTest("temp").createCache();   
        
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
    new MultiRegionFunctionExecutionDUnitTest("temp").createCache();   
        
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
    new MultiRegionFunctionExecutionDUnitTest("temp").createCache();   
        
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
    new MultiRegionFunctionExecutionDUnitTest("temp").createCache();   
        
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
