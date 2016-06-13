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
package com.gemstone.gemfire.cache30;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskWriteAttributesFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.internal.cache.functions.TestFunction;
import com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.FunctionServiceCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

/**
 * Tests 5.8 cache.xml features.
 * 
 * @since GemFire 5.8
 */

@Category(DistributedTest.class)
public class CacheXml58DUnitTest extends CacheXml57DUnitTest
{

  // ////// Constructors

  public CacheXml58DUnitTest() {
    super();
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_5_8;
  }


  /**
   * Tests that a region created with a named attributes set programmatically
   * for partition-resolver has the correct attributes.
   * 
   */
  @Test
  public void testPartitionedRegionAttributesForCustomPartitioning() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    
    CacheXMLPartitionResolver partitionResolver = new CacheXMLPartitionResolver();
    Properties params = new Properties();
    params.setProperty("initial-index-value", "1000");
    params.setProperty("secondary-index-value", "5000");
    partitionResolver.init(params);
    
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setPartitionResolver(partitionResolver);
    
    attrs.setPartitionAttributes(paf.create());
    
    cache.createRegion("parRoot", attrs);
    
    Region r = cache.getRegion("parRoot");
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(),1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(),100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(),500);
    assertEquals(r.getAttributes().getPartitionAttributes().getPartitionResolver(),partitionResolver);
    
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    PartitionAttributes pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), partitionResolver);
  }
  
  /**
   * Tests that a cache created with FunctionService and registered FabricFunction
   * has correct registered Function
   * 
   */
  @Test
  public void testCacheCreationWithFuntionService() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    FunctionServiceCreation fsc = new FunctionServiceCreation();
    TestFunction function1 = new TestFunction(true,TestFunction.TEST_FUNCTION2);
    TestFunction function2 = new TestFunction(true, TestFunction.TEST_FUNCTION3);
    TestFunction function3 = new TestFunction(true, TestFunction.TEST_FUNCTION4);
    fsc.registerFunction(function1);
    fsc.registerFunction(function2);
    fsc.registerFunction(function3);
    fsc.create();
    cache.setFunctionServiceCreation(fsc);
    
    testXml(cache);
    getCache();    
    Map<String, Function> functionIdMap = FunctionService.getRegisteredFunctions();
    assertEquals(3, functionIdMap.size());

    assertTrue(function1.equals(functionIdMap.get(function1.getId())));
    assertTrue(function2.equals(functionIdMap.get(function2.getId())));
    assertTrue(function3.equals(functionIdMap.get(function3.getId())));
  }
  
  /**
   * Tests that a Partitioned Region can be created with a named attributes set programmatically
   * for ExpirationAttributes
   * 
   */
  @Test
  public void testPartitionedRegionAttributesForExpiration() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    RegionAttributes rootAttrs = null;
    ExpirationAttributes expiration = new ExpirationAttributes(60,ExpirationAction.DESTROY);
    
    CacheXMLPartitionResolver partitionResolver = new CacheXMLPartitionResolver();
    Properties params = new Properties();
    params.setProperty("initial-index-value", "1000");
    params.setProperty("secondary-index-value", "5000");
    partitionResolver.init(params);
    
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setPartitionResolver(partitionResolver);
    
    AttributesFactory fac = new AttributesFactory(attrs);
    fac.setEntryTimeToLive(expiration);
    fac.setEntryIdleTimeout(expiration);
    
    fac.setPartitionAttributes(paf.create());
    rootAttrs = fac.create();
    cache.createRegion("parRoot", rootAttrs);
    
    Region r = cache.getRegion("parRoot");
    assertNotNull(r);
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(),1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(),100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(),500);
    assertEquals(r.getAttributes().getPartitionAttributes().getPartitionResolver(),partitionResolver);
    
    assertEquals(r.getAttributes().getEntryIdleTimeout().getTimeout(),expiration.getTimeout());
    assertEquals(r.getAttributes().getEntryTimeToLive().getTimeout(), expiration.getTimeout());

    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    PartitionAttributes pa = regionAttrs.getPartitionAttributes();

    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);    
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), partitionResolver);
    
    assertEquals(regionAttrs.getEntryIdleTimeout().getTimeout(), expiration.getTimeout());
    assertEquals(regionAttrs.getEntryTimeToLive().getTimeout(), expiration.getTimeout());    
    
  }
  
  
  /**
   * Tests that a Partitioned Region can be created with a named attributes set programmatically
   * for ExpirationAttributes
   * 
   */
  @Test
  public void testPartitionedRegionAttributesForEviction() throws CacheException
  {
    final int redundantCopies = 1;
    CacheCreation cache = new CacheCreation();
    if (getGemFireVersion().equals(CacheXml.VERSION_6_0)) {
      ResourceManagerCreation rm = new ResourceManagerCreation();
      rm.setCriticalHeapPercentage(95);
      cache.setResourceManagerCreation(rm);
    }
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    RegionAttributes rootAttrs = null;
    
    ExpirationAttributes expiration = new ExpirationAttributes(60,ExpirationAction.DESTROY);
    
    CacheXMLPartitionResolver partitionResolver = new CacheXMLPartitionResolver();
    Properties params = new Properties();
    params.setProperty("initial-index-value", "1000");
    params.setProperty("secondary-index-value", "5000");
    partitionResolver.init(params);
    
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setPartitionResolver(partitionResolver);
    
    AttributesFactory fac = new AttributesFactory(attrs);

//  TODO mthomas 01/20/09 Move test back to using LRUHeap when config issues have settled
//    if (getGemFireVersion().equals(CacheXml.GEMFIRE_6_0)) {
//      fac.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(null,
//          EvictionAction.OVERFLOW_TO_DISK));
//    } else {
      fac.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(100, null,
          EvictionAction.OVERFLOW_TO_DISK));
//    }

    fac.setEntryTimeToLive(expiration);
    fac.setEntryIdleTimeout(expiration);
    
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    /*fac.setDiskWriteAttributes(dwaf.create());
    File[] diskDirs = new File[1];
    diskDirs[0] = new File("overflowDir/" + "_"
        + OSProcess.getId());
    diskDirs[0].mkdirs();
    fac.setDiskDirs(diskDirs);*/
    
    fac.setPartitionAttributes(paf.create());
    rootAttrs = fac.create();
    cache.createRegion("parRoot", rootAttrs);
    
    Region r = cache.getRegion("parRoot");
    assertNotNull(r);
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(),redundantCopies);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(),100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(),500);
    assertEquals(r.getAttributes().getPartitionAttributes().getPartitionResolver(),partitionResolver);
    
    assertEquals(r.getAttributes().getEntryIdleTimeout().getTimeout(),expiration.getTimeout());
    assertEquals(r.getAttributes().getEntryTimeToLive().getTimeout(), expiration.getTimeout());

    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    PartitionAttributes pa = regionAttrs.getPartitionAttributes();
    EvictionAttributes ea = regionAttrs.getEvictionAttributes();
    
    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);    
    assertNotNull(pa.getPartitionResolver().getClass());
    assertEquals(pa.getPartitionResolver(), partitionResolver);
    
    assertEquals(regionAttrs.getEntryIdleTimeout().getTimeout(), expiration.getTimeout());
    assertEquals(regionAttrs.getEntryTimeToLive().getTimeout(), expiration.getTimeout());    
//  TODO mthomas 01/20/09 Move test back to using LRUHeap when config issues have settled
//    if (getGemFireVersion().equals(CacheXml.GEMFIRE_6_0)) {
//      assertIndexDetailsEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_HEAP);
//    } else {
    assertEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_MEMORY);
//    }
    assertEquals(ea.getAction(), EvictionAction.OVERFLOW_TO_DISK);    
  }

  @Test
  public void testPartitionedRegionAttributesForCoLocation(){
    closeCache();
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation custAttrs = new RegionAttributesCreation(cache);
    RegionAttributesCreation orderAttrs = new RegionAttributesCreation(cache);
    PartitionAttributesFactory custPaf = new PartitionAttributesFactory();
    PartitionAttributesFactory orderPaf = new PartitionAttributesFactory();
    custPaf.setRedundantCopies(1);
    custPaf.setTotalMaxMemory(500);
    custPaf.setLocalMaxMemory(100);    
    custAttrs.setPartitionAttributes(custPaf.create());    
    cache.createRegion("Customer",custAttrs);
    
    orderPaf.setRedundantCopies(1);
    orderPaf.setTotalMaxMemory(500);
    orderPaf.setLocalMaxMemory(100);
    orderPaf.setColocatedWith("Customer");
    orderAttrs.setPartitionAttributes(orderPaf.create());
    cache.createRegion("Order", orderAttrs);
    
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);
    
    Region cust = c.getRegion(Region.SEPARATOR+"Customer");
    assertNotNull(cust);
    Region order = c.getRegion(Region.SEPARATOR+"Order");
    assertNotNull(order);    
    String coLocatedRegion = order.getAttributes().getPartitionAttributes().getColocatedWith();    
    assertEquals("Customer", coLocatedRegion);
    
  }
  
  @Test
  public void testPartitionedRegionAttributesForCoLocation2(){
    closeCache();
    setXmlFile(findFile("coLocation.xml"));    
    Cache c = getCache();
    assertNotNull(c);
    Region cust = c.getRegion(Region.SEPARATOR+"Customer");
    assertNotNull(cust);
    Region order = c.getRegion(Region.SEPARATOR+"Order");
    assertNotNull(order);
    
    assertTrue(cust.getAttributes().getPartitionAttributes().getColocatedWith()==null);
    assertTrue(order.getAttributes().getPartitionAttributes().getColocatedWith().equals("Customer"));
    
  }
  
  @Test
  public void testPartitionedRegionAttributesForMemLruWithoutMaxMem() throws CacheException
  {
    final int redundantCopies = 1;
    CacheCreation cache = new CacheCreation();
      
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
        
    AttributesFactory fac = new AttributesFactory(attrs);
    fac.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes( null,
          EvictionAction.LOCAL_DESTROY));
    fac.setPartitionAttributes(paf.create());
     cache.createRegion("parRoot", fac.create());
    
      
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    PartitionAttributes pa = regionAttrs.getPartitionAttributes();
    EvictionAttributes ea = regionAttrs.getEvictionAttributes();
    
    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);    
    
    
    assertEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.LOCAL_DESTROY);
    assertEquals(ea.getMaximum(), pa.getLocalMaxMemory());
  }
  
  @Test
  public void testPartitionedRegionAttributesForMemLruWithMaxMem() throws CacheException
  {
    final int redundantCopies = 1;
    final int maxMem=25;
    CacheCreation cache = new CacheCreation();
      
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setStatisticsEnabled(true);
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
        
    AttributesFactory fac = new AttributesFactory(attrs);
    fac.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(maxMem, null,
          EvictionAction.LOCAL_DESTROY));
    fac.setPartitionAttributes(paf.create());
     cache.createRegion("parRoot", fac.create());
    
      
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    PartitionAttributes pa = regionAttrs.getPartitionAttributes();
    EvictionAttributes ea = regionAttrs.getEvictionAttributes();
    
    assertEquals(pa.getRedundantCopies(), 1);
    assertEquals(pa.getLocalMaxMemory(), 100);
    assertEquals(pa.getTotalMaxMemory(), 500);    
    
    
    assertEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.LOCAL_DESTROY);
    assertNotSame(ea.getMaximum(), maxMem);
    assertEquals(ea.getMaximum(), pa.getLocalMaxMemory());
  }
  
  @Test
  public void testReplicatedRegionAttributesForMemLruWithoutMaxMem() throws CacheException
  {
    final int redundantCopies = 1;
    CacheCreation cache = new CacheCreation();
  
    AttributesFactory fac = new AttributesFactory();
    fac.setDataPolicy(DataPolicy.REPLICATE);
    fac.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes( null,
          EvictionAction.OVERFLOW_TO_DISK));
    cache.createRegion("parRoot", fac.create());
    
      
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    EvictionAttributes ea = regionAttrs.getEvictionAttributes();
   
    assertEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.OVERFLOW_TO_DISK);
    assertEquals(ea.getMaximum(), MemLRUCapacityController.DEFAULT_MAXIMUM_MEGABYTES);
  }
  
  @Test
  public void testReplicatedRegionAttributesForMemLruWithMaxMem() throws CacheException
  {
    final int redundantCopies = 1;
    final int maxMem=25;
    CacheCreation cache = new CacheCreation();
      
    AttributesFactory fac = new AttributesFactory();
    fac.setDataPolicy(DataPolicy.REPLICATE);
    fac.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes( maxMem,null,
          EvictionAction.OVERFLOW_TO_DISK));
    cache.createRegion("parRoot", fac.create());
    
    
      
    testXml(cache);
    
    Cache c = getCache();
    assertNotNull(c);

    Region region = c.getRegion("parRoot");
    assertNotNull(region);

    RegionAttributes regionAttrs = region.getAttributes();
    EvictionAttributes ea = regionAttrs.getEvictionAttributes();
    assertEquals(ea.getAlgorithm(),EvictionAlgorithm.LRU_MEMORY);
    assertEquals(ea.getAction(), EvictionAction.OVERFLOW_TO_DISK);
    assertEquals(ea.getMaximum(), maxMem);
  }
  
}
