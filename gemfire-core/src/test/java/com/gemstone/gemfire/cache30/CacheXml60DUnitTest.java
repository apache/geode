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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.InternalInstantiator;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.SerializerCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;

/**
 * Tests 6.0 cache.xml features.
 * 
 * @author skumar
 * @since 6.0
 */

public class CacheXml60DUnitTest extends CacheXml58DUnitTest
{

  // ////// Constructors

  public CacheXml60DUnitTest(String name) {
    super(name);
  }

  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_6_0;
  }


  /**
   * Tests that a region created with a named attributes set programatically
   * for recovery-delay has the correct attributes.
   * 
   */
  public void testRecoveryDelayAttributes() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    paf.setRecoveryDelay(33);
    paf.setStartupRecoveryDelay(270);
    
    attrs.setPartitionAttributes(paf.create());
    
    cache.createRegion("parRoot", attrs);
    
    Region r = cache.getRegion("parRoot");
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(),1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(),100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(),500);
    assertEquals(33, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(270, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());
    
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
    assertEquals(33, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(270, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());
  }
  
  /**
   * Tests that a region created with a named attributes set programmatically
   * for recovery-delay has the correct attributes.
   * 
   */
  public void testDefaultRecoveryDelayAttributes() throws CacheException
  {
    CacheCreation cache = new CacheCreation();
    RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(1);
    paf.setTotalMaxMemory(500);
    paf.setLocalMaxMemory(100);
    attrs.setPartitionAttributes(paf.create());
    
    cache.createRegion("parRoot", attrs);
    
    Region r = cache.getRegion("parRoot");
    assertEquals(r.getAttributes().getPartitionAttributes().getRedundantCopies(),1);
    assertEquals(r.getAttributes().getPartitionAttributes().getLocalMaxMemory(),100);
    assertEquals(r.getAttributes().getPartitionAttributes().getTotalMaxMemory(),500);
    assertEquals(-1, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(0, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());
    
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
    assertEquals(-1, r.getAttributes().getPartitionAttributes().getRecoveryDelay());
    assertEquals(0, r.getAttributes().getPartitionAttributes().getStartupRecoveryDelay());
  }
  
  /**
   * Test the ResourceManager element's critical-heap-percentage and 
   * eviction-heap-percentage attributes
   * @throws Exception
   */
  public void testResourceManagerThresholds() throws Exception {
    CacheCreation cache = new CacheCreation();
    final float low = 90.0f;
    final float high = 95.0f;

    Cache c;
    ResourceManagerCreation rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(low);
    rmc.setCriticalHeapPercentage(high);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionHeapPercentage());
      assertEquals(high, c.getResourceManager().getCriticalHeapPercentage());
    }
    closeCache();
    
    rmc = new ResourceManagerCreation();
    // Set them to similar values
    rmc.setEvictionHeapPercentage(low);
    rmc.setCriticalHeapPercentage(low + 1);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionHeapPercentage());
      assertEquals(low + 1, c.getResourceManager().getCriticalHeapPercentage());
    }
    closeCache();

    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(high);
    rmc.setCriticalHeapPercentage(low);
    cache.setResourceManagerCreation(rmc);
    ExpectedException expectedException = CacheTestCase.addExpectedException(LocalizedStrings.MemoryMonitor_EVICTION_PERCENTAGE_LTE_CRITICAL_PERCENTAGE.toLocalizedString());
    try {
      testXml(cache);
      assertTrue(false);
    } catch (IllegalArgumentException expected) {
    } finally {
      expectedException.remove();
      closeCache();
    }

    // Disable eviction
    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(0);
    rmc.setCriticalHeapPercentage(low);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(0f, c.getResourceManager().getEvictionHeapPercentage());
      assertEquals(low, c.getResourceManager().getCriticalHeapPercentage());
    }
    closeCache();

    // Disable refusing ops in "red zone"
    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(low);
    rmc.setCriticalHeapPercentage(0);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionHeapPercentage());
      assertEquals(0f, c.getResourceManager().getCriticalHeapPercentage());
    }
    closeCache();

    // Disable both
    rmc = new ResourceManagerCreation();
    rmc.setEvictionHeapPercentage(0);
    rmc.setCriticalHeapPercentage(0);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    c = getCache();
    assertEquals(0f, c.getResourceManager().getEvictionHeapPercentage());
    assertEquals(0f, c.getResourceManager().getCriticalHeapPercentage());
  }
  
  // A bunch of classes for use in testing the serialization schtuff
  public static class DS1 implements DataSerializable {
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {}
    public void toData(DataOutput out) throws IOException 
    {}      
  };
  
  public static class DS2 implements DataSerializable {
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {}
    public void toData(DataOutput out) throws IOException 
    {}      
  };
  
  public static class NotDataSerializable implements Serializable{}
  
  public static class GoodSerializer extends DataSerializer {
    public GoodSerializer(){}
    @Override
    public Object fromData(DataInput in) throws IOException,
        ClassNotFoundException {return null;}
    @Override
    public int getId() {return 101;}
    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {DS1.class};
    }
    @Override
    public boolean toData(Object o, DataOutput out) throws IOException 
    {return false;}      
  }

  public static class BadSerializer extends DataSerializer {
    @Override
    public Object fromData(DataInput in) throws IOException,
        ClassNotFoundException {return null;}
    @Override
    public int getId() {return 101;}
    @Override
    public Class[] getSupportedClasses() {
      return null;
    }
    @Override
    public boolean toData(Object o, DataOutput out) throws IOException 
    {return false;}      
  }
    
  public void testSerializationRegistration()
  {
    CacheCreation cc = new CacheCreation();
    SerializerCreation sc = new SerializerCreation();    
    
    cc.setSerializerCreation(sc);
        
    sc.registerInstantiator(DS1.class, 15);    
    sc.registerInstantiator(DS2.class, 16);
    sc.registerSerializer(GoodSerializer.class);
    
    testXml(cc);
    
    //Now make sure all of the classes were registered....
    assertEquals(15, InternalInstantiator.getClassId(DS1.class));
    assertEquals(16, InternalInstantiator.getClassId(DS2.class));
    assertEquals(GoodSerializer.class, InternalDataSerializer.getSerializer(101).getClass());
    
    sc = new SerializerCreation();
    sc.registerInstantiator(NotDataSerializable.class, 15);
    closeCache();
    cc.setSerializerCreation(sc);

    ExpectedException expectedException = DistributedTestCase.addExpectedException("While reading Cache XML file");
    try {
      testXml(cc);
      fail("Instantiator should not have registered due to bad class.");
    } catch(Exception e) {
    } finally {
      expectedException.remove();
    }
    
    sc = new SerializerCreation();
    sc.registerSerializer(BadSerializer.class);
    closeCache();
    cc.setSerializerCreation(sc);

    ExpectedException expectedException1 = DistributedTestCase.addExpectedException("While reading Cache XML file");
    try {
      testXml(cc);
      fail("Serializer should not have registered due to bad class.");
    } catch(Exception e){
    } finally {
      expectedException1.remove();
    }
  }
}
