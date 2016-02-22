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

/**
 * 
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.dunit.IgnoredException;


public class CacheXml90DUnitTest extends CacheXml81DUnitTest {
  private static final long serialVersionUID = -6437436147079728413L;

  public CacheXml90DUnitTest(String name) {
    super(name);
  }

  
  // ////// Helper methods

  protected String getGemFireVersion()
  {
    return CacheXml.VERSION_9_0;
  }

  @SuppressWarnings("rawtypes")
  public void testEnableOffHeapMemory() {
    try {
      System.setProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
      
      final String regionName = "testEnableOffHeapMemory";
      
      final CacheCreation cache = new CacheCreation();
      final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
      attrs.setOffHeap(true);
      assertEquals(true, attrs.getOffHeap());
      
      final Region regionBefore = cache.createRegion(regionName, attrs);
      assertNotNull(regionBefore);
      assertEquals(true, regionBefore.getAttributes().getOffHeap());
  
      testXml(cache);
      
      final Cache c = getCache();
      assertNotNull(c);
  
      final Region regionAfter = c.getRegion(regionName);
      assertNotNull(regionAfter);
      assertEquals(true, regionAfter.getAttributes().getOffHeap());
      assertEquals(true, ((LocalRegion)regionAfter).getOffHeap());
      regionAfter.localDestroyRegion();
    } finally {
      System.clearProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    }
  }

  @SuppressWarnings("rawtypes")
  public void testEnableOffHeapMemoryRootRegionWithoutOffHeapMemoryThrowsException() {
    final String regionName = getUniqueName();
    
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setOffHeap(true);
    assertEquals(true, attrs.getOffHeap());
    
    final Region regionBefore = cache.createRegion(regionName, attrs);
    assertNotNull(regionBefore);
    assertEquals(true, regionBefore.getAttributes().getOffHeap());

    IgnoredException expectedException = IgnoredException.addIgnoredException(LocalizedStrings.
        LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED.toLocalizedString("/"+regionName));
    try {
      testXml(cache);
    } catch (IllegalStateException e) {
      // expected
      String msg = LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED.toLocalizedString("/"+regionName);
      assertEquals(msg, e.getMessage());
    } finally {
      expectedException.remove();
    }
  }
  
  @SuppressWarnings({ "rawtypes", "deprecation", "unchecked" })
  public void testEnableOffHeapMemorySubRegionWithoutOffHeapMemoryThrowsException() {
    final String rootRegionName = getUniqueName();
    final String subRegionName = "subRegion";
    
    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation rootRegionAttrs = new RegionAttributesCreation(cache);
    assertEquals(false, rootRegionAttrs.getOffHeap());
    
    final Region rootRegionBefore = cache.createRegion(rootRegionName, rootRegionAttrs);
    assertNotNull(rootRegionBefore);
    assertEquals(false, rootRegionBefore.getAttributes().getOffHeap());
    
    final RegionAttributesCreation subRegionAttrs = new RegionAttributesCreation(cache);
    subRegionAttrs.setOffHeap(true);
    assertEquals(true, subRegionAttrs.getOffHeap());
    
    final Region subRegionBefore = rootRegionBefore.createSubregion(subRegionName, subRegionAttrs);
    assertNotNull(subRegionBefore);
    assertEquals(true, subRegionBefore.getAttributes().getOffHeap());

    IgnoredException expectedException = IgnoredException.addIgnoredException(LocalizedStrings.
        LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED.toLocalizedString("/"+rootRegionName+"/"+subRegionName));
    try {
      testXml(cache);
    } catch (IllegalStateException e) {
      // expected
      final String msg = LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED.
          toLocalizedString("/" + rootRegionName + "/" + subRegionName);
      assertEquals(msg, e.getMessage());
    } finally {
      expectedException.remove();
    }
  }

  /**
   * Test the ResourceManager element's critical-off-heap-percentage and 
   * eviction-off-heap-percentage attributes
   * @throws Exception
   */
  public void testResourceManagerThresholds() throws Exception {
    CacheCreation cache = new CacheCreation();
    final float low = 90.0f;
    final float high = 95.0f;

    try {
      System.setProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");

      Cache c;
      ResourceManagerCreation rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(low);
      rmc.setCriticalOffHeapPercentage(high);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(high, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
      
      rmc = new ResourceManagerCreation();
      // Set them to similar values
      rmc.setEvictionOffHeapPercentage(low);
      rmc.setCriticalOffHeapPercentage(low + 1);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(low + 1, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
  
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(high);
      rmc.setCriticalOffHeapPercentage(low);
      cache.setResourceManagerCreation(rmc);
      IgnoredException expectedException = IgnoredException.addIgnoredException(LocalizedStrings.MemoryMonitor_EVICTION_PERCENTAGE_LTE_CRITICAL_PERCENTAGE.toLocalizedString());
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
      rmc.setEvictionOffHeapPercentage(0);
      rmc.setCriticalOffHeapPercentage(low);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(0f, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(low, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
  
      // Disable refusing ops in "red zone"
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(low);
      rmc.setCriticalOffHeapPercentage(0);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      {
        c = getCache();
        assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage());
        assertEquals(0f, c.getResourceManager().getCriticalOffHeapPercentage());
      }
      closeCache();
  
      // Disable both
      rmc = new ResourceManagerCreation();
      rmc.setEvictionOffHeapPercentage(0);
      rmc.setCriticalOffHeapPercentage(0);
      cache.setResourceManagerCreation(rmc);
      testXml(cache);
      c = getCache();
      assertEquals(0f, c.getResourceManager().getEvictionOffHeapPercentage());
      assertEquals(0f, c.getResourceManager().getCriticalOffHeapPercentage());
    } finally {
      System.clearProperty("gemfire."+DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME);
    }
  }
}
