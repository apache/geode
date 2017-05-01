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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.AsyncEventQueueFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.internal.cache.xmlcache.ResourceManagerCreation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class CacheXmlGeode10DUnitTest extends CacheXml81DUnitTest {

  @Override
  protected String getGemFireVersion() {
    return CacheXml.VERSION_1_0;
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testEnableOffHeapMemory() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + OFF_HEAP_MEMORY_SIZE, "1m");

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
    assertEquals(true, ((RegionEntryContext) regionAfter).getOffHeap());
    regionAfter.localDestroyRegion();
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testEnableOffHeapMemoryRootRegionWithoutOffHeapMemoryThrowsException()
      throws Exception {
    final String regionName = getUniqueName();

    final CacheCreation cache = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.setOffHeap(true);
    assertEquals(true, attrs.getOffHeap());

    final Region regionBefore = cache.createRegion(regionName, attrs);
    assertNotNull(regionBefore);
    assertEquals(true, regionBefore.getAttributes().getOffHeap());

    IgnoredException.addIgnoredException(
        LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED
            .toLocalizedString("/" + regionName));

    try {
      testXml(cache);
      fail("Expected IllegalStateException to be thrown");
    } catch (IllegalStateException e) {
      // expected
      String msg =
          LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED
              .toLocalizedString("/" + regionName);
      assertEquals(msg, e.getMessage());
    }
  }

  @SuppressWarnings({"rawtypes", "deprecation", "unchecked"})
  @Test
  public void testEnableOffHeapMemorySubRegionWithoutOffHeapMemoryThrowsException()
      throws Exception {
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

    IgnoredException.addIgnoredException(
        LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED
            .toLocalizedString("/" + rootRegionName + "/" + subRegionName));

    try {
      testXml(cache);
      fail("Expected IllegalStateException to be thrown");
    } catch (IllegalStateException e) {
      // expected
      final String msg =
          LocalizedStrings.LocalRegion_THE_REGION_0_WAS_CONFIGURED_TO_USE_OFF_HEAP_MEMORY_BUT_OFF_HEAP_NOT_CONFIGURED
              .toLocalizedString("/" + rootRegionName + "/" + subRegionName);
      assertEquals(msg, e.getMessage());
    }
  }

  /**
   * Test the ResourceManager element's critical-off-heap-percentage and
   * eviction-off-heap-percentage attributes
   */
  @Test
  public void testResourceManagerThresholds() throws Exception {
    CacheCreation cache = new CacheCreation();
    final float low = 90.0f;
    final float high = 95.0f;

    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + OFF_HEAP_MEMORY_SIZE, "1m");

    Cache c;
    ResourceManagerCreation rmc = new ResourceManagerCreation();
    rmc.setEvictionOffHeapPercentage(low);
    rmc.setCriticalOffHeapPercentage(high);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    {
      c = getCache();
      assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage(), 0);
      assertEquals(high, c.getResourceManager().getCriticalOffHeapPercentage(), 0);
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
      assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage(), 0);
      assertEquals(low + 1, c.getResourceManager().getCriticalOffHeapPercentage(), 0);
    }
    closeCache();

    rmc = new ResourceManagerCreation();
    rmc.setEvictionOffHeapPercentage(high);
    rmc.setCriticalOffHeapPercentage(low);
    cache.setResourceManagerCreation(rmc);
    IgnoredException expectedException = IgnoredException.addIgnoredException(
        LocalizedStrings.MemoryMonitor_EVICTION_PERCENTAGE_LTE_CRITICAL_PERCENTAGE
            .toLocalizedString());
    try {
      testXml(cache);
      fail("Expected IllegalArgumentException to be thrown");
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
      assertEquals(0f, c.getResourceManager().getEvictionOffHeapPercentage(), 0);
      assertEquals(low, c.getResourceManager().getCriticalOffHeapPercentage(), 0);
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
      assertEquals(low, c.getResourceManager().getEvictionOffHeapPercentage(), 0);
      assertEquals(0f, c.getResourceManager().getCriticalOffHeapPercentage(), 0);
    }
    closeCache();

    // Disable both
    rmc = new ResourceManagerCreation();
    rmc.setEvictionOffHeapPercentage(0);
    rmc.setCriticalOffHeapPercentage(0);
    cache.setResourceManagerCreation(rmc);
    testXml(cache);
    c = getCache();
    assertEquals(0f, c.getResourceManager().getEvictionOffHeapPercentage(), 0);
    assertEquals(0f, c.getResourceManager().getCriticalOffHeapPercentage(), 0);
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testAsyncEventQueueIsForwardExpirationDestroyAttribute() throws Exception {
    final String regionName = this.testName.getMethodName();

    // Create AsyncEventQueue with Listener
    final CacheCreation cache = new CacheCreation();
    AsyncEventQueueFactory factory = cache.createAsyncEventQueueFactory();

    AsyncEventListener listener = new MyAsyncEventListenerGeode10();

    // Test for default forwardExpirationDestroy attribute value (which is false)
    String aeqId1 = "aeqWithDefaultFED";
    factory.create(aeqId1, listener);
    AsyncEventQueue aeq1 = cache.getAsyncEventQueue(aeqId1);
    assertFalse(aeq1.isForwardExpirationDestroy());

    // Test by setting forwardExpirationDestroy attribute value.
    String aeqId2 = "aeqWithFEDsetToTrue";
    factory.setForwardExpirationDestroy(true);
    factory.create(aeqId2, listener);

    AsyncEventQueue aeq2 = cache.getAsyncEventQueue(aeqId2);
    assertTrue(aeq2.isForwardExpirationDestroy());

    // Create region and set the AsyncEventQueue
    final RegionAttributesCreation attrs = new RegionAttributesCreation(cache);
    attrs.addAsyncEventQueueId(aeqId2);

    final Region regionBefore = cache.createRegion(regionName, attrs);
    assertNotNull(regionBefore);
    assertTrue(regionBefore.getAttributes().getAsyncEventQueueIds().size() == 1);

    testXml(cache);

    final Cache c = getCache();
    assertNotNull(c);

    aeq1 = c.getAsyncEventQueue(aeqId1);
    assertFalse(aeq1.isForwardExpirationDestroy());

    aeq2 = c.getAsyncEventQueue(aeqId2);
    assertTrue(aeq2.isForwardExpirationDestroy());

    final Region regionAfter = c.getRegion(regionName);
    assertNotNull(regionAfter);
    assertTrue(regionAfter.getAttributes().getAsyncEventQueueIds().size() == 1);

    regionAfter.localDestroyRegion();

    // Clear AsyncEventQueues.
    c.close();
  }

  public static class MyAsyncEventListenerGeode10 implements AsyncEventListener, Declarable {

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      return true;
    }

    @Override
    public void close() {}

    @Override
    public void init(Properties properties) {}
  }

}
