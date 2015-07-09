/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.UUID;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.VMThinRegionEntryHeapIntKey;
import com.gemstone.gemfire.internal.cache.VMThinRegionEntryHeapLongKey;
import com.gemstone.gemfire.internal.cache.VMThinRegionEntryHeapObjectKey;
import com.gemstone.gemfire.internal.cache.VMThinRegionEntryHeapStringKey1;
import com.gemstone.gemfire.internal.cache.VMThinRegionEntryHeapStringKey2;
import com.gemstone.gemfire.internal.cache.VMThinRegionEntryHeapUUIDKey;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class InlineKeyJUnitTest {
  private GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty("locators", "");
    props.setProperty("mcast-port", "0");
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }
  private void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }
  
  @Test
  public void testInlineKeys() {
    GemFireCacheImpl gfc = createCache();
    try {
      Region r = gfc.createRegionFactory(RegionShortcut.LOCAL).setConcurrencyChecksEnabled(false).create("inlineKeyRegion");
      LocalRegion lr = (LocalRegion) r;
      Object key = Integer.valueOf(1);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected int entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapIntKey);
      key = Long.valueOf(2);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected long entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapLongKey);
      key = new UUID(1L, 2L);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected uuid entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapUUIDKey);
      key = "";
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey1);

      for (int i=1; i <= 7; i++) {
        key = getAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey1);
      }
      for (int i=8; i <= 15; i++) {
        key = getAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey2);
      }

      key = getAsciiString(16);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected object entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapObjectKey);
      
      for (int i=1; i <= 3; i++) {
        key = getNonAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey1);
      }
      for (int i=4; i <= 7; i++) {
        key = getNonAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey2);
      }

      key = getNonAsciiString(8);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected object entry but was " + lr.getRegionEntry(key).getClass(), lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapObjectKey);

    } finally {
      closeCache(gfc);
    }
  }

  private static String getAsciiString(int len) {
    StringBuilder sb = new StringBuilder();
    char asciiChar = 'a';
    for (int i=0; i < len; i++) {
      sb.append(asciiChar);
    }
    return sb.toString();
  }

  private static String getNonAsciiString(int len) {
    StringBuilder sb = new StringBuilder();
    char nonAsciiChar = '\u8888';
    for (int i=0; i < len; i++) {
      sb.append(nonAsciiChar);
    }
    return sb.toString();
  }
  
  private static int getMemSize(Object o) {
    return ObjectSizer.REFLECTION_SIZE.sizeof(o);
  }
  
  @Test
  public void testMemoryOverhead() {
    Object re = new VMThinRegionEntryHeapIntKey(null, 1, null);
    //System.out.println("VMThinRegionEntryIntKey=" + getMemSize(re));
    Object re2 = new VMThinRegionEntryHeapObjectKey(null, 1, null);
    //System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));
    
    re = new VMThinRegionEntryHeapLongKey(null, 1L, null);
    //System.out.println("VMThinRegionEntryLongKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, 1L, null);
    //System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));
    
    re = new VMThinRegionEntryHeapUUIDKey(null, new UUID(1L, 2L), null);
    //System.out.println("VMThinRegionEntryUUIDKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, new UUID(1L, 2L), null);
    //System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));

    re = new VMThinRegionEntryHeapStringKey1(null, "1234567", null, true);
    //System.out.println("VMThinRegionEntryStringKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, "1234567", null);
    //System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));

    re = new VMThinRegionEntryHeapStringKey2(null, "123456789012345", null, true);
    //System.out.println("VMThinRegionEntryStringKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, "123456789012345", null);
    //System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));
  }
}
