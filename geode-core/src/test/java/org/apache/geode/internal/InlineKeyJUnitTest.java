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
package org.apache.geode.internal;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapIntKey;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapLongKey;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapObjectKey;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapStringKey1;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapStringKey2;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapUUIDKey;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;
import java.util.UUID;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class InlineKeyJUnitTest {
  private GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
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
      Region r = gfc.createRegionFactory(RegionShortcut.LOCAL).setConcurrencyChecksEnabled(false)
          .create("inlineKeyRegion");
      LocalRegion lr = (LocalRegion) r;
      Object key = Integer.valueOf(1);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected int entry but was " + lr.getRegionEntry(key).getClass(),
          lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapIntKey);
      key = Long.valueOf(2);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected long entry but was " + lr.getRegionEntry(key).getClass(),
          lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapLongKey);
      key = new UUID(1L, 2L);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected uuid entry but was " + lr.getRegionEntry(key).getClass(),
          lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapUUIDKey);
      key = "";
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(),
          lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey1);

      for (int i = 1; i <= 7; i++) {
        key = getAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(),
            lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey1);
      }
      for (int i = 8; i <= 15; i++) {
        key = getAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(),
            lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey2);
      }

      key = getAsciiString(16);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected object entry but was " + lr.getRegionEntry(key).getClass(),
          lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapObjectKey);

      for (int i = 1; i <= 3; i++) {
        key = getNonAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(),
            lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey1);
      }
      for (int i = 4; i <= 7; i++) {
        key = getNonAsciiString(i);
        r.create(key, null);
        assertEquals(true, r.containsKey(key));
        assertTrue("expected string entry but was " + lr.getRegionEntry(key).getClass(),
            lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapStringKey2);
      }

      key = getNonAsciiString(8);
      r.create(key, null);
      assertEquals(true, r.containsKey(key));
      assertTrue("expected object entry but was " + lr.getRegionEntry(key).getClass(),
          lr.getRegionEntry(key) instanceof VMThinRegionEntryHeapObjectKey);

    } finally {
      closeCache(gfc);
    }
  }

  private static String getAsciiString(int len) {
    StringBuilder sb = new StringBuilder();
    char asciiChar = 'a';
    for (int i = 0; i < len; i++) {
      sb.append(asciiChar);
    }
    return sb.toString();
  }

  private static String getNonAsciiString(int len) {
    StringBuilder sb = new StringBuilder();
    char nonAsciiChar = '\u8888';
    for (int i = 0; i < len; i++) {
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
    // System.out.println("VMThinRegionEntryIntKey=" + getMemSize(re));
    Object re2 = new VMThinRegionEntryHeapObjectKey(null, 1, null);
    // System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));

    re = new VMThinRegionEntryHeapLongKey(null, 1L, null);
    // System.out.println("VMThinRegionEntryLongKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, 1L, null);
    // System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));

    re = new VMThinRegionEntryHeapUUIDKey(null, new UUID(1L, 2L), null);
    // System.out.println("VMThinRegionEntryUUIDKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, new UUID(1L, 2L), null);
    // System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));

    re = new VMThinRegionEntryHeapStringKey1(null, "1234567", null, true);
    // System.out.println("VMThinRegionEntryStringKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, "1234567", null);
    // System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));

    re = new VMThinRegionEntryHeapStringKey2(null, "123456789012345", null, true);
    // System.out.println("VMThinRegionEntryStringKey=" + getMemSize(re));
    re2 = new VMThinRegionEntryHeapObjectKey(null, "123456789012345", null);
    // System.out.println("VMThinRegionEntryObjectKey=" + getMemSize(re2));
    assertTrue(getMemSize(re) < getMemSize(re2));
  }
}
