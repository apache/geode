/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.UUID;

public abstract class VMStatsLRURegionEntryHeap extends VMStatsLRURegionEntry {
  public VMStatsLRURegionEntryHeap(RegionEntryContext context, Object value) {
    super(context, value);
  }
  private static final VMStatsLRURegionEntryHeapFactory factory = new VMStatsLRURegionEntryHeapFactory();
  
  public static RegionEntryFactory getEntryFactory() {
    return factory;
  }
  private static class VMStatsLRURegionEntryHeapFactory implements RegionEntryFactory {
    public final RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      if (InlineKeyHelper.INLINE_REGION_KEYS) {
        Class<?> keyClass = key.getClass();
        if (keyClass == Integer.class) {
          return new VMStatsLRURegionEntryHeapIntKey(context, (Integer)key, value);
        } else if (keyClass == Long.class) {
          return new VMStatsLRURegionEntryHeapLongKey(context, (Long)key, value);
        } else if (keyClass == String.class) {
          final String skey = (String) key;
          final Boolean info = InlineKeyHelper.canStringBeInlineEncoded(skey);
          if (info != null) {
            final boolean byteEncoded = info;
            if (skey.length() <= InlineKeyHelper.getMaxInlineStringKey(1, byteEncoded)) {
              return new VMStatsLRURegionEntryHeapStringKey1(context, skey, value, byteEncoded);
            } else {
              return new VMStatsLRURegionEntryHeapStringKey2(context, skey, value, byteEncoded);
            }
          }
        } else if (keyClass == UUID.class) {
          return new VMStatsLRURegionEntryHeapUUIDKey(context, (UUID)key, value);
        }
      }
      return new VMStatsLRURegionEntryHeapObjectKey(context, key, value);
    }

    public final Class getEntryClass() {
      // The class returned from this method is used to estimate the memory size.
      // TODO OFFHEAP: This estimate will not take into account the memory saved by inlining the keys.
      return VMStatsLRURegionEntryHeapObjectKey.class;
    }
    public RegionEntryFactory makeVersioned() {
      return VersionedStatsLRURegionEntryHeap.getEntryFactory();
    }
  }
}
