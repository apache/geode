/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.UUID;

public abstract class VersionedThinRegionEntryOffHeap extends VersionedThinRegionEntry implements OffHeapRegionEntry {
  public VersionedThinRegionEntryOffHeap(RegionEntryContext context, Object value) {
    super(context, value);
  }
  private static final VersionedThinRegionEntryOffHeapFactory factory = new VersionedThinRegionEntryOffHeapFactory();
  
  public static RegionEntryFactory getEntryFactory() {
    return factory;
  }
  private static class VersionedThinRegionEntryOffHeapFactory implements RegionEntryFactory {
    public final RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      if (InlineKeyHelper.INLINE_REGION_KEYS) {
        Class<?> keyClass = key.getClass();
        if (keyClass == Integer.class) {
          return new VersionedThinRegionEntryOffHeapIntKey(context, (Integer)key, value);
        } else if (keyClass == Long.class) {
          return new VersionedThinRegionEntryOffHeapLongKey(context, (Long)key, value);
        } else if (keyClass == String.class) {
          final String skey = (String) key;
          final Boolean info = InlineKeyHelper.canStringBeInlineEncoded(skey);
          if (info != null) {
            final boolean byteEncoded = info;
            if (skey.length() <= InlineKeyHelper.getMaxInlineStringKey(1, byteEncoded)) {
              return new VersionedThinRegionEntryOffHeapStringKey1(context, skey, value, byteEncoded);
            } else {
              return new VersionedThinRegionEntryOffHeapStringKey2(context, skey, value, byteEncoded);
            }
          }
        } else if (keyClass == UUID.class) {
          return new VersionedThinRegionEntryOffHeapUUIDKey(context, (UUID)key, value);
        }
      }
      return new VersionedThinRegionEntryOffHeapObjectKey(context, key, value);
    }

    public final Class getEntryClass() {
      // The class returned from this method is used to estimate the memory size.
      // TODO OFFHEAP: This estimate will not take into account the memory saved by inlining the keys.
      return VersionedThinRegionEntryOffHeapObjectKey.class;
    }
    public RegionEntryFactory makeVersioned() {
      return this;
    }
	@Override
    public RegionEntryFactory makeOnHeap() {
      return VersionedThinRegionEntryHeap.getEntryFactory();
    }
  }
}
