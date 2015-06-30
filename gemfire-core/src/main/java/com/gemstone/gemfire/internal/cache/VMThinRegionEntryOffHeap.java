package com.gemstone.gemfire.internal.cache;

import java.util.UUID;

public abstract class VMThinRegionEntryOffHeap extends VMThinRegionEntry implements OffHeapRegionEntry {

  public VMThinRegionEntryOffHeap(RegionEntryContext context, Object value) {
    super(context, value);
  }
  
  private static final VMThinRegionEntryOffHeapFactory factory = new VMThinRegionEntryOffHeapFactory();
  
  public static RegionEntryFactory getEntryFactory() {
    return factory;
  }
  private static class VMThinRegionEntryOffHeapFactory implements RegionEntryFactory {
    public final RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      if (InlineKeyHelper.INLINE_REGION_KEYS) {
        Class<?> keyClass = key.getClass();
        if (keyClass == Integer.class) {
          return new VMThinRegionEntryOffHeapIntKey(context, (Integer)key, value);
        } else if (keyClass == Long.class) {
          return new VMThinRegionEntryOffHeapLongKey(context, (Long)key, value);
        } else if (keyClass == String.class) {
          final String skey = (String) key;
          final Boolean info = InlineKeyHelper.canStringBeInlineEncoded(skey);
          if (info != null) {
            final boolean byteEncoded = info;
            if (skey.length() <= InlineKeyHelper.getMaxInlineStringKey(1, byteEncoded)) {
              return new VMThinRegionEntryOffHeapStringKey1(context, skey, value, byteEncoded);
            } else {
              return new VMThinRegionEntryOffHeapStringKey2(context, skey, value, byteEncoded);
            }
          }
        } else if (keyClass == UUID.class) {
          return new VMThinRegionEntryOffHeapUUIDKey(context, (UUID)key, value);
        }
      }
      return new VMThinRegionEntryOffHeapObjectKey(context, key, value);
    }

    public final Class getEntryClass() {
      // The class returned from this method is used to estimate the memory size.
      // TODO OFFHEAP: This estimate will not take into account the memory saved by inlining the keys.
      return VMThinRegionEntryOffHeapObjectKey.class;
    }
    public RegionEntryFactory makeVersioned() {
      return VersionedThinRegionEntryOffHeap.getEntryFactory();
    }
	@Override
    public RegionEntryFactory makeOnHeap() {
      return VMThinRegionEntryHeap.getEntryFactory();
    }
  }
}
