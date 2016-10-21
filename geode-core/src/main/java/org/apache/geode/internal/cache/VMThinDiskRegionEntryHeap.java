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
package org.apache.geode.internal.cache;

import java.util.UUID;

public abstract class VMThinDiskRegionEntryHeap extends VMThinDiskRegionEntry {
  public VMThinDiskRegionEntryHeap(RegionEntryContext context, Object value) {
    super(context, value);
  }

  private static final VMThinDiskRegionEntryHeapFactory factory =
      new VMThinDiskRegionEntryHeapFactory();

  public static RegionEntryFactory getEntryFactory() {
    return factory;
  }

  private static class VMThinDiskRegionEntryHeapFactory implements RegionEntryFactory {
    public final RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      if (InlineKeyHelper.INLINE_REGION_KEYS) {
        Class<?> keyClass = key.getClass();
        if (keyClass == Integer.class) {
          return new VMThinDiskRegionEntryHeapIntKey(context, (Integer) key, value);
        } else if (keyClass == Long.class) {
          return new VMThinDiskRegionEntryHeapLongKey(context, (Long) key, value);
        } else if (keyClass == String.class) {
          final String skey = (String) key;
          final Boolean info = InlineKeyHelper.canStringBeInlineEncoded(skey);
          if (info != null) {
            final boolean byteEncoded = info;
            if (skey.length() <= InlineKeyHelper.getMaxInlineStringKey(1, byteEncoded)) {
              return new VMThinDiskRegionEntryHeapStringKey1(context, skey, value, byteEncoded);
            } else {
              return new VMThinDiskRegionEntryHeapStringKey2(context, skey, value, byteEncoded);
            }
          }
        } else if (keyClass == UUID.class) {
          return new VMThinDiskRegionEntryHeapUUIDKey(context, (UUID) key, value);
        }
      }
      return new VMThinDiskRegionEntryHeapObjectKey(context, key, value);
    }

    public final Class getEntryClass() {
      // The class returned from this method is used to estimate the memory size.
      // This estimate will not take into account the memory saved by inlining the keys.
      return VMThinDiskRegionEntryHeapObjectKey.class;
    }

    public RegionEntryFactory makeVersioned() {
      return VersionedThinDiskRegionEntryHeap.getEntryFactory();
    }

    @Override
    public RegionEntryFactory makeOnHeap() {
      return this;
    }
  }
}
