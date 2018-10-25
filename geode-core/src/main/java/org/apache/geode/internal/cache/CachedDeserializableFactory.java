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

import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.NullDataOutputStream;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.pdx.PdxInstance;

/**
 * Produces instances that implement CachedDeserializable.
 *
 * @since GemFire 5.0.2
 *
 */
public class CachedDeserializableFactory {
  public static boolean PREFER_DESERIALIZED =
      !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "PREFER_SERIALIZED");
  public static boolean STORE_ALL_VALUE_FORMS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "STORE_ALL_VALUE_FORMS");

  /**
   * Creates and returns an instance of CachedDeserializable that contains the specified byte array.
   */
  public static CachedDeserializable create(byte[] v, InternalCache cache) {
    if (STORE_ALL_VALUE_FORMS) {
      return new StoreAllCachedDeserializable(v);
    } else if (PREFER_DESERIALIZED) {
      if (isPdxEncoded(v) && cachePrefersPdx(cache)) {
        return new PreferBytesCachedDeserializable(v);
      } else {
        return new VMCachedDeserializable(v);
      }
    } else {
      return new PreferBytesCachedDeserializable(v);
    }
  }

  private static boolean isPdxEncoded(byte[] v) {
    // assert v != null;
    if (v.length > 0) {
      return v[0] == DSCODE.PDX.toByte();
    }
    return false;
  }

  /**
   * Creates and returns an instance of CachedDeserializable that contains the specified object
   * (that is not a byte[]).
   */
  public static CachedDeserializable create(Object object, int serializedSize,
      InternalCache cache) {
    if (STORE_ALL_VALUE_FORMS) {
      return new StoreAllCachedDeserializable(object);
    } else if (PREFER_DESERIALIZED) {
      if (object instanceof PdxInstance && cachePrefersPdx(cache)) {
        return new PreferBytesCachedDeserializable(object);

      } else {
        return new VMCachedDeserializable(object, serializedSize);
      }
    } else {
      return new PreferBytesCachedDeserializable(object);
    }
  }

  private static boolean cachePrefersPdx(InternalCache internalCache) {
    // InternalCache internalCache = GemFireCacheImpl.getInstance();
    return internalCache != null && internalCache.getPdxReadSerialized();
  }

  /**
   * Wrap cd in a new CachedDeserializable.
   */
  public static CachedDeserializable create(CachedDeserializable cd) {
    if (STORE_ALL_VALUE_FORMS) {
      // storeAll cds are immutable just return it w/o wrapping
      return cd;
    } else if (PREFER_DESERIALIZED) {
      if (cd instanceof PreferBytesCachedDeserializable) {
        return cd;
      } else {
        return new VMCachedDeserializable((VMCachedDeserializable) cd);
      }
    } else {
      // preferBytes cds are immutable so just return it w/o wrapping
      return cd;
    }
  }

  /**
   * Return the heap overhead in bytes for each CachedDeserializable instance.
   */
  public static int overhead() {
    // TODO: revisit this code. If we move to per-region cds then this can no longer be static.
    // TODO: This method also does not work well with the way off heap is determined using the
    // cache.

    if (STORE_ALL_VALUE_FORMS) {
      return StoreAllCachedDeserializable.MEM_OVERHEAD;
    } else if (PREFER_DESERIALIZED) {
      // PDX: this may instead be PreferBytesCachedDeserializable.MEM_OVERHEAD
      return VMCachedDeserializable.MEM_OVERHEAD;
    } else {
      return PreferBytesCachedDeserializable.MEM_OVERHEAD;
    }

  }

  /**
   * Return the number of bytes the specified byte array will consume of heap memory.
   */
  public static int getByteSize(byte[] serializedValue) {
    // add 4 for the length field of the byte[]
    return serializedValue.length + Sizeable.PER_OBJECT_OVERHEAD + 4;
  }

  public static int getArrayOfBytesSize(final byte[][] value, final boolean addObjectOverhead) {
    int result = 4 * (value.length + 1);
    if (addObjectOverhead) {
      result += Sizeable.PER_OBJECT_OVERHEAD * (value.length + 1);
    }
    for (byte[] bytes : value) {
      if (bytes != null) {
        result += bytes.length;
      }
    }
    return result;
  }

  /**
   * Return an estimate of the amount of heap memory used for the object. If it is not a byte[] then
   * account for CachedDeserializable overhead. when it is wrapped by a CachedDeserializable.
   */
  public static int calcMemSize(Object o) {
    return calcMemSize(o, null, true);
  }

  public static int calcMemSize(Object o, ObjectSizer os, boolean addOverhead) {
    return calcMemSize(o, os, addOverhead, true);
  }

  /**
   * If not calcSerializedSize then return -1 if we can't figure out the mem size.
   */
  public static int calcMemSize(Object o, ObjectSizer os, boolean addOverhead,
      boolean calcSerializedSize) {
    int result;
    if (o instanceof byte[]) {
      // does not need to be wrapped so overhead never added
      result = getByteSize((byte[]) o);
      addOverhead = false;
    } else if (o == null) {
      // does not need to be wrapped so overhead never added
      result = 0;
      addOverhead = false;
    } else if (o instanceof String) {
      result = (((String) o).length() * 2) + 4 // for the length of the char[]
          + (Sizeable.PER_OBJECT_OVERHEAD * 2) // for String obj and Char[] obj
          + 4 // for obj ref to char[] on String; note should be 8 on 64-bit vm
          + 4 // for offset int field on String
          + 4 // for count int field on String
          + 4 // for hash int field on String
      ;
    } else if (o instanceof byte[][]) {
      result = getArrayOfBytesSize((byte[][]) o, true);
      addOverhead = false;
    } else if (o instanceof CachedDeserializable) {
      // overhead never added
      result = ((CachedDeserializable) o).getSizeInBytes();
      addOverhead = false;
    } else if (o instanceof Sizeable) {
      result = ((Sizeable) o).getSizeInBytes();
    } else if (os != null) {
      result = os.sizeof(o);
    } else if (calcSerializedSize) {
      result = Sizeable.PER_OBJECT_OVERHEAD + 4;
      NullDataOutputStream dos = new NullDataOutputStream();
      try {
        DataSerializer.writeObject(o, dos);
        result += dos.size();
      } catch (IOException ex) {
        RuntimeException ex2 = new IllegalArgumentException(
            "Could not calculate size of object");
        ex2.initCause(ex);
        throw ex2;
      }
    } else {
      // return -1 to signal the caller that we did not compute the size
      result = -1;
      addOverhead = false;
    }
    if (addOverhead) {
      result += overhead();
    }
    // GemFireCache.getInstance().getLogger().info("DEBUG calcMemSize: o=<" + o + "> o.class=" + (o
    // != null ? o.getClass() : "<null>") + " os=" + os + " result=" + result, new
    // RuntimeException("STACK");
    return result;
  }

  /**
   * Return an estimate of the number of bytes this object will consume when serialized. This is the
   * number of bytes that will be written on the wire including the 4 bytes needed to encode the
   * length.
   */
  public static int calcSerializedSize(Object o) {
    int result;
    if (o instanceof byte[]) {
      result = getByteSize((byte[]) o) - Sizeable.PER_OBJECT_OVERHEAD;
    } else if (o instanceof byte[][]) {
      result = getArrayOfBytesSize((byte[][]) o, false);
    } else if (o instanceof CachedDeserializable) {
      result = ((CachedDeserializable) o).getSizeInBytes() + 4 - overhead();
    } else if (o instanceof Sizeable) {
      result = ((Sizeable) o).getSizeInBytes() + 4;
    } else if (o instanceof HeapDataOutputStream) {
      result = ((HeapDataOutputStream) o).size() + 4;
    } else {
      result = 4;
      NullDataOutputStream dos = new NullDataOutputStream();
      try {
        DataSerializer.writeObject(o, dos);
        result += dos.size();
      } catch (IOException ex) {
        RuntimeException ex2 = new IllegalArgumentException(
            "Could not calculate size of object");
        ex2.initCause(ex);
        throw ex2;
      }
    }
    // GemFireCache.getInstance().getLogger().info("DEBUG calcSerializedSize: o=<" + o + ">
    // o.class=" + (o != null ? o.getClass() : "<null>") + " result=" + result, new
    // RuntimeException("STACK");
    return result;
  }

  /**
   * Return how much memory this object will consume if it is in serialized form
   *
   * @since GemFire 6.1.2.9
   */
  public static int calcSerializedMemSize(Object o) {
    int result = calcSerializedSize(o);
    result += Sizeable.PER_OBJECT_OVERHEAD;
    if (!(o instanceof byte[])) {
      result += overhead();
    }
    return result;
  }
}
