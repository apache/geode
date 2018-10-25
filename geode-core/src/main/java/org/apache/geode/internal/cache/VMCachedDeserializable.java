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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.pdx.PdxInstance;

/**
 * The first time someone asks this instance for its Object it will deserialize the bytes and from
 * then on keep a reference to the deserialized form. So it "prefers deserialization".
 *
 */
public class VMCachedDeserializable implements CachedDeserializable, DataSerializableFixedID {

  /** The cached value */
  private volatile Object value;
  private int valueSize; // only set in constructor or fromData

  /**
   * +PER_OBJECT_OVERHEAD for VMCachedDeserializable object +4 for value field +4 for valueSize
   * field
   */
  static final int MEM_OVERHEAD = PER_OBJECT_OVERHEAD + 4 + 4;

  /**
   * zero-arg constructor for serialization only
   */
  public VMCachedDeserializable() {}

  /**
   * Creates a new instance of <code>VMCachedDeserializable</code>.
   *
   * Note that, in general, instances of this class should be obtained via
   * {@link CachedDeserializableFactory}.
   */
  public VMCachedDeserializable(byte[] serializedValue) {
    if (serializedValue == null)
      throw new NullPointerException(
          "value must not be null");
    this.value = serializedValue;
    this.valueSize = CachedDeserializableFactory.getByteSize(serializedValue);
  }

  VMCachedDeserializable(VMCachedDeserializable cd) {
    this.value = cd.value;
    this.valueSize = cd.valueSize;
  }

  /**
   * Create a new instance with an object and it's size. Note the caller decides if objectSize is
   * the memory size or the serialized size.
   *
   */
  public VMCachedDeserializable(Object object, int objectSize) {
    this.value = object;
    this.valueSize = objectSize;
  }

  public Object getDeserializedValue(Region r, RegionEntry re) {
    Object v = this.value;
    if (v instanceof byte[]) {
      // org.apache.geode.internal.cache.GemFireCache.getInstance().getLogger().info("DEBUG
      // getDeserializedValue r=" + r + " re=" + re, new RuntimeException("STACK"));
      EvictableEntry le = null;
      if (re != null) {
        assert r != null;
        if (re instanceof EvictableEntry) {
          le = (EvictableEntry) re;
        }
      }
      if (le != null) {
        if (r instanceof PartitionedRegion) {
          r = ((PartitionedRegion) r).getBucketRegion(re.getKey());
        }
        boolean callFinish = false;
        RegionMap regionMap = null;
        if (r != null) { // fix for bug 44795
          regionMap = ((InternalRegion) r).getRegionMap();
        }
        boolean threadAlreadySynced = Thread.holdsLock(le);
        boolean isCacheListenerInvoked = re.isCacheListenerInvocationInProgress();
        synchronized (le) {
          v = this.value;
          if (!(v instanceof byte[]))
            return v;
          v = EntryEventImpl.deserialize((byte[]) v);
          if (threadAlreadySynced && !isCacheListenerInvoked) {
            // to fix bug 43355 and 43409 don't change the value form
            // if the thread that called us was already synced.
            return v;
          }
          if (!(v instanceof PdxInstance)) {
            this.value = v;
            if (regionMap != null) {
              callFinish = regionMap.beginChangeValueForm(le, this, v);
            }
          }
        }
        if (callFinish && !isCacheListenerInvoked) {
          regionMap.finishChangeValueForm();
        }
      } else {
        // we sync on this so we will only do one deserialize
        synchronized (this) {
          v = this.value;
          if (!(v instanceof byte[]))
            return v;
          v = EntryEventImpl.deserialize((byte[]) v);
          if (!(v instanceof PdxInstance)) {
            this.value = v;
          }
          // ObjectSizer os = null;
          // if (r != null) {
          // EvictionAttributes ea = r.getAttributes().getEvictionAttributes();
          // if (ea != null) {
          // os = ea.getObjectSizer();
          // }
          // int vSize = CachedDeserializableFactory.calcMemSize(v, os, false, false);
          // if (vSize != -1) {
          // int oldSize = this.valueSize;
          // this.valueSize = vSize;
          // if (r instanceof BucketRegion) {
          // BucketRegion br = (BucketRegion)r;
          // br.updateBucketMemoryStats(vSize - oldSize);
          // }
          // // @todo do we need to update some lru stats since the size changed?
          // }
          // // If vSize == -1 then leave valueSize as is which is the serialized size.
        }
      }
    }
    return v;
  }

  public Object getDeserializedForReading() {
    Object v = this.value;
    if (v instanceof byte[]) {
      return EntryEventImpl.deserialize((byte[]) v);
    } else {
      return v;
    }
  }

  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    Object v = this.value;
    if (v instanceof byte[]) {
      Object result = EntryEventImpl.deserialize((byte[]) v);
      if (CopyHelper.isWellKnownImmutableInstance(result) && !(result instanceof PdxInstance)) {
        // Since it is immutable go ahead and change the form
        // since we can return the immutable instance each time.
        result = getDeserializedValue(r, re);
      }
      return result;
    } else {
      return CopyHelper.copy(v);
    }
  }

  /**
   * Return the serialized value as a byte[]
   */
  public byte[] getSerializedValue() {
    Object v = this.value;
    if (v instanceof byte[])
      return (byte[]) v;
    return EntryEventImpl.serialize(v);
  }

  /**
   * Return current value regardless of whether it is serialized or deserialized: if it was
   * serialized than it is a byte[], otherwise it is not a byte[].
   */
  public Object getValue() {
    return this.value;
  }

  public int getSizeInBytes() {
    return MEM_OVERHEAD + this.valueSize;
  }

  public int getValueSizeInBytes() {
    return valueSize;
  }

  public int getDSFID() {
    return VM_CACHED_DESERIALIZABLE;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    // fix for bug 38309
    byte[] bytes = DataSerializer.readByteArray(in);
    this.valueSize = bytes.length;
    this.value = bytes;
  }

  public void toData(DataOutput out) throws IOException {
    // fix for bug 38309
    DataSerializer.writeObjectAsByteArray(getValue(), out);
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  @Override
  public String toString() {
    return getShortClassName() + "@" + this.hashCode();
  }

  public void writeValueAsByteArray(DataOutput out) throws IOException {
    toData(out);
  }

  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
    Object v = this.value;
    if (v instanceof byte[]) {
      wrapper.setData((byte[]) v, userBits, ((byte[]) v).length,
          false /* Not Reusable as it refers to underlying value */);
    } else {
      EntryEventImpl.fillSerializedValue(wrapper, v, userBits);
    }

  }

  public String getStringForm() {
    try {
      return StringUtils.forceToString(getDeserializedForReading());
    } catch (RuntimeException ex) {
      return "Could not convert object to string because " + ex;
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public boolean isSerialized() {
    return true;
  }

  @Override
  public boolean usesHeapForStorage() {
    return true;
  }
}
