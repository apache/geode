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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * This cache deserializable always keeps its byte[] in serialized form and the object form.
 *
 * @since GemFire 5.5
 *
 */
public class StoreAllCachedDeserializable implements CachedDeserializable, DataSerializableFixedID {
  /**
   * empty constructor for serialization only
   */
  public StoreAllCachedDeserializable() {}


  private /* final */ Object objValue;

  /** The cached value */
  private /* final */ byte[] value;

  /**
   * +PER_OBJECT_OVERHEAD for VMCachedDeserializable object +4 for value field
   */
  static final int MEM_OVERHEAD = PER_OBJECT_OVERHEAD + 8;

  /**
   * Creates a new instance of <code>StoreAllCachedDeserializable</code>.
   *
   * Note that, in general, instances of this class should be obtained via
   * {@link CachedDeserializableFactory}.
   */
  StoreAllCachedDeserializable(byte[] serializedValue) {
    if (serializedValue == null) {
      throw new NullPointerException(
          "value must not be null");
    }
    this.value = serializedValue;
    this.objValue = EntryEventImpl.deserialize(this.value);
  }

  public StoreAllCachedDeserializable(Object object) {
    this.objValue = object;
    this.value = EntryEventImpl.serialize(object);
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return this.objValue;
  }

  @Override
  public Object getDeserializedForReading() {
    return this.objValue;
  }

  @Override
  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    return EntryEventImpl.deserialize(this.value);
  }

  /**
   * Return the serialized value as a byte[]
   */
  @Override
  public byte[] getSerializedValue() {
    return this.value;
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
    wrapper.setData(this.value, userBits, this.value.length,
        false /* Not Reusable as it refers to underlying value */);
  }

  /**
   * Return current value regardless of whether it is serialized or deserialized: if it was
   * serialized than it is a byte[], otherwise it is not a byte[].
   */
  @Override
  public Object getValue() {
    return this.value;
  }

  @Override
  public int getSizeInBytes() {
    return MEM_OVERHEAD + CachedDeserializableFactory.getByteSize(this.value) * 2;
  }

  @Override
  public int getValueSizeInBytes() {
    return CachedDeserializableFactory.getByteSize(this.value);
  }

  @Override
  public int getDSFID() {
    return STORE_ALL_CACHED_DESERIALIZABLE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.value = DataSerializer.readByteArray(in);
    this.objValue = EntryEventImpl.deserialize(this.value);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeByteArray(this.value, out);
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  @Override
  public String toString() {
    return getShortClassName() + "@" + this.hashCode();
  }

  @Override
  public void writeValueAsByteArray(DataOutput out) throws IOException {
    toData(out, InternalDataSerializer.createSerializationContext(out));
  }

  @Override
  public String getStringForm() {
    return StringUtils.forceToString(this.objValue);
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
