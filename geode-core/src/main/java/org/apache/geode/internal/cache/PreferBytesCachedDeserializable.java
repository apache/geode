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
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This cache deserializable always keeps its byte[] in serialized form. You can ask it for its
 * Object in which case it always has to deserialize. So it "prefers serialization (aka bytes)".
 *
 * @since GemFire 5.0.2
 *
 */
public class PreferBytesCachedDeserializable
    implements CachedDeserializable, DataSerializableFixedID {


  /**
   * empty constructor for serialization only
   */
  public PreferBytesCachedDeserializable() {}


  /** The cached value */
  private byte[] value;

  /**
   * +PER_OBJECT_OVERHEAD for VMCachedDeserializable object +4 for value field
   */
  static final int MEM_OVERHEAD = PER_OBJECT_OVERHEAD + 4;

  /**
   * Creates a new instance of <code>PreferBytesCachedDeserializable</code>.
   *
   * Note that, in general, instances of this class should be obtained via
   * {@link CachedDeserializableFactory}.
   */
  PreferBytesCachedDeserializable(byte[] serializedValue) {
    value = serializedValue;
    if (serializedValue == null) {
      throw new NullPointerException(
          "value must not be null");
    }
  }

  public PreferBytesCachedDeserializable(Object object) {
    value = EntryEventImpl.serialize(object);
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return EntryEventImpl.deserialize(value);
  }

  @Override
  public Object getDeserializedForReading() {
    return getDeserializedValue(null, null);
  }

  @Override
  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    return getDeserializedValue(r, re);
  }

  /**
   * Return the serialized value as a byte[]
   */
  @Override
  public byte[] getSerializedValue() {
    return value;
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
    wrapper.setData(value, userBits, value.length,
        false /* Not Reusable as it refers to underlying value */);
  }


  /**
   * Return current value regardless of whether it is serialized or deserialized: if it was
   * serialized than it is a byte[], otherwise it is not a byte[].
   */
  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public int getSizeInBytes() {
    return MEM_OVERHEAD + CachedDeserializableFactory.getByteSize(value);
  }

  @Override
  public int getValueSizeInBytes() {
    return CachedDeserializableFactory.getByteSize(value);
  }

  @Override
  public int getDSFID() {
    return PREFER_BYTES_CACHED_DESERIALIZABLE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    value = DataSerializer.readByteArray(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeByteArray(value, out);
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  @Override
  public String toString() {
    return getShortClassName() + "@" + hashCode();
  }

  @Override
  public void writeValueAsByteArray(DataOutput out) throws IOException {
    toData(out, InternalDataSerializer.createSerializationContext(out));
  }

  @Override
  public String getStringForm() {
    try {
      return StringUtils.forceToString(getDeserializedForReading());
    } catch (RuntimeException ex) {
      return "Could not convert object to string because " + ex;
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
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
