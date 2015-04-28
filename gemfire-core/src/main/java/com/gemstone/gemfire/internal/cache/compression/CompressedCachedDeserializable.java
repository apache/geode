/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.compression;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * An abstract implementation of {@link CachedDeserializable} that prefers serialization and compresses
 * the internal serialized value.
 * 
 * @author rholmes
 */
public abstract class CompressedCachedDeserializable implements
    CachedDeserializable, DataSerializableFixedID {
  /**
   * +PER_OBJECT_OVERHEAD for CompressedCachedDeserializable object
   * +4 for value field
   */
  static final int BASE_MEM_OVERHEAD = PER_OBJECT_OVERHEAD + 4;

  /**
   * Compressed region entry value.
   */
  protected byte[] value = null;
  
  /**
   * Empty constructor for serialization.
   */
  public CompressedCachedDeserializable() {
  }

  /**
   * @return A {@link Compressor} for compressing/decompressing the region entry value.
   */
  protected abstract Compressor getCompressor();
  
  /**
   * @return the memory overhead for this implementation CompressedCachedDeserializable.
   */
  protected abstract int getMemoryOverhead();
  
  /**
   * @see DataSerializableFixedID#getDSFID()
   */
  @Override
  public abstract int getDSFID();

  /**
   * Creates a new {@link CompressedCachedDeserializable} with a serialized value.
   * @param serializedValue a region entry value that has already been serialized.
   */
  public CompressedCachedDeserializable(final byte[] serializedValue) {    
    if (serializedValue == null) {
      throw new NullPointerException(LocalizedStrings.PreferBytesCachedDeserializable_VALUE_MUST_NOT_BE_NULL.toLocalizedString());
    }
    
    this.value = getCompressor().compress(serializedValue);
  }
  
  /**
   * Creates a new {@link CompressedCachedDeserializable} with an unserialized value.
   * @param obj a region entry value.
   */
  public CompressedCachedDeserializable(final Object obj) {
    if (obj == null)
      throw new NullPointerException(LocalizedStrings.PreferBytesCachedDeserializable_VALUE_MUST_NOT_BE_NULL.toLocalizedString());
    
    this.value = getCompressor().compress(EntryEventImpl.serialize(obj));
  }
  
  /**
   * @see Sizeable#getSizeInBytes()
   */
  @Override
  public int getSizeInBytes() {
    return getMemoryOverhead() + CachedDeserializableFactory.getByteSize(this.value);
  }

  /**
   * @see DataSerializableFixedID#toData(DataOutput)
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(getCompressor().decompress(this.value), out);
  }

  /**
   * @see DataSerializableFixedID#fromData(DataInput)
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.value = getCompressor().compress(DataSerializer.readByteArray(in));
  }
  
  /**
   * Returns the serialized value of the region entry contained by this CompressedCachedDeserializable.  This value 
   * is the uncompressed value.
   * @see CachedDeserializable#getSerializedValue()
   */
  @Override
  public byte[] getSerializedValue() {
    return getCompressor().decompress(this.value);
  }

  /**
   * Returns the deserialized value.  However, unlike the specification of this method as described in
   * @link {@link CachedDeserializable#getDeserializedForReading()} this method does not 
   * optimize for future calls leaving the value serialized and compressed.
   */
  @Override
  public Object getDeserializedForReading() {
    return getDeserializedValue(null,null);
  }

  /**
   * @see CachedDeserializable#getStringForm()
   */
  @Override
  public String getStringForm() {
    try {
      return StringUtils.forceToString(getDeserializedForReading());
    } catch (RuntimeException ex) {
      return "Could not convert object to string because " + ex;
    }
  }

  /**
   * @see CachedDeserializable#getDeserializedWritableCopy(Region, RegionEntry)
   */
  @Override
  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    return getDeserializedValue(r,re);
  }

  /**
   * @see CachedDeserializable#getDeserializedValue(Region, RegionEntry)
   */
  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return EntryEventImpl.deserialize(getCompressor().decompress(this.value));
  }

  /**
   * @see CachedDeserializable#getValue()
   */
  @Override
  public Object getValue() {
    return getCompressor().decompress(this.value);
  }

  @Override
  public void writeValueAsByteArray(DataOutput out) throws IOException {
    toData(out);
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper,
      byte userBits) {
    byte[] uncompressed = getCompressor().decompress(this.value);
    wrapper.setData(uncompressed, userBits, uncompressed.length, false);
  }

  @Override
  public int getValueSizeInBytes() {
    return CachedDeserializableFactory.getByteSize(this.value);
  }
}