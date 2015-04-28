/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.compression;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.internal.Version;

/**
 * An implementation of {@link CompressedCachedDeserializable} that uses the
 * built in Snappy compression codec and favors absolute minimal region entry
 * value overhead by sharing the same Snappy {@link Compressor} instance with
 * all instances of this class within the {@link Cache}.
 * 
 * @author rholmes
 */
public class SnappyCompressedCachedDeserializable extends
    CompressedCachedDeserializable {

  /**
   * Empty constructor for serialization.
   */
  public SnappyCompressedCachedDeserializable() {
    super();
  }

  /**
   * Creates a new {@link SnappyCompressedCachedDeserializable} with a serialized value or raw byte array.
   * @param serializedValue a region entry value that has already been serialized or is a raw byte array.
   */  
  public SnappyCompressedCachedDeserializable(final byte[] serializedValue) {
    super(serializedValue);
  }

  /**
   * Creates a new {@link SnappyCompressedCachedDeserializable} with an unserialized value.
   * @param obj a region entry value.
   */  
  public SnappyCompressedCachedDeserializable(final Object obj) {
    super(obj);
  }
  
  /**
   * Shared Snappy {@link Compressor} instance.
   */
  private static final Compressor compressor = SnappyCompressor.getDefaultInstance();

  @Override
  protected Compressor getCompressor() {
    return compressor;
  }

  @Override
  protected int getMemoryOverhead() {
    return BASE_MEM_OVERHEAD;
  }

  @Override
  public int getDSFID() {
    return SNAPPY_COMPRESSED_CACHED_DESERIALIZABLE;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}