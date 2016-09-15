/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.internal.cache.compression;

import org.apache.geode.cache.Cache;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.Version;

/**
 * An implementation of {@link CompressedCachedDeserializable} that uses the
 * built in Snappy compression codec and favors absolute minimal region entry
 * value overhead by sharing the same Snappy {@link Compressor} instance with
 * all instances of this class within the {@link Cache}.
 * 
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

  @Override
  public boolean isSerialized() {
    return true;
  }

  @Override
  public boolean usesHeapForStorage() {
    return true;
  }
}
