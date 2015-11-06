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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Creates and compares composite keys.
 * 
 * @author bakera
 */
public interface CompositeSerializedComparator extends SerializedComparator  {
  /**
   * Constructs a composite key consisting of a primary key and a secondary key.
   * 
   * @param key1 the primary key
   * @param key2 the secondary key
   * @return the composite key
   */
  public byte[] createCompositeKey(byte[] key1, byte[] key2);
  
  /**
   * Constructs a composite key by combining the supplied keys.  The number of
   * keys and their order must match the comparator set.
   * <p>
   * The <code>WILDCARD_KEY</code> token may be used to match all subkeys in the
   * given ordinal position.  This is useful when constructing a search key to
   * retrieve all keys for a given primary key, ignoring the remaining subkeys.
   * 
   * @param keys the keys, ordered by sort priority
   * @return the composite key
   */
  public byte[] createCompositeKey(byte[]... keys);
  
  /**
   * Returns subkey for the given ordinal position.
   * @param key the composite key
   * @return the subkey
   */
  public ByteBuffer getKey(ByteBuffer key, int ordinal);
}
