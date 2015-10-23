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
package com.gemstone.gemfire.internal.offheap;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.cache.CachedDeserializable;

/**
 * Represents an object stored in the cache.
 * Currently this interface is only used for values stored in off-heap regions.
 * At some point in the future it may also be used for values stored in heap regions. 
 * 
 * @author darrel
 * @since 9.0
 */
public interface StoredObject extends OffHeapReference, CachedDeserializable {
  /**
   * Take all the bytes in the object and write them to the data output as a byte array.
   * If the StoredObject is not serialized then its raw byte array is sent.
   * But if it is serialized then the serialized byte array is sent.
   * The corresponding de-serialization will need to call readByteArray.
   * 
   * @param out
   *          the data output to send this object to
   * @throws IOException
   */
  void sendAsByteArray(DataOutput out) throws IOException;
  /**
   * Take all the bytes in the object and write them to the data output as a byte array.
   * If the StoredObject is not serialized then an exception will be thrown.
   * The corresponding deserialization will need to call readObject and will get an
   * instance of VMCachedDeserializable.
   * 
   * @param out
   *          the data output to send this object to
   * @throws IOException
   */
  void sendAsCachedDeserializable(DataOutput out) throws IOException;
}
