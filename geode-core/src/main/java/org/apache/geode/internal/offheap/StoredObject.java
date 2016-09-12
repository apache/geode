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
package org.apache.geode.internal.offheap;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.geode.internal.Sendable;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * Represents an object stored in the cache.
 * Currently this interface is only used for values stored in off-heap regions.
 * This interface provides methods that let you read and write the bytes
 * of addressable memory used to store the bytes of the object.
 * A reference count is used to determine if the object is still allocated.
 * To increment the count call {@link #retain()}.
 * To decrement the count call {@link #release()}.
 * At some point in the future it may also be used for values stored in heap regions. 
 * 
 * @since Geode 1.0
 */
public interface StoredObject extends Sendable, CachedDeserializable, Releasable {
  /**
   * Returns true if the value stored in this memory chunk is compressed. Returns false if it is uncompressed.
   */
  public boolean isCompressed();

  /**
   * Returns the data stored in this object as a deserialized heap object.
   * If it is not serialized then the result will be a byte[].
   * Otherwise the deserialized heap form of the stored object is returned.
   * @return the data stored in this object as a deserialized heap object.
   */
  public Object getValueAsDeserializedHeapObject();

  /**
   * Returns the data stored in this object as a heap byte array.
   * If it is not serialized then the result will only contain the raw bytes stored in this object.
   * Otherwise the serialized heap form of the stored object is returned.
   * @return the data stored in this object as a heap byte array.
   */
  public byte[] getValueAsHeapByteArray();
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
  
  /**
   * Call to indicate that this object's memory is in use by the caller.
   * The memory will stay allocated until {@link #release()} is called.
   * It is ok for a thread other than the one that called this method to call release.
   * This method is called implicitly at the time the chunk is allocated.
   * Note: @Retained tells you that "this" is retained by this method.
   * 
   * @throws IllegalStateException if the max ref count is exceeded.
   * @return true if we are able to retain this chunk; false if we need to retry
   */
  @Retained
  public boolean retain();

  /**
   * Returns true if this type of StoredObject uses a references count; false otherwise.
   */
  public boolean hasRefCount();
   /**
   * Returns the number of users of this memory. If this type of StoredObject does not
   * have a reference count then -1 is returned.
   */
  public int getRefCount();
  
  /**
   * Returns the address of the memory used to store this object.
   * This address may not be to the first byte of stored data since
   * the implementation may store some internal data in the first bytes of the memory.
   * This address can be used with AddressableMemoryManager.
   */
  public long getAddress();

  /**
   * Returns the number of bytes of memory used by this object to store an object.
   * This size includes any bytes used for padding and meta-information.
   */
  public int getSize();
  
  /**
   * Returns the number of bytes of memory used to store the object.
   * This size does not include any bytes used for padding.
   */
  public int getDataSize();
  public byte readDataByte(int offset);
  public void writeDataByte(int offset, byte value);
  public void readDataBytes(int offset, byte[] bytes);
  public void writeDataBytes(int offset, byte[] bytes);
  public void readDataBytes(int offset, byte[] bytes, int bytesOffset, int size);
  public void writeDataBytes(int offset, byte[] bytes, int bytesOffset, int size);
  /**
   * Returns an address that can read data from this StoredObject at the given offset.
   */
  public long getAddressForReadingData(int offset, int size);
  
  /**
   * Returns a StoredObject that acts as if its data is our data starting
   * at the given offset and limited to the given number of bytes.
   */
  public StoredObject slice(int offset, int limit);
  
  /**
   * Returns true if our data is equal to other's data; false otherwise.
   */
  public boolean checkDataEquals(StoredObject other);
  /**
   * Returns true if the given bytes are equal to our data bytes; false otherwise
   */
  public boolean checkDataEquals(byte[] serializedObj);

  /**
   * Creates and returns a direct ByteBuffer that contains the data of this stored object.
   * Note that the returned ByteBuffer has a reference to the
   * address of this stored object so it can only be used while this stored object is retained.
   * @return the created direct byte buffer or null if it could not be created.
   */
  @Unretained
  public ByteBuffer createDirectByteBuffer();
  /**
   * Returns true if the data is serialized with PDX
   */
  public boolean isSerializedPdxInstance();
  
  /**
   * Returns a StoredObject that does not cache the heap form.
   * If a StoredObject is going to be kept around for a while then
   * it is good to call this so that it will not also keep the heap
   * form in memory.
   */
  public StoredObject getStoredObjectWithoutHeapForm();

  /**
   * Return true if the given "o" is reference to off-heap memory.
   */
  public static boolean isOffHeapReference(Object o) {
    return (o instanceof StoredObject) && ((StoredObject)o).hasRefCount();
  }
}
