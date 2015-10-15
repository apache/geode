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
