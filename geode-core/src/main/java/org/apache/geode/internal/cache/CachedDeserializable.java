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

package org.apache.geode.internal.cache;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.cache.lru.Sizeable;
import org.apache.geode.internal.util.BlobHelper;

/**
 * Provides protocol for getting the deserialized value from a potentially
 * encapsulated object.
 *
 *
 */
public interface CachedDeserializable extends Sizeable
{
  
  /**
   * Returns the raw byte[] that represents this cache value.
   * @return the raw byte[] that represents this cache value
   */
  public byte[] getSerializedValue();

  /**
   * Gets a deserialized value for reading.
   * Differs from getDeserializedValue by leaving the value in a form
   * that will optimize future calls.
   * @since GemFire 4.0
   */
  public Object getDeserializedForReading();
  
  /**
   * Gets the string form of the cached object. If an exception
   * is thrown while converting to a string then the exception
   * will be caught and put in the returned string.
   * @return a string that represents the cached object.
   * @since GemFire 6.6
   */
  public String getStringForm();
  
  /**
   * Always makes a copy of the deserialized object and returns it.
   * Leaves the value in a form that will optimize future calls.
   * @param r the region that owns this object or null if no owner
   * @param re the region entry that owns this object or null if no owner
   * @return the deserialized object for this cache value
   */
  public Object getDeserializedWritableCopy(Region r, RegionEntry re);
  
  /**
   * Returns the deserialized object for this cache value.
   * @param r the region that owns this object or null if no owner
   * @param re the region entry that owns this object or null if no owner
   * @return the deserialized object for this cache value
   */
  public Object getDeserializedValue(Region r, RegionEntry re);

  /**
   * Return current value regardless of whether it is serialized or
   * deserialized: if it was serialized than it is a byte[], otherwise
   * it is not a byte[].
   */
  public Object getValue();

  /**
   * Write out the value contained in this instance to the stream 
   * as a byte array (versus serialized form).  Anything reading from the
   * stream will have to perform two operations to reconstitute the value:
   *  1) read the byte array off the stream {@link DataSerializer#readByteArray(java.io.DataInput)}
   *  2) de-serialize the byte array using {@link BlobHelper#deserializeBlob(byte[])}
   * into an object.  The idea is to delay de-serialization until the last possible moment
   * to provide better parallelism
   * @param out the stream to write on
   * @throws IOException
   */
  public void writeValueAsByteArray(DataOutput out) throws IOException;
  
  /**
   * Sets the serialized value of the Object in the wrapper along with
   * appropriate user bit & valid length. If the Object is already in a
   * serialized form then the byte array is set in the wrapper along with
   * boolean reusable as false
   * 
   * @param wrapper object of type BytesAndBitsForCompactor 
   */
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits);
  
  /**
   * Return the size of the value, not including the overhead
   * added by this wrapper class.
   */
  public int getValueSizeInBytes();
  /**
   * Returns true if the value stored in this memory chunk is a serialized object. Returns false if it is a byte array.
   */
  public boolean isSerialized();
  /**
   * Return true if the value uses the java heap; false if not.
   */
  public boolean usesHeapForStorage();
}
