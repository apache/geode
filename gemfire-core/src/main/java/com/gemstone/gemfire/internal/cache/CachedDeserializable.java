/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.internal.util.BlobHelper;

/**
 * Provides protocol for getting the deserialized value from a potentially
 * encapsulated object.
 *
 * @author Eric Zoerner
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
   * @since 4.0
   */
  public Object getDeserializedForReading();
  
  /**
   * Gets the string form of the cached object. If an exception
   * is thrown while converting to a string then the exception
   * will be caught and put in the returned string.
   * @return a string that represents the cached object.
   * @since 6.6
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
}
