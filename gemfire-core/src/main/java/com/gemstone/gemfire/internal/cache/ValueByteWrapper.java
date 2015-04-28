/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * A wrapper class for the operation which has been recovered to be recovered by
 * the htree
 */

class ValueByteWrapper
{
  /** stores the bytes that were read * */
  private final byte[] valueBytes;

  private byte userBit;

  /**
   * Constructs the wrapper object
   * 
   * @param value
   *          byte[] bytes read from oplog
   * @param userBit
   *          A byte describing the nature of the value i.e whether it is:
   *          serialized , invalid or empty byte array etc.
   */

  ValueByteWrapper(byte[] value, byte userBit) {
    this.valueBytes = value;
    this.userBit = userBit;
  }

  /**
   * @return boolean returns true if the value bytes are a serialized object
   * 
   * boolean isSerialized() { return isSerialized; }
   */

  /**
   * @return byte[] returns the value bytes stored
   */
  byte[] getValueBytes()
  {
    return this.valueBytes;
  }

  /**
   * Getter method for the userBit assosciated with the value read from Oplog
   * during initialization
   * 
   * @return byte value
   */
  byte getUserBit()
  {
    return this.userBit;
  }
}
