/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * Used to fetch a record's raw bytes and user bits.
 * The actual data length in byte array may be less than
 * the size of the byte array itself. An integer field contains
 * the valid length. This class is used exclusively by the Oplog Compactor
 * for rolling the entries. The reason for this class is to reuse the
 * underlying byte array for rolling multiple entries there by
 * reducing the garbage.
 * @author Asif 
 * @since 5.5
 */
public class BytesAndBitsForCompactor {
  private  byte[] data;
  private  byte userBits=0;
  // length of the data present in the byte array 
  private  int validLength;
  private static final byte[] INIT_FOR_WRAPPER = new byte[0];
  // boolean indicating if the object can be reused.
  // Typically if the data stores the reference of a value byte [] directly
  // from the RegionEntry than this byte array cannot be reused for
  //storing another entry's data 
  private boolean isReusable ;

  public BytesAndBitsForCompactor() {
    this.data = INIT_FOR_WRAPPER;
    //this.userBits = userBits;
    this.validLength = INIT_FOR_WRAPPER.length;
    this.isReusable = true;
  }

  public final byte[] getBytes() {
    return this.data;
  }
  public final byte getBits() {
    return this.userBits;
  }
  
  public final int getValidLength() {
    return this.validLength;
  }
  
  public boolean isReusable() {
    return this.isReusable;
  }
  
  /**
   * 
   * @param data byte array storing the data 
   * @param userBits byte with appropriate bits set
   * @param validLength  The number of bytes representing the data , starting from 0 as offset
   * @param isReusable true if this object is safe for reuse as a data holder
   */
  public void setData(byte[] data, byte userBits, int validLength, boolean isReusable) {
    this.data = data;
    this.userBits = userBits;
    this.validLength = validLength;    
    this.isReusable = isReusable;
  }
}
