/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

/**
 * Provides hexadecimal conversion and display utilities.
 * 
 * @author bakera
 */
public class Hex {
  /** hex chars */
  private static final char[] HEX = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
  
  /**
   * Converts the byte array to a hex string.
   * @param buf the buffer to convert
   * @return the hex string
   */
  public static String toHex(byte[] buf) {
    return toHex(buf, 0, buf.length);
  }

  /**
   * Converts the byte array subset to a hex string.
   * @param buf the buffer to convert
   * @param offset the offset
   * @param length the length
   * @return the hex string
   */
  public static String toHex(byte[] buf, int offset, int length) {
    if (buf == null) {
      return null;
    }
    
    char[] hex = new char[2 * length];
    for (int i = 0; i < length; i++) {
      int b = buf[i + offset] & 0xff;
      hex[2 * i] = HEX[b >>> 4];
      hex[2 * i + 1] = HEX[b & 0xf];
    }
    return new String(hex);
  }
}
