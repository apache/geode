/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

/**
 * No instances of this class. Just some static method having to do with inline keys.
 * @author darrel
 *
 */
public class InlineKeyHelper {
  public static boolean INLINE_REGION_KEYS = !Boolean.getBoolean("gemfire.DISABLE_INLINE_REGION_KEYS");

  /**
   * The maximum number of longs any of region entry subclasses use
   * to store the string key inline.
   */
  public static final int MAX_LONGS_USED_FOR_STRING_KEY = 2;
  
  /**
   * Given the number of longs used to encode the inline string
   * return the maximum number of characters that can be encoded
   * into that many longs.
   */
  public static final int getMaxInlineStringKey(int longCount, boolean byteEncoded) {
    return (longCount * (byteEncoded ? 8 : 4)) - 1;
  }

  /**
   * Return null if the given string can not be encoded inline.
   * Return true if the given string can be encoded inline as bytes.
   * Return false if the given string can be encoded inline as chars.
   */
  public static Boolean canStringBeInlineEncoded(String skey) {
    if (skey.length() > getMaxInlineStringKey(MAX_LONGS_USED_FOR_STRING_KEY, true)) {
      return null;
    }
    if (isByteEncodingOk(skey)) {
      return Boolean.TRUE;
    } else {
      if (skey.length() > getMaxInlineStringKey(MAX_LONGS_USED_FOR_STRING_KEY, false)) {
        return null;
      } else {
        return Boolean.FALSE;
      }
    }
  }

  private static boolean isByteEncodingOk(String skey) {
    for (int i=0; i < skey.length(); i++) {
      if (skey.charAt(i) > 0x7f) {
        return false;
      }
    }
    return true;
  }

}
