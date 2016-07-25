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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;

/**
 * No instances of this class. Just some static method having to do with inline keys.
 *
 */
public class InlineKeyHelper {
  public static boolean INLINE_REGION_KEYS = !Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "DISABLE_INLINE_REGION_KEYS");

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
