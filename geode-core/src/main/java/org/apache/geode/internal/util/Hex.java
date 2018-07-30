/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.util;

/**
 * Provides hexadecimal conversion and display utilities.
 *
 */
public class Hex {
  /** hex chars */
  private static final char[] HEX =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

  /**
   * Converts the byte array to a hex string.
   *
   * @param buf the buffer to convert
   * @return the hex string
   */
  public static String toHex(byte[] buf) {
    return toHex(buf, 0, buf.length);
  }

  /**
   * Converts the byte array subset to a hex string.
   *
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

  /**
   * Converts a hex string to a byte array.
   *
   * @param hex the hex string
   * @return the byte array
   */
  public static byte[] toByteArray(String hex) {
    byte[] bytes = new byte[hex.length() / 2];
    for (int i = 0; i < bytes.length; i++) {
      int index = i * 2;
      int v = Integer.parseInt(hex.substring(index, index + 2), 16);
      bytes[i] = (byte) v;
    }
    return bytes;
  }
}
