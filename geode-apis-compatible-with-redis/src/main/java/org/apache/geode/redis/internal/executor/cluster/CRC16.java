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

package org.apache.geode.redis.internal.executor.cluster;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;


/**
 * Helper class to calculate the CRC-16/XMODEM value of a byte array. This is the same algorithm
 * that Redis uses.
 *
 * Derived from https://www.source-code.biz/snippets/java/crc16/
 */
public class CRC16 {

  // CCITT/SDLC/HDLC x^16 + x^12 + x^5 + 1 (CRC-16-CCITT)
  private static final int CCITT_POLY = 0x8408;
  private static final short[] crcTable = new short[256];

  // Create the table up front
  static {
    int poly = reverseInt16(CCITT_POLY);

    for (int x = 0; x < 256; x++) {
      int w = x << 8;
      for (int i = 0; i < 8; i++) {
        if ((w & 0x8000) != 0) {
          w = (w << 1) ^ poly;
        } else {
          w = w << 1;
        }
      }
      crcTable[x] = (short) w;
    }
  }

  /**
   * Calculate CRC with most significant byte first. Assume all inputs are valid.
   *
   * @param data the byte array to use
   * @param start starting index into the byte array
   * @param end ending index (exclusive) into the byte array
   */
  public static int calculate(byte[] data, int start, int end) {
    int crc = 0;
    for (int i = start; i < end; i++) {
      crc = ((crc << 8) & 0xFF00) ^ (crcTable[(crc >> 8) ^ (data[i] & 0xFF)] & 0xFFFF);
    }
    return crc;
  }

  public static int calculate(String data) {
    byte[] bytes = stringToBytes(data);
    return calculate(bytes, 0, bytes.length);
  }

  // Reverses the bits of a 16 bit integer.
  private static int reverseInt16(int i) {
    i = (i & 0x5555) << 1 | (i >>> 1) & 0x5555;
    i = (i & 0x3333) << 2 | (i >>> 2) & 0x3333;
    i = (i & 0x0F0F) << 4 | (i >>> 4) & 0x0F0F;
    i = (i & 0x00FF) << 8 | (i >>> 8);
    return i;
  }

}
