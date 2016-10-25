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
package org.apache.geode.internal.util;

import java.nio.ByteBuffer;

/**
 * Provides utilities for converting from byte[] to primitive values.
 * 
 */
public class Bytes {
  /**
   * Inserts the integer value into the array at the requested offset.
   * 
   * @param val the value
   * @param buf the array
   * @param off the offset
   */
  public static void putInt(int val, byte[] buf, int off) {
    assert off + 4 <= buf.length;
    
    buf[off    ] = int0(val);
    buf[off + 1] = int1(val);
    buf[off + 2] = int2(val);
    buf[off + 3] = int3(val);
  }
  
  /**
   * Inserts the long value into the array at the requested offset.
   * 
   * @param val the value
   * @param buf the array
   * @param off the offset
   */
  public static void putLong(long val, byte[] buf, int off) {
    assert off + 4 <= buf.length;
    
    buf[off    ] = long0(val);
    buf[off + 1] = long1(val);
    buf[off + 2] = long2(val);
    buf[off + 3] = long3(val);
    buf[off + 4] = long4(val);
    buf[off + 5] = long5(val);
    buf[off + 6] = long6(val);
    buf[off + 7] = long7(val);
  }

  /**
   * Extracts the protobuf varint from the buffer.
   * 
   * @param buf the buffer
   * @return the varint
   */
  public static int getVarInt(ByteBuffer buf) {
    byte b;
    int val;
    
    // unrolled! :-)
    b = buf.get(); val  = (b & 0x7f);       if ((b & 0x80) == 0) return val;
    b = buf.get(); val |= (b & 0x7f) <<  7; if ((b & 0x80) == 0) return val;
    b = buf.get(); val |= (b & 0x7f) << 14; if ((b & 0x80) == 0) return val;
    b = buf.get(); val |= (b & 0x7f) << 21; if ((b & 0x80) == 0) return val;
    b = buf.get(); val |= (b & 0x7f) << 28; 
    
    return val;
  }
  
  /**
   * Extracts the protbuf varint from the array.
   * @param buf the array
   * @param off the offset
   * @return the varint
   */
  public static int getVarInt(byte[] buf, int off) {
    byte b;
    int val;
    
    // unrolled! :-)
    b = buf[off++]; val  = (b & 0x7f);       if ((b & 0x80) == 0) return val;
    b = buf[off++]; val |= (b & 0x7f) <<  7; if ((b & 0x80) == 0) return val;
    b = buf[off++]; val |= (b & 0x7f) << 14; if ((b & 0x80) == 0) return val;
    b = buf[off++]; val |= (b & 0x7f) << 21; if ((b & 0x80) == 0) return val;
    b = buf[off++]; val |= (b & 0x7f) << 28; 
    
    return val;
  }
  
  /**
   * Inserts the protobuf varint into the buffer at the current position.
   * 
   * @param val the value
   * @param buf the buffer
   * @return the buffer
   */
  public static ByteBuffer putVarInt(int val, ByteBuffer buf) {
    assert val >= 0;

    // protobuf-style varint encoding
    // set the MSB as continuation bit for each byte except the last byte
    // pack the bytes in reverse order
    // packed size is (bits / 7) + 1
    while ((val & ~0x7f) != 0) {
      buf.put((byte) ((val & 0x7f) | 0x80));
      val >>= 7;
    }
    return buf.put((byte) val);
  }
  
  /**
   * Inserts the protobuf varint into the array at the requested offset.
   * 
   * @param val the value
   * @param buf the array
   * @param off the offset
   * @return the updated offset
   */
  public static int putVarInt(int val, byte[] buf, int off) {
    assert val >= 0;

    // protobuf-style varint encoding
    // set the MSB as continuation bit for each byte except the last byte
    // TODO see if unrolling is faster
    while (val > 0x7f) {
      buf[off++] = (byte) ((val & 0x7f) | 0x80);
      val >>= 7;
    }
    buf[off++] = (byte) val;
    return off;
  }
  
  /**
   * Returns the bytes required to store a protobuf varint.
   * 
   * @param val the value
   * @return the varint size
   */
  public static int sizeofVarInt(int val) {
    assert val >= 0;
    
    if (val < (1 << 7)) {
      return 1;
    } else if (val < (1 << 14)) {
      return 2;
    } else if (val < (1 << 21)) {
      return 3;
    } else if (val < (1 << 28)) {
      return 4;
    }
    return 5;
  }
  
  /**
   * Creates a short value from two bytes.
   * 
   * @param b0 the first byte
   * @param b1 the second byte
   * @return the value
   */
  public static short toShort(byte b0, byte b1) {
    return (short) ((b0 << 8) | (b1 & 0xff));
  }
  
  /**
   * Creates a char value from two bytes.
   * 
   * @param b0 the first byte
   * @param b1 the second byte
   * @return the value
   */
  public static char toChar(byte b0, byte b1) {
    return (char) ((b0 << 8) | (b1 & 0xff));
  }

  /**
   * Creates an unsigned short from two bytes.
   * 
   * @param b0 the first byte
   * @param b1 the second byte
   * @return the value
   */
  public static int toUnsignedShort(byte b0, byte b1) {
    return ((b0 & 0xff) << 8) | (b1 & 0xff);
  }
  
  /**
   * Creates an integer from four bytes.
   * 
   * @param b0 the first byte
   * @param b1 the second byte
   * @param b2 the third byte
   * @param b3 the fourth byte
   * @return the value
   */
  public static int toInt(byte b0, byte b1, byte b2, byte b3) {
    return (b0 << 24) 
        | ((b1 & 0xff) << 16) 
        | ((b2 & 0xff) << 8) 
        |  (b3 & 0xff); 
  }
  
  /**
   * Creates a long from eight bytes.
   * 
   * @param b0 the first byte
   * @param b1 the second byte
   * @param b2 the third byte
   * @param b3 the fourth byte
   * @param b4 the fifth byte
   * @param b5 the sixth byte
   * @param b6 the seventh byte
   * @param b7 the eighth byte
   * @return the value
   */
  public static long toLong(byte b0, byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7) {
    return ((long) b0 << 56)
        | (((long) b1 & 0xff) << 48) 
        | (((long) b2 & 0xff) << 40) 
        | (((long) b3 & 0xff) << 32) 
        | (((long) b4 & 0xff) << 24) 
        | (((long) b5 & 0xff) << 16) 
        | (((long) b6 & 0xff) << 8)
        |  ((long) b7 & 0xff); 
  }
  
  public static byte char0(char value)   { return (byte) (value >> 8); }
  public static byte char1(char value)   { return (byte)  value; }

  public static byte short0(short value) { return (byte) (value >> 8); }
  public static byte short1(short value) { return (byte)  value; }

  public static byte int0(int value)     { return (byte) (value >> 24); }
  public static byte int1(int value)     { return (byte) (value >> 16); }
  public static byte int2(int value)     { return (byte) (value >> 8); }
  public static byte int3(int value)     { return (byte)  value; }

  public static byte long0(long value)   { return (byte) (value >> 56); }
  public static byte long1(long value)   { return (byte) (value >> 48); }
  public static byte long2(long value)   { return (byte) (value >> 40); }
  public static byte long3(long value)   { return (byte) (value >> 32); }
  public static byte long4(long value)   { return (byte) (value >> 24); }
  public static byte long5(long value)   { return (byte) (value >> 16); }
  public static byte long6(long value)   { return (byte) (value >> 8); }
  public static byte long7(long value)   { return (byte)  value; }
}
