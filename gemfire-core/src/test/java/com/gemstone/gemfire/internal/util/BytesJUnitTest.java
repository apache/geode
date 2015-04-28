/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.util;

import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class BytesJUnitTest extends TestCase {
  private ByteBuffer buf = ByteBuffer.allocate(8);
  
  public void testShort() {
    short[] val = { 666, -1, Short.MIN_VALUE, 0, 12, Short.MAX_VALUE };
    for (int i = 0; i < val.length; i++) {
      buf.putShort(val[i]).flip();
      assertEquals(val[i], Bytes.toShort(buf.get(), buf.get()));
      
      buf.rewind();
    }
  }
  
  public void testChar() {
    char[] val = { 'a', 'b', 'c' };
    for (int i = 0; i < val.length; i++) {
      buf.putChar(val[i]).flip();
      assertEquals(val[i], Bytes.toChar(buf.get(), buf.get()));
      
      buf.rewind();
    }
  }
  
  public void testUnsignedShort() {
    int[] val = { 0, 1, Short.MAX_VALUE + 1, 2 * Short.MAX_VALUE };
    for (int i = 0; i < val.length; i++) {
      buf.put(Bytes.int2(val[i])).put(Bytes.int3(val[i])).flip();
      assertEquals(val[i], Bytes.toUnsignedShort(buf.get(), buf.get()));
      
      buf.rewind();
    }
  }
  
  public void testInt() {
    int[] val = { 666, -1, Integer.MIN_VALUE, 0, 1, Integer.MAX_VALUE };
    for (int i = 0; i < val.length; i++) {
      buf.putInt(val[i]).flip();
      assertEquals(val[i], Bytes.toInt(buf.get(), buf.get(), buf.get(), buf.get()));
      
      buf.rewind();
      
      byte[] bytes = new byte[4];
      Bytes.putInt(val[i], bytes, 0);
      assertEquals(val[i], Bytes.toInt(bytes[0], bytes[1], bytes[2], bytes[3]));
    }
  }
  
  public void testLong() {
    long[] val = { 666, -1, Long.MIN_VALUE, 0, 1, Long.MAX_VALUE };
    for (int i = 0; i < val.length; i++) {
      buf.putLong(val[i]).flip();
      assertEquals(val[i], Bytes.toLong(buf.get(), buf.get(), buf.get(), buf.get(),
          buf.get(), buf.get(), buf.get(), buf.get()));
      
      buf.rewind();
    }
  }
  
  
  public void testVarint() {
    ByteBuffer buf = ByteBuffer.allocate(5);
    checkVarint(0, buf);
    
    // 1 byte
    checkVarint(1, buf);
    checkVarint(0x7f, buf);
    
    // 2 byte
    checkVarint(0x80, buf);
    checkVarint(0x7fff, buf);
    
    // 3 byte
    checkVarint(0x8000, buf);
    checkVarint(0x7fffff, buf);
    
    // 4 byte
    checkVarint(0x800000, buf);
    checkVarint(0x7fffffff, buf);
  }
  
  private void checkVarint(int v, ByteBuffer buf) {
    Bytes.putVarInt(v, buf);
    buf.rewind();
    
    int v2 = Bytes.getVarInt(buf);
    assertEquals(v, v2);
    buf.clear();
  }
}
