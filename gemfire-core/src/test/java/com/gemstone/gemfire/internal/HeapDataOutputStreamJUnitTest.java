/*=========================================================================
 * Copyright (c) 2011-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.nio.ByteBuffer;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * Test of methods on HeapDataOutputStream
 * 
 * TODO right now this just tests the new
 * write(ByteBuffer) method. We might want
 * to add some unit tests for the existing methods.
 *
 */
@Category(UnitTest.class)
public class HeapDataOutputStreamJUnitTest extends TestCase {
  
  public void testWriteByteBuffer() {
    HeapDataOutputStream out = new HeapDataOutputStream(32, Version.CURRENT);
    
    byte[] bytes = "1234567890qwertyuiopasdfghjklzxcvbnm,./;'".getBytes();
    out.write(ByteBuffer.wrap(bytes, 0, 2));
    out.write(ByteBuffer.wrap(bytes, 2, bytes.length - 2));
    
    byte[] actual = out.toByteArray();
    
    assertEquals(new String(bytes) , new String(actual));
  }

}
