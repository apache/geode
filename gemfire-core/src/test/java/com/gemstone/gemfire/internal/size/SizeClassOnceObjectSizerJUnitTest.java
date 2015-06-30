/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;
import static com.gemstone.gemfire.internal.size.SizeTestUtil.*;

import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class SizeClassOnceObjectSizerJUnitTest extends TestCase{
  
  public void test() {
    byte[] b1 = new byte[5];
    byte[] b2 = new byte[15];
    
    //Make sure that we actually size byte arrays each time
    assertEquals(roundup(OBJECT_SIZE +4 + 5), ObjectSizer.SIZE_CLASS_ONCE.sizeof(b1));
    assertEquals(roundup(OBJECT_SIZE +4 + 15), ObjectSizer.SIZE_CLASS_ONCE.sizeof(b2));
    
    String s1 = "12345";
    String s2 = "1234567890";
    
    //The size of a string varies based on the JDK version. With 1.7.0_06
    //a couple of fields were removed. So just measure the size of an empty string.
    String emptyString = "";
    int emptySize = ObjectSizer.SIZE_CLASS_ONCE.sizeof(emptyString) - ObjectSizer.SIZE_CLASS_ONCE.sizeof(new char[0]);
    
    //Make sure that we actually size strings each time
    assertEquals(emptySize + roundup(OBJECT_SIZE +4 + 5*2), ObjectSizer.SIZE_CLASS_ONCE.sizeof(s1));
    assertEquals(emptySize + roundup(OBJECT_SIZE +4 + 10*2), ObjectSizer.SIZE_CLASS_ONCE.sizeof(s2));
    
    TestObject t1 = new TestObject(5);
    TestObject t2 = new TestObject(15);
    int t1Size = ObjectSizer.SIZE_CLASS_ONCE.sizeof(t1);
    assertEquals(roundup(OBJECT_SIZE + REFERENCE_SIZE) + roundup(OBJECT_SIZE + 4 + 5), t1Size);
    // Since we are using SIZE_CLASS_ONCE t2 should have the same size as t1
    assertEquals(t1Size, ObjectSizer.SIZE_CLASS_ONCE.sizeof(t2));
  }
  
  private static class TestObject {
    private final byte[] field;
    
    public TestObject(int size) {
      this.field = new byte[size];
    }
  }

}
