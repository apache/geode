/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;
import static com.gemstone.gemfire.internal.size.SizeTestUtil.*;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class WellKnownClassSizerJUnitTest extends TestCase {
  
  public void testByteArrays() {
    byte[] test1 = new byte[5];
    byte[] test2 = new byte[8];
    
    ReflectionSingleObjectSizer referenceSizer = new ReflectionSingleObjectSizer();
    
    assertEquals(referenceSizer.sizeof(test1), WellKnownClassSizer.sizeof(test1));
    assertEquals(referenceSizer.sizeof(test2), WellKnownClassSizer.sizeof(test2));
    
    assertEquals(0, WellKnownClassSizer.sizeof(new Object()));
  }
  
  public void testStrings() {
    String test1 = "123";
    String test2 = "012345678";
    
    ReflectionSingleObjectSizer referenceSizer = new ReflectionSingleObjectSizer();
    test1.toCharArray();
    
    //The size of a string varies based on the JDK version. With 1.7.0_06
    //a couple of fields were removed. So just measure the size of an empty string.
    String emptyString = "";
    int emptySize = ObjectSizer.SIZE_CLASS_ONCE.sizeof(emptyString) - ObjectSizer.SIZE_CLASS_ONCE.sizeof(new char[0]);
    
    assertEquals(emptySize + roundup(OBJECT_SIZE +4 + 3*2), WellKnownClassSizer.sizeof(test1));
    assertEquals(emptySize + roundup(OBJECT_SIZE +4 + 9*2), WellKnownClassSizer.sizeof(test2));
  }

}
