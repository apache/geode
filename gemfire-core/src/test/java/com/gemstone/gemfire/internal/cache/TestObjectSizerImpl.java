/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.Serializable;

import com.gemstone.gemfire.cache.util.ObjectSizer;

/**
 * Test object which implements ObjectSizer, used as Key/Value in put operation
 * as well as used as a Sizer for HeapLru testing.
 * 
 * @author arajpal
 * 
 */
public class TestObjectSizerImpl implements ObjectSizer, Serializable {

  private static final long serialVersionUID = 0L;

  String one;

  String two;

  String three;

  public TestObjectSizerImpl() {
    one = "ONE";
    two = "TWO";
    three = "THREE";
  }

  public int sizeof(Object o) {
    if (o instanceof Integer) {
     return 5;
    }
    else if (o instanceof TestNonSizerObject) {
      return ((TestNonSizerObject)o).getTestString().length();
    } else if (o instanceof CachedDeserializable) { 
      throw new IllegalStateException("ObjectSize should not be passed (see bug 40718) instances of CachedDeserializable: " + o); 
            //       if (((VMCachedDeserializable)o).getDeserializedCopy() instanceof TestObjectSizerImpl) { 
            //         TestObjectSizerImpl obj = (TestObjectSizerImpl)((VMCachedDeserializable)o) 
            //           .getDeserializedCopy(); 
            //         return Sizeof.sizeof(obj.one) + Sizeof.sizeof(obj.two) 
            //           + Sizeof.sizeof(obj.three); 
            //       } 
            //       else if (((VMCachedDeserializable)o).getDeserializedCopy() instanceof TestNonSizerObject) { 

//      TestNonSizerObject testNonObjectSizerObject = (TestNonSizerObject)((VMCachedDeserializable)o)
//          .getDeserializedCopy();
//      return testNonObjectSizerObject.getTestString().length();
    }
    else {
      return ObjectSizer.DEFAULT.sizeof(o);
    }

  }

}
