/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.size;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;

/**
 * @author dsmith
 *
 */
public class ObjectTraverserPerf {
  
  private static final int ITERATIONS = 1000;
  private static final int OBJECT_DEPTH = 1000;
  private static final boolean USE_SERIALIZATION = true;


  public static void main(String[] args) throws IllegalArgumentException, IllegalAccessException, IOException {
    TestObject testObject = new TestObject(null);
    
    for(int i =0; i < OBJECT_DEPTH; i++) {
      testObject = new TestObject(testObject);
    }
    
    //warm up
    for(int i = 0; i < ITERATIONS; i++) {
      calcSize(testObject);
    }
    
    long start = System.nanoTime();
    for(int i = 0; i < ITERATIONS; i++) {
      calcSize(testObject);
    }
    long end = System.nanoTime();
    
    System.out.println("Sized object of depth " + OBJECT_DEPTH + " for " + ITERATIONS + " iterations elapsed(ns) :" + (end - start));
  }


  private static void calcSize(TestObject testObject)
      throws IllegalAccessException, IOException {
    if(USE_SERIALIZATION) {
//      NullDataOutputStream out = new NullDataOutputStream();
      HeapDataOutputStream out= new HeapDataOutputStream(Version.CURRENT);
      testObject.toData(out);
    } else {
      ObjectGraphSizer.size(testObject);
    }
  }
  
  
  public static class TestObject implements DataSerializable {
    public int field1;
    public int field2;
    
    public final String field3 = new String("hello");
    public final DataSerializable field4;
    
    public TestObject(DataSerializable field4) {
      this.field4 = field4;
    }

    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      throw new UnsupportedOperationException("Don't need this method for the test");
      
    }

    public void toData(DataOutput out) throws IOException {
      out.write(field1);
      out.write(field2);
      DataSerializer.writeString(field3, out);
      if(field4 != null) {
        field4.toData(out);
      }
    }
  }

}
