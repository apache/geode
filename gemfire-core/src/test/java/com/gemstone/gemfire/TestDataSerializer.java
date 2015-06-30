/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import util.TestException;

import com.gemstone.gemfire.internal.cache.tier.sockets.DataSerializerPropogationDUnitTest;

/**
 * @author ashetkar
 *
 */
public class TestDataSerializer extends DataSerializer {
  
  static {
    DataSerializerPropogationDUnitTest.successfullyLoadedTestDataSerializer = true;
  }

  private String name;
  private int age;

  public TestDataSerializer() {
  }

  public TestDataSerializer(String str, int val) {
    this.name = str;
    this.age = val;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#getSupportedClasses()
   */
  @Override
  public Class<?>[] getSupportedClasses() {
    return new Class[] { TestSupportedClass1.class, TestSupportedClass2.class, TestSupportedClass3.class};
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#toData(java.lang.Object, java.io.DataOutput)
   */
  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    writeString(this.name, out);
    writePrimitiveInt(this.age, out);
    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#fromData(java.io.DataInput)
   */
  @Override
  public Object fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    return new TestDataSerializer(readString(in), readPrimitiveInt(in));
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#getId()
   */
  @Override
  public int getId() {
    return 91;
  }

}

class TestSupportedClass1 {
  private int field = 10;
  public void setField(int f) {
    this.field = f;
  }
  public int getField() {
    return this.field;
  }
}

class TestSupportedClass2 {
  private int field = 20;
  public void setField(int f) {
    this.field = f;
  }
  public int getField() {
    return this.field;
  }
}

class TestSupportedClass3 {
  private int field = 30;
  public void setField(int f) {
    this.field = f;
  }
  public int getField() {
    return this.field;
  }
}

