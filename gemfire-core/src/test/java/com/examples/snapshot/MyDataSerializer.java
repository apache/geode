/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.snapshot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;

public class MyDataSerializer extends DataSerializer {
  @Override
  public Class<?>[] getSupportedClasses() {
    return new Class[] { MyObjectDataSerializable2.class};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    MyObject obj = (MyObject) o;
    out.writeLong(obj.f1);
    out.writeUTF(obj.f2);
    
    return true;
  }

  @Override
  public Object fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    MyObjectDataSerializable2 obj = new MyObjectDataSerializable2();
    obj.f1 = in.readLong();
    obj.f2 = in.readUTF();
    
    return obj;
  }

  @Override
  public int getId() {
    return 8892;
  }
  
  public static class MyObjectDataSerializable2 extends MyObject {
    public MyObjectDataSerializable2() {
    }

    public MyObjectDataSerializable2(long number, String s) {
      super(number, s);
    }
  }
}
