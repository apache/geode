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

import com.gemstone.gemfire.DataSerializable;

public class MyObjectDataSerializable extends MyObject implements DataSerializable {
  public MyObjectDataSerializable() {
  }

  public MyObjectDataSerializable(long number, String s) {
    super(number, s);
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(f1);
    out.writeUTF(f2);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    f1 = in.readLong();
    f2 = in.readUTF();
  }
}