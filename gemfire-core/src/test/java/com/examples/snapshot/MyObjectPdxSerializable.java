/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.snapshot;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

public class MyObjectPdxSerializable extends MyObject implements PdxSerializable {
  public MyObjectPdxSerializable() {
  }

  public MyObjectPdxSerializable(long number, String s) {
    super(number, s);
  }

  @Override
  public void toData(PdxWriter writer) {
    writer.writeLong("f1", f1);
    writer.writeString("f2", f2);
  }

  @Override
  public void fromData(PdxReader reader) {
    f1 = reader.readLong("f1");
    f2 = reader.readString("f2");
  }
}
