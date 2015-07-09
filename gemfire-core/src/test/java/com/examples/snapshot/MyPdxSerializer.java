/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.examples.snapshot;

import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer;

public class MyPdxSerializer implements PdxSerializer, Declarable {
  private final PdxSerializer auto = new ReflectionBasedAutoSerializer("com.examples.snapshot.My.*Pdx");
  
  @Override
  public void init(Properties props) {
  }
  @Override
  public boolean toData(Object o, PdxWriter out) {
    if (o instanceof MyObjectPdx2) {
      MyObjectPdx2 obj = (MyObjectPdx2) o;
      out.writeLong("f1", obj.f1);
      out.writeString("f2", obj.f2);
      return true;
    }
    return auto.toData(o, out);
  }

  @Override
  public Object fromData(Class<?> clazz, PdxReader in) {
    if (clazz == MyObjectPdx2.class) {
      MyObjectPdx2 obj = new MyObjectPdx2();
      obj.f1 = in.readLong("f1");
      obj.f2 = in.readString("f2");
      
      return obj;
    }
    return auto.fromData(clazz, in);
  }
  
  public static class MyObjectPdx2 extends MyObject {
    public MyObjectPdx2() {
    }

    public MyObjectPdx2(long number, String s) {
      super(number, s);
    }
  }
}
