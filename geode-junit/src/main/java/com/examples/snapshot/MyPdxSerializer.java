/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.examples.snapshot;

import java.util.Properties;

import org.apache.geode.cache.Declarable;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializer;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

public class MyPdxSerializer implements PdxSerializer, Declarable {
  private final PdxSerializer auto =
      new ReflectionBasedAutoSerializer("com.examples.snapshot.My.*Pdx");

  @Override
  public void init(Properties props) {}

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
    public MyObjectPdx2() {}

    public MyObjectPdx2(long number, String s) {
      super(number, s);
    }
  }
}
