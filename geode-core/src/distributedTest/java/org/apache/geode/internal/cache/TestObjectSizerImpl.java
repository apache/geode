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
package org.apache.geode.internal.cache;

import java.io.Serializable;

import org.apache.geode.cache.util.ObjectSizer;

/**
 * Test object which implements ObjectSizer, used as Key/Value in put operation as well as used as a
 * Sizer for HeapLru testing.
 *
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
    } else if (o instanceof TestNonSizerObject) {
      return ((TestNonSizerObject) o).getTestString().length();
    } else if (o instanceof CachedDeserializable) {
      throw new IllegalStateException(
          "ObjectSize should not be passed (see bug 40718) instances of CachedDeserializable: "
              + o);
      // if (((VMCachedDeserializable)o).getDeserializedCopy() instanceof TestObjectSizerImpl) {
      // TestObjectSizerImpl obj = (TestObjectSizerImpl)((VMCachedDeserializable)o)
      // .getDeserializedCopy();
      // return Sizeof.sizeof(obj.one) + Sizeof.sizeof(obj.two)
      // + Sizeof.sizeof(obj.three);
      // }
      // else if (((VMCachedDeserializable)o).getDeserializedCopy() instanceof TestNonSizerObject) {

      // TestNonSizerObject testNonObjectSizerObject =
      // (TestNonSizerObject)((VMCachedDeserializable)o)
      // .getDeserializedCopy();
      // return testNonObjectSizerObject.getTestString().length();
    } else {
      return ObjectSizer.DEFAULT.sizeof(o);
    }

  }

}
