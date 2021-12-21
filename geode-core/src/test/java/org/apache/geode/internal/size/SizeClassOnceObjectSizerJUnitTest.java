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
package org.apache.geode.internal.size;

import static org.apache.geode.internal.size.SizeTestUtil.OBJECT_SIZE;
import static org.apache.geode.internal.size.SizeTestUtil.REFERENCE_SIZE;
import static org.apache.geode.internal.size.SizeTestUtil.roundup;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.geode.cache.util.ObjectSizer;

public class SizeClassOnceObjectSizerJUnitTest {

  @Test
  public void test() {
    byte[] b1 = new byte[5];
    byte[] b2 = new byte[15];

    // Make sure that we actually size byte arrays each time
    assertEquals(roundup(OBJECT_SIZE + 4 + 5), ObjectSizer.SIZE_CLASS_ONCE.sizeof(b1));
    assertEquals(roundup(OBJECT_SIZE + 4 + 15), ObjectSizer.SIZE_CLASS_ONCE.sizeof(b2));

    String s1 = "12345";
    String s2 = "1234567890";

    // The size of a string varies based on the JDK version. With 1.7.0_06
    // a couple of fields were removed. So just measure the size of an empty string.
    String emptyString = "";
    int emptySize = ObjectSizer.SIZE_CLASS_ONCE.sizeof(emptyString)
        - ObjectSizer.SIZE_CLASS_ONCE.sizeof(new char[0]);

    // Make sure that we actually size strings each time
    assertEquals(emptySize + roundup(OBJECT_SIZE + 4 + 5 * 2),
        ObjectSizer.SIZE_CLASS_ONCE.sizeof(s1));
    assertEquals(emptySize + roundup(OBJECT_SIZE + 4 + 10 * 2),
        ObjectSizer.SIZE_CLASS_ONCE.sizeof(s2));

    TestObject t1 = new TestObject(5);
    TestObject t2 = new TestObject(15);
    int t1Size = ObjectSizer.SIZE_CLASS_ONCE.sizeof(t1);
    assertEquals(roundup(OBJECT_SIZE + REFERENCE_SIZE) + roundup(OBJECT_SIZE + 4 + 5), t1Size);
    // Since we are using SIZE_CLASS_ONCE t2 should have the same size as t1
    assertEquals(t1Size, ObjectSizer.SIZE_CLASS_ONCE.sizeof(t2));
  }

  private static class TestObject {
    private final byte[] field;

    public TestObject(int size) {
      field = new byte[size];
    }
  }

}
