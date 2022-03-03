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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.serialization.KnownVersion;

public class ObjectTraverserPerf {

  private static final int ITERATIONS = 1000;
  private static final int OBJECT_DEPTH = 1000;
  private static final boolean USE_SERIALIZATION = true;


  public static void main(String[] args)
      throws IllegalArgumentException, IllegalAccessException, IOException {
    TestObject testObject = new TestObject(null);

    for (int i = 0; i < OBJECT_DEPTH; i++) {
      testObject = new TestObject(testObject);
    }

    // warm up
    for (int i = 0; i < ITERATIONS; i++) {
      calcSize(testObject);
    }

    long start = System.nanoTime();
    for (int i = 0; i < ITERATIONS; i++) {
      calcSize(testObject);
    }
    long end = System.nanoTime();

    System.out.println("Sized object of depth " + OBJECT_DEPTH + " for " + ITERATIONS
        + " iterations elapsed(ns) :" + (end - start));
  }


  private static void calcSize(TestObject testObject) throws IllegalAccessException, IOException {
    if (USE_SERIALIZATION) {
      // NullDataOutputStream out = new NullDataOutputStream();
      HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
      testObject.toData(out);
    } else {
      ObjectGraphSizer.size(testObject);
    }
  }


  public static class TestObject implements DataSerializable {
    public int field1;
    public int field2;

    public final String field3 = "hello";
    public final DataSerializable field4;

    public TestObject(DataSerializable field4) {
      this.field4 = field4;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException("Don't need this method for the test");

    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.write(field1);
      out.write(field2);
      DataSerializer.writeString(field3, out);
      if (field4 != null) {
        field4.toData(out);
      }
    }
  }

}
