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
package org.apache.geode.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import junit.framework.TestCase;
import org.junit.After;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DataSerializerHolderJUnitTest extends TestCase {

  @After
  public void tearDown() {
    InternalDataSerializer.reinitialize();
  }

  public void testHandshakeDatSerializerRegistrationDoesNotHitNPE() throws Throwable {
    final Throwable[] error = new Throwable[1];

    final CountDownLatch serializersRegistered = new CountDownLatch(1);
    final CountDownLatch serializersQueried = new CountDownLatch(1);

    Thread handshakeThread = new Thread("Fake handshake thread") {
      public void run() {
        try {
          Class[] serializers = new Class[] {DataSerializer1.class, DataSerializer2.class,
              DataSerializer3.class, DataSerializer4.class, DataSerializer5.class,
              DataSerializer6.class, DataSerializer7.class, DataSerializer8.class,
              DataSerializer9.class, DataSerializer10.class, DataSerializer11.class,
              DataSerializer12.class, DataSerializer13.class};
          for (int index = 0; index < serializers.length; index++) {
            int id = InternalDataSerializer.newInstance(serializers[index]).getId();
            InternalDataSerializer.register(serializers[index].getName(), false, null, null, id);
          }
          serializersRegistered.countDown();
          serializersQueried.await();
          Map<Integer, List<String>> supportedClasses = new HashMap<>();
          for (int index = 0; index < serializers.length; index++) {
            DataSerializer serializer = InternalDataSerializer.newInstance(serializers[index]);
            List<String> classes = Arrays.<Class>asList(serializer.getSupportedClasses()).stream()
                .map((clazz) -> clazz.getName()).collect(Collectors.toList());
            supportedClasses.put(serializer.getId(), classes);
          }
          InternalDataSerializer.updateSupportedClassesMap(supportedClasses);
        } catch (Throwable t) {
          error[0] = t;
        }
      }
    };
    handshakeThread.setDaemon(true);
    handshakeThread.start();

    // wait until serializers have been registered but not their supported classes
    serializersRegistered.await();

    // find all of the serializers. This should resolve all of the DataSerializer holders
    // that were used to avoid loading the classes in InternalDataSerializer and clear
    // out the idsToHolders collection. This was causing an NPE when supportedClasses were
    // registered by the client/server handshake code
    InternalDataSerializer.getSerializers();
    serializersQueried.countDown();

    handshakeThread.join(10000);

    if (error[0] != null) {
      throw error[0];
    }
  }

  public static class DataSerializer1 extends DataSerializer {
    int tempField = 5;

    public DataSerializer1() {

    }

    @Override
    public int getId() {
      return 1;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {MyClass1.class, MyClass2.class, MyClass3.class, MyClass4.class,
          MyClass5.class, MyClass6.class, MyClass7.class, MyClass8.class, MyClass9.class,
          MyClass10.class};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      return true;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer2 extends DataSerializer {
    int tempField = 15;

    public DataSerializer2() {}

    @Override
    public int getId() {
      return 2;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {MyClass11.class, MyClass12.class, MyClass13.class, MyClass14.class,
          MyClass15.class, MyClass16.class, MyClass17.class, MyClass18.class, MyClass19.class,
          MyClass20.class};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      return true;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer3 extends DataSerializer {
    int tempField = 25;

    public DataSerializer3() {}

    @Override
    public int getId() {
      return 3;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer4 extends DataSerializer {
    int tempField = 5;

    public DataSerializer4() {

    }

    @Override
    public int getId() {
      return 4;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer5 extends DataSerializer {
    int tempField = 15;

    public DataSerializer5() {}

    @Override
    public int getId() {
      return 5;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer6 extends DataSerializer {
    int tempField = 25;

    public DataSerializer6() {}

    @Override
    public int getId() {
      return 6;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer7 extends DataSerializer {
    int tempField = 5;

    public DataSerializer7() {

    }

    @Override
    public int getId() {
      return 7;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer8 extends DataSerializer {
    int tempField = 15;

    public DataSerializer8() {}

    @Override
    public int getId() {
      return 8;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer9 extends DataSerializer {
    int tempField = 25;

    public DataSerializer9() {}

    @Override
    public int getId() {
      return 9;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer10 extends DataSerializer {
    int tempField = 5;

    public DataSerializer10() {

    }

    @Override
    public int getId() {
      return 10;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer11 extends DataSerializer {
    int tempField = 15;

    public DataSerializer11() {}

    @Override
    public int getId() {
      return 11;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer12 extends DataSerializer {
    int tempField = 25;

    public DataSerializer12() {}

    @Override
    public int getId() {
      return 12;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class DataSerializer13 extends DataSerializer {
    int tempField = 25;

    public DataSerializer13() {}

    @Override
    public int getId() {
      return 19;
    }

    @Override
    public Class[] getSupportedClasses() {
      return new Class[] {this.getClass()};
    }

    @Override
    public boolean toData(Object o, DataOutput out) throws IOException {
      out.write(tempField);
      return false;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      readInteger(in);
      return null;
    }
  }

  public static class MyClass1 {
    public MyClass1() {}
  }
  public static class MyClass2 {
    public MyClass2() {}
  }
  public static class MyClass3 {
    public MyClass3() {}
  }
  public static class MyClass4 {
    public MyClass4() {}
  }
  public static class MyClass5 {
    public MyClass5() {}
  }
  public static class MyClass6 {
    public MyClass6() {}
  }
  public static class MyClass7 {
    public MyClass7() {}
  }
  public static class MyClass8 {
    public MyClass8() {}
  }
  public static class MyClass9 {
    public MyClass9() {}
  }
  public static class MyClass10 {
    public MyClass10() {}
  }
  public static class MyClass11 {
    public MyClass11() {}
  }
  public static class MyClass12 {
    public MyClass12() {}
  }
  public static class MyClass13 {
    public MyClass13() {}
  }
  public static class MyClass14 {
    public MyClass14() {}
  }
  public static class MyClass15 {
    public MyClass15() {}
  }
  public static class MyClass16 {
    public MyClass16() {}
  }
  public static class MyClass17 {
    public MyClass17() {}
  }
  public static class MyClass18 {
    public MyClass18() {}
  }
  public static class MyClass19 {
    public MyClass19() {}
  }
  public static class MyClass20 {
    public MyClass20() {}
  }

}
