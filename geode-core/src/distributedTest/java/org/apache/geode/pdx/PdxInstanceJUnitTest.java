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

package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.pdx.internal.EnumInfo.PdxInstanceEnumInfo;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxInstanceJUnitTest extends JUnit4CacheTestCase {

  private GemFireCacheImpl cache;
  private int allFieldCount;

  @Before
  public void setUp() {
    // make it a loner
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0")
        .set(DISTRIBUTED_SYSTEM_ID, "255").setPdxReadSerialized(true).create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testGetField() throws IOException, ClassNotFoundException {
    PdxInstance instance = getPdx(new TestPdx() {
      @Override
      public void toData(PdxWriter out) {
        out.writeBoolean("field1", false);
        out.writeInt("field2", 53);
        out.writeObject("field3", new TestPdx() {
          @Override
          public void toData(PdxWriter writer) {
            writer.writeString("afield", "hello");
          }

        });
      }
    });

    assertThat(instance.getFieldNames()).containsExactly("field1", "field2", "field3");
    assertThat(instance.getField("field2")).isEqualTo(53);
    assertThat(instance.getField("field1")).isEqualTo(false);
    PdxInstance fieldInstance = (PdxInstance) instance.getField("field3");
    assertThat(fieldInstance.getFieldNames()).containsExactly("afield");
    assertThat(fieldInstance.getField("afield")).isEqualTo("hello");
  }

  @Test
  public void testHashCodeAndEqualsSameType() throws IOException, ClassNotFoundException {
    PdxInstance instance = getAllFields(0);
    assertThat(instance).isEqualTo(getAllFields(0));
    assertThat(instance.hashCode()).isEqualTo(getAllFields(0).hashCode());

    for (int i = 1; i < allFieldCount + 1; i++) {
      PdxInstance other = getAllFields(i);
      assertThat(instance.equals(other))
          .withFailMessage("One field " + i + " hashcode have been unequal but were equal"
              + instance.getField("field" + (i - 1)) + ", " + other.getField("field" + (i - 1))
              + ", "
              + instance + ", " + other)
          .isFalse();
      // Technically, this could be true. If this asserts fails I guess we can just change the test.
      assertThat(instance.hashCode()).withFailMessage(
          "One field " + i + " hashcode have been unequal but were equal" + instance + ", " + other)
          .isNotEqualTo(other.hashCode());
    }
  }

  @Test
  public void testEquals() {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testEquals", false, cache);
    c.writeInt("intField", 37);
    PdxInstance pi = c.create();
    assertThat(pi.equals(null)).isFalse();
    assertThat(pi.equals(new Date(37))).isFalse();
    c = PdxInstanceFactoryImpl.newCreator("testEquals", false, cache);
    c.writeInt("intField", 37);
    c.writeInt("intField2", 38);
    PdxInstance pi2 = c.create();
    assertThat(pi.hashCode()).isNotEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isFalse();
    assertThat(pi2.equals(pi)).isFalse();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new Date());
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", null);
    pi2 = c.create();
    assertThat(pi.hashCode()).isNotEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isFalse();
    assertThat(pi2.equals(pi)).isFalse();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new int[] {1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new byte[] {(byte) 1});
    pi2 = c.create();
    assertThat(pi.hashCode()).isNotEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isFalse();
    assertThat(pi2.equals(pi)).isFalse();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new int[] {1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new int[] {2});
    pi2 = c.create();
    assertThat(pi.hashCode()).isNotEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isFalse();
    assertThat(pi2.equals(pi)).isFalse();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new int[] {1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new int[] {1});
    pi2 = c.create();
    assertThat(pi.hashCode()).isEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isTrue();
    assertThat(pi2.equals(pi)).isTrue();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new int[] {1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new Date());
    pi2 = c.create();
    assertThat(pi.hashCode()).isNotEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isFalse();
    assertThat(pi2.equals(pi)).isFalse();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new Date[] {new Date(1)});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new Date[] {new Date(2)});
    pi2 = c.create();
    assertThat(pi.hashCode()).isNotEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isFalse();
    assertThat(pi2.equals(pi)).isFalse();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new Date[] {new Date(1)});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", new Date[] {new Date(1)});
    pi2 = c.create();
    assertThat(pi.hashCode()).isEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isTrue();
    assertThat(pi2.equals(pi)).isTrue();

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", MyEnum.ONE);
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false, cache);
    c.writeObject("objField", MyEnum.ONE);
    pi2 = c.create();
    assertThat(pi.hashCode()).isEqualTo(pi2.hashCode());
    assertThat(pi.equals(pi2)).isTrue();
    assertThat(pi2.equals(pi)).isTrue();
  }

  public enum MyEnum {
    ONE, TWO
  }

  public enum MyComplexEnum {
    ONE {},
    TWO {}
  }

  @Test
  public void testPdxComplexEnum() {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testPdxEnum", false, cache);
    c.writeObject("enumField", MyComplexEnum.ONE);
    PdxInstance pi = c.create();
    Object f = pi.getField("enumField");
    assertThat(f).isInstanceOf(PdxInstanceEnumInfo.class);
    PdxInstanceEnumInfo e = (PdxInstanceEnumInfo) f;
    assertThat(e.getName()).isEqualTo("ONE");
    cache.getPdxRegistry().flushCache();
    assertThat(e.getObject()).isEqualTo(MyComplexEnum.ONE);
  }

  @Test
  public void testPdxSimpleEnum() {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testPdxEnum", false, cache);
    c.writeObject("enumField", MyEnum.ONE);
    PdxInstance pi = c.create();
    Object f = pi.getField("enumField");
    assertThat(f).isInstanceOf(PdxInstanceEnumInfo.class);
    PdxInstanceEnumInfo e = (PdxInstanceEnumInfo) f;
    assertThat(e.getName()).isEqualTo("ONE");
    cache.getPdxRegistry().flushCache();
    assertThat(e.getObject()).isEqualTo(MyEnum.ONE);
  }

  @Test
  public void testEqualsDifferentTypes() throws IOException, ClassNotFoundException {
    PdxInstance instance1 = getPdx(new TestPdx() {
      @Override
      public void toData(PdxWriter writer) {
        writer.writeBoolean("field1", true);
      }
    });

    PdxInstance instance2 = getPdx(new TestPdx() {
      @Override
      public void toData(PdxWriter writer) {
        writer.writeBoolean("field1", true);
      }
    });

    // These are different classes, so they shouldn't match.
    assertThat(instance1.equals(instance2)).isFalse();
    assertThat(instance1.isIdentityField("field1")).isFalse();
  }

  @Test
  public void testHashCodeEqualsOutOfOrderFields()
      throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    PdxSerializable serializable1 = getSeparateClassLoadedPdx(true);
    PdxInstance instance1 = getPdx(serializable1);
    PdxSerializable serializable2 = getSeparateClassLoadedPdx(false);
    PdxInstance instance2 = getPdx(serializable2);

    assertThat(instance2).isEqualTo(instance1);
    assertThat(instance2.hashCode()).isEqualTo(instance1.hashCode());
  }

  @Test
  public void testIdentityFields() throws IOException, ClassNotFoundException {
    PdxInstance instance1 = getPdx(getPdxSerializable(new TestPdx() {
      @Override
      public void toData(PdxWriter out) {
        out.writeInt("field2", 53);
        out.writeBoolean("field1", false);
        out.markIdentityField("field2");
      }
    }));

    PdxInstance instance2 = getPdx(getPdxSerializable(new TestPdx() {
      @Override
      public void toData(PdxWriter out) {
        out.writeInt("field2", 53);
        out.writeBoolean("field1", true);
        out.markIdentityField("field2");
      }
    }));

    assertThat(instance2).isEqualTo(instance1);
    assertThat(instance2.hashCode()).isEqualTo(instance1.hashCode());
    assertThat(instance1.isIdentityField("field1")).isFalse();
    assertThat(instance1.isIdentityField("field2")).isTrue();
  }

  // This is hack to make sure the classnames are the same
  // for every call to this method, even if the toDatas are different.
  private PdxSerializable getPdxSerializable(final TestPdx toData) {
    return new TestPdx() {
      @Override
      public void toData(PdxWriter writer) {
        toData.toData(writer);
      }
    };
  }

  private PdxSerializable getSeparateClassLoadedPdx(boolean field1First)
      throws ClassNotFoundException, NoSuchMethodException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    ClassLoader loader1 = new NonDelegatingLoader(parent);
    Class<?> clazz1 =
        loader1.loadClass(getClass().getPackage().getName() + ".SeparateClassloaderPdx");
    Constructor<?> constructor = clazz1.getConstructor(boolean.class);
    constructor.setAccessible(true);
    return (PdxSerializable) constructor.newInstance(field1First);
  }

  private PdxInstance getAllFields(final int change) throws IOException, ClassNotFoundException {
    return getPdx(new TestPdx() {
      @Override
      public void toData(PdxWriter out) {
        int x = 0;
        Number serializable1 = new BigInteger("1234");
        Number serializable2 = new BigInteger("1235");
        Collection<Number> collection1 = new ArrayList<>();
        collection1.add(serializable1);
        collection1.add(serializable2);
        Collection<Number> collection2 = new ArrayList<>();
        collection2.add(serializable2);
        collection2.add(serializable1);
        SimpleClass testPdx1 = new SimpleClass(5, (byte) 5);
        SimpleClass testPdx2 = new SimpleClass(6, (byte) 6);
        HashMap<Number, Number> map1 = new HashMap<>();
        HashMap<Number, Number> map2 = new HashMap<>();
        map2.put(serializable1, serializable2);


        out.writeChar("field" + x++, change == x ? 'c' : 'd');
        out.writeBoolean("field" + x++, change == x);
        out.writeByte("field" + x++, (byte) (change == x ? 0x5 : 0x6));
        out.writeShort("field" + x++, (short) (change == x ? 7 : 8));
        out.writeInt("field" + x++, change == x ? 9 : 10);
        out.writeLong("field" + x++, change == x ? 55555555 : 666666666);
        out.writeFloat("field" + x++, change == x ? 23.44f : 23.55f);
        out.writeDouble("field" + x++, change == x ? 28.7777 : 29.3333);
        out.writeDate("field" + x++, change == x ? new Date(23L) : new Date(25L));
        // out.writeEnum("field" + x++, change==x ? Enum<?> e: );
        out.writeString("field" + x++, change == x ? "hello" : "world");
        out.writeObject("field" + x++, change == x ? serializable1 : serializable2);
        out.writeObject("field" + x++, change == x ? testPdx1 : testPdx2);
        out.writeObject("field" + x++, change == x ? map1 : map2);
        out.writeObject("field" + x++, change == x ? collection1 : collection2);
        // out.writeRegion("field" + x++, change==x ? region1: region2);
        out.writeBooleanArray("field" + x++,
            change == x ? new boolean[] {false, true} : new boolean[] {true, false});
        out.writeCharArray("field" + x++,
            change == x ? new char[] {'a', 'b'} : new char[] {'a', 'c'});
        out.writeByteArray("field" + x++,
            change == x ? new byte[] {0, 1, 2} : new byte[] {0, 1, 3});
        out.writeShortArray("field" + x++,
            change == x ? new short[] {0, 1, 4} : new short[] {0, 1, 5});
        out.writeIntArray("field" + x++, change == x ? new int[] {0, 1, 4} : new int[] {0, 1, 5});
        out.writeLongArray("field" + x++,
            change == x ? new long[] {0, 1, 4} : new long[] {0, 1, 5});
        out.writeFloatArray("field" + x++,
            change == x ? new float[] {0, 1, 4} : new float[] {0, 1, 5});
        out.writeDoubleArray("field" + x++,
            change == x ? new double[] {0, 1, 4} : new double[] {0, 1, 5});
        out.writeStringArray("field" + x++,
            change == x ? new String[] {"a", "b"} : new String[] {"a", "c"});
        out.writeObjectArray("field" + x++,
            change == x ? new Object[] {collection1, serializable1} : new Object[] {serializable2});
        out.writeArrayOfByteArrays("field" + x++,
            change == x ? new byte[][] {{1, 2}} : new byte[][] {{2, 2}});
        allFieldCount = x;
      }
    });
  }

  private PdxInstance getPdx(PdxSerializable toData) throws IOException, ClassNotFoundException {
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(toData, out);
    return DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
  }



  abstract static class TestPdx implements PdxSerializable {

    @Override
    public void fromData(PdxReader in) {}

  }

}
