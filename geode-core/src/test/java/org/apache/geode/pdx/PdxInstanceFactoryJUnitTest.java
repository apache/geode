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

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import org.apache.geode.pdx.internal.*;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.util.*;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class PdxInstanceFactoryJUnitTest {
  private GemFireCacheImpl cache;

  @Before
  public void setUp() {
    // make it a loner
    this.cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0")
        .setPdxReadSerialized(true).create();
  }

  @After
  public void tearDown() {
    this.cache.close();
  }

  public enum Coin {
    HEADS, TAILS, EDGE
  };

  @Test
  public void testBasics() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("basics", false);
    c.writeInt("intField", 37);
    PdxInstance pi = c.create();
    WritablePdxInstance wpi = pi.createWriter();
    assertEquals(true, wpi.hasField("intField"));
    assertEquals(37, wpi.getField("intField"));
    assertEquals(false, wpi.isIdentityField("intField"));
    wpi.setField("intField", 38);
    assertEquals(38, wpi.getField("intField"));
    checkPdxInstance(wpi);
    PdxType t1 = ((PdxInstanceImpl) pi).getPdxType();
    PdxInstanceFactory c2 = PdxInstanceFactoryImpl.newCreator("basics", false);
    c2.writeInt("intField", 46);
    PdxInstance pi2 = c2.create();
    PdxType t2 = ((PdxInstanceImpl) pi2).getPdxType();
    assertEquals(t1, t2);
    assertEquals(t1.getTypeId(), t2.getTypeId());
  }

  @Test
  public void testEnums() throws IOException, ClassNotFoundException {
    PdxInstance e0 =
        this.cache.createPdxEnum(Coin.class.getName(), Coin.HEADS.name(), Coin.HEADS.ordinal());
    assertEquals(true, e0.isEnum());
    assertEquals(true, e0.hasField("name"));
    assertEquals(true, e0.hasField("ordinal"));
    assertEquals(false, e0.hasField("bogus"));
    assertEquals(null, e0.getField("bogus"));
    assertEquals(Coin.class.getName(), e0.getClassName());
    assertEquals(Coin.HEADS.name(), e0.getField("name"));
    assertEquals(Coin.HEADS.ordinal(), e0.getField("ordinal"));
    assertEquals(Coin.HEADS, e0.getObject());

    PdxInstance e1 =
        this.cache.createPdxEnum(Coin.class.getName(), Coin.TAILS.name(), Coin.TAILS.ordinal());
    PdxInstance e2 =
        this.cache.createPdxEnum(Coin.class.getName(), Coin.EDGE.name(), Coin.EDGE.ordinal());
    try {
      this.cache.createPdxEnum(Coin.class.getName(), Coin.EDGE.name(), 79);
      throw new RuntimeException("expected PdxSerializationException");
    } catch (PdxSerializationException expected) {
    }
    Comparable<Object> c0 = (Comparable<Object>) e0;
    Comparable c1 = (Comparable) e1;
    Comparable c2 = (Comparable) e2;
    assertEquals(true, c0.compareTo(c1) < 0);
    assertEquals(true, c1.compareTo(c0) > 0);
    assertEquals(true, c1.compareTo(c2) < 0);
    assertEquals(true, c2.compareTo(c1) > 0);
    assertEquals(true, c1.compareTo(c1) == 0);
    assertEquals(false, e1 instanceof PdxInstanceEnum);
    PdxInstanceEnum pie1 = new PdxInstanceEnum(Coin.TAILS);
    assertEquals(e1, pie1);
    assertEquals(e1.hashCode(), pie1.hashCode());
    assertEquals(0, c1.compareTo(pie1));
    assertEquals(true, c0.compareTo(pie1) < 0);
    assertEquals(true, c2.compareTo(pie1) > 0);
  }

  @Test
  public void testPortableWriteObject() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("portable", false);
    c.writeObject("f1", Byte.valueOf((byte) 1), true);
    c.writeObject("f2", Boolean.TRUE, true);
    c.writeObject("f3", Character.CURRENCY_SYMBOL, true);
    c.writeObject("f4", Short.valueOf((short) 1), true);
    c.writeObject("f5", Integer.valueOf(1), true);
    c.writeObject("f6", Long.valueOf(1), true);
    c.writeObject("f7", new Float(1.2), true);
    c.writeObject("f8", new Double(1.2), true);
    c.writeObject("f9", "string", true);
    c.writeObject("f10", new Date(123), true);
    c.writeObject("f11", new byte[1], true);
    c.writeObject("f12", new boolean[1], true);
    c.writeObject("f13", new char[1], true);
    c.writeObject("f14", new short[1], true);
    c.writeObject("f15", new int[1], true);
    c.writeObject("f16", new long[1], true);
    c.writeObject("f17", new float[1], true);
    c.writeObject("f18", new double[1], true);
    c.writeObject("f19", new String[1], true);
    c.writeObject("f20", new byte[1][1], true);
    c.writeObject("f21", new Object[1], true);
    c.writeObject("f22", new HashMap(), true);
    c.writeObject("f23", new Hashtable(), true);
    c.writeObject("f24", new ArrayList(), true);
    c.writeObject("f25", new Vector(), true);
    c.writeObject("f26", new HashSet(), true);
    c.writeObject("f27", new LinkedHashSet(), true);
    c.writeObject("f28", new File("file"), false);
    try {
      c.writeObject("f29", new File("file"), true);
      throw new RuntimeException("expected NonPortableClassException");
    } catch (NonPortableClassException expected) {
    }
    c.writeObject("f30", Coin.TAILS, true);
    c.writeObject("f31",
        this.cache.createPdxEnum(Coin.class.getName(), Coin.TAILS.name(), Coin.TAILS.ordinal()),
        true);
  }

  @Test
  public void testPortableWriteObjectArray() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("portable", false);
    c.writeObjectArray("f1", new Object[] {Byte.valueOf((byte) 1)}, true);
    c.writeObjectArray("f2", new Object[] {Boolean.TRUE}, true);
    c.writeObjectArray("f3", new Object[] {Character.CURRENCY_SYMBOL}, true);
    c.writeObjectArray("f4", new Object[] {Short.valueOf((short) 1)}, true);
    c.writeObjectArray("f5", new Object[] {Integer.valueOf(1)}, true);
    c.writeObjectArray("f6", new Object[] {Long.valueOf(1)}, true);
    c.writeObjectArray("f7", new Object[] {new Float(1.2)}, true);
    c.writeObjectArray("f8", new Object[] {new Double(1.2)}, true);
    c.writeObjectArray("f9", new Object[] {"string"}, true);
    c.writeObjectArray("f10", new Object[] {new Date(123)}, true);
    c.writeObjectArray("f11", new Object[] {new byte[1]}, true);
    c.writeObjectArray("f12", new Object[] {new boolean[1]}, true);
    c.writeObjectArray("f13", new Object[] {new char[1]}, true);
    c.writeObjectArray("f14", new Object[] {new short[1]}, true);
    c.writeObjectArray("f15", new Object[] {new int[1]}, true);
    c.writeObjectArray("f16", new Object[] {new long[1]}, true);
    c.writeObjectArray("f17", new Object[] {new float[1]}, true);
    c.writeObjectArray("f18", new Object[] {new double[1]}, true);
    c.writeObjectArray("f19", new Object[] {new String[1]}, true);
    c.writeObjectArray("f20", new Object[] {new byte[1][1]}, true);
    c.writeObjectArray("f21", new Object[] {new Object[1]}, true);
    c.writeObjectArray("f22", new Object[] {new HashMap()}, true);
    c.writeObjectArray("f23", new Object[] {new Hashtable()}, true);
    c.writeObjectArray("f24", new Object[] {new ArrayList()}, true);
    c.writeObjectArray("f25", new Object[] {new Vector()}, true);
    c.writeObjectArray("f26", new Object[] {new HashSet()}, true);
    c.writeObjectArray("f27", new Object[] {new LinkedHashSet()}, true);
    c.writeObjectArray("f28", new Object[] {new File("file")}, false);
    try {
      c.writeObjectArray("f29", new Object[] {new File("file")}, true);
      throw new RuntimeException("expected NonPortableClassException");
    } catch (NonPortableClassException expected) {
    }
    c.writeObjectArray("f30", new Object[] {Coin.TAILS}, true);
    c.writeObjectArray("f31", new Object[] {
        this.cache.createPdxEnum(Coin.class.getName(), Coin.TAILS.name(), Coin.TAILS.ordinal())},
        true);
  }

  @Test
  public void testPortableWriteField() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("portable", false);
    c.writeField("f1", Byte.valueOf((byte) 1), Object.class, true);
    c.writeField("f2", Boolean.TRUE, Object.class, true);
    c.writeField("f3", Character.CURRENCY_SYMBOL, Object.class, true);
    c.writeField("f4", Short.valueOf((short) 1), Object.class, true);
    c.writeField("f5", Integer.valueOf(1), Object.class, true);
    c.writeField("f6", Long.valueOf(1), Object.class, true);
    c.writeField("f7", new Float(1.2), Object.class, true);
    c.writeField("f8", new Double(1.2), Object.class, true);
    c.writeField("f9", "string", Object.class, true);
    c.writeField("f10", new Date(123), Object.class, true);
    c.writeField("f11", new byte[1], Object.class, true);
    c.writeField("f12", new boolean[1], Object.class, true);
    c.writeField("f13", new char[1], Object.class, true);
    c.writeField("f14", new short[1], Object.class, true);
    c.writeField("f15", new int[1], Object.class, true);
    c.writeField("f16", new long[1], Object.class, true);
    c.writeField("f17", new float[1], Object.class, true);
    c.writeField("f18", new double[1], Object.class, true);
    c.writeField("f19", new String[1], Object.class, true);
    c.writeField("f20", new byte[1][1], Object.class, true);
    c.writeField("f21", new Object[1], Object.class, true);
    c.writeField("f22", new HashMap(), Object.class, true);
    c.writeField("f23", new Hashtable(), Object.class, true);
    c.writeField("f24", new ArrayList(), Object.class, true);
    c.writeField("f25", new Vector(), Object.class, true);
    c.writeField("f26", new HashSet(), Object.class, true);
    c.writeField("f27", new LinkedHashSet(), Object.class, true);
    c.writeField("f28", new File("file"), Object.class, false);
    try {
      c.writeField("f29", new File("file"), Object.class, true);
      throw new RuntimeException("expected NonPortableClassException");
    } catch (NonPortableClassException expected) {
    }
    c.writeField("f30", Coin.TAILS, Object.class, true);
    c.writeField("f31",
        this.cache.createPdxEnum(Coin.class.getName(), Coin.TAILS.name(), Coin.TAILS.ordinal()),
        Object.class, true);
  }

  public static class MyDS implements DataSerializable {
    Long[] longArray = new Long[] {Long.valueOf(1)};

    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObjectArray(this.longArray, out);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.longArray = (Long[]) DataSerializer.readObjectArray(in);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(longArray);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      MyDS other = (MyDS) obj;
      if (!Arrays.equals(longArray, other.longArray))
        return false;
      return true;
    }
  }

  /**
   * Have a PDX with an Object[] whose element is a DataSerializable which has a Long[]. Its a bug
   * if the array nested in the DS becomes an Object[] during deserialization. Demonstrates bug
   * 43838.
   */
  @Test
  public void testNestedDS() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("nestedDS", false);
    c.writeObject("obj", new MyDS());
    PdxInstance pi = c.create();
    pi.getField("obj");
    checkPdxInstance(pi);
  }

  public static class MyPdx implements PdxSerializable {
    public void toData(PdxWriter writer) {}

    public void fromData(PdxReader reader) {}
  }

  /**
   * Have a pdx with an array of pdx. Make sure the array class does not get loaded during getField,
   * hashCode, and equals.
   */
  @Test
  public void testNestedArray() throws IOException, ClassNotFoundException {
    PdxInstance pi;
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("nestedArray", false);
      MyPdx[] array = new MyPdx[] {new MyPdx()};
      c.writeObjectArray("array", array);
      pi = c.create();
    }
    pi.getField("array");
    checkPdxInstance(pi);
  }

  @Test
  public void testSerializable() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("basics", false);
    c.writeInt("intField", 37);
    PdxInstance pi = c.create();
    checkSerializable(pi);
  }

  private void checkSerializable(Serializable before) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(before);
    oos.close();
    byte[] bytes = baos.toByteArray();
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    Object after = ois.readObject();
    assertEquals(before, after);
  }

  @Test
  public void testMark() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("markField", false);
    try {
      c.markIdentityField("intField1");
      throw new RuntimeException("expected exception");
    } catch (PdxFieldDoesNotExistException expected) {
      // expected
    }
    c.writeInt("intField1", 1);
    c.writeInt("intField2", 2);
    c.writeInt("intField3", 3);
    c.markIdentityField("intField1");
    WritablePdxInstance pi = c.create().createWriter();
    assertEquals(true, pi.hasField("intField1"));
    assertEquals(true, pi.hasField("intField2"));
    assertEquals(true, pi.hasField("intField3"));
    assertEquals(true, pi.isIdentityField("intField1"));
    assertEquals(false, pi.isIdentityField("intField2"));
    assertEquals(false, pi.isIdentityField("intField3"));
    assertEquals(Arrays.asList(new String[] {"intField1", "intField2", "intField3"}),
        pi.getFieldNames());
    assertEquals(1, pi.getField("intField1"));
    assertEquals(2, pi.getField("intField2"));
    assertEquals(3, pi.getField("intField3"));
    assertEquals(null, pi.getField("bogusField"));
    pi.setField("intField1", 11);
    pi.setField("intField2", 22);
    assertEquals(3, pi.getField("intField3"));
    assertEquals(11, pi.getField("intField1"));
    assertEquals(22, pi.getField("intField2"));

    checkPdxInstance(pi);
  }

  private PdxInstance checkPdxInstance(PdxInstance pi) throws IOException, ClassNotFoundException {
    // serialize the pi and make sure it can be deserialized
    PdxInstance pi2 = (PdxInstance) serializeAndDeserialize(pi);
    assertEquals(pi, pi2);
    assertEquals(pi.hashCode(), pi2.hashCode());
    assertEquals(pi.getFieldNames(), pi2.getFieldNames());
    return pi2;
  }

  @Test
  public void testFieldTypes() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("byteField", false);
    c.writeByte("f", (byte) 37);
    WritablePdxInstance pi = c.create().createWriter();
    assertEquals((byte) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((byte) 0, pi.getField("f"));
    pi.setField("f", (byte) 38);
    assertEquals((byte) 38, pi.getField("f"));
    checkPdxInstance(pi);
    try {
      pi.setField("bogusFieldName", null);
      throw new RuntimeException("expected PdxFieldDoesNotExistException");
    } catch (PdxFieldDoesNotExistException ex) {
      // expected
    }

    c = PdxInstanceFactoryImpl.newCreator("booleanField", false);
    c.writeBoolean("f", true);
    pi = c.create().createWriter();
    assertEquals(true, pi.getField("f"));
    try {
      pi.setField("f", 3);
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(false, pi.getField("f"));
    pi.setField("f", false);
    assertEquals(false, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("charField", false);
    c.writeChar("f", (char) 37);
    pi = c.create().createWriter();
    assertEquals((char) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((char) 0, pi.getField("f"));
    pi.setField("f", (char) 38);
    assertEquals((char) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("shortField", false);
    c.writeShort("f", (short) 37);
    pi = c.create().createWriter();
    assertEquals((short) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((short) 0, pi.getField("f"));
    pi.setField("f", (short) 38);
    assertEquals((short) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("intField", false);
    c.writeInt("f", (int) 37);
    pi = c.create().createWriter();
    assertEquals((int) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((int) 0, pi.getField("f"));
    pi.setField("f", (int) 38);
    assertEquals((int) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("longField", false);
    c.writeLong("f", (long) 37);
    pi = c.create().createWriter();
    assertEquals((long) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((long) 0, pi.getField("f"));
    pi.setField("f", (long) 38);
    assertEquals((long) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("floatField", false);
    c.writeFloat("f", (float) 37);
    pi = c.create().createWriter();
    assertEquals((float) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((float) 0, pi.getField("f"));
    pi.setField("f", (float) 38);
    assertEquals((float) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("doubleField", false);
    c.writeDouble("f", (double) 37);
    pi = c.create().createWriter();
    assertEquals((double) 37, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals((double) 0, pi.getField("f"));
    pi.setField("f", (double) 38);
    assertEquals((double) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("dateField", false);
    Date d1 = new Date(37);
    c.writeDate("f", d1);
    pi = c.create().createWriter();
    assertEquals(d1, pi.getField("f"));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    Date d2 = new Date(38);
    pi.setField("f", d2);
    assertEquals(d2, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("stringField", false);
    c.writeString("f", "37");
    pi = c.create().createWriter();
    assertEquals("37", pi.getField("f"));
    try {
      pi.setField("f", false);
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", "38");
    assertEquals("38", pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("objectField", false);
    Date o1 = new Date(23);
    c.writeObject("f", o1);
    pi = c.create().createWriter();
    assertEquals(o1, pi.getField("f"));
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    Date o2 = new Date(24);
    pi.setField("f", o2);
    assertEquals(o2, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("booleanArrayField", false);
    c.writeBooleanArray("f", new boolean[] {true, false, true});
    pi = c.create().createWriter();
    assertEquals(true,
        Arrays.equals(new boolean[] {true, false, true}, (boolean[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new boolean[] {false, true, false});
    assertEquals(true,
        Arrays.equals(new boolean[] {false, true, false}, (boolean[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("charArrayField", false);
    c.writeCharArray("f", new char[] {'1', '2', '3'});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new char[] {'1', '2', '3'}, (char[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new char[] {'a', 'b', 'c'});
    assertEquals(true, Arrays.equals(new char[] {'a', 'b', 'c'}, (char[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("byteArrayField", false);
    c.writeByteArray("f", new byte[] {(byte) 1});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new byte[] {(byte) 1}, (byte[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new byte[] {(byte) 2});
    assertEquals(true, Arrays.equals(new byte[] {(byte) 2}, (byte[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("shortArrayField", false);
    c.writeShortArray("f", new short[] {(short) 1});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new short[] {(short) 1}, (short[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new short[] {(short) 2});
    assertEquals(true, Arrays.equals(new short[] {(short) 2}, (short[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("intArrayField", false);
    c.writeIntArray("f", new int[] {(int) 1});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new int[] {(int) 1}, (int[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new int[] {(int) 2});
    assertEquals(true, Arrays.equals(new int[] {(int) 2}, (int[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("longArrayField", false);
    c.writeLongArray("f", new long[] {(long) 1});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new long[] {(long) 1}, (long[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new long[] {(long) 2});
    assertEquals(true, Arrays.equals(new long[] {(long) 2}, (long[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("floatArrayField", false);
    c.writeFloatArray("f", new float[] {(float) 1});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new float[] {(float) 1}, (float[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new float[] {(float) 2});
    assertEquals(true, Arrays.equals(new float[] {(float) 2}, (float[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("doubleArrayField", false);
    c.writeDoubleArray("f", new double[] {(double) 1});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new double[] {(double) 1}, (double[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new double[] {(double) 2});
    assertEquals(true, Arrays.equals(new double[] {(double) 2}, (double[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("StringArrayField", false);
    c.writeStringArray("f", new String[] {"1", "2", "3"});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new String[] {"1", "2", "3"}, (String[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new String[] {"a", "b", "c"});
    assertEquals(true, Arrays.equals(new String[] {"a", "b", "c"}, (String[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("ObjectArrayField", false);
    c.writeObjectArray("f", new Object[] {"1", "2", "3"});
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new Object[] {"1", "2", "3"}, (Object[]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new Object[] {"a", "b", "c"});
    assertEquals(true, Arrays.equals(new Object[] {"a", "b", "c"}, (Object[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("byteArrayOfBAField", false);
    c.writeArrayOfByteArrays("f", new byte[][] {new byte[] {(byte) 1}});
    pi = c.create().createWriter();
    assertEquals(true,
        Arrays.deepEquals(new byte[][] {new byte[] {(byte) 1}}, (byte[][]) pi.getField("f")));
    try {
      pi.setField("f", "Bogus");
      throw new RuntimeException("expected PdxFieldTypeMismatchException");
    } catch (PdxFieldTypeMismatchException ex) {
      // expected
    }
    pi.setField("f", null);
    assertEquals(null, pi.getField("f"));
    pi.setField("f", new byte[][] {new byte[] {(byte) 2}});
    assertEquals(true,
        Arrays.deepEquals(new byte[][] {new byte[] {(byte) 2}}, (byte[][]) pi.getField("f")));
    checkPdxInstance(pi);
  }

  @Test
  public void testWriteField() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("byteField", false);
    c.writeField("f", (byte) 37, Byte.class);
    WritablePdxInstance pi = c.create().createWriter();
    assertEquals((byte) 37, pi.getField("f"));
    pi.setField("f", (byte) 38);
    assertEquals((byte) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("booleanField", false);
    c.writeField("f", true, Boolean.class);
    pi = c.create().createWriter();
    assertEquals(true, pi.getField("f"));
    pi.setField("f", false);
    assertEquals(false, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("charField", false);
    c.writeField("f", (char) 37, Character.class);
    pi = c.create().createWriter();
    assertEquals((char) 37, pi.getField("f"));
    pi.setField("f", (char) 38);
    assertEquals((char) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("shortField", false);
    c.writeField("f", (short) 37, Short.class);
    pi = c.create().createWriter();
    assertEquals((short) 37, pi.getField("f"));
    pi.setField("f", (short) 38);
    assertEquals((short) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("intField", false);
    c.writeField("f", (int) 37, Integer.class);
    pi = c.create().createWriter();
    assertEquals((int) 37, pi.getField("f"));
    pi.setField("f", (int) 38);
    assertEquals((int) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("longField", false);
    c.writeField("f", (long) 37, Long.class);
    pi = c.create().createWriter();
    assertEquals((long) 37, pi.getField("f"));
    pi.setField("f", (long) 38);
    assertEquals((long) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("floatField", false);
    c.writeField("f", (float) 37, Float.class);
    pi = c.create().createWriter();
    assertEquals((float) 37, pi.getField("f"));
    pi.setField("f", (float) 38);
    assertEquals((float) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("doubleField", false);
    c.writeField("f", (double) 37, Double.class);
    pi = c.create().createWriter();
    assertEquals((double) 37, pi.getField("f"));
    pi.setField("f", (double) 38);
    assertEquals((double) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("dateField", false);
    Date d1 = new Date(37);
    c.writeField("f", d1, Date.class);
    pi = c.create().createWriter();
    assertEquals(d1, pi.getField("f"));
    Date d2 = new Date(38);
    pi.setField("f", d2);
    assertEquals(d2, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("stringField", false);
    c.writeField("f", "37", String.class);
    pi = c.create().createWriter();
    assertEquals("37", pi.getField("f"));
    pi.setField("f", "38");
    assertEquals("38", pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("objectField", false);
    Date o1 = new Date(23);
    c.writeField("f", o1, Object.class);
    pi = c.create().createWriter();
    assertEquals(o1, pi.getField("f"));
    Date o2 = new Date(24);
    pi.setField("f", o2);
    assertEquals(o2, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("booleanArrayField", false);
    c.writeField("f", new boolean[] {true, false, true}, boolean[].class);
    pi = c.create().createWriter();
    assertEquals(true,
        Arrays.equals(new boolean[] {true, false, true}, (boolean[]) pi.getField("f")));
    pi.setField("f", new boolean[] {false, true, false});
    assertEquals(true,
        Arrays.equals(new boolean[] {false, true, false}, (boolean[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("charArrayField", false);
    c.writeField("f", new char[] {'1', '2', '3'}, char[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new char[] {'1', '2', '3'}, (char[]) pi.getField("f")));
    pi.setField("f", new char[] {'a', 'b', 'c'});
    assertEquals(true, Arrays.equals(new char[] {'a', 'b', 'c'}, (char[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("byteArrayField", false);
    c.writeField("f", new byte[] {(byte) 1}, byte[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new byte[] {(byte) 1}, (byte[]) pi.getField("f")));
    pi.setField("f", new byte[] {(byte) 2});
    assertEquals(true, Arrays.equals(new byte[] {(byte) 2}, (byte[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("shortArrayField", false);
    c.writeField("f", new short[] {(short) 1}, short[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new short[] {(short) 1}, (short[]) pi.getField("f")));
    pi.setField("f", new short[] {(short) 2});
    assertEquals(true, Arrays.equals(new short[] {(short) 2}, (short[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("intArrayField", false);
    c.writeField("f", new int[] {(int) 1}, int[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new int[] {(int) 1}, (int[]) pi.getField("f")));
    pi.setField("f", new int[] {(int) 2});
    assertEquals(true, Arrays.equals(new int[] {(int) 2}, (int[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("longArrayField", false);
    c.writeField("f", new long[] {(long) 1}, long[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new long[] {(long) 1}, (long[]) pi.getField("f")));
    pi.setField("f", new long[] {(long) 2});
    assertEquals(true, Arrays.equals(new long[] {(long) 2}, (long[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("floatArrayField", false);
    c.writeField("f", new float[] {(float) 1}, float[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new float[] {(float) 1}, (float[]) pi.getField("f")));
    pi.setField("f", new float[] {(float) 2});
    assertEquals(true, Arrays.equals(new float[] {(float) 2}, (float[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("doubleArrayField", false);
    c.writeField("f", new double[] {(double) 1}, double[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new double[] {(double) 1}, (double[]) pi.getField("f")));
    pi.setField("f", new double[] {(double) 2});
    assertEquals(true, Arrays.equals(new double[] {(double) 2}, (double[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("StringArrayField", false);
    c.writeField("f", new String[] {"1", "2", "3"}, String[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new String[] {"1", "2", "3"}, (String[]) pi.getField("f")));
    pi.setField("f", new String[] {"a", "b", "c"});
    assertEquals(true, Arrays.equals(new String[] {"a", "b", "c"}, (String[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("ObjectArrayField", false);
    c.writeField("f", new Object[] {"1", "2", "3"}, Object[].class);
    pi = c.create().createWriter();
    assertEquals(true, Arrays.equals(new Object[] {"1", "2", "3"}, (Object[]) pi.getField("f")));
    pi.setField("f", new Object[] {"a", "b", "c"});
    assertEquals(true, Arrays.equals(new Object[] {"a", "b", "c"}, (Object[]) pi.getField("f")));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("byteArrayOfBAField", false);
    c.writeField("f", new byte[][] {new byte[] {(byte) 1}}, byte[][].class);
    pi = c.create().createWriter();
    assertEquals(true,
        Arrays.deepEquals(new byte[][] {new byte[] {(byte) 1}}, (byte[][]) pi.getField("f")));
    pi.setField("f", new byte[][] {new byte[] {(byte) 2}});
    assertEquals(true,
        Arrays.deepEquals(new byte[][] {new byte[] {(byte) 2}}, (byte[][]) pi.getField("f")));
    checkPdxInstance(pi);
  }

  @Test
  public void testWritePrimitiveField() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("byteField", false);
    c.writeField("f", (byte) 37, byte.class);
    WritablePdxInstance pi = c.create().createWriter();
    assertEquals((byte) 37, pi.getField("f"));
    pi.setField("f", (byte) 38);
    assertEquals((byte) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("booleanField", false);
    c.writeField("f", true, boolean.class);
    pi = c.create().createWriter();
    assertEquals(true, pi.getField("f"));
    pi.setField("f", false);
    assertEquals(false, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("charField", false);
    c.writeField("f", (char) 37, char.class);
    pi = c.create().createWriter();
    assertEquals((char) 37, pi.getField("f"));
    pi.setField("f", (char) 38);
    assertEquals((char) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("shortField", false);
    c.writeField("f", (short) 37, short.class);
    pi = c.create().createWriter();
    assertEquals((short) 37, pi.getField("f"));
    pi.setField("f", (short) 38);
    assertEquals((short) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("intField", false);
    c.writeField("f", (int) 37, int.class);
    pi = c.create().createWriter();
    assertEquals((int) 37, pi.getField("f"));
    pi.setField("f", (int) 38);
    assertEquals((int) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("longField", false);
    c.writeField("f", (long) 37, long.class);
    pi = c.create().createWriter();
    assertEquals((long) 37, pi.getField("f"));
    pi.setField("f", (long) 38);
    assertEquals((long) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("floatField", false);
    c.writeField("f", (float) 37, float.class);
    pi = c.create().createWriter();
    assertEquals((float) 37, pi.getField("f"));
    pi.setField("f", (float) 38);
    assertEquals((float) 38, pi.getField("f"));
    checkPdxInstance(pi);

    c = PdxInstanceFactoryImpl.newCreator("doubleField", false);
    c.writeField("f", (double) 37, double.class);
    pi = c.create().createWriter();
    assertEquals((double) 37, pi.getField("f"));
    pi.setField("f", (double) 38);
    assertEquals((double) 38, pi.getField("f"));
    checkPdxInstance(pi);
  }

  @Test
  public void testPdxInstancePut() throws IOException {
    Region r = cache.createRegionFactory(RegionShortcut.LOCAL).create("testPdxInstancePut");
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testPdxInstancePut", false);
    c.writeField("f", 37, int.class);
    PdxInstance pi = c.create();
    r.put("key", pi);
    PdxInstance pi2 = (PdxInstance) r.get("key");
    assertEquals(pi, pi2);
  }

  @Test
  public void testNestedPdxInstance() throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testNestedPdxInstanceChild", false);
    c.writeField("f", 37, int.class);
    PdxInstance child = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testNestedPdxInstanceParent", false);
    c.writeObject("f", child);
    WritablePdxInstance parent = c.create().createWriter();
    checkPdxInstance(child);
    checkPdxInstance(parent);
    WritablePdxInstance child2 = ((PdxInstance) parent.getField("f")).createWriter();
    assertEquals(child, child2);
    assertTrue(child != child2);
    child2.setField("f", 38);
    assertFalse(child.equals(child2));
    assertEquals(child, parent.getField("f"));
    parent.setField("f", child2);
    assertFalse(child.equals(parent.getField("f")));
    assertEquals(child2, parent.getField("f"));
    PdxInstance parentCopy = checkPdxInstance(parent);
    assertEquals(child2, parentCopy.getField("f"));
  }

  private void checkDefaultBytes(PdxInstance pi, String fieldName) {
    PdxInstanceImpl impl = (PdxInstanceImpl) pi;
    PdxType t = impl.getPdxType();
    PdxField f = t.getPdxField(fieldName);
    assertEquals(ByteSourceFactory.create(f.getFieldType().getDefaultBytes()),
        impl.getRaw(f.getFieldIndex()));
  }

  @Test
  public void testDefaults() throws IOException, ClassNotFoundException {
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("byteField", false);
      c.writeField("f", (byte) 0, byte.class);
      PdxInstance pi = c.create();
      assertEquals((byte) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("booleanField", false);
      c.writeField("f", false, boolean.class);
      PdxInstance pi = c.create();
      assertEquals(false, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("charField", false);
      c.writeField("f", (char) 0, char.class);
      PdxInstance pi = c.create();
      assertEquals((char) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("shortField", false);
      c.writeField("f", (short) 0, short.class);
      PdxInstance pi = c.create();
      assertEquals((short) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("intField", false);
      c.writeField("f", (int) 0, int.class);
      PdxInstance pi = c.create();
      assertEquals((int) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("longField", false);
      c.writeField("f", (long) 0, long.class);
      PdxInstance pi = c.create();
      assertEquals((long) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("floatField", false);
      c.writeField("f", (float) 0.0, float.class);
      PdxInstance pi = c.create();
      assertEquals((float) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("doubleField", false);
      c.writeField("f", (double) 0.0, double.class);
      PdxInstance pi = c.create();
      assertEquals((double) 0, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("dateField", false);
      c.writeField("f", null, Date.class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("stringField", false);
      c.writeField("f", null, String.class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("objectField", false);
      c.writeField("f", null, Object.class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("byteArrayField", false);
      c.writeField("f", null, byte[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("booleanArrayField", false);
      c.writeField("f", null, boolean[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("charArrayField", false);
      c.writeField("f", null, char[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("shortArrayField", false);
      c.writeField("f", null, short[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("intArrayField", false);
      c.writeField("f", null, int[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("longArrayField", false);
      c.writeField("f", null, long[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("floatArrayField", false);
      c.writeField("f", null, float[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("doubleArrayField", false);
      c.writeField("f", null, double[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("StringArrayField", false);
      c.writeField("f", null, String[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("ObjectArrayField", false);
      c.writeField("f", null, Object[].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
    {
      PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("byteArrayArrayField", false);
      c.writeField("f", null, byte[][].class);
      PdxInstance pi = c.create();
      assertEquals(null, pi.getField("f"));
      checkDefaultBytes(pi, "f");
    }
  }

  private Object serializeAndDeserialize(Object in) throws IOException, ClassNotFoundException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(in, hdos);
    byte[] bytes = hdos.toByteArray();
    // System.out.println("Serialized bytes = " + Arrays.toString(bytes));
    return DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

}
