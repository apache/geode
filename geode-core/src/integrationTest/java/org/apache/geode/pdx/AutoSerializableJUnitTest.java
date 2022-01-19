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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.SerializationException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.PdxSerializerObject;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.pdx.internal.AutoSerializableManager;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class AutoSerializableJUnitTest {

  private GemFireCacheImpl c;

  private AutoSerializableManager manager;

  private ReflectionBasedAutoSerializer serializer;

  private final String[] stdSerializableClasses =
      new String[] {"org.apache.geode.pdx.DomainObject.*"};

  public AutoSerializableJUnitTest() {
    super();
  }

  @Before
  public void setUp() throws Exception {
    System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
  }

  @After
  public void tearDown() {
    c.close();
  }

  private void setupSerializer(String... classPatterns) {
    setupSerializer(new ReflectionBasedAutoSerializer(classPatterns), true);
  }

  private void setupSerializer(boolean checkPortability, String... classPatterns) {
    setupSerializer(new ReflectionBasedAutoSerializer(checkPortability, classPatterns), true);
  }

  private void setupSerializer(boolean checkPortability, boolean readSerialized,
      String... classPatterns) {
    setupSerializer(new ReflectionBasedAutoSerializer(checkPortability, classPatterns),
        readSerialized);
  }

  private void setupSerializer(ReflectionBasedAutoSerializer s, boolean readSerialized) {
    serializer = s;
    manager = (AutoSerializableManager) s.getManager();
    c = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0")
        .setPdxReadSerialized(readSerialized).setPdxSerializer(s).create();
  }

  /*
   * Test basic functionality.
   */
  @Test
  public void testSanity() throws Exception {
    setupSerializer("org.apache.geode.pdx.DomainObjectPdxAuto");
    DomainObject objOut = new DomainObjectPdxAuto(4);
    objOut.set("string_0", "test string value");
    objOut.set("long_0", 99L);
    objOut.set("string_immediate", "right now");
    objOut.set("anEnum", DomainObjectPdxAuto.Day.FRIDAY);

    List<String> list = new ArrayList<>(4);
    list.add("string one");
    list.add("string two");
    list.add("string three");
    list.add("string four");
    objOut.set("string_list", list);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(99L, pdxIn.getField("long_0"));
    assertEquals("test string value", pdxIn.getField("string_0"));
    assertEquals("right now", pdxIn.getField("string_immediate"));
    {
      PdxInstance epi = (PdxInstance) pdxIn.getField("anEnum");
      assertEquals(true, epi.isEnum());
      assertEquals(true, epi.hasField("name"));
      assertEquals(true, epi.hasField("ordinal"));
      assertEquals("org.apache.geode.pdx.DomainObjectPdxAuto$Day", epi.getClassName());
      assertEquals("FRIDAY", epi.getField("name"));
      assertEquals(DomainObjectPdxAuto.Day.FRIDAY.ordinal(), epi.getField("ordinal"));
      assertEquals(DomainObjectPdxAuto.Day.FRIDAY, epi.getObject());
    }
    assertEquals(4, ((List) pdxIn.getField("string_list")).size());

    {
      DomainObjectPdxAuto result = (DomainObjectPdxAuto) pdxIn.getObject();
      assertEquals(99L, result.get("long_0"));
      assertEquals("test string value", result.get("string_0"));
      assertEquals("right now", result.get("string_immediate"));
      assertEquals(DomainObjectPdxAuto.Day.FRIDAY, result.get("anEnum"));
      assertEquals(4, ((List) result.get("string_list")).size());
    }

    // disable pdx instances to make sure we can deserialize without calling PdxInstance.getObject
    PdxInstanceImpl.setPdxReadSerialized(false);
    try {
      DomainObjectPdxAuto result = DataSerializer
          .readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
      assertEquals(99L, result.get("long_0"));
      assertEquals("test string value", result.get("string_0"));
      assertEquals("right now", result.get("string_immediate"));
      assertEquals(DomainObjectPdxAuto.Day.FRIDAY, result.get("anEnum"));
      assertEquals(4, ((List) result.get("string_list")).size());
    } finally {
      PdxInstanceImpl.setPdxReadSerialized(true);
    }
  }

  @Test
  public void testConcurrentHashMap() throws Exception {
    setupSerializer("java.util.concurrent..*");
    ConcurrentHashMap<String, String> m = new ConcurrentHashMap<>();
    m.put("k1", "v1");
    m.put("k2", "v2");
    m.put("k3", "v3");
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(m, out);

    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(m, dObj);
  }

  @Test
  public void testMonth() throws Exception {
    setupSerializer(false, false, "org.apache.geode.pdx.AutoSerializableJUnitTest.MyMonth");
    MyMonth m = new MyMonth(1);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(m, out);

    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(m, dObj);
  }

  // This class can only be serialized by the auto serializer
  // if the unsafe code is available.
  // If unsafe is not available it automatically ignores this class
  // since it does not have a public no-arg constructor
  public static class MyMonth implements Serializable, PdxSerializerObject {
    private final int month;

    MyMonth(int month) {
      this.month = month;
    }

    public int hashCode() {
      return month;
    }

    public boolean equals(Object obj) {
      if (obj instanceof MyMonth) {
        return month == ((MyMonth) obj).month;
      } else {
        return false;
      }
    }

  }

  public static class MyExternalizable implements Externalizable, PdxSerializerObject {
    private int v;

    public MyExternalizable() {}

    public MyExternalizable(int v) {
      this.v = v;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(v);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      v = in.readInt();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + v;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      MyExternalizable other = (MyExternalizable) obj;
      return v == other.v;
    }
  }

  @Test
  public void testExternalizable() throws Exception {
    setupSerializer("org.apache.geode.pdx.AutoSerializableJUnitTest.MyExternalizable");
    MyExternalizable o = new MyExternalizable(79);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(o, out);

    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(o, dObj);
  }

  public static class MyWriteReplace implements Serializable, PdxSerializerObject {
    private final String v;

    public MyWriteReplace(String v) {
      this.v = v;
    }

    private Object writeReplace() throws ObjectStreamException {
      return v;
    }
  }

  /**
   * A serializable with a writeReplace method should use standard serialization.
   */
  @Test
  public void testWriteReplace() throws Exception {
    setupSerializer("org.apache.geode.pdx.AutoSerializableJUnitTest.MyWriteReplace");
    MyWriteReplace o = new MyWriteReplace("79");
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(o, out);

    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals("79", dObj);
  }

  public static class MyComparator implements Comparator, PdxSerializerObject {
    public MyComparator() {}

    @Override
    public int compare(Object o1, Object o2) {
      return 0;
    }
  }

  /**
   * A serializable with a writeReplace method should use standard serialization.
   */
  @Test
  public void testComparator() throws Exception {
    setupSerializer("org.apache.geode.pdx.AutoSerializableJUnitTest.MyComparator");
    TreeSet o = new TreeSet(new MyComparator());
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(o, out);

    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(o, dObj);
  }

  public static class PrimitiveObjectHolder implements PdxSerializerObject, Serializable {
    public Boolean bool;
    public Byte b;
    public Character c;
    public Short s;
    public Integer i;
    public Long l;
    public Float f;
    public Double d;

    public PrimitiveObjectHolder() {}

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((b == null) ? 0 : b.hashCode());
      result = prime * result + ((bool == null) ? 0 : bool.hashCode());
      result = prime * result + ((c == null) ? 0 : c.hashCode());
      result = prime * result + ((d == null) ? 0 : d.hashCode());
      result = prime * result + ((f == null) ? 0 : f.hashCode());
      result = prime * result + ((i == null) ? 0 : i.hashCode());
      result = prime * result + ((l == null) ? 0 : l.hashCode());
      result = prime * result + ((s == null) ? 0 : s.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      PrimitiveObjectHolder other = (PrimitiveObjectHolder) obj;
      if (b == null) {
        if (other.b != null) {
          return false;
        }
      } else if (!b.equals(other.b)) {
        return false;
      }
      if (bool == null) {
        if (other.bool != null) {
          return false;
        }
      } else if (!bool.equals(other.bool)) {
        return false;
      }
      if (c == null) {
        if (other.c != null) {
          return false;
        }
      } else if (!c.equals(other.c)) {
        return false;
      }
      if (d == null) {
        if (other.d != null) {
          return false;
        }
      } else if (!d.equals(other.d)) {
        return false;
      }
      if (f == null) {
        if (other.f != null) {
          return false;
        }
      } else if (!f.equals(other.f)) {
        return false;
      }
      if (i == null) {
        if (other.i != null) {
          return false;
        }
      } else if (!i.equals(other.i)) {
        return false;
      }
      if (l == null) {
        if (other.l != null) {
          return false;
        }
      } else if (!l.equals(other.l)) {
        return false;
      }
      if (s == null) {
        return other.s == null;
      } else
        return s.equals(other.s);
    }

  }
  public static class BigHolder implements PdxSerializerObject, Serializable {
    private BigInteger bi;
    private BigDecimal bd;

    public BigHolder() {}

    public BigHolder(BigInteger bi, BigDecimal bd) {
      this.bi = bi;
      this.bd = bd;
    }

    public BigHolder(int i) {
      bi = new BigInteger("" + i);
      bd = new BigDecimal("" + i);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((bd == null) ? 0 : bd.hashCode());
      result = prime * result + ((bi == null) ? 0 : bi.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      BigHolder other = (BigHolder) obj;
      if (bd == null) {
        if (other.bd != null) {
          return false;
        }
      } else if (!bd.equals(other.bd)) {
        return false;
      }
      if (bi == null) {
        return other.bi == null;
      } else
        return bi.equals(other.bi);
    }
  }
  public static class CHMHolder implements PdxSerializerObject {
    private ConcurrentHashMap chm;

    public CHMHolder() {}

    public CHMHolder(ConcurrentHashMap chm) {
      this.chm = chm;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((chm == null) ? 0 : chm.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      CHMHolder other = (CHMHolder) obj;
      if (chm == null) {
        return other.chm == null;
      } else
        return chm.equals(other.chm);
    }
  }

  @Test
  public void testCheckPortablity() throws Exception {
    setupSerializer(true, "org.apache.geode.pdx.AutoSerializableJUnitTest.BigHolder");

    BigInteger bi = new BigInteger("12345678901234567890");
    BigDecimal bd = new BigDecimal("1234567890.1234567890");
    BigHolder bih = new BigHolder(bi, bd);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    try {
      DataSerializer.writeObject(bih, out);
      throw new RuntimeException("expected NonPortableClassException");
    } catch (NonPortableClassException ignored) {
    }
  }

  public static class ExplicitClassNameAutoSerializer extends ReflectionBasedAutoSerializer {
    public ExplicitClassNameAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public boolean isClassAutoSerialized(Class<?> clazz) {
      return DomainObjectPdxAuto.class.equals(clazz);
    }
  }

  @Test
  public void testIsClassAutoSerialized() throws IOException, ClassNotFoundException {
    setupSerializer(new ExplicitClassNameAutoSerializer(false, ""), false);
    DomainObject objOut = new DomainObjectPdxAuto(4);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    DomainObject objIn = DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(objOut, objIn);
  }

  public static class ExplicitIdentityAutoSerializer extends ReflectionBasedAutoSerializer {
    public ExplicitIdentityAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public boolean isIdentityField(Field f, Class<?> clazz) {
      return f.getName().equals("long_0");
    }
  }

  @Test
  public void testIsIdentityField() throws IOException, ClassNotFoundException {
    setupSerializer(
        new ExplicitIdentityAutoSerializer(false, "org.apache.geode.pdx.DomainObjectPdxAuto"),
        true);
    DomainObject objOut = new DomainObjectPdxAuto(4);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);
    DataSerializer.writeObject(objOut, out);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    PdxInstance pi = DataSerializer.readObject(dis);
    PdxInstance pi2 = DataSerializer.readObject(dis);
    assertEquals(true, pi.isIdentityField("long_0"));
    assertEquals(false, pi.isIdentityField("string_0"));
    assertEquals(false, pi.isEnum());
    assertEquals(objOut.getClass().getName(), pi.getClassName());
    assertEquals(objOut, pi.getObject());
    assertEquals(objOut, pi2.getObject());
  }

  public static class ExplicitIncludedAutoSerializer extends ReflectionBasedAutoSerializer {
    public ExplicitIncludedAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public boolean isFieldIncluded(Field f, Class<?> clazz) {
      return f.getName().equals("long_0");
    }
  }

  @Test
  public void testIsFieldIncluded() throws IOException, ClassNotFoundException {
    setupSerializer(
        new ExplicitIncludedAutoSerializer(false, "org.apache.geode.pdx.DomainObjectPdxAuto"),
        true);
    DomainObject objOut = new DomainObjectPdxAuto(4);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pi = DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(true, pi.hasField("long_0"));
    assertEquals(false, pi.hasField("string_0"));
  }

  public static class ExplicitFieldNameAutoSerializer extends ReflectionBasedAutoSerializer {
    public ExplicitFieldNameAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public String getFieldName(Field f, Class<?> clazz) {
      if (f.getName().equals("long_0")) {
        return "_long_0";
      } else {
        return super.getFieldName(f, clazz);
      }
    }
  }

  @Test
  public void testGetFieldName() throws IOException, ClassNotFoundException {
    setupSerializer(
        new ExplicitFieldNameAutoSerializer(false, "org.apache.geode.pdx.DomainObjectPdxAuto"),
        true);
    DomainObject objOut = new DomainObjectPdxAuto(4);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pi = DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    System.out.println("fieldNames=" + pi.getFieldNames());
    assertEquals(false, pi.hasField("long_0"));
    assertEquals(true, pi.hasField("_long_0"));
    assertEquals(true, pi.hasField("string_0"));
    assertEquals(objOut, pi.getObject());
  }

  public static class BigIntegerAutoSerializer extends ReflectionBasedAutoSerializer {
    public BigIntegerAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public FieldType getFieldType(Field f, Class<?> clazz) {
      if (f.getType().equals(BigInteger.class)) {
        return FieldType.BYTE_ARRAY;
      } else if (f.getType().equals(BigDecimal.class)) {
        return FieldType.STRING;
      } else {
        return super.getFieldType(f, clazz);
      }
    }

    @Override
    public boolean transformFieldValue(Field f, Class<?> clazz) {
      if (f.getType().equals(BigInteger.class)) {
        return true;
      } else if (f.getType().equals(BigDecimal.class)) {
        return true;
      } else {
        return super.transformFieldValue(f, clazz);
      }
    }

    @Override
    public Object writeTransform(Field f, Class<?> clazz, Object originalValue) {
      if (f.getType().equals(BigInteger.class)) {
        byte[] result = null;
        if (originalValue != null) {
          BigInteger bi = (BigInteger) originalValue;
          result = bi.toByteArray();
        }
        return result;
      } else if (f.getType().equals(BigDecimal.class)) {
        Object result = null;
        if (originalValue != null) {
          BigDecimal bd = (BigDecimal) originalValue;
          result = bd.toString();
        }
        return result;
      } else {
        return super.writeTransform(f, clazz, originalValue);
      }
    }

    @Override
    public Object readTransform(Field f, Class<?> clazz, Object serializedValue) {
      if (f.getType().equals(BigInteger.class)) {
        BigInteger result = null;
        if (serializedValue != null) {
          result = new BigInteger((byte[]) serializedValue);
        }
        return result;
      } else if (f.getType().equals(BigDecimal.class)) {
        BigDecimal result = null;
        if (serializedValue != null) {
          result = new BigDecimal((String) serializedValue);
        }
        return result;
      } else {
        return super.readTransform(f, clazz, serializedValue);
      }
    }

  }

  public static class PrimitiveObjectsAutoSerializer extends ReflectionBasedAutoSerializer {
    public PrimitiveObjectsAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public FieldType getFieldType(Field f, Class<?> clazz) {
      if (f.getType().equals(Boolean.class)) {
        return FieldType.BOOLEAN;
      } else if (f.getType().equals(Byte.class)) {
        return FieldType.BYTE;
      } else if (f.getType().equals(Character.class)) {
        return FieldType.CHAR;
      } else if (f.getType().equals(Short.class)) {
        return FieldType.SHORT;
      } else if (f.getType().equals(Integer.class)) {
        return FieldType.INT;
      } else if (f.getType().equals(Long.class)) {
        return FieldType.LONG;
      } else if (f.getType().equals(Float.class)) {
        return FieldType.FLOAT;
      } else if (f.getType().equals(Double.class)) {
        return FieldType.DOUBLE;
      } else {
        return super.getFieldType(f, clazz);
      }
    }

    @Override
    public boolean transformFieldValue(Field f, Class<?> clazz) {
      if (f.getType().equals(Boolean.class)) {
        return true;
      } else if (f.getType().equals(Byte.class)) {
        return true;
      } else if (f.getType().equals(Character.class)) {
        return true;
      } else if (f.getType().equals(Short.class)) {
        return true;
      } else if (f.getType().equals(Integer.class)) {
        return true;
      } else if (f.getType().equals(Long.class)) {
        return true;
      } else if (f.getType().equals(Float.class)) {
        return true;
      } else if (f.getType().equals(Double.class)) {
        return true;
      } else {
        return super.transformFieldValue(f, clazz);
      }
    }

    @Override
    public Object writeTransform(Field f, Class<?> clazz, Object originalValue) {
      if (f.getType().equals(Boolean.class)) {
        if (originalValue == null) {
          return false;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Byte.class)) {
        if (originalValue == null) {
          return (byte) 0;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Character.class)) {
        if (originalValue == null) {
          return (char) 0;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Short.class)) {
        if (originalValue == null) {
          return (short) 0;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Integer.class)) {
        if (originalValue == null) {
          return 0;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Long.class)) {
        if (originalValue == null) {
          return 0L;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Float.class)) {
        if (originalValue == null) {
          return (float) 0;
        } else {
          return originalValue;
        }
      } else if (f.getType().equals(Double.class)) {
        if (originalValue == null) {
          return (double) 0;
        } else {
          return originalValue;
        }
      } else {
        return super.writeTransform(f, clazz, originalValue);
      }
    }

    @Override
    public Object readTransform(Field f, Class<?> clazz, Object serializedValue) {
      if (f.getType().equals(Boolean.class)) {
        return serializedValue;
      } else if (f.getType().equals(Byte.class)) {
        return serializedValue;
      } else if (f.getType().equals(Character.class)) {
        return serializedValue;
      } else if (f.getType().equals(Short.class)) {
        return serializedValue;
      } else if (f.getType().equals(Integer.class)) {
        return serializedValue;
      } else if (f.getType().equals(Long.class)) {
        return serializedValue;
      } else if (f.getType().equals(Float.class)) {
        return serializedValue;
      } else if (f.getType().equals(Double.class)) {
        return serializedValue;
      } else {
        return super.readTransform(f, clazz, serializedValue);
      }
    }
  }

  @Test
  public void testPrimitiveObjects() throws Exception {
    setupSerializer(new PrimitiveObjectsAutoSerializer(true,
        "org.apache.geode.pdx.AutoSerializableJUnitTest.PrimitiveObjectHolder"), true);
    PrimitiveObjectHolder nullHolder = new PrimitiveObjectHolder();
    PrimitiveObjectHolder defaultHolder = new PrimitiveObjectHolder();
    defaultHolder.bool = false;
    defaultHolder.b = 0;
    defaultHolder.c = 0;
    defaultHolder.s = 0;
    defaultHolder.i = 0;
    defaultHolder.l = 0L;
    defaultHolder.f = 0.0f;
    defaultHolder.d = 0.0;
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(nullHolder, out);
    PdxInstance pi =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    PdxField pf = ((PdxInstanceImpl) pi).getPdxField("f");
    assertEquals(FieldType.FLOAT, pf.getFieldType());
    Object dObj = pi.getObject();
    assertFalse(nullHolder.equals(dObj));
    assertEquals(defaultHolder, dObj);

    out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(defaultHolder, out);
    pi = DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    dObj = pi.getObject();
    assertEquals(defaultHolder, dObj);
  }

  @Test
  public void testExtensibility() throws Exception {
    setupSerializer(new BigIntegerAutoSerializer(true,
        "org.apache.geode.pdx.AutoSerializableJUnitTest.BigHolder"), false);

    doExtensible("with autoSerializer handling Big*");
  }

  @Test
  public void testNoExtensibility() throws Exception {
    setupSerializer(false, false, "org.apache.geode.pdx.AutoSerializableJUnitTest.BigHolder");

    doExtensible("using standard serialization of Big*");
  }

  private void doExtensible(String msg) throws IOException, ClassNotFoundException {
    BigInteger bi = new BigInteger("12345678901234567890");
    BigDecimal bd = new BigDecimal("1234567890.1234567890");
    BigHolder bih = new BigHolder(bi, bd);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(bih, out);
    System.out.println(msg + " out.size=" + out.size());
    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(bih, dObj);

  }

  public static class ConcurrentHashMapAutoSerializer extends BigIntegerAutoSerializer {
    public ConcurrentHashMapAutoSerializer(boolean checkPortability, String... patterns) {
      super(checkPortability, patterns);
    }

    @Override
    public FieldType getFieldType(Field f, Class<?> clazz) {
      if (f.getType().equals(ConcurrentHashMap.class)) {
        return FieldType.OBJECT_ARRAY;
      } else {
        return super.getFieldType(f, clazz);
      }
    }

    @Override
    public boolean transformFieldValue(Field f, Class<?> clazz) {
      if (f.getType().equals(ConcurrentHashMap.class)) {
        return true;
      } else {
        return super.transformFieldValue(f, clazz);
      }
    }

    @Override
    public Object writeTransform(Field f, Class<?> clazz, Object originalValue) {
      if (f.getType().equals(ConcurrentHashMap.class)) {
        Object[] result = null;
        if (originalValue != null) {
          ConcurrentHashMap<?, ?> m = (ConcurrentHashMap<?, ?>) originalValue;
          result = new Object[m.size() * 2];
          int i = 0;
          for (Map.Entry<?, ?> e : m.entrySet()) {
            result[i++] = e.getKey();
            result[i++] = e.getValue();
          }
        }
        return result;
      } else {
        return super.writeTransform(f, clazz, originalValue);
      }
    }

    @Override
    public Object readTransform(Field f, Class<?> clazz, Object serializedValue) {
      if (f.getType().equals(ConcurrentHashMap.class)) {
        ConcurrentHashMap result = null;
        if (serializedValue != null) {
          Object[] data = (Object[]) serializedValue;
          result = new ConcurrentHashMap(data.length / 2);
          int i = 0;
          while (i < data.length) {
            Object key = data[i++];
            Object value = data[i++];
            result.put(key, value);
          }
        }
        return result;
      } else {
        return super.readTransform(f, clazz, serializedValue);
      }
    }
  }

  @Test
  public void testCHM() throws Exception {
    setupSerializer(new ConcurrentHashMapAutoSerializer(false,
        "org.apache.geode.pdx.AutoSerializableJUnitTest.*Holder"), false);
    doCHM("with autoSerializer handling ConcurrentHashMap");
  }

  @Test
  public void testNoCHM() throws Exception {
    setupSerializer(false, false, "org.apache.geode.pdx.AutoSerializableJUnitTest.*Holder");
    doCHM("without autoSerializer handling ConcurrentHashMap");
  }

  private void doCHM(String msg) throws IOException, ClassNotFoundException {
    ConcurrentHashMap<String, BigHolder> chm = new ConcurrentHashMap<>();
    for (int i = 1; i < 32; i++) {
      chm.put("key" + i, new BigHolder(i));
    }
    CHMHolder h = new CHMHolder(chm);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(h, out);
    System.out.println(msg + " out.size=" + out.size());
    Object dObj =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    assertEquals(h, dObj);
  }

  /*
   * Test the versioning of basic primitives where no fields are written at first, but then all
   * fields need to be read back.
   */
  @Test
  public void testReadNullPrimitives() throws Exception {
    setupSerializer(stdSerializableClasses);
    // Don't want to write any fields
    manager.addExcludePattern(".*DomainObjectPdxAuto",
        "a(Char|Boolean|Byte|Short|Int|Long|Float|Double)");
    DomainObject objOut = new DomainObjectPdxAuto(4);
    objOut.set("aString", "aString has a value");

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    // Now we want to read all fields.
    manager.resetCaches();

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    // Force the object to be de-serialized without any exceptions being thrown
    DomainObjectPdxAuto result = (DomainObjectPdxAuto) pdxIn.getObject();

    assertEquals('\u0000', result.aChar);
    assertFalse(result.aBoolean);
    assertEquals(0, result.aByte);
    assertEquals(0, result.aShort);
    assertEquals(0, result.anInt);
    assertEquals(0L, result.aLong);
    assertEquals(0.0f, result.aFloat, 0.0f);
    assertEquals(0.0d, result.aDouble, 0.0f);
    assertEquals("aString has a value", result.get("aString"));
  }

  /*
   * Test that when primitive wrapper classes are null, and get serialized, they remain as null when
   * being deserialized.
   */
  @Test
  public void testReadNullObjects() throws Exception {
    setupSerializer(stdSerializableClasses);
    // Don't want to write any fields
    DomainObjectPdxAuto objOut = new DomainObjectPdxAuto(4);
    objOut.anInteger = null;

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    // Now we want to read all fields.
    manager.resetCaches();

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
    DomainObjectPdxAuto result = (DomainObjectPdxAuto) pdxIn.getObject();

    assertNull(result.anInteger);
    assertNull(result.anEnum);
  }

  /*
   * Test what happens with a class without a zero-arg constructor
   */
  @Test
  public void testNoZeroArgConstructor() throws Exception {
    setupSerializer(stdSerializableClasses);
    DomainObject objOut = new DomainObjectPdxAutoNoDefaultConstructor(4);
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    try {
      DataSerializer.writeObject(objOut, out);
    } catch (NotSerializableException ex) {
      // This passes the test
    } catch (Exception ex) {
      throw new RuntimeException("Expected NotSerializableException here but got " + ex);
    }
  }

  /*
   * Check that an exception is appropriately thrown for a 'bad' object. In this case the bad class
   * masks a field from a superclass and defines it as a different type.
   */
  @Test
  public void testException() throws Exception {
    setupSerializer(stdSerializableClasses);
    DomainObject objOut = new DomainObjectBad();
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);

    try {
      DataSerializer.writeObject(objOut, out);
    } catch (SerializationException ex) {
      // Pass
    } catch (Exception ignored) {
    }
  }

  /*
   * Test basic declarative configuration
   */
  @Test
  public void testBasicConfig() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes", "org.apache.geode.pdx.DomainObject");
    serializer.initialize(null, props);

    assertEquals(4, manager.getFields(DomainObject.class).size());
  }

  /*
   * Test declarative configuration with excludes
   */
  @Test
  public void testConfigWithExclude1() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes", "org.apache.geode.pdx.DomainObject#exclude=long.*");
    serializer.initialize(c, props);

    assertEquals(3, manager.getFields(DomainObject.class).size());
  }

  @Test
  public void testConfigWithExclude2() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes", "org.apache.geode.pdx.DomainObject#exclude=string.* ,");
    serializer.initialize(c, props);

    assertEquals(1, manager.getFields(DomainObject.class).size());
  }

  /*
   * Test use of the identity param
   */
  @Test
  public void testConfigWithIdentity1() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes", "org.apache.geode.pdx.DomainObjectPdxAuto#identity=long.*");
    serializer.initialize(c, props);

    DomainObject objOut = new DomainObjectPdxAuto(4);
    objOut.set("string_0", "test string value");
    objOut.set("long_0", 99L);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    assertEquals(99L, pdxIn.getField("long_0"));
    assertTrue(pdxIn.isIdentityField("long_0"));
  }

  /*
   * Test both identity and exclude parameters
   */
  @Test
  public void testConfigWithIdentityAndExclude1() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes",
        "org.apache.geode.pdx.DomainObjectPdxAuto#identity=long.*#exclude=string.*");
    serializer.initialize(c, props);

    assertEquals(27, manager.getFields(DomainObjectPdxAuto.class).size());

    DomainObject objOut = new DomainObjectPdxAuto(4);
    objOut.set("string_0", "test string value");
    objOut.set("long_0", 99L);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    // This means we're not doing anything with this string
    assertNull(pdxIn.getField("string_0"));
    assertTrue(pdxIn.isIdentityField("long_0"));
  }

  /*
   * Variation of the config param
   */
  @Test
  public void testConfigWithIdentityAndExclude2() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes",
        "org.apache.geode.pdx.DomainObjectPdxAuto#identity=long.*#exclude=string.*#, com.another.class.Foo");
    serializer.initialize(c, props);

    assertEquals(27, manager.getFields(DomainObjectPdxAuto.class).size());

    DomainObject objOut = new DomainObjectPdxAuto(4);
    objOut.set("string_0", "test string value");
    objOut.set("long_0", 99L);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    // This means we're not doing anything with this string
    assertNull(pdxIn.getField("string_0"));
    assertTrue(pdxIn.isIdentityField("long_0"));
  }

  /*
   * Variation of the config param
   */
  @Test
  public void testConfigWithIdentityAndExclude3() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes",
        "org.apache.geode.pdx.DomainObjectPdxAuto#identity=long.*#exclude=string.*, com.another.class.Foo");
    serializer.initialize(c, props);

    assertEquals(27, manager.getFields(DomainObjectPdxAuto.class).size());

    DomainObject objOut = new DomainObjectPdxAuto(4);
    objOut.set("string_0", "test string value");
    objOut.set("long_0", 99L);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(objOut, out);

    PdxInstance pdxIn =
        DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

    // This means we're not doing anything with this string
    assertNull(pdxIn.getField("string_0"));
    assertTrue(pdxIn.isIdentityField("long_0"));
  }

  /*
   * Check repeat class definitions
   */
  @Test
  public void testConfigWithIdentityAndExclude4() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes", "org.apache.geode.pdx.DomainObjectPdxAuto#exclude=string.*, "
        + "org.apache.geode.pdx.DomainObjectPdxAuto#exclude=long.*");
    serializer.initialize(c, props);

    assertEquals(26, manager.getFields(DomainObjectPdxAuto.class).size());
  }

  /*
   * Test correct config comes back via getConfig.
   */
  @Test
  public void testGetConfig() throws Exception {
    setupSerializer();
    Properties props = new Properties();
    props.put("classes",
        "Pdx#exclude=string.*#exclude=badField, Pdx#identity=id.*, PdxAuto#exclude=long.*#identity=id.*");
    serializer.initialize(c, props);

    Properties result = serializer.getConfig();
    assertEquals(
        "Pdx, PdxAuto, Pdx#identity=id.*, PdxAuto#identity=id.*, Pdx#exclude=string.*, Pdx#exclude=badField, PdxAuto#exclude=long.*",
        result.getProperty("classes"));

    manager.resetCaches();
    serializer.initialize(c, result);

    result = serializer.getConfig();
    assertEquals(
        "Pdx, PdxAuto, Pdx#identity=id.*, PdxAuto#identity=id.*, Pdx#exclude=string.*, Pdx#exclude=badField, PdxAuto#exclude=long.*",
        result.getProperty("classes"));
  }

  /*
   * Tests the exclusion algorithm to verify that it can be disabled.
   */
  @Test
  public void testNoHardCodedExcludes() {
    System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "true");
    setupSerializer();
    assertFalse(manager.isExcluded("com.gemstone.gemfire.GemFireException"));
    assertFalse(manager.isExcluded("com.gemstoneplussuffix.gemfire.GemFireException"));
    assertFalse(manager.isExcluded("org.apache.geode.GemFireException"));
    assertFalse(manager.isExcluded("org.apache.geodeplussuffix.gemfire.GemFireException"));
    assertFalse(manager.isExcluded("javax.management.MBeanException"));
    assertFalse(manager.isExcluded("javaxplussuffix.management.MBeanException"));
    assertFalse(manager.isExcluded("java.lang.Exception"));
    assertFalse(manager.isExcluded("javaplussuffix.lang.Exception"));
    assertFalse(manager.isExcluded("com.example.Moof"));
  }

  /*
   * Tests the exclusion algorithm to verify that it does not cast too wide of a net.
   */
  @Test
  public void testHardCodedExcludes() {
    System.setProperty(AutoSerializableManager.NO_HARDCODED_EXCLUDES_PARAM, "false");
    setupSerializer();
    assertTrue(manager.isExcluded("com.gemstone.gemfire.GemFireException"));
    assertFalse(manager.isExcluded("com.gemstoneplussuffix.gemfire.GemFireException"));
    assertTrue(manager.isExcluded("org.apache.geode.GemFireException"));
    assertFalse(manager.isExcluded("org.apache.geodeplussuffix.gemfire.GemFireException"));
    assertTrue(manager.isExcluded("javax.management.MBeanException"));
    assertFalse(manager.isExcluded("javaxplussuffix.management.MBeanException"));
    assertTrue(manager.isExcluded("java.lang.Exception"));
    assertFalse(manager.isExcluded("javaplussuffix.lang.Exception"));
    assertFalse(manager.isExcluded("com.example.Moof"));
  }

  /*
   * This test intends to simulate what happens when differing class loaders hold references to the
   * same named class - something that would happen in the context of an app server. We need to
   * ensure that the AutoSerializableManager tracks these as separate classes even though they might
   * have the same name.
   */
  @Test
  public void testMultipleClassLoaders() throws Exception {
    setupSerializer(stdSerializableClasses);
    ChildFirstClassLoader cfcl =
        new ChildFirstClassLoader(javaClassPathToUrl(), getClass().getClassLoader());
    cfcl.addIncludedClass("org\\.apache.*");
    // Need to exclude DomainObject as that is what the newly created objects
    // get cast to.
    cfcl.addExcludedClass(".*DomainObject");
    Class clazz = cfcl.loadClass("org.apache.geode.pdx.DomainObjectClassLoadable");

    // Create our object with a special class loader
    DomainObject obj1 = (DomainObject) clazz.newInstance();
    obj1.set("string_0", "test string value");
    obj1.set("long_0", 99L);

    HeapDataOutputStream out1 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(obj1, out1);

    PdxInstance pdxIn = DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out1.toByteArray())));

    assertEquals(99L, pdxIn.getField("long_0"));
    assertEquals("test string value", pdxIn.getField("string_0"));

    // Create our object with the 'standard' class loader
    DomainObject obj2 = new DomainObjectClassLoadable();
    obj2.set("string_0", "different string value");
    obj2.set("long_0", 1009L);

    // They are definitely not the same class
    assertFalse(obj1.getClass() == obj2.getClass());

    HeapDataOutputStream out2 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(obj2, out2);

    pdxIn = DataSerializer
        .readObject(new DataInputStream(new ByteArrayInputStream(out2.toByteArray())));

    assertEquals(1009L, pdxIn.getField("long_0"));
    assertEquals("different string value", pdxIn.getField("string_0"));

    // Make sure the manager is holding separate classes
    assertEquals(2, manager.getClassMap().size());
  }

  private URL[] javaClassPathToUrl() throws MalformedURLException {
    List<URL> urls = new ArrayList<>();
    String classPathStr = System.getProperty("java.class.path");
    if (classPathStr != null) {
      String[] cpList = classPathStr.split(System.getProperty("path.separator"));
      for (String u : cpList) {
        urls.add(new File(u).toURI().toURL());
      }
    }

    return urls.toArray(new URL[] {});
  }

  /*
   * Custom class loader which will attempt to load classes before delegating to the parent.
   */
  private class ChildFirstClassLoader extends URLClassLoader {

    private final List<String> includedClasses = new ArrayList<>();

    private final List<String> excludedClasses = new ArrayList<>();

    public ChildFirstClassLoader() {
      super(new URL[] {});
    }

    public ChildFirstClassLoader(URL[] urls) {
      super(urls);
    }

    public ChildFirstClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, parent);
    }

    public void addIncludedClass(String clazz) {
      includedClasses.add(clazz);
    }

    public void addExcludedClass(String clazz) {
      excludedClasses.add(clazz);
    }

    public boolean isRelevant(String clazz) {
      boolean result = false;
      for (String s : includedClasses) {
        if (clazz.matches(s)) {
          result = true;
          break;
        }
      }

      for (String s : excludedClasses) {
        if (clazz.matches(s)) {
          result = false;
          break;
        }
      }

      return result;
    }

    @Override
    public void addURL(URL url) {
      super.addURL(url);
    }

    @Override
    public Class loadClass(String name) throws ClassNotFoundException {
      return loadClass(name, false);
    }

    /**
     * We override the parent-first behavior established by java.lang.Classloader.
     */
    @Override
    public Class loadClass(String name, boolean resolve) throws ClassNotFoundException {
      Class c = null;

      if (isRelevant(name)) {
        // First, check if the class has already been loaded
        c = findLoadedClass(name);

        // if not loaded, search the local (child) resources
        if (c == null) {
          try {
            c = findClass(name);
          } catch (ClassNotFoundException cnfe) {
            // ignore
          }
        }
      }

      // if we could not find it, delegate to parent
      // Note that we don't attempt to catch any ClassNotFoundException
      if (c == null) {
        if (getParent() != null) {
          c = getParent().loadClass(name);
        } else {
          c = getSystemClassLoader().loadClass(name);
        }
      }

      if (resolve) {
        resolveClass(c);
      }

      return c;
    }
  }
}
