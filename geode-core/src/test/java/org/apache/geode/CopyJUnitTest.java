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
package org.apache.geode;

import org.apache.geode.cache.*;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Tests the functionality of the {@link CopyHelper#copy} method and the builtin copy-on-get Cache
 * functions.
 *
 * @since GemFire 4.0
 *
 */
@Category(IntegrationTest.class)
public class CopyJUnitTest {

  private Cache cache;
  private Region region;

  protected Object oldValue;
  protected Object newValue;

  private void createCache(boolean copyOnRead) throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    this.cache = CacheFactory.create(DistributedSystem.connect(p));
    this.cache.setCopyOnRead(copyOnRead);

    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    af.setCacheListener(new CacheListenerAdapter() {
      public void afterCreate(EntryEvent event) {
        oldValue = event.getOldValue();
        newValue = event.getNewValue();
      }

      public void afterUpdate(EntryEvent event) {
        oldValue = event.getOldValue();
        newValue = event.getNewValue();
      }

      public void afterInvalidate(EntryEvent event) {
        oldValue = event.getOldValue();
        newValue = event.getNewValue();
      }

      public void afterDestroy(EntryEvent event) {
        oldValue = event.getOldValue();
        newValue = event.getNewValue();
      }

      public void afterRegionInvalidate(RegionEvent event) {
        // ignore
      }

      public void afterRegionDestroy(RegionEvent event) {
        // ignore
      }

      public void close() {
        oldValue = null;
        newValue = null;
      }
    });
    this.region = this.cache.createRegion("CopyJUnitTest", af.create());
  }

  private void closeCache() {
    if (this.cache != null) {
      this.region = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }

  @Test
  public void testSimpleCopies() {
    assertTrue(null == CopyHelper.copy(null));
    try {
      CopyHelper.copy(new Object());
      fail("Expected CopyException");
    } catch (CopyException ok) {
    }
    CopyHelper.copy(new CloneImpl());
  }

  protected static class CloneImpl implements Cloneable {
    public Object clone() {
      return this;
    }
  }

  @Test
  public void testReferences() throws Exception {
    createCache(false);
    try {
      final Object ov = new Integer(6);
      final Object v = new Integer(7);
      this.region.put("key", ov);
      this.region.put("key", v);
      assertTrue("expected listener getOldValue to return reference to ov", this.oldValue == ov);
      assertTrue("expected listener getNewValue to return reference to v", this.newValue == v);
      assertTrue("expected get to return reference to v", this.region.get("key") == v);
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return reference to v", re.getValue() == v);
      Collection c = this.region.values();
      Object[] cArray = c.toArray();
      assertTrue("expected values().toArray() to return reference to v", cArray[0] == v);
      assertTrue("expected values().iterator().next() to return reference to v",
          c.iterator().next() == v);
    } finally {
      closeCache();
    }
  }

  public static class ModifiableInteger implements Serializable {
    private static final long serialVersionUID = 9085003409748155613L;
    private final int v;

    public ModifiableInteger(int v) {
      this.v = v;
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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ModifiableInteger other = (ModifiableInteger) obj;
      if (v != other.v)
        return false;
      return true;
    }
  }

  @Test
  public void testCopies() throws Exception {
    createCache(true);
    try {
      final Object ov = new ModifiableInteger(1);
      final Object v = new ModifiableInteger(2);
      this.region.put("key", ov);
      this.region.put("key", v);
      assertTrue("expected listener getOldValue to return copy of ov", this.oldValue != ov);
      assertEquals(ov, this.oldValue);
      assertTrue("expected listener getNewValue to return copy of v", this.newValue != v);
      assertEquals(v, this.newValue);
      assertTrue("expected get to return copy of v", this.region.get("key") != v);
      assertEquals(v, this.region.get("key"));
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return copy of v", re.getValue() != v);
      assertEquals(v, re.getValue());
      Collection c = this.region.values();
      Object[] cArray = c.toArray();
      assertTrue("expected values().toArray() to return copy of v", cArray[0] != v);
      assertEquals(v, cArray[0]);

      assertTrue("expected values().iterator().next() to return copy of v",
          c.iterator().next() != v);
      assertEquals(v, c.iterator().next());
    } finally {
      closeCache();
    }
  }

  @Test
  public void testImmutable() throws Exception {
    createCache(true);
    try {
      // Integer is immutable so copies should not be made
      final Object ov = new Integer(6);
      final Object v = new Integer(7);
      this.region.put("key", ov);
      this.region.put("key", v);
      assertSame(ov, this.oldValue);
      assertSame(v, this.newValue);
      assertSame(v, this.region.get("key"));
      Region.Entry re = this.region.getEntry("key");
      assertSame(v, re.getValue());
      Collection c = this.region.values();
      Object[] cArray = c.toArray();
      assertSame(v, cArray[0]);

      assertSame(v, c.iterator().next());
    } finally {
      closeCache();
    }
  }

  @Test
  public void testPrimitiveArrays() {
    {
      byte[] ba1 = new byte[] {1, 2, 3};
      byte[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      boolean[] ba1 = new boolean[] {true, false, true};
      boolean[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      char[] ba1 = new char[] {1, 2, 3};
      char[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      short[] ba1 = new short[] {1, 2, 3};
      short[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      int[] ba1 = new int[] {1, 2, 3};
      int[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      long[] ba1 = new long[] {1, 2, 3};
      long[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      float[] ba1 = new float[] {1, 2, 3};
      float[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      double[] ba1 = new double[] {1, 2, 3};
      double[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
  }

  @Test
  public void testObjectArray() {
    Object[] oa1 = new Object[] {1, 2, 3};
    Object[] oa2 = CopyHelper.copy(oa1);
    if (oa1 == oa2) {
      fail("expected new instance of object array");
    }
    if (!Arrays.equals(oa1, oa2)) {
      fail("expected contents of arrays to be equal");
    }
  }

  @Test
  public void testIsWellKnownImmutableInstance() {
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance("abc"));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Integer.valueOf(0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Long.valueOf(0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Byte.valueOf((byte) 0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Short.valueOf((short) 0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Float.valueOf((float) 1.2)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Double.valueOf(1.2)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Character.valueOf((char) 0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(new BigInteger("1234")));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(new BigDecimal("123.4556")));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(new UUID(1L, 2L)));
    PdxInstance pi = new PdxInstance() {
      public Object getObject() {
        return null;
      }

      public Object getObject(Object pdxObject) {
        return null;
      }

      public boolean hasField(String fieldName) {
        return false;
      }

      public List<String> getFieldNames() {
        return null;
      }

      public boolean isIdentityField(String fieldName) {
        return false;
      }

      public Object getField(String fieldName) {
        return null;
      }

      public WritablePdxInstance createWriter() {
        return null;
      }

      public String getClassName() {
        return null;
      }

      public boolean isEnum() {
        return false;
      }
    };
    WritablePdxInstance wpi = new WritablePdxInstance() {
      public Object getObject() {
        return null;
      }

      public Object getObject(Object pdxObject) {
        return null;
      }

      public boolean hasField(String fieldName) {
        return false;
      }

      public List<String> getFieldNames() {
        return null;
      }

      public boolean isIdentityField(String fieldName) {
        return false;
      }

      public Object getField(String fieldName) {
        return null;
      }

      public WritablePdxInstance createWriter() {
        return null;
      }

      public void setField(String fieldName, Object value) {}

      public String getClassName() {
        return null;
      }

      public boolean isEnum() {
        return false;
      }
    };
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(pi));
    assertEquals(false, CopyHelper.isWellKnownImmutableInstance(wpi));
    assertEquals(false, CopyHelper.isWellKnownImmutableInstance(new Object()));
  }

  @Test
  public void testTxReferences() throws Exception {
    createCache(false);
    final CacheTransactionManager txMgr = this.cache.getCacheTransactionManager();
    txMgr.begin();
    try {
      final Object v = new Integer(7);
      this.region.put("key", v);
      assertTrue("expected get to return reference to v", this.region.get("key") == v);
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return reference to v", re.getValue() == v);
      txMgr.rollback();
    } finally {
      try {
        txMgr.rollback();
      } catch (IllegalStateException ignore) {
      }
      closeCache();
    }
  }

  @Test
  public void testTxCopies() throws Exception {
    createCache(true);
    final CacheTransactionManager txMgr = this.cache.getCacheTransactionManager();
    txMgr.begin();
    try {
      final Object v = new ModifiableInteger(7);
      this.region.put("key", v);
      assertTrue("expected get to return copy of v", this.region.get("key") != v);
      assertEquals(v, this.region.get("key"));
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return copy of v", re.getValue() != v);
      assertEquals(v, re.getValue());
      txMgr.rollback();
    } finally {
      try {
        txMgr.rollback();
      } catch (IllegalStateException ignore) {
      }
      closeCache();
    }
  }

  @Test
  public void testComplexObjectDeepCopy() {
    Complex1 in1 = new Complex1();
    in1.hashMap.put(Season.SUMMER, 2);
    Complex2 out1 = new Complex2();
    out1.innerList.add(in1);

    Complex2 out2 = CopyHelper.deepCopy(out1);

    assertEquals(out1, out2);

    // make sure that the member variables do not refer to the same instance
    assertNotSame(out1, out2);
    assertNotSame(out1.str, out2.str);
    assertNotSame(out1.innerList, out2.innerList);
    Complex1 i1 = out1.innerList.get(0);
    Complex1 i2 = out2.innerList.get(0);
    assertNotSame(i1, i2);

    Complex1 in2 = new Complex1();
    out2.innerList.add(in2);

    assertEquals(out1.innerList.size(), 1);
    assertEquals(out2.innerList.size(), 2);
  }

  @Test
  public void testMapDeepCopy() {
    Map<Season, Complex1> map1 = new HashMap<Season, Complex1>();
    Complex1 in1 = new Complex1();
    in1.hashMap.put(Season.SUMMER, 2);
    map1.put(Season.SUMMER, in1);

    Map<Season, Complex1> map2 = CopyHelper.deepCopy(map1);

    assertEquals(map1, map2);

    Complex1 data1 = map1.get(Season.SUMMER);
    Complex1 data2 = map2.get(Season.SUMMER);

    assertEquals(data1, data2);
    assertNotSame(data1, data2);
  }

  @Test
  public void testNonSerializableDeepCopy() {
    NonSerializable n = new NonSerializable();
    try {
      NonSerializable m = CopyHelper.deepCopy(n);
      fail("expected a CopyException for a non serializable");
    } catch (final CopyException ok) {
    }
  }

  static enum Season {
    SPRING, SUMMER, FALL, WINTER
  }

  static class NonSerializable {
    int i = 1;
  }

  static class Complex1 implements Serializable {
    private static final long serialVersionUID = 1L;
    Season season = Season.SPRING;
    Map<Season, Integer> hashMap = new HashMap<Season, Integer>();

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((hashMap == null) ? 0 : hashMap.hashCode());
      result = prime * result + ((season == null) ? 0 : season.hashCode());
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
      Complex1 other = (Complex1) obj;
      if (hashMap == null) {
        if (other.hashMap != null)
          return false;
      } else if (!hashMap.equals(other.hashMap))
        return false;
      if (season != other.season)
        return false;
      return true;
    }
  }

  static class Complex2 implements Serializable {
    private static final long serialVersionUID = 1L;
    int id = 1;
    String str = "Hello there!";
    List<Complex1> innerList = new ArrayList<Complex1>();

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + id;
      result = prime * result + ((innerList == null) ? 0 : innerList.hashCode());
      result = prime * result + ((str == null) ? 0 : str.hashCode());
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
      Complex2 other = (Complex2) obj;
      if (id != other.id)
        return false;
      if (innerList == null) {
        if (other.innerList != null)
          return false;
      } else if (!innerList.equals(other.innerList))
        return false;
      if (str == null) {
        if (other.str != null)
          return false;
      } else if (!str.equals(other.str))
        return false;
      return true;
    }
  }

}
