/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.internal.EnumInfo.PdxInstanceEnumInfo;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.*;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

/**
 *
 */
@Category(IntegrationTest.class)
public class PdxInstanceJUnitTest {
  
  private GemFireCacheImpl c;
  private int allFieldCount;

  @Before
  public void setUp() {
    // make it a loner
    this.c = (GemFireCacheImpl) new CacheFactory()
        .set(MCAST_PORT, "0")
        .set(DISTRIBUTED_SYSTEM_ID, "255")
        .setPdxReadSerialized(true)
        .create();
  }

  @After
  public void tearDown() {
    this.c.close();
  }
  
  @Test
  public void testGetField() throws IOException, ClassNotFoundException {
    PdxInstance instance = getPdx(new TestPdx() {
      public void toData(PdxWriter out) {
        out.writeBoolean("field1", false);
        out.writeInt("field2", 53);
        out.writeObject("field3", new TestPdx() {
          public void toData(PdxWriter writer) {
            writer.writeString("afield", "hello");
          }
          
        });
      }
    });
    
    assertEquals(Arrays.asList(new String[]{"field1", "field2", "field3"}), instance.getFieldNames());
    assertEquals(instance.getField("field2"), Integer.valueOf(53));
    assertEquals(instance.getField("field1"), Boolean.FALSE);
    PdxInstance fieldInstance = (PdxInstance) instance.getField("field3");
    assertEquals(Arrays.asList(new String[]{"afield"}), fieldInstance.getFieldNames());
    assertEquals("hello", fieldInstance.getField("afield"));
  }
  
  @Test
  public void testHashCodeAndEqualsSameType() throws IOException, ClassNotFoundException {
    PdxInstance instance = getAllFields(0);
    assertEquals(instance, getAllFields(0));
    assertEquals(instance.hashCode(), getAllFields(0).hashCode());
    
    for(int i =1; i < allFieldCount + 1; i++) {
      PdxInstance other = getAllFields(i);
      assertFalse("One field " + i + " hashcode have been unequal but were equal" + instance.getField("field" + (i-1)) +", " + other.getField("field" + (i-1)) + ", " + instance + ", " + other, instance.equals(other));
      //Technically, this could be true. If this asserts fails I guess we can just change the test.
      assertFalse("One field " + i + " hashcode have been unequal but were equal" + instance +", " + other, instance.hashCode() == other.hashCode());
    }
  }
  
  @Test
  public void testEquals()  throws IOException, ClassNotFoundException {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testEquals", false);
    c.writeInt("intField", 37);
    PdxInstance pi = c.create();
    assertEquals(false, pi.equals(null));
    assertEquals(false, pi.equals(new Date(37)));
    c = PdxInstanceFactoryImpl.newCreator("testEquals", false);
    c.writeInt("intField", 37);
    c.writeInt("intField2", 38);
    PdxInstance pi2 = c.create();
    pi.hashCode();
    pi2.hashCode();
    assertEquals(false, pi.equals(pi2));
    assertEquals(false, pi2.equals(pi));

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new Date());
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", null);
    pi2 = c.create();
    pi.hashCode();
    pi2.hashCode();
    assertEquals(false, pi.equals(pi2));
    assertEquals(false, pi2.equals(pi));
    
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new int[]{1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new byte[]{(byte)1});
    pi2 = c.create();
    pi.hashCode();
    pi2.hashCode();
    assertEquals(false, pi.equals(pi2));
    assertEquals(false, pi2.equals(pi));
    
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new int[]{1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new int[]{2});
    pi2 = c.create();
    pi.hashCode();
    pi2.hashCode();
    assertEquals(false, pi.equals(pi2));
    assertEquals(false, pi2.equals(pi));
    
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new int[]{1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new int[]{1});
    pi2 = c.create();
    assertEquals(pi.hashCode(), pi2.hashCode());
    assertEquals(true, pi.equals(pi2));
    assertEquals(true, pi2.equals(pi));

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new int[]{1});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new Date());
    pi2 = c.create();
    pi.hashCode();
    pi2.hashCode();
    assertEquals(false, pi.equals(pi2));
    assertEquals(false, pi2.equals(pi));

    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new Date[]{new Date(1)});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new Date[]{new Date(2)});
    pi2 = c.create();
    pi.hashCode();
    pi2.hashCode();
    assertEquals(false, pi.equals(pi2));
    assertEquals(false, pi2.equals(pi));
    
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new Date[]{new Date(1)});
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", new Date[]{new Date(1)});
    pi2 = c.create();
    assertEquals(pi.hashCode(), pi2.hashCode());
    assertEquals(true, pi.equals(pi2));
    assertEquals(true, pi2.equals(pi));
    
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", MyEnum.ONE);
    pi = c.create();
    c = PdxInstanceFactoryImpl.newCreator("testEqualsOF", false);
    c.writeObject("objField", MyEnum.ONE);
    pi2 = c.create();
    assertEquals(pi.hashCode(), pi2.hashCode());
    assertEquals(true, pi.equals(pi2));
    assertEquals(true, pi2.equals(pi));
    
   }
  public enum MyEnum {ONE, TWO};

  public enum MyComplexEnum {ONE{}, TWO{}};
  
  public void testPdxComplexEnum() {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testPdxEnum", false);
    c.writeObject("enumField", MyComplexEnum.ONE);
    PdxInstance pi = c.create();
    Object f = pi.getField("enumField");
    if (f instanceof PdxInstanceEnumInfo) {
      PdxInstanceEnumInfo e = (PdxInstanceEnumInfo) f;
      assertEquals("ONE", e.getName());
      GemFireCacheImpl theCache = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
      theCache.getPdxRegistry().flushCache();
      assertEquals(MyComplexEnum.ONE, e.getObject());
    } else {
      fail("Expected enumField to be a PdxInstanceEnumInfo but it was a " + f.getClass());
    }
  }
  
  public void testPdxSimpleEnum() {
    PdxInstanceFactory c = PdxInstanceFactoryImpl.newCreator("testPdxEnum", false);
    c.writeObject("enumField", MyEnum.ONE);
    PdxInstance pi = c.create();
    Object f = pi.getField("enumField");
    if (f instanceof PdxInstanceEnumInfo) {
      PdxInstanceEnumInfo e = (PdxInstanceEnumInfo) f;
      assertEquals("ONE", e.getName());
      GemFireCacheImpl theCache = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.");
      theCache.getPdxRegistry().flushCache();
      assertEquals(MyEnum.ONE, e.getObject());
    } else {
      fail("Expected enumField to be a PdxInstanceEnumInfo but it was a " + f.getClass());
    }
  }
  
  @Test
  public void testEqualsDifferentTypes() throws IOException, ClassNotFoundException {
    PdxInstance instance1 = getPdx(new TestPdx() {
      public void toData(PdxWriter writer) {
        writer.writeBoolean("field1", true);
      }
    });
    
    PdxInstance instance2 = getPdx(new TestPdx() {
      public void toData(PdxWriter writer) {
        writer.writeBoolean("field1", true);
      }
    });
    
    //These are different classes, so they shouldn't match.
    assertFalse(instance1.equals(instance2));
    assertFalse(instance1.isIdentityField("field1"));
  }
  
  @Test
  public void testHashCodeEqualsOutOfOrderFields() throws IOException, ClassNotFoundException, NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
    PdxSerializable serializable1 = getSeparateClassLoadedPdx(true);
    PdxInstance instance1 = getPdx(serializable1);
    PdxSerializable serializable2 = getSeparateClassLoadedPdx(false);
    PdxInstance instance2 = getPdx(serializable2);
    
    assertEquals(instance1, instance2);
    assertEquals(instance1.hashCode(), instance2.hashCode());
  }
  
  @Test
  public void testIdentityFields() throws IOException, ClassNotFoundException {
    PdxInstance instance1 = getPdx(getPdxSerializable(new TestPdx() {
      public void toData(PdxWriter out) {
        out.writeInt("field2", 53);
        out.writeBoolean("field1", false);
        out.markIdentityField("field2");
      }
    }));
    
    PdxInstance instance2 = getPdx(getPdxSerializable(new TestPdx() {
      public void toData(PdxWriter out) {
        out.writeInt("field2", 53);
        out.writeBoolean("field1", true);
        out.markIdentityField("field2");
      }
    }));
    
    assertEquals(instance1, instance2);
    assertEquals(instance1.hashCode(), instance2.hashCode());
    assertFalse(instance1.isIdentityField("field1"));
    assertTrue(instance1.isIdentityField("field2"));
  }
  
  //This is hack to make sure the classnames are the same
  //for every call to this method, even if the toDatas are different.
  private PdxSerializable getPdxSerializable(final TestPdx toData) {
    return new TestPdx() {
      public void toData(PdxWriter writer) {
        toData.toData(writer);
      }
    };
  }

  private PdxSerializable getSeparateClassLoadedPdx(boolean field1First)
      throws ClassNotFoundException, NoSuchMethodException,
      InstantiationException, IllegalAccessException, InvocationTargetException {
    ClassLoader parent = Thread.currentThread().getContextClassLoader();
    ClassLoader loader1 = new NonDelegatingLoader(parent);
    Class clazz1 = loader1.loadClass(getClass().getPackage().getName() + ".SeparateClassloaderPdx");
    Constructor constructor = clazz1.getConstructor(boolean.class);
    constructor.setAccessible(true);
    PdxSerializable serializable = (PdxSerializable) constructor.newInstance(Boolean.valueOf(field1First));
    return serializable;
  }
  
  public PdxInstance getAllFields(final int change) throws IOException, ClassNotFoundException {
    PdxInstance instance = getPdx(new TestPdx() {
      public void toData(PdxWriter out) {
        int x = 0;
        Number serializable1 = new BigInteger("1234");
        Number serializable2 = new BigInteger("1235");
        Collection collection1 = new ArrayList();
        collection1.add(serializable1);
        collection1.add(serializable2);
        Collection collection2 = new ArrayList();
        collection2.add(serializable2);
        collection2.add(serializable1);
        SimpleClass testPdx1 = new SimpleClass(5, (byte) 5);
        SimpleClass testPdx2 = new SimpleClass(6, (byte) 6);
        HashMap map1 = new HashMap();
        HashMap map2 = new HashMap();
        map2.put(serializable1, serializable2);
        

        out.writeChar("field" + x++, change==x ?  'c': 'd');
        out.writeBoolean("field" + x++, change==x ?  true: false);
        out.writeByte("field" + x++, (byte) (change==x ?   0x5: 0x6));
        out.writeShort("field" + x++, (short) (change==x ?  7 : 8 ));
        out.writeInt("field" + x++, change==x ?  9 : 10 );
        out.writeLong("field" + x++, change==x ?  55555555: 666666666);
        out.writeFloat("field" + x++, change==x ?  23.44f: 23.55f);
        out.writeDouble("field" + x++, change==x ?  28.7777: 29.3333);
        out.writeDate("field" + x++, change==x ?  new Date(23L): new Date(25L));
//        out.writeEnum("field" + x++, change==x ?  Enum<?> e: );
        out.writeString("field" + x++, change==x ?  "hello": "world");
        out.writeObject("field" + x++, change==x ?  serializable1: serializable2 );
        out.writeObject("field" + x++, change==x ?  testPdx1: testPdx2 );
        out.writeObject("field" + x++, change==x ?  map1: map2);
        out.writeObject("field" + x++, change==x ?  collection1:  collection2);
//        out.writeRegion("field" + x++, change==x ?  region1: region2);
        out.writeBooleanArray("field" + x++, change==x ?  new boolean[] {false, true}: new boolean[] {true, false});
        out.writeCharArray("field" + x++, change==x ?  new char[] {'a', 'b'}: new char[] {'a', 'c'} );
        out.writeByteArray("field" + x++, change==x ?  new byte[] {0, 1, 2}: new byte[] {0, 1, 3});
        out.writeShortArray("field" + x++, change==x ?  new short[] {0, 1, 4}: new short[] {0, 1, 5});
        out.writeIntArray("field" + x++, change==x ?  new int[] {0, 1, 4}: new int[] {0, 1, 5});
        out.writeLongArray("field" + x++, change==x ?  new long[] {0, 1, 4}: new long[] {0, 1, 5});
        out.writeFloatArray("field" + x++, change==x ?  new float[] {0, 1, 4}: new float[] {0, 1, 5});
        out.writeDoubleArray("field" + x++, change==x ?  new double[] {0, 1, 4}: new double[] {0, 1, 5});
        out.writeStringArray("field" + x++, change==x ?  new String[] {"a", "b"}: new String[] {"a", "c"});
        out.writeObjectArray("field" + x++, change==x ?  new Object[] {collection1, serializable1}: new Object[] {serializable2});
        out.writeArrayOfByteArrays("field" + x++, change==x ?  new byte[][] { {1, 2}}: new byte[][] { {2, 2}});
        allFieldCount = x;
      }
    });
    
    return instance;
  }
  
  private PdxInstance getPdx(PdxSerializable toData) throws IOException, ClassNotFoundException {
    HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(toData, out);
    return DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
  }

  

  static abstract class TestPdx implements PdxSerializable {

    public void fromData(PdxReader in) {
    }
    
  }

}
