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
package org.apache.geode.pdx.internal;

import static org.apache.commons.lang.StringUtils.substringAfter;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({IntegrationTest.class, SerializationTest.class})
public class PdxInstanceImplTest {
  private GemFireCacheImpl cache;

  @Before
  public void setUp() {
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0")
        .set(DISTRIBUTED_SYSTEM_ID, "255").setPdxReadSerialized(true).create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testToStringForEmpty() {
    PdxInstance instance =
        PdxInstanceFactoryImpl.newCreator("testToStringForEmpty", false, cache).create();
    assertEquals("testToStringForEmpty]{}", substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForInteger() {
    PdxInstance instance = PdxInstanceFactoryImpl.newCreator("testToStringForInteger", false, cache)
        .writeInt("intField", 37).create();
    assertEquals("testToStringForInteger]{intField=37}", substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForString() {
    PdxInstance instance = PdxInstanceFactoryImpl.newCreator("testToStringForString", false, cache)
        .writeString("stringField", "MOOF!").create();
    assertEquals("testToStringForString]{stringField=MOOF!}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForBooleanLongDoubleAndString() {
    PdxInstance instance =
        PdxInstanceFactoryImpl.newCreator("testToStringForBooleanLongDoubleAndString", false, cache)
            .writeBoolean("booleanField", Boolean.TRUE).writeLong("longField", 37L)
            .writeDouble("doubleField", 3.1415).writeString("stringField", "MOOF!").create();
    assertEquals(
        "testToStringForBooleanLongDoubleAndString]{booleanField=true, doubleField=3.1415, longField=37, stringField=MOOF!}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForObject() {
    PdxInstance instance = PdxInstanceFactoryImpl.newCreator("testToStringForObject", false, cache)
        .writeObject("objectField", new SerializableObject("Dave")).create();
    assertEquals("testToStringForObject]{objectField=Dave}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForByteArray() {
    PdxInstance instance = PdxInstanceFactoryImpl
        .newCreator("testToStringForByteArray", false, cache).writeByteArray("byteArrayField",
            new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF})
        .create();
    assertEquals("testToStringForByteArray]{byteArrayField=DEADBEEF}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForObjectArray() {
    PdxInstance instance =
        PdxInstanceFactoryImpl.newCreator("testToStringForObjectArray", false, cache)
            .writeObjectArray("objectArrayField",
                new Object[] {new SerializableObject("Dave"), new SerializableObject("Stewart")})
            .create();
    assertEquals("testToStringForObjectArray]{objectArrayField=[Dave, Stewart]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForIntArray() {
    PdxInstance instance =
        PdxInstanceFactoryImpl.newCreator("testToStringForIntArray", false, cache)
            .writeObjectArray("intArrayField", new Integer[] {new Integer(37), new Integer(42)})
            .create();
    assertEquals("testToStringForIntArray]{intArrayField=[37, 42]}",
        substringAfter(instance.toString(), ","));
  }

  static class SerializableObject implements Serializable {
    String name;

    public SerializableObject() {
      // Do nothing.
    }

    public SerializableObject(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return StringUtils.trimToEmpty(name);
    }
  }
}
