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

import static org.apache.geode.internal.serialization.DataSerializableFixedID.ENUM_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class EnumIdTest {
  static final int ID = 0xDEADBEEF;

  @Test
  public void testNoArgConstructor() {
    final EnumId enumId = new EnumId();
    assertEquals(0, enumId.intValue());
  }

  @Test
  public void testSingleArgConstructor() {
    final EnumId enumId = new EnumId(ID);
    assertEquals(ID, enumId.intValue());
  }

  @Test
  public void testGetDSFID() {
    final EnumId enumId = new EnumId(ID);
    assertEquals(ENUM_ID, enumId.getDSFID());
  }

  @Test
  public void testGetDSID() {
    final EnumId enumId = new EnumId(ID);
    assertEquals(0xDE, enumId.getDSId());
  }

  @Test
  public void testEnumNum() {
    final EnumId enumId = new EnumId(ID);
    assertEquals(0xADBEEF, enumId.getEnumNum());
  }

  @Test
  public void testGetSerializationVersions() {
    final EnumId enumId = new EnumId(ID);
    assertNull(enumId.getSerializationVersions());
  }

  @Test
  public void testHashCode() {
    final EnumId enumId = new EnumId(ID);
    assertEquals(enumId.hashCode(), enumId.hashCode());

    final EnumId sameId = new EnumId(ID);
    assertEquals(enumId.hashCode(), sameId.hashCode());

    final EnumId differentId = new EnumId(ID + 1);
    assertNotEquals(enumId.hashCode(), differentId.hashCode());
  }

  @Test
  public void testEquals() {
    final EnumId enumId = new EnumId(ID);
    assertTrue(enumId.equals(enumId));
    assertFalse(enumId.equals(null));
    assertFalse(enumId.equals(new Object()));

    final EnumId sameId = new EnumId(ID);
    assertTrue(enumId.equals(sameId));

    final EnumId differentId = new EnumId(ID + 1);
    assertFalse(enumId.equals(differentId));
  }

  @Test
  public void testToString() {
    final EnumId enumId = new EnumId(ID);
    final String str = enumId.toString();
    assertNotEquals(-1, str.indexOf(Integer.toString(0xDE)));
    assertNotEquals(-1, str.indexOf(Integer.toString(0xADBEEF)));
  }

  @Test
  public void testToDataAndFromData() throws IOException, ClassNotFoundException {
    final EnumId before = new EnumId(ID);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    before.toData(dataOutputStream, null);
    dataOutputStream.close();

    final EnumId after = new EnumId();
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    after.fromData(dataInputStream, null);

    assertEquals(before.intValue(), after.intValue());
  }
}
