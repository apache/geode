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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.pdx.FieldType;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category(SerializationTest.class)
public class PdxTypeTest {
  static final String TYPE_NAME = "typeName";
  static final boolean EXPECT_DOMAIN_CLASS = true;
  static final PdxField FIELD_0 = new PdxField("field0", 0, 0, FieldType.INT, true);
  static final PdxField FIELD_1 = new PdxField("field1", 1, 1, FieldType.STRING, true);
  static final PdxField FIELD_2 = new PdxField("field2", 2, 0, FieldType.BOOLEAN, false);
  static final PdxField FIELD_3 = new PdxField("field3", 3, 0, FieldType.DOUBLE, false);
  static final PdxField FIELD_4 = new PdxField("field4", 4, 2, FieldType.OBJECT_ARRAY, false);

  static {
    FIELD_3.setDeleted(true);
  }

  @Test
  public void testNoArgConstructor() {
    final PdxType type = new PdxType();
    assertEquals(0, type.getVariableLengthFieldCount());
    assertNull(type.getClassName());
    assertFalse(type.getNoDomainClass());
    assertEquals(0, type.getTypeId());
    assertEquals(0, type.getFieldCount());
    assertFalse(type.getHasDeletedField());
  }

  @Test
  public void testSomeArgsConstructor() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    assertEquals(0, type.getVariableLengthFieldCount());
    assertEquals(TYPE_NAME, type.getClassName());
    assertEquals(!EXPECT_DOMAIN_CLASS, type.getNoDomainClass());
    assertEquals(0, type.getTypeId());
    assertEquals(0, type.getFieldCount());
    assertFalse(type.getHasDeletedField());
  }

  @Test
  public void testGetDSId() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.setTypeId(0xDEADBEEF);
    assertEquals(0xDE, type.getDSId());
  }

  @Test
  public void testGetTypeNum() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.setTypeId(0xDEADBEEF);
    assertEquals(0xADBEEF, type.getTypeNum());
  }

  @Test
  public void testAddField() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    assertEquals(0, type.getFieldCount());
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    assertEquals(3, type.getFieldCount());
    final List<PdxField> fields = type.getFields();
    assertTrue(fields.contains(FIELD_0));
    assertTrue(fields.contains(FIELD_1));
    assertFalse(fields.contains(FIELD_2));
    assertFalse(fields.contains(FIELD_3));
    assertTrue(fields.contains(FIELD_4));
  }

  @Test
  public void testGetPdxField() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    assertSame(FIELD_0, type.getPdxField(FIELD_0.getFieldName()));
    assertSame(FIELD_1, type.getPdxField(FIELD_1.getFieldName()));
    assertNull(type.getPdxField(FIELD_2.getFieldName()));
    assertNull(type.getPdxField(FIELD_3.getFieldName()));
    assertSame(FIELD_4, type.getPdxField(FIELD_4.getFieldName()));
  }

  @Test
  public void testGetPdxFieldByIndex() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    assertSame(FIELD_0, type.getPdxFieldByIndex(1));
    assertSame(FIELD_1, type.getPdxFieldByIndex(2));
    assertSame(FIELD_4, type.getPdxFieldByIndex(0));
  }

  @Test
  public void testGetUndeletedFieldCount() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.setHasDeletedField(true);
    type.addField(FIELD_4);
    type.addField(FIELD_3);
    type.addField(FIELD_2);
    assertEquals(2, type.getUndeletedFieldCount());
  }

  @Test
  public void testGetFieldNames() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    final List<String> fieldNames = type.getFieldNames();
    assertEquals(3, fieldNames.size());
    assertTrue(fieldNames.contains(FIELD_0.getFieldName()));
    assertTrue(fieldNames.contains(FIELD_1.getFieldName()));
    assertFalse(fieldNames.contains(FIELD_2.getFieldName()));
    assertFalse(fieldNames.contains(FIELD_3.getFieldName()));
    assertTrue(fieldNames.contains(FIELD_4.getFieldName()));
  }

  @Test
  public void testSortedFields() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.setHasDeletedField(true);
    type.addField(FIELD_3);
    type.addField(FIELD_2);
    type.addField(FIELD_1);
    final Collection<PdxField> fields = type.getSortedFields();
    assertEquals(2, fields.size());
    assertFalse(fields.contains(FIELD_0));
    assertTrue(fields.contains(FIELD_1));
    assertTrue(fields.contains(FIELD_2));
    assertFalse(fields.contains(FIELD_3));
    assertFalse(fields.contains(FIELD_4));
  }

  @Test
  public void testSortedIdentityFields() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.setHasDeletedField(true);
    type.addField(FIELD_3);
    type.addField(FIELD_2);
    type.addField(FIELD_1);
    final Collection<PdxField> fields = type.getSortedIdentityFields();
    assertEquals(1, fields.size());
    assertFalse(fields.contains(FIELD_0));
    assertTrue(fields.contains(FIELD_1));
    assertFalse(fields.contains(FIELD_2));
    assertFalse(fields.contains(FIELD_3));
    assertFalse(fields.contains(FIELD_4));
  }

  @Test
  public void testHasExtraFields() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.setHasDeletedField(true);
    type.addField(FIELD_3);
    type.addField(FIELD_2);
    type.addField(FIELD_1);
    type.addField(FIELD_0);
    assertFalse(type.hasExtraFields(type));

    final PdxType noDeletedField = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    noDeletedField.addField(FIELD_2);
    noDeletedField.addField(FIELD_1);
    noDeletedField.addField(FIELD_0);
    assertFalse(type.hasExtraFields(noDeletedField));

    final PdxType fewerFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    fewerFields.addField(FIELD_2);
    fewerFields.addField(FIELD_1);
    assertTrue(type.hasExtraFields(fewerFields));

    final PdxType moreFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    moreFields.addField(FIELD_4);
    moreFields.addField(FIELD_2);
    moreFields.addField(FIELD_1);
    moreFields.addField(FIELD_0);
    assertFalse(type.hasExtraFields(moreFields));
  }

  @Test
  public void testCompatible() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    assertTrue(type.compatible(type));
    assertFalse(type.compatible(null));

    final PdxType sameTypeNameAndFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFields.addField(FIELD_4);
    sameTypeNameAndFields.addField(FIELD_0);
    sameTypeNameAndFields.addField(FIELD_1);
    assertTrue(type.compatible(sameTypeNameAndFields));

    final PdxType sameTypeNameAndFieldsDifferentDomain =
        new PdxType(TYPE_NAME, !EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_4);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_0);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_1);
    assertTrue(type.compatible(sameTypeNameAndFieldsDifferentDomain));

    final PdxType sameTypeNameAndDifferentFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndDifferentFields.addField(FIELD_3);
    sameTypeNameAndDifferentFields.addField(FIELD_2);
    sameTypeNameAndDifferentFields.addField(FIELD_1);
    assertFalse(type.compatible(sameTypeNameAndDifferentFields));


    final PdxType sameTypeNameAndFieldsInDifferentOrder =
        new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFieldsInDifferentOrder.addField(FIELD_1);
    sameTypeNameAndFieldsInDifferentOrder.addField(FIELD_0);
    sameTypeNameAndFieldsInDifferentOrder.addField(FIELD_4);
    assertTrue(type.compatible(sameTypeNameAndFieldsInDifferentOrder));
  }

  @Test
  public void testHashCode() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    assertEquals(type.hashCode(), type.hashCode());

    final PdxType sameTypeNameAndFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFields.addField(FIELD_4);
    sameTypeNameAndFields.addField(FIELD_0);
    sameTypeNameAndFields.addField(FIELD_1);
    assertEquals(type.hashCode(), sameTypeNameAndFields.hashCode());

    final PdxType sameTypeNameAndFieldsDifferentDomain =
        new PdxType(TYPE_NAME, !EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_4);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_0);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_1);
    assertEquals(type.hashCode(), sameTypeNameAndFieldsDifferentDomain.hashCode());

    final PdxType differentTypeNameAndSameFields =
        new PdxType("Not " + TYPE_NAME, EXPECT_DOMAIN_CLASS);
    differentTypeNameAndSameFields.addField(FIELD_4);
    differentTypeNameAndSameFields.addField(FIELD_0);
    differentTypeNameAndSameFields.addField(FIELD_1);
    assertNotEquals(type.hashCode(), differentTypeNameAndSameFields.hashCode());

    final PdxType sameTypeNameAndDifferentFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndDifferentFields.addField(FIELD_3);
    sameTypeNameAndDifferentFields.addField(FIELD_2);
    sameTypeNameAndDifferentFields.addField(FIELD_1);
    assertNotEquals(type.hashCode(), sameTypeNameAndDifferentFields.hashCode());
  }

  @Test
  public void testEquals() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    assertTrue(type.equals(type));
    assertFalse(type.equals(null));
    assertFalse(type.equals(new Object()));

    final PdxType sameTypeNameAndFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFields.addField(FIELD_4);
    sameTypeNameAndFields.addField(FIELD_0);
    sameTypeNameAndFields.addField(FIELD_1);
    assertTrue(type.equals(sameTypeNameAndFields));

    final PdxType sameTypeNameAndFieldsDifferentDomain =
        new PdxType(TYPE_NAME, !EXPECT_DOMAIN_CLASS);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_4);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_0);
    sameTypeNameAndFieldsDifferentDomain.addField(FIELD_1);
    assertFalse(type.equals(sameTypeNameAndFieldsDifferentDomain));

    final PdxType differentTypeNameAndSameFields =
        new PdxType("Not " + TYPE_NAME, EXPECT_DOMAIN_CLASS);
    differentTypeNameAndSameFields.addField(FIELD_4);
    differentTypeNameAndSameFields.addField(FIELD_0);
    differentTypeNameAndSameFields.addField(FIELD_1);
    assertFalse(type.equals(differentTypeNameAndSameFields));

    final PdxType sameTypeNameAndDifferentFields = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    sameTypeNameAndDifferentFields.addField(FIELD_3);
    sameTypeNameAndDifferentFields.addField(FIELD_2);
    sameTypeNameAndDifferentFields.addField(FIELD_1);
    assertFalse(type.equals(sameTypeNameAndDifferentFields));
  }

  @Test
  public void testToFormattedString() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    final String str = type.toFormattedString();
    assertNotEquals(-1, str.indexOf(TYPE_NAME));
    assertNotEquals(-1, str.indexOf(FIELD_0.getFieldName()));
    assertNotEquals(-1, str.indexOf(FIELD_1.getFieldName()));
    assertNotEquals(-1, str.indexOf(FIELD_4.getFieldName()));
  }

  @Test
  public void testToString() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    final String str = type.toString();
    assertNotEquals(-1, str.indexOf(TYPE_NAME));
    assertNotEquals(-1, str.indexOf(FIELD_0.getFieldName()));
    assertNotEquals(-1, str.indexOf(FIELD_1.getFieldName()));
    assertNotEquals(-1, str.indexOf(FIELD_4.getFieldName()));
  }

  @Test
  public void testToStream() {
    final PdxType type = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    type.addField(FIELD_4);
    type.addField(FIELD_0);
    type.addField(FIELD_1);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    type.toStream(new PrintStream(byteArrayOutputStream), true);
    final String str = byteArrayOutputStream.toString();
    assertNotEquals(-1, str.indexOf(TYPE_NAME));
    assertNotEquals(-1, str.indexOf(FIELD_0.getFieldName()));
    assertNotEquals(-1, str.indexOf(FIELD_1.getFieldName()));
    assertNotEquals(-1, str.indexOf(FIELD_4.getFieldName()));
  }

  @Test
  public void testToDataAndFromData() throws IOException, ClassNotFoundException {
    final PdxType before = new PdxType(TYPE_NAME, EXPECT_DOMAIN_CLASS);
    before.setHasDeletedField(true);
    before.addField(FIELD_4);
    before.addField(FIELD_3);
    before.addField(FIELD_2);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    before.toData(dataOutputStream);
    dataOutputStream.close();

    final PdxType after = new PdxType();
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    after.fromData(dataInputStream);

    assertEquals(before.getVariableLengthFieldCount(), after.getVariableLengthFieldCount());
    assertEquals(before.getClassName(), after.getClassName());
    assertEquals(before.getNoDomainClass(), after.getNoDomainClass());
    assertEquals(before.getTypeId(), after.getTypeId());
    assertEquals(before.getFieldCount(), after.getFieldCount());
    assertEquals(before.getHasDeletedField(), after.getHasDeletedField());
  }
}
