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
import static org.apache.geode.distributed.internal.locks.GrantorRequestProcessor.GrantorRequestContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.SerializationTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, SerializationTest.class})
public class PdxInstanceImplTest {
  PdxType emptyPdxType;
  PdxField nonExistentField;
  PdxType pdxType;
  PdxField nonIdentityField;
  PdxField booleanField;
  PdxField doubleField;
  PdxField intField;
  PdxField longField;
  PdxField objectField;
  PdxField stringField;
  PdxField byteArrayField;
  PdxField objectArrayField;
  PdxField integerArrayField;
  PdxField shortArrayField;
  PdxField intArrayField;
  PdxField longArrayField;
  PdxField charArrayField;
  PdxField floatArrayField;
  PdxField doubleArrayField;
  PdxField booleanArrayField;
  TypeRegistry pdxRegistry;
  PdxInstance instance;

  @Before
  public void setUp() throws IOException, ClassNotFoundException {
    CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    doNothing().when(cancelCriterion).checkCancelInProgress(any());

    InternalDistributedMember distributedMember = mock(InternalDistributedMember.class);

    DistributionManager distributionManager = mock(DistributionManager.class);
    when(distributionManager.getDistributedSystemId()).thenReturn(-1);
    when(distributionManager.getCancelCriterion()).thenReturn(cancelCriterion);
    when(distributionManager.getId()).thenReturn(distributedMember);
    when(distributionManager.getElderId()).thenReturn(distributedMember);

    GrantorRequestContext grantorRequestContext = new GrantorRequestContext(cancelCriterion);

    InternalDistributedSystem distributedSystem = mock(InternalDistributedSystem.class);
    when(distributedSystem.isLoner()).thenReturn(true);
    when(distributedSystem.getCancelCriterion()).thenReturn(cancelCriterion);
    when(distributedSystem.getGrantorRequestContext()).thenReturn(grantorRequestContext);

    // Cyclical dependency...
    when(distributedSystem.getDistributionManager()).thenReturn(distributionManager);
    when(distributionManager.getSystem()).thenReturn(distributedSystem);

    InternalCache internalCache = mock(InternalCache.class);
    when(internalCache.getInternalDistributedSystem()).thenReturn(distributedSystem);
    when(internalCache.getPdxPersistent()).thenReturn(false);

    Region<Object, Object> region = mock(Region.class);
    when(internalCache.createVMRegion(any(), any(), any())).thenReturn(region);

    DistributedLockService distributedLockService = mock(DistributedLockService.class);

    emptyPdxType = new PdxType("PdxInstanceImplTest.EmptyPdxType", false);
    nonExistentField = new PdxField("nonExistentField", 0, 0, FieldType.OBJECT, false);

    pdxType = new PdxType("PdxInstanceImplTest.PdxType", false);
    int index = 0;
    int varId = 0;
    nonIdentityField = new PdxField("nonIdentityField", index++, 0, FieldType.INT, false);
    pdxType.addField(nonIdentityField);
    booleanField = new PdxField("booleanField", index++, 0, FieldType.BOOLEAN, true);
    pdxType.addField(booleanField);
    doubleField = new PdxField("doubleField", index++, 0, FieldType.DOUBLE, true);
    pdxType.addField(doubleField);
    intField = new PdxField("intField", index++, 0, FieldType.INT, true);
    pdxType.addField(intField);
    longField = new PdxField("longField", index++, 0, FieldType.LONG, true);
    pdxType.addField(longField);
    objectField = new PdxField("objectField", index++, varId++, FieldType.OBJECT, true);
    pdxType.addField(objectField);
    stringField = new PdxField("stringField", index++, varId++, FieldType.STRING, true);
    pdxType.addField(stringField);
    byteArrayField =
        new PdxField("byteArrayField", index++, varId++, FieldType.BYTE_ARRAY, true);
    pdxType.addField(byteArrayField);
    objectArrayField =
        new PdxField("objectArrayField", index++, varId++, FieldType.OBJECT_ARRAY, true);
    pdxType.addField(objectArrayField);
    integerArrayField =
        new PdxField("integerArrayField", index++, varId++, FieldType.OBJECT_ARRAY, true);
    pdxType.addField(integerArrayField);
    shortArrayField =
        new PdxField("shortArrayField", index++, varId++, FieldType.SHORT_ARRAY, true);
    pdxType.addField(shortArrayField);
    intArrayField = new PdxField("intArrayField", index++, varId++, FieldType.INT_ARRAY, true);
    pdxType.addField(intArrayField);
    longArrayField =
        new PdxField("longArrayField", index++, varId++, FieldType.LONG_ARRAY, true);
    pdxType.addField(longArrayField);
    charArrayField =
        new PdxField("charArrayField", index++, varId++, FieldType.CHAR_ARRAY, true);
    pdxType.addField(charArrayField);
    floatArrayField =
        new PdxField("floatArrayField", index++, varId++, FieldType.FLOAT_ARRAY, true);
    pdxType.addField(floatArrayField);
    doubleArrayField =
        new PdxField("doubleArrayField", index++, varId++, FieldType.DOUBLE_ARRAY, true);
    pdxType.addField(doubleArrayField);
    booleanArrayField =
        new PdxField("booleanArrayField", index++, varId++, FieldType.BOOLEAN_ARRAY, true);
    pdxType.addField(booleanArrayField);

    PeerTypeRegistration testTypeRegistration = mock(PeerTypeRegistration.class);
    when(testTypeRegistration.getLockService()).thenReturn(distributedLockService);
    when(testTypeRegistration.defineType(emptyPdxType)).thenReturn(1);
    when(testTypeRegistration.defineType(pdxType)).thenReturn(2);
    when(testTypeRegistration.getType(1)).thenReturn(emptyPdxType);
    when(testTypeRegistration.getType(2)).thenReturn(pdxType);

    TypeRegistration typeRegistration =
        new TestLonerTypeRegistration(internalCache, testTypeRegistration);

    pdxRegistry = new TypeRegistry(internalCache, typeRegistration);
    pdxRegistry.initialize();
    pdxRegistry.defineType(emptyPdxType);
    pdxRegistry.defineType(pdxType);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeInt(13);
    writer.writeBoolean(true);
    writer.writeDouble(3.1415);
    writer.writeInt(37);
    writer.writeLong(42);
    writer.writeObject(new SerializableObject("Dave"));
    writer.writeString("MOOF!");
    writer.writeByteArray(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
    writer.writeObjectArray(
        new Object[] {new SerializableObject("Dave"), new SerializableObject("Stewart")});
    writer.writeObjectArray(new Integer[] {new Integer(37), new Integer(42)});
    writer.writeShortArray(new short[] {(short) 37, (short) 42});
    writer.writeIntArray(new int[] {37, 42});
    writer.writeLongArray(new long[] {37L, 42L});
    writer.writeCharArray(new char[] {'o', 'k'});
    writer.writeFloatArray(new float[] {3.14159F, 2.71828F});
    writer.writeDoubleArray(new double[] {3.14159, 2.71828});
    writer.writeBooleanArray(new boolean[] {false, true});
    writer.completeByteStreamGeneration();

    instance = writer.makePdxInstance();
  }

  @After
  public void tearDown() {
    // Do nothing.
  }

  @Test
  public void testToStringForEmpty() {
    final PdxWriterImpl writer =
        new PdxWriterImpl(emptyPdxType, pdxRegistry, new PdxOutputStream());
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals(emptyPdxType.getClassName() + "]{}", substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToString() {
    assertEquals(
        pdxType.getClassName() + "]{" +
            booleanArrayField.getFieldName() + "=[false, true]" +
            ", " +
            booleanField.getFieldName() + "=true" +
            ", " +
            byteArrayField.getFieldName() + "=DEADBEEF" +
            ", " +
            charArrayField.getFieldName() + "=[o, k]" +
            ", " +
            doubleArrayField.getFieldName() + "=[3.14159, 2.71828]" +
            ", " +
            doubleField.getFieldName() + "=3.1415" +
            ", " +
            floatArrayField.getFieldName() + "=[3.14159, 2.71828]" +
            ", " +
            intArrayField.getFieldName() + "=[37, 42]" +
            ", " +
            intField.getFieldName() + "=37" +
            ", " +
            integerArrayField.getFieldName() + "=[37, 42]" +
            ", " +
            longArrayField.getFieldName() + "=[37, 42]" +
            ", " +
            longField.getFieldName() + "=42" +
            ", " +
            objectArrayField.getFieldName() + "=[Dave, Stewart]" +
            ", " +
            objectField.getFieldName() + "=Dave" +
            ", " +
            shortArrayField.getFieldName() + "=[37, 42]" +
            ", " +
            stringField.getFieldName() + "=MOOF!" +
            "}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testGetField() {
    assertNull(instance.getField(nonExistentField.getFieldName()));
    assertEquals(Integer.class, instance.getField(nonIdentityField.getFieldName()).getClass());
    assertEquals(Boolean.class, instance.getField(booleanField.getFieldName()).getClass());
    assertEquals(Double.class, instance.getField(doubleField.getFieldName()).getClass());
    assertEquals(Integer.class, instance.getField(intField.getFieldName()).getClass());
    assertEquals(Long.class, instance.getField(longField.getFieldName()).getClass());
    assertEquals(SerializableObject.class,
        instance.getField(objectField.getFieldName()).getClass());
    assertEquals(String.class, instance.getField(stringField.getFieldName()).getClass());
    assertEquals(byte[].class, instance.getField(byteArrayField.getFieldName()).getClass());
    assertEquals(Object[].class, instance.getField(objectArrayField.getFieldName()).getClass());
    assertEquals(Integer[].class, instance.getField(integerArrayField.getFieldName()).getClass());
    assertEquals(short[].class, instance.getField(shortArrayField.getFieldName()).getClass());
    assertEquals(int[].class, instance.getField(intArrayField.getFieldName()).getClass());
    assertEquals(long[].class, instance.getField(longArrayField.getFieldName()).getClass());
    assertEquals(char[].class, instance.getField(charArrayField.getFieldName()).getClass());
    assertEquals(float[].class, instance.getField(floatArrayField.getFieldName()).getClass());
    assertEquals(double[].class, instance.getField(doubleArrayField.getFieldName()).getClass());
    assertEquals(boolean[].class, instance.getField(booleanArrayField.getFieldName()).getClass());
  }

  @Test
  public void testHasField() {
    assertEquals(false, instance.hasField(nonExistentField.getFieldName()));
    assertEquals(true, instance.hasField(nonIdentityField.getFieldName()));
    assertEquals(true, instance.hasField(booleanField.getFieldName()));
    assertEquals(true, instance.hasField(doubleField.getFieldName()));
    assertEquals(true, instance.hasField(intField.getFieldName()));
    assertEquals(true, instance.hasField(longField.getFieldName()));
    assertEquals(true,
        instance.hasField(objectField.getFieldName()));
    assertEquals(true, instance.hasField(stringField.getFieldName()));
    assertEquals(true, instance.hasField(byteArrayField.getFieldName()));
    assertEquals(true, instance.hasField(objectArrayField.getFieldName()));
    assertEquals(true, instance.hasField(integerArrayField.getFieldName()));
    assertEquals(true, instance.hasField(shortArrayField.getFieldName()));
    assertEquals(true, instance.hasField(intArrayField.getFieldName()));
    assertEquals(true, instance.hasField(longArrayField.getFieldName()));
    assertEquals(true, instance.hasField(charArrayField.getFieldName()));
    assertEquals(true, instance.hasField(floatArrayField.getFieldName()));
    assertEquals(true, instance.hasField(doubleArrayField.getFieldName()));
    assertEquals(true, instance.hasField(booleanArrayField.getFieldName()));
  }

  @Test
  public void testIsEnum() {
    assertFalse(instance.isEnum());
  }

  @Test
  public void testGetClassName() {
    assertEquals(pdxType.getClassName(), instance.getClassName());
  }

  @Test
  public void testGetFieldNames() {
    assertEquals(Arrays.asList(
        nonIdentityField.getFieldName(), booleanField.getFieldName(), doubleField.getFieldName(),
        intField.getFieldName(),
        longField.getFieldName(), objectField.getFieldName(), stringField.getFieldName(),
        byteArrayField.getFieldName(), objectArrayField.getFieldName(),
        integerArrayField.getFieldName(), shortArrayField.getFieldName(),
        intArrayField.getFieldName(), longArrayField.getFieldName(), charArrayField.getFieldName(),
        floatArrayField.getFieldName(), doubleArrayField.getFieldName(),
        booleanArrayField.getFieldName()), instance.getFieldNames());
  }

  @Test
  public void testIsIdentityField() {
    assertEquals(false, instance.isIdentityField(nonExistentField.getFieldName()));
    assertEquals(false, instance.isIdentityField(nonIdentityField.getFieldName()));
    assertEquals(true, instance.isIdentityField(booleanField.getFieldName()));
    assertEquals(true, instance.isIdentityField(doubleField.getFieldName()));
    assertEquals(true, instance.isIdentityField(intField.getFieldName()));
    assertEquals(true, instance.isIdentityField(longField.getFieldName()));
    assertEquals(true,
        instance.isIdentityField(objectField.getFieldName()));
    assertEquals(true, instance.isIdentityField(stringField.getFieldName()));
    assertEquals(true, instance.isIdentityField(byteArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(objectArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(integerArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(shortArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(intArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(longArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(charArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(floatArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(doubleArrayField.getFieldName()));
    assertEquals(true, instance.isIdentityField(booleanArrayField.getFieldName()));
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

  private static final class TestLonerTypeRegistration extends LonerTypeRegistration {
    TypeRegistration testTypeRegistration;

    public TestLonerTypeRegistration(InternalCache cache, TypeRegistration testTypeRegistration) {
      super(cache);
      this.testTypeRegistration = testTypeRegistration;
    }

    @Override
    protected TypeRegistration createTypeRegistration(boolean client) {
      return testTypeRegistration;
    }
  }
}
