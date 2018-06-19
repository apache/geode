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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doNothing;

import java.io.IOException;
import java.io.Serializable;

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
  protected TypeRegistry pdxRegistry;

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

    PeerTypeRegistration testTypeRegistration = mock(PeerTypeRegistration.class);
    when(testTypeRegistration.getLockService()).thenReturn(distributedLockService);

    TypeRegistration typeRegistration =
        new TestLonerTypeRegistration(internalCache, testTypeRegistration);

    pdxRegistry = new TypeRegistry(internalCache, typeRegistration);
    pdxRegistry.initialize();
  }

  @After
  public void tearDown() {
    // Do nothing.
  }

  @Test
  public void testToStringForEmpty() throws Exception {
    final String name = "testToStringForEmpty";
    final PdxType pdxType = new PdxType(name, false);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream());
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals(name + "]{}", substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForInteger() {
    final String name = "testToStringForInteger";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField intField = new PdxField("intField", index++, 0, FieldType.INT, false);
    pdxType.addField(intField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeInt(37);
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals(name + "]{intField=37}", substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForString() {
    final String name = "testToStringForString";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField stringField = new PdxField("stringField", index++, 0, FieldType.STRING, false);
    pdxType.addField(stringField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeString("MOOF!");
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForString]{stringField=MOOF!}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForBooleanLongDoubleAndString() {
    final String name = "testToStringForBooleanLongDoubleAndString";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField booleanField = new PdxField("booleanField", index++, 0, FieldType.BOOLEAN, false);
    pdxType.addField(booleanField);
    PdxField doubleField = new PdxField("doubleField", index++, 0, FieldType.DOUBLE, false);
    pdxType.addField(doubleField);
    PdxField longField = new PdxField("longField", index++, 0, FieldType.LONG, false);
    pdxType.addField(longField);
    PdxField stringField = new PdxField("stringField", index++, 0, FieldType.STRING, false);
    pdxType.addField(stringField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeBoolean(true);
    writer.writeDouble(3.1415);
    writer.writeLong(37);
    writer.writeString("MOOF!");
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals(
        "testToStringForBooleanLongDoubleAndString]{booleanField=true, doubleField=3.1415, longField=37, stringField=MOOF!}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForObject() {
    final String name = "testToStringForObject";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField objectField = new PdxField("objectField", index++, 0, FieldType.OBJECT, false);
    pdxType.addField(objectField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeObject(new SerializableObject("Dave"));
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForObject]{objectField=Dave}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForByteArray() {
    final String name = "testToStringForByteArray";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField byteArrayField =
        new PdxField("byteArrayField", index++, 0, FieldType.BYTE_ARRAY, false);
    pdxType.addField(byteArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeByteArray(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForByteArray]{byteArrayField=DEADBEEF}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForObjectArray() {
    final String name = "testToStringForObjectArray";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField objectArrayField =
        new PdxField("objectArrayField", index++, 0, FieldType.OBJECT_ARRAY, false);
    pdxType.addField(objectArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeObjectArray(
        new Object[] {new SerializableObject("Dave"), new SerializableObject("Stewart")});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForObjectArray]{objectArrayField=[Dave, Stewart]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForIntegerArray() {
    final String name = "testToStringForIntegerArray";
    final PdxType pdxType = new PdxType(name, false);

    int index = 0;
    PdxField integerArrayField =
        new PdxField("integerArrayField", index++, 0, FieldType.OBJECT_ARRAY, false);
    pdxType.addField(integerArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeObjectArray(new Integer[] {new Integer(37), new Integer(42)});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForIntegerArray]{integerArrayField=[37, 42]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForShortArray() {
    final String name = "testToStringForShortArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField shortArrayField =
        new PdxField("shortArrayField", index++, 0, FieldType.SHORT_ARRAY, false);
    pdxType.addField(shortArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeShortArray(new short[] {(short) 37, (short) 42});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForShortArray]{shortArrayField=[37, 42]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForIntArray() {
    final String name = "testToStringForIntArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField intArrayField = new PdxField("intArrayField", index++, 0, FieldType.INT_ARRAY, false);
    pdxType.addField(intArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeIntArray(new int[] {37, 42});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForIntArray]{intArrayField=[37, 42]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForLongArray() {
    final String name = "testToStringForLongArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField longArrayField =
        new PdxField("longArrayField", index++, 0, FieldType.LONG_ARRAY, false);
    pdxType.addField(longArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeLongArray(new long[] {37L, 42L});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForLongArray]{longArrayField=[37, 42]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForCharArray() {
    final String name = "testToStringForCharArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField charArrayField =
        new PdxField("charArrayField", index++, 0, FieldType.CHAR_ARRAY, false);
    pdxType.addField(charArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeCharArray(new char[] {'o', 'k'});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForCharArray]{charArrayField=[o, k]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForFloatArray() {
    final String name = "testToStringForFloatArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField floatArrayField =
        new PdxField("floatArrayField", index++, 0, FieldType.FLOAT_ARRAY, false);
    pdxType.addField(floatArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeFloatArray(new float[] {3.14159F, 2.71828F});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForFloatArray]{floatArrayField=[3.14159, 2.71828]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForDoubleArray() {
    final String name = "testToStringForDoubleArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField doubleArrayField =
        new PdxField("doubleArrayField", index++, 0, FieldType.DOUBLE_ARRAY, false);
    pdxType.addField(doubleArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeDoubleArray(new double[] {3.14159, 2.71828});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForDoubleArray]{doubleArrayField=[3.14159, 2.71828]}",
        substringAfter(instance.toString(), ","));
  }

  @Test
  public void testToStringForBooleanArray() {
    final String name = "testToStringForBooleanArray";
    final PdxType pdxType = new PdxType(name, false);

    short index = 0;
    PdxField booleanArrayField =
        new PdxField("booleanArrayField", index++, 0, FieldType.BOOLEAN_ARRAY, false);
    pdxType.addField(booleanArrayField);

    final PdxWriterImpl writer = new PdxWriterImpl(pdxType, pdxRegistry, new PdxOutputStream(1024));
    writer.writeBooleanArray(new boolean[] {false, true});
    writer.completeByteStreamGeneration();

    final PdxInstance instance = writer.makePdxInstance();
    assertEquals("testToStringForBooleanArray]{booleanArrayField=[false, true]}",
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
