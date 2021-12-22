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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CopyHelper;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.DeltaTestImpl;
import org.apache.geode.ToDataException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.PdxSerializerObject;
import org.apache.geode.internal.SystemAdmin;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSourceFactory;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.pdx.internal.DataSize;
import org.apache.geode.pdx.internal.PdxReaderImpl;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PdxWriterImpl;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class PdxSerializableJUnitTest {

  private GemFireCacheImpl cache;

  @Before
  public void setUp() {
    // make it a loner
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  private int getPdxTypeIdForClass(Class c) {
    // here we are assuming Dsid == 0
    return cache.getPdxRegistry().getExistingTypeForClass(c).hashCode()
        & PeerTypeRegistration.PLACE_HOLDER_FOR_TYPE_ID;
  }

  private int getNumPdxTypes() {
    return cache.getPdxRegistry().typeMap().size();
  }

  @Test
  public void testNoDiskStore() throws Exception {
    cache.close();
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").setPdxPersistent(true)
        .setPdxDiskStore("doesNotExist").create();
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    PdxSerializable object = new SimpleClass(1, (byte) 5, null);
    try {
      DataSerializer.writeObject(object, out);
      throw new Exception("expected PdxInitializationException");
    } catch (PdxInitializationException expected) {
    }
  }

  // for bugs 44271 and 44914
  @Test
  public void testPdxPersistentKeys() throws Exception {
    cache.close();
    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").setPdxPersistent(true)
        .setPdxDiskStore("pdxDS").create();
    try {
      DiskStoreFactory dsf = cache.createDiskStoreFactory();
      dsf.create("pdxDS");
      cache.createDiskStoreFactory().create("r2DS");
      Region r1 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).create("r1");
      r1.put(new SimpleClass(1, (byte) 1), "1");
      r1.put(new SimpleClass(2, (byte) 2), "2");
      r1.put(new SimpleClass(1, (byte) 1), "1.2"); // so we have something to compact offline
      Region r2 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
          .setDiskStoreName("r2DS").create("r2");
      r2.put(new SimpleClass(1, (byte) 1), new SimpleClass(1, (byte) 1));
      r2.put(new SimpleClass(2, (byte) 2), new SimpleClass(2, (byte) 2));
      cache.close();
      cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").setPdxPersistent(true)
          .setPdxDiskStore("pdxDS").create();
      dsf = cache.createDiskStoreFactory();
      dsf.create("pdxDS");
      cache.createDiskStoreFactory().create("r2DS");
      r1 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).create("r1");
      r2 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setDiskStoreName("r2DS")
          .create("r2");
      assertEquals(true, r1.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r1.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(true, r2.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r2.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(new SimpleClass(1, (byte) 1), r2.get(new SimpleClass(1, (byte) 1)));
      assertEquals(new SimpleClass(2, (byte) 2), r2.get(new SimpleClass(2, (byte) 2)));
      cache.close();
      // use a cache.xml to recover
      cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos), true);
      pw.println("<?xml version=\"1.0\"?>");
      pw.println("<!DOCTYPE cache PUBLIC");
      pw.println("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"");
      pw.println("  \"http://www.gemstone.com/dtd/cache7_0.dtd\">");
      pw.println("<cache>");
      pw.println("  <disk-store name=\"r2DS\"/>");
      pw.println("  <disk-store name=\"pdxDS\"/>");
      pw.println("  <pdx persistent=\"true\" disk-store-name=\"pdxDS\"/>");
      pw.println("  <region name=\"r1\" refid=\"LOCAL_PERSISTENT\"/>");
      pw.println("  <region name=\"r2\" refid=\"LOCAL_PERSISTENT\">");
      pw.println("    <region-attributes disk-store-name=\"r2DS\"/>");
      pw.println("  </region>");
      pw.println("</cache>");
      pw.close();
      byte[] bytes = baos.toByteArray();
      cache.loadCacheXml(new ByteArrayInputStream(bytes));
      r1 = cache.getRegion(SEPARATOR + "r1");
      r2 = cache.getRegion(SEPARATOR + "r2");
      assertEquals(true, r1.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r1.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(true, r2.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r2.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(new SimpleClass(1, (byte) 1), r2.get(new SimpleClass(1, (byte) 1)));
      assertEquals(new SimpleClass(2, (byte) 2), r2.get(new SimpleClass(2, (byte) 2)));
      cache.close();
      // make sure offlines tools work with disk store that has pdx keys
      SystemAdmin.validateDiskStore("DEFAULT", ".");
      SystemAdmin.compactDiskStore("DEFAULT", ".");
      SystemAdmin.modifyDiskStore("DEFAULT", ".");
      SystemAdmin.validateDiskStore("r2DS", ".");
      SystemAdmin.compactDiskStore("r2DS", ".");
      SystemAdmin.modifyDiskStore("r2DS", ".");
      SystemAdmin.validateDiskStore("pdxDS", ".");
      SystemAdmin.compactDiskStore("pdxDS", ".");
      SystemAdmin.modifyDiskStore("pdxDS", ".");
    } finally {
      try {
        cache.close();
      } finally {
        Pattern pattern = Pattern.compile("BACKUP(DEFAULT|pdxDS|r2DS).*");
        File[] files = new File(".").listFiles((dir1, name) -> pattern.matcher(name).matches());
        if (files != null) {
          for (File file : files) {
            Files.delete(file.toPath());
          }
        }
      }
    }
  }

  @Test
  public void testPdxPersistentKeysDefDS() throws Exception {
    cache.close();
    cache =
        (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").setPdxPersistent(true).create();
    try {
      cache.createDiskStoreFactory().create("r2DS");
      Region r1 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
          .setDiskStoreName("r2DS").create("r1");
      r1.put(new SimpleClass(1, (byte) 1), "1");
      r1.put(new SimpleClass(2, (byte) 2), "2");
      r1.put(new SimpleClass(1, (byte) 1), "1.2"); // so we have something to compact offline
      Region r2 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT)
          .setDiskStoreName("r2DS").create("r2");
      r2.put(new SimpleClass(1, (byte) 1), new SimpleClass(1, (byte) 1));
      r2.put(new SimpleClass(2, (byte) 2), new SimpleClass(2, (byte) 2));
      cache.close();
      cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").setPdxPersistent(true)
          .create();
      cache.createDiskStoreFactory().create("r2DS");
      r1 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setDiskStoreName("r2DS")
          .create("r1");
      r2 = cache.createRegionFactory(RegionShortcut.LOCAL_PERSISTENT).setDiskStoreName("r2DS")
          .create("r2");
      assertEquals(true, r1.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r1.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(true, r2.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r2.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(new SimpleClass(1, (byte) 1), r2.get(new SimpleClass(1, (byte) 1)));
      assertEquals(new SimpleClass(2, (byte) 2), r2.get(new SimpleClass(2, (byte) 2)));
      cache.close();
      // use a cache.xml to recover
      cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos), true);
      pw.println("<?xml version=\"1.0\"?>");
      pw.println("<!DOCTYPE cache PUBLIC");
      pw.println("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"");
      pw.println("  \"http://www.gemstone.com/dtd/cache7_0.dtd\">");
      pw.println("<cache>");
      pw.println("  <disk-store name=\"r2DS\"/>");
      pw.println("  <pdx persistent=\"true\"/>");
      pw.println("  <region name=\"r1\" refid=\"LOCAL_PERSISTENT\">");
      pw.println("    <region-attributes disk-store-name=\"r2DS\"/>");
      pw.println("  </region>");
      pw.println("  <region name=\"r2\" refid=\"LOCAL_PERSISTENT\">");
      pw.println("    <region-attributes disk-store-name=\"r2DS\"/>");
      pw.println("  </region>");
      pw.println("</cache>");
      pw.close();
      byte[] bytes = baos.toByteArray();
      cache.loadCacheXml(new ByteArrayInputStream(bytes));
      r1 = cache.getRegion(SEPARATOR + "r1");
      r2 = cache.getRegion(SEPARATOR + "r2");
      assertEquals(true, r1.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r1.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(true, r2.containsKey(new SimpleClass(1, (byte) 1)));
      assertEquals(true, r2.containsKey(new SimpleClass(2, (byte) 2)));
      assertEquals(new SimpleClass(1, (byte) 1), r2.get(new SimpleClass(1, (byte) 1)));
      assertEquals(new SimpleClass(2, (byte) 2), r2.get(new SimpleClass(2, (byte) 2)));
      cache.close();
      // make sure offlines tools work with disk store that has pdx keys
      SystemAdmin.validateDiskStore("DEFAULT", ".");
      SystemAdmin.compactDiskStore("DEFAULT", ".");
      SystemAdmin.modifyDiskStore("DEFAULT", ".");
      SystemAdmin.validateDiskStore("r2DS", ".");
      SystemAdmin.compactDiskStore("r2DS", ".");
      SystemAdmin.modifyDiskStore("r2DS", ".");
    } finally {
      try {
        cache.close();
      } finally {
        Pattern pattern = Pattern.compile("BACKUP(DEFAULT|r2DS).*");
        File[] files = new File(".").listFiles((dir1, name) -> pattern.matcher(name).matches());
        if (files != null) {
          for (File file : files) {
            Files.delete(file.toPath());
          }
        }
      }
    }
  }

  @Test
  public void testByteFormatForSimpleClass() throws Exception {
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    PdxSerializable object = new SimpleClass(1, (byte) 5, null);
    DataSerializer.writeObject(object, out);
    int typeId = getPdxTypeIdForClass(SimpleClass.class);
    byte[] actual = out.toByteArray();
    byte[] expected = new byte[] {DSCODE.PDX.toByte(), // byte
        0, 0, 0, 4 + 1 + 1, // int - length of byte stream = 4(myInt) + 1(myByte) + 1 (myEnum)
        (byte) (typeId >> 24), (byte) (typeId >> 16), (byte) (typeId >> 8), (byte) typeId, // int -
                                                                                           // typeId
        0, 0, 0, 1, // int - myInt = 1
        5, // byte - myByte = 5
        DSCODE.NULL.toByte()};

    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
    System.out.println("\n");

    DataInput in = new DataInputStream(new ByteArrayInputStream(actual));
    SimpleClass actualVal = DataSerializer.readObject(in);
    // System.out.println("actualVal..."+actualVal);
    assertTrue(
        "Mismatch in write and read value: Value Write..." + object + " Value Read..." + actualVal,
        object.equals(actualVal));

  }

  @Test
  public void testByteFormatForStrings() throws Exception {
    boolean myFlag = true;
    short myShort = 25;
    String myString1 = "Class4_myString1";
    long myLong = 15654;
    String myString2 = "Class4_myString2";
    String myString3 = "Class4_myString3";
    int myInt = 1420;
    float myFloat = 123.023f;

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    SimpleClass1 pdx =
        new SimpleClass1(myFlag, myShort, myString1, myLong, myString2, myString3, myInt, myFloat);
    DataSerializer.writeObject(pdx, out);
    int typeId = getPdxTypeIdForClass(SimpleClass1.class);

    HeapDataOutputStream hdos1 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString1, hdos1);
    byte[] str1Bytes = hdos1.toByteArray();
    HeapDataOutputStream hdos2 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString2, hdos2);
    byte[] str2Bytes = hdos2.toByteArray();
    HeapDataOutputStream hdos3 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString3, hdos3);
    byte[] str3Bytes = hdos3.toByteArray();

    int length = 1 /* myFlag */ + 2 /* myShort */
        + 8 /* myLong */ + 4 /* myInt */ + 4 /* myFloat */ + str1Bytes.length + str2Bytes.length
        + str3Bytes.length + 2 /* byte offset for 3 strings */;

    int offset1 = 1 + 2;
    int offset2 = offset1 + 8 + str1Bytes.length;
    int offset3 = offset1 + 8 + str1Bytes.length + str2Bytes.length;
    byte[] actual = out.toByteArray();
    int floatBytes = Float.floatToRawIntBits(myFloat);
    Byte[] expected = new Byte[] {DSCODE.PDX.toByte(), // byte
        (byte) (length >> 24), (byte) (length >> 16), (byte) (length >> 8), (byte) length, // int -
                                                                                           // length
                                                                                           // of
                                                                                           // byte
                                                                                           // stream
        (byte) (typeId >> 24), (byte) (typeId >> 16), (byte) (typeId >> 8), (byte) typeId, // int -
                                                                                           // typeId
        1, // boolean - myFlag = true
        (byte) (myShort >> 8), (byte) myShort, // short - myShort
        (byte) (myLong >> 56), (byte) (myLong >> 48), (byte) (myLong >> 40), (byte) (myLong >> 32),
        (byte) (myLong >> 24), (byte) (myLong >> 16), (byte) (myLong >> 8), (byte) myLong, // long -
                                                                                           // myLong
        (byte) (myInt >> 24), (byte) (myInt >> 16), (byte) (myInt >> 8), (byte) myInt, // int -
                                                                                       // myInt
        (byte) (floatBytes >> 24), (byte) (floatBytes >> 16), (byte) (floatBytes >> 8),
        (byte) floatBytes, // float - myFloat
        (byte) offset3, // offset of myString3
        (byte) offset2, // offset of myString2
    };

    for (int i = (str1Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset1 + PdxWriterImpl.HEADER_SIZE, str1Bytes[i]); // +
                                                                                                   // 5
                                                                                                   // for:
                                                                                                   // 1
                                                                                                   // for
                                                                                                   // DSCODE.PDX.toByte()
                                                                                                   // and
                                                                                                   // 4
                                                                                                   // for
                                                                                                   // byte
                                                                                                   // stream
                                                                                                   // length
    }
    for (int i = (str2Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset2 + PdxWriterImpl.HEADER_SIZE, str2Bytes[i]);
    }
    for (int i = (str3Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset3 + PdxWriterImpl.HEADER_SIZE, str3Bytes[i]);
    }

    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    if (actual.length != expected.length) {
      System.out.println(msg);
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
    System.out.println("\n");

    DataInput in = new DataInputStream(new ByteArrayInputStream(actual));
    SimpleClass1 actualVal = DataSerializer.readObject(in);
    // System.out.println("actualVal..."+actualVal);
    assertTrue(
        "Mismatch in write and read value: Value Write..." + pdx + " Value Read..." + actualVal,
        pdx.equals(actualVal));

    cache.setReadSerializedForTest(true);
    try {
      in = new DataInputStream(new ByteArrayInputStream(actual));
      PdxInstance pi = DataSerializer.readObject(in);
      actualVal = (SimpleClass1) pi.getObject();
      assertTrue(
          "Mismatch in write and read value: Value Write..." + pdx + " Value Read..." + actualVal,
          pdx.equals(actualVal));
      assertTrue(pi.hasField("myFlag"));
      assertTrue(pi.hasField("myShort"));
      assertTrue(pi.hasField("myString1"));
      assertTrue(pi.hasField("myLong"));
      assertTrue(pi.hasField("myString2"));
      assertTrue(pi.hasField("myString3"));
      assertTrue(pi.hasField("myInt"));
      assertTrue(pi.hasField("myFloat"));
      assertEquals(pdx.isMyFlag(), pi.getField("myFlag"));
      assertEquals(pdx.getMyShort(), pi.getField("myShort"));
      assertEquals(pdx.getMyString1(), pi.getField("myString1"));
      assertEquals(pdx.getMyLong(), pi.getField("myLong"));
      assertEquals(pdx.getMyString2(), pi.getField("myString2"));
      assertEquals(pdx.getMyString3(), pi.getField("myString3"));
      assertEquals(pdx.getMyInt(), pi.getField("myInt"));
      assertEquals(pdx.getMyFloat(), pi.getField("myFloat"));
      PdxReaderImpl reader = (PdxReaderImpl) pi;
      PdxType type = reader.getPdxType();
      assertEquals(SimpleClass1.class.getName(), type.getClassName());
      assertEquals(8, type.getFieldCount());
      assertEquals(2, type.getVariableLengthFieldCount());

      assertEquals(0, type.getPdxField("myFlag").getFieldIndex());
      assertEquals(1, type.getPdxField("myShort").getFieldIndex());
      assertEquals(2, type.getPdxField("myString1").getFieldIndex());
      assertEquals(3, type.getPdxField("myLong").getFieldIndex());
      assertEquals(4, type.getPdxField("myString2").getFieldIndex());
      assertEquals(5, type.getPdxField("myString3").getFieldIndex());
      assertEquals(6, type.getPdxField("myInt").getFieldIndex());
      assertEquals(7, type.getPdxField("myFloat").getFieldIndex());

      assertEquals(FieldType.BOOLEAN, type.getPdxField("myFlag").getFieldType());
      assertEquals(FieldType.SHORT, type.getPdxField("myShort").getFieldType());
      assertEquals(FieldType.STRING, type.getPdxField("myString1").getFieldType());
      assertEquals(FieldType.LONG, type.getPdxField("myLong").getFieldType());
      assertEquals(FieldType.STRING, type.getPdxField("myString2").getFieldType());
      assertEquals(FieldType.STRING, type.getPdxField("myString3").getFieldType());
      assertEquals(FieldType.INT, type.getPdxField("myInt").getFieldType());
      assertEquals(FieldType.FLOAT, type.getPdxField("myFloat").getFieldType());

      assertEquals("myFlag", type.getPdxField("myFlag").getFieldName());
      assertEquals("myShort", type.getPdxField("myShort").getFieldName());
      assertEquals("myString1", type.getPdxField("myString1").getFieldName());
      assertEquals("myLong", type.getPdxField("myLong").getFieldName());
      assertEquals("myString2", type.getPdxField("myString2").getFieldName());
      assertEquals("myString3", type.getPdxField("myString3").getFieldName());
      assertEquals("myInt", type.getPdxField("myInt").getFieldName());
      assertEquals("myFloat", type.getPdxField("myFloat").getFieldName());

      assertEquals(0, type.getPdxField("myFlag").getVarLenFieldSeqId());
      assertEquals(0, type.getPdxField("myShort").getVarLenFieldSeqId());
      assertEquals(0, type.getPdxField("myString1").getVarLenFieldSeqId());
      assertEquals(0, type.getPdxField("myLong").getVarLenFieldSeqId());
      assertEquals(1, type.getPdxField("myString2").getVarLenFieldSeqId());
      assertEquals(2, type.getPdxField("myString3").getVarLenFieldSeqId());
      assertEquals(2, type.getPdxField("myInt").getVarLenFieldSeqId());
      assertEquals(2, type.getPdxField("myFloat").getVarLenFieldSeqId());

      assertEquals(false, type.getPdxField("myFlag").isVariableLengthType());
      assertEquals(false, type.getPdxField("myShort").isVariableLengthType());
      assertEquals(true, type.getPdxField("myString1").isVariableLengthType());
      assertEquals(false, type.getPdxField("myLong").isVariableLengthType());
      assertEquals(true, type.getPdxField("myString2").isVariableLengthType());
      assertEquals(true, type.getPdxField("myString3").isVariableLengthType());
      assertEquals(false, type.getPdxField("myInt").isVariableLengthType());
      assertEquals(false, type.getPdxField("myFloat").isVariableLengthType());

      assertEquals(ByteSourceFactory.wrap(new byte[] {(byte) (pdx.isMyFlag() ? 1 : 0)}),
          reader.getRaw(0));
      assertEquals(
          ByteSourceFactory
              .wrap(new byte[] {(byte) (pdx.getMyShort() >> 8), (byte) pdx.getMyShort()}),
          reader.getRaw(1));
      assertEquals(ByteSourceFactory.wrap(str1Bytes), reader.getRaw(2));
      assertEquals(ByteSourceFactory.wrap(new byte[] {(byte) (pdx.getMyLong() >> 56),
          (byte) (pdx.getMyLong() >> 48), (byte) (pdx.getMyLong() >> 40),
          (byte) (pdx.getMyLong() >> 32), (byte) (pdx.getMyLong() >> 24),
          (byte) (pdx.getMyLong() >> 16), (byte) (pdx.getMyLong() >> 8), (byte) pdx.getMyLong(),}),
          reader.getRaw(3));
      assertEquals(ByteSourceFactory.wrap(str2Bytes), reader.getRaw(4));
      assertEquals(ByteSourceFactory.wrap(str3Bytes), reader.getRaw(5));
      assertEquals(
          ByteSourceFactory.wrap(new byte[] {(byte) (pdx.getMyInt() >> 24),
              (byte) (pdx.getMyInt() >> 16), (byte) (pdx.getMyInt() >> 8), (byte) pdx.getMyInt()}),
          reader.getRaw(6));
      assertEquals(ByteSourceFactory.wrap(new byte[] {(byte) (floatBytes >> 24),
          (byte) (floatBytes >> 16), (byte) (floatBytes >> 8), (byte) floatBytes}),
          reader.getRaw(7));
    } finally {
      cache.setReadSerializedForTest(false);
    }
  }


  @Test
  public void testByteFormatForLongStrings() throws Exception {
    boolean myFlag = true;
    short myShort = 25;
    String myString1 =
        "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1."
            + "A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.A very long string1.";
    long myLong = 15654;
    String myString2 = "Class4_myString2";
    String myString3 =
        "Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. "
            + "Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. "
            + "Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. "
            + "Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. "
            + "Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. "
            + "Even longer string3. Even longer string3. Even longer string3. Even longer string3. Even longer string3. ";
    int myInt = 1420;
    float myFloat = 123.023f;

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    SimpleClass1 pdx =
        new SimpleClass1(myFlag, myShort, myString1, myLong, myString2, myString3, myInt, myFloat);
    DataSerializer.writeObject(pdx, out);
    int typeId = getPdxTypeIdForClass(SimpleClass1.class);

    HeapDataOutputStream hdos1 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString1, hdos1);
    byte[] str1Bytes = hdos1.toByteArray();
    HeapDataOutputStream hdos2 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString2, hdos2);
    byte[] str2Bytes = hdos2.toByteArray();
    HeapDataOutputStream hdos3 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString3, hdos3);
    byte[] str3Bytes = hdos3.toByteArray();

    int length = 1 /* myFlag */ + 2 /* myShort */
        + 8 /* myLong */ + 4 /* myInt */ + 4 /* myFloat */ + str1Bytes.length + str2Bytes.length
        + str3Bytes.length + (2 * 2) /* short offset for 3 strings */;

    int offset1 = 1 + 2;
    int offset2 = offset1 + 8 + str1Bytes.length;
    int offset3 = offset1 + 8 + str1Bytes.length + str2Bytes.length;
    byte[] actual = out.toByteArray();
    int floatBytes = Float.floatToRawIntBits(myFloat);
    Byte[] expected = new Byte[] {DSCODE.PDX.toByte(), // byte
        (byte) (length >> 24), (byte) (length >> 16), (byte) (length >> 8), (byte) length, // int -
                                                                                           // length
                                                                                           // of
                                                                                           // byte
                                                                                           // stream
        (byte) (typeId >> 24), (byte) (typeId >> 16), (byte) (typeId >> 8), (byte) typeId, // int -
                                                                                           // typeId
        1, // boolean - myFlag = true
        (byte) (myShort >> 8), (byte) myShort, // short - myShort
        (byte) (myLong >> 56), (byte) (myLong >> 48), (byte) (myLong >> 40), (byte) (myLong >> 32),
        (byte) (myLong >> 24), (byte) (myLong >> 16), (byte) (myLong >> 8), (byte) myLong, // long -
                                                                                           // myLong
        (byte) (myInt >> 24), (byte) (myInt >> 16), (byte) (myInt >> 8), (byte) myInt, // int -
                                                                                       // myInt
        (byte) (floatBytes >> 24), (byte) (floatBytes >> 16), (byte) (floatBytes >> 8),
        (byte) floatBytes, // float - myFloat
        (byte) (offset3 >> 8), (byte) offset3, // offset of myString3
        (byte) (offset2 >> 8), (byte) offset2, // offset of myString2
    };

    for (int i = (str1Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset1 + PdxWriterImpl.HEADER_SIZE, str1Bytes[i]); // +
                                                                                                   // 5
                                                                                                   // for:
                                                                                                   // 1
                                                                                                   // for
                                                                                                   // DSCODE.PDX.toByte()
                                                                                                   // and
                                                                                                   // 4
                                                                                                   // for
                                                                                                   // byte
                                                                                                   // stream
                                                                                                   // length
    }
    for (int i = (str2Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset2 + PdxWriterImpl.HEADER_SIZE, str2Bytes[i]);
    }
    for (int i = (str3Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset3 + PdxWriterImpl.HEADER_SIZE, str3Bytes[i]);
    }

    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    if (actual.length != expected.length) {
      System.out.println(msg);
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
    System.out.println("\n");

    DataInput in = new DataInputStream(new ByteArrayInputStream(actual));
    SimpleClass1 actualVal = DataSerializer.readObject(in);
    // System.out.println("actualVal..."+actualVal);
    assertTrue(
        "Mismatch in write and read value: Value Write..." + pdx + " Value Read..." + actualVal,
        pdx.equals(actualVal));
    cache.setReadSerializedForTest(true);
    try {
      in = new DataInputStream(new ByteArrayInputStream(actual));
      PdxInstance pi = DataSerializer.readObject(in);
      actualVal = (SimpleClass1) pi.getObject();
      assertTrue(
          "Mismatch in write and read value: Value Write..." + pdx + " Value Read..." + actualVal,
          pdx.equals(actualVal));
      assertTrue(pi.hasField("myFlag"));
      assertTrue(pi.hasField("myShort"));
      assertTrue(pi.hasField("myString1"));
      assertTrue(pi.hasField("myLong"));
      assertTrue(pi.hasField("myString2"));
      assertTrue(pi.hasField("myString3"));
      assertTrue(pi.hasField("myInt"));
      assertTrue(pi.hasField("myFloat"));
      assertEquals(pdx.isMyFlag(), pi.getField("myFlag"));
      assertEquals(pdx.getMyShort(), pi.getField("myShort"));
      assertEquals(pdx.getMyString1(), pi.getField("myString1"));
      assertEquals(pdx.getMyLong(), pi.getField("myLong"));
      assertEquals(pdx.getMyString2(), pi.getField("myString2"));
      assertEquals(pdx.getMyString3(), pi.getField("myString3"));
      assertEquals(pdx.getMyInt(), pi.getField("myInt"));
      assertEquals(pdx.getMyFloat(), pi.getField("myFloat"));
      PdxReaderImpl reader = (PdxReaderImpl) pi;
      PdxType type = reader.getPdxType();
      assertEquals(SimpleClass1.class.getName(), type.getClassName());
      assertEquals(8, type.getFieldCount());
      assertEquals(2, type.getVariableLengthFieldCount());

      assertEquals(0, type.getPdxField("myFlag").getFieldIndex());
      assertEquals(1, type.getPdxField("myShort").getFieldIndex());
      assertEquals(2, type.getPdxField("myString1").getFieldIndex());
      assertEquals(3, type.getPdxField("myLong").getFieldIndex());
      assertEquals(4, type.getPdxField("myString2").getFieldIndex());
      assertEquals(5, type.getPdxField("myString3").getFieldIndex());
      assertEquals(6, type.getPdxField("myInt").getFieldIndex());
      assertEquals(7, type.getPdxField("myFloat").getFieldIndex());

      assertEquals(FieldType.BOOLEAN, type.getPdxField("myFlag").getFieldType());
      assertEquals(FieldType.SHORT, type.getPdxField("myShort").getFieldType());
      assertEquals(FieldType.STRING, type.getPdxField("myString1").getFieldType());
      assertEquals(FieldType.LONG, type.getPdxField("myLong").getFieldType());
      assertEquals(FieldType.STRING, type.getPdxField("myString2").getFieldType());
      assertEquals(FieldType.STRING, type.getPdxField("myString3").getFieldType());
      assertEquals(FieldType.INT, type.getPdxField("myInt").getFieldType());
      assertEquals(FieldType.FLOAT, type.getPdxField("myFloat").getFieldType());

      assertEquals("myFlag", type.getPdxField("myFlag").getFieldName());
      assertEquals("myShort", type.getPdxField("myShort").getFieldName());
      assertEquals("myString1", type.getPdxField("myString1").getFieldName());
      assertEquals("myLong", type.getPdxField("myLong").getFieldName());
      assertEquals("myString2", type.getPdxField("myString2").getFieldName());
      assertEquals("myString3", type.getPdxField("myString3").getFieldName());
      assertEquals("myInt", type.getPdxField("myInt").getFieldName());
      assertEquals("myFloat", type.getPdxField("myFloat").getFieldName());

      assertEquals(0, type.getPdxField("myFlag").getVarLenFieldSeqId());
      assertEquals(0, type.getPdxField("myShort").getVarLenFieldSeqId());
      assertEquals(0, type.getPdxField("myString1").getVarLenFieldSeqId());
      assertEquals(0, type.getPdxField("myLong").getVarLenFieldSeqId());
      assertEquals(1, type.getPdxField("myString2").getVarLenFieldSeqId());
      assertEquals(2, type.getPdxField("myString3").getVarLenFieldSeqId());
      assertEquals(2, type.getPdxField("myInt").getVarLenFieldSeqId());
      assertEquals(2, type.getPdxField("myFloat").getVarLenFieldSeqId());

      assertEquals(false, type.getPdxField("myFlag").isVariableLengthType());
      assertEquals(false, type.getPdxField("myShort").isVariableLengthType());
      assertEquals(true, type.getPdxField("myString1").isVariableLengthType());
      assertEquals(false, type.getPdxField("myLong").isVariableLengthType());
      assertEquals(true, type.getPdxField("myString2").isVariableLengthType());
      assertEquals(true, type.getPdxField("myString3").isVariableLengthType());
      assertEquals(false, type.getPdxField("myInt").isVariableLengthType());
      assertEquals(false, type.getPdxField("myFloat").isVariableLengthType());

      assertEquals(ByteSourceFactory.wrap(new byte[] {(byte) (pdx.isMyFlag() ? 1 : 0)}),
          reader.getRaw(0));
      assertEquals(
          ByteSourceFactory
              .wrap(new byte[] {(byte) (pdx.getMyShort() >> 8), (byte) pdx.getMyShort()}),
          reader.getRaw(1));
      assertEquals(ByteSourceFactory.wrap(str1Bytes), reader.getRaw(2));
      assertEquals(ByteSourceFactory.wrap(new byte[] {(byte) (pdx.getMyLong() >> 56),
          (byte) (pdx.getMyLong() >> 48), (byte) (pdx.getMyLong() >> 40),
          (byte) (pdx.getMyLong() >> 32), (byte) (pdx.getMyLong() >> 24),
          (byte) (pdx.getMyLong() >> 16), (byte) (pdx.getMyLong() >> 8), (byte) pdx.getMyLong(),}),
          reader.getRaw(3));
      assertEquals(ByteSourceFactory.wrap(str2Bytes), reader.getRaw(4));
      assertEquals(ByteSourceFactory.wrap(str3Bytes), reader.getRaw(5));
      assertEquals(
          ByteSourceFactory.wrap(new byte[] {(byte) (pdx.getMyInt() >> 24),
              (byte) (pdx.getMyInt() >> 16), (byte) (pdx.getMyInt() >> 8), (byte) pdx.getMyInt()}),
          reader.getRaw(6));
      assertEquals(ByteSourceFactory.wrap(new byte[] {(byte) (floatBytes >> 24),
          (byte) (floatBytes >> 16), (byte) (floatBytes >> 8), (byte) floatBytes}),
          reader.getRaw(7));
    } finally {
      cache.setReadSerializedForTest(false);
    }
  }


  @Test
  public void testByteFormatForNestedPDX() throws Exception {
    String myString1 = "ComplexClass1_myString1";
    long myLong = 15654;
    HashMap<String, PdxSerializable> myHashMap = new HashMap<>();
    String myString2 = "ComplexClass1_myString2";
    float myFloat = 123.023f;

    for (int i = 0; i < 5; i++) {
      myHashMap.put("KEY_" + i, new SimpleClass(i, (byte) 5));
    }

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    PdxSerializable pdx = new NestedPdx(myString1, myLong, myHashMap, myString2, myFloat);
    DataSerializer.writeObject(pdx, out);
    int typeId = getPdxTypeIdForClass(NestedPdx.class);

    HeapDataOutputStream hdos1 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString1, hdos1);
    byte[] str1Bytes = hdos1.toByteArray();
    HeapDataOutputStream hdosForMap = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(myHashMap, hdosForMap);
    byte[] mapBytes = hdosForMap.toByteArray();
    HeapDataOutputStream hdos2 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString2, hdos2);
    byte[] str2Bytes = hdos2.toByteArray();

    int length = str1Bytes.length + 8 /* myLong */
        + mapBytes.length + str2Bytes.length + 4 /* myFloat */
        + (2 * 1) /*
                   * offset for 3 var-length fields
                   */;

    int offset1 = 0;
    int offset2 = offset1 + 8 + str1Bytes.length;
    int offset3 = offset1 + 8 + str1Bytes.length + mapBytes.length;
    byte[] actual = out.toByteArray();
    int floatBytes = Float.floatToRawIntBits(myFloat);
    Byte[] expected = new Byte[] {DSCODE.PDX.toByte(), // byte
        (byte) (length >> 24), (byte) (length >> 16), (byte) (length >> 8), (byte) length, // int -
                                                                                           // length
                                                                                           // of
                                                                                           // byte
                                                                                           // stream
        (byte) (typeId >> 24), (byte) (typeId >> 16), (byte) (typeId >> 8), (byte) typeId, // int -
                                                                                           // typeId
        (byte) (myLong >> 56), (byte) (myLong >> 48), (byte) (myLong >> 40), (byte) (myLong >> 32),
        (byte) (myLong >> 24), (byte) (myLong >> 16), (byte) (myLong >> 8), (byte) myLong, // long -
                                                                                           // myLong
        (byte) (floatBytes >> 24), (byte) (floatBytes >> 16), (byte) (floatBytes >> 8),
        (byte) floatBytes, // float - myFloat
        (byte) offset3, // offset of myString2
        (byte) offset2, // offset of myHashMap
    };

    for (int i = (str1Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset1 + PdxWriterImpl.HEADER_SIZE, str1Bytes[i]);
    }
    for (int i = (mapBytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset2 + PdxWriterImpl.HEADER_SIZE, mapBytes[i]);
    }
    for (int i = (str2Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset3 + PdxWriterImpl.HEADER_SIZE, str2Bytes[i]);
    }

    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    if (actual.length != expected.length) {
      System.out.println(msg);
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
    System.out.println("\n");
    DataInput in = new DataInputStream(new ByteArrayInputStream(actual));
    NestedPdx actualVal = DataSerializer.readObject(in);
    // System.out.println("actualVal..."+actualVal);
    assertTrue(
        "Mismatch in write and read value: Value Write..." + pdx + " Value Read..." + actualVal,
        pdx.equals(actualVal));
    System.out.println("\n");
  }

  @Test
  public void testByteFormatForDSInsidePDX() throws Exception {
    String myString1 = "ComplexClass1_myString1";
    long myLong = 15654;
    DataSerializable myDS = new DeltaTestImpl(100, "value");
    String myString2 = "ComplexClass1_myString2";
    float myFloat = 123.023f;

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    PdxSerializable pdx = new DSInsidePdx(myString1, myLong, myDS, myString2, myFloat);
    DataSerializer.writeObject(pdx, out);
    int typeId = getPdxTypeIdForClass(DSInsidePdx.class);

    HeapDataOutputStream hdos1 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString1, hdos1);
    byte[] str1Bytes = hdos1.toByteArray();
    System.out.println("Length of string1: " + str1Bytes.length);

    HeapDataOutputStream hdos2 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(myDS, hdos2);
    byte[] dsBytes = hdos2.toByteArray();
    System.out.println("Length of DS: " + dsBytes.length);

    HeapDataOutputStream hdos3 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString2, hdos3);
    byte[] str2Bytes = hdos3.toByteArray();
    System.out.println("Length of string2: " + str2Bytes.length);

    int length = str1Bytes.length + 8 /* myLong */
        + dsBytes.length + str2Bytes.length + 4 /* myFloat */
        + (2 * 2) /*
                   * offset for 3 car-length fields
                   */;

    int offset1 = 0;
    int offset2 = offset1 + 8 + str1Bytes.length;
    int offset3 = offset1 + 8 + str1Bytes.length + dsBytes.length;
    byte[] actual = out.toByteArray();
    int floatBytes = Float.floatToRawIntBits(myFloat);
    Byte[] expected = new Byte[] {DSCODE.PDX.toByte(), // byte
        (byte) (length >> 24), (byte) (length >> 16), (byte) (length >> 8), (byte) length, // int -
                                                                                           // length
                                                                                           // of
                                                                                           // byte
                                                                                           // stream
        (byte) (typeId >> 24), (byte) (typeId >> 16), (byte) (typeId >> 8), (byte) typeId, // int -
                                                                                           // typeId
        (byte) (myLong >> 56), (byte) (myLong >> 48), (byte) (myLong >> 40), (byte) (myLong >> 32),
        (byte) (myLong >> 24), (byte) (myLong >> 16), (byte) (myLong >> 8), (byte) myLong, // long -
                                                                                           // myLong
        (byte) (floatBytes >> 24), (byte) (floatBytes >> 16), (byte) (floatBytes >> 8),
        (byte) floatBytes, // float - myFloat
        (byte) (offset3 >> 8), (byte) offset3, // offset of myString2
        (byte) (offset2 >> 8), (byte) offset2, // offset of myHashMap
    };

    for (int i = (str1Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset1 + PdxWriterImpl.HEADER_SIZE, str1Bytes[i]); // +
                                                                                                   // 5
                                                                                                   // for:
                                                                                                   // 1
                                                                                                   // for
                                                                                                   // DSCODE.PDX.toByte()
                                                                                                   // and
                                                                                                   // 4
                                                                                                   // for
                                                                                                   // byte
                                                                                                   // stream
                                                                                                   // length
    }
    for (int i = (dsBytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset2 + PdxWriterImpl.HEADER_SIZE, dsBytes[i]);
    }
    for (int i = (str2Bytes.length - 1); i >= 0; i--) {
      expected =
          (Byte[]) ArrayUtils.insert(expected, offset3 + PdxWriterImpl.HEADER_SIZE, str2Bytes[i]);
    }

    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
    System.out.println("\n");

    DataInput in = new DataInputStream(new ByteArrayInputStream(actual));
    DSInsidePdx actualVal = DataSerializer.readObject(in);
    // System.out.println("actualVal..."+actualVal);
    assertTrue(
        "Mismatch in write and read value: Value Write..." + pdx + " Value Read..." + actualVal,
        pdx.equals(actualVal));
    System.out.println("\n");
  }

  @Test
  public void testByteFormatForPDXInsideDS() throws Exception {
    String myString1 = "ComplexClass5_myString1";
    long myLong = 15654;
    String myString2 = "ComplexClass5_myString2";
    float myFloat = 123.023f;

    HashMap<String, PdxSerializable> myHashMap = new HashMap<>();
    for (int i = 0; i < 2; i++) {
      myHashMap.put("KEY_" + i, new SimpleClass(i, (byte) i));
    }
    PdxSerializable myPDX = new NestedPdx(myString1, myLong, myHashMap, myString2, myFloat);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializable ds = new PdxInsideDS(myString1, myLong, myPDX, myString2);
    DataSerializer.writeObject(ds, out);

    HeapDataOutputStream hdosString1 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString1, hdosString1);
    byte[] str1Bytes = hdosString1.toByteArray();
    System.out.println("Length of string1: " + str1Bytes.length);

    HeapDataOutputStream hdosMyPDX = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(myPDX, hdosMyPDX);
    byte[] pdxBytes = hdosMyPDX.toByteArray();
    System.out.println("Length of myPDX: " + pdxBytes.length);

    HeapDataOutputStream hdosString2 = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeString(myString2, hdosString2);
    byte[] str2Bytes = hdosString2.toByteArray();
    System.out.println("Length of string2: " + str2Bytes.length);

    Class classInstance = ds.getClass();
    HeapDataOutputStream hdosClass = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeClass(classInstance, hdosClass);
    byte[] dsInitBytes = hdosClass.toByteArray();

    int offsetStr1 = 1 + dsInitBytes.length;
    int offsetPDX = 1 + dsInitBytes.length + str1Bytes.length + 8;
    int offsetStr2 = 1 + dsInitBytes.length + str1Bytes.length + 8 + pdxBytes.length;

    byte[] actual = out.toByteArray();
    int floatBytes = Float.floatToRawIntBits(myFloat);
    Byte[] expected = new Byte[] {DSCODE.DATA_SERIALIZABLE.toByte(), // byte
        (byte) (myLong >> 56), (byte) (myLong >> 48), (byte) (myLong >> 40), (byte) (myLong >> 32),
        (byte) (myLong >> 24), (byte) (myLong >> 16), (byte) (myLong >> 8), (byte) myLong, // long -
                                                                                           // myLong
    };

    for (int i = (dsInitBytes.length - 1); i >= 0; i--) {
      expected = (Byte[]) ArrayUtils.insert(expected, 1, dsInitBytes[i]);
    }
    for (int i = (str1Bytes.length - 1); i >= 0; i--) {
      expected = (Byte[]) ArrayUtils.insert(expected, offsetStr1, str1Bytes[i]);
    }
    for (int i = (pdxBytes.length - 1); i >= 0; i--) {
      expected = (Byte[]) ArrayUtils.insert(expected, offsetPDX, pdxBytes[i]);
    }
    for (int i = (str2Bytes.length - 1); i >= 0; i--) {
      expected = (Byte[]) ArrayUtils.insert(expected, offsetStr2, str2Bytes[i]);
    }

    checkBytes(expected, actual);

    DataInput in = new DataInputStream(new ByteArrayInputStream(actual));
    PdxInsideDS actualVal = DataSerializer.readObject(in);
    // System.out.println("actualVal..."+actualVal);
    assertTrue(
        "Mismatch in write and read value: Value Write..." + ds + " Value Read..." + actualVal,
        ds.equals(actualVal));
  }

  private void checkBytes(Byte[] expected, byte[] actual) {
    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    if (actual.length != expected.length) {
      System.out.println(msg);
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
  }

  private void checkBytes(byte[] expected, byte[] actual) {
    StringBuilder msg = new StringBuilder("Actual output: ");
    for (byte val : actual) {
      msg.append(val + ", ");
    }
    msg.append("\nExpected output: ");
    for (byte val : expected) {
      msg.append(val + ", ");
    }
    if (actual.length != expected.length) {
      System.out.println(msg);
    }
    assertTrue("Mismatch in length, actual.length: " + actual.length + " and expected length: "
        + expected.length, actual.length == expected.length);
    for (int i = 0; i < actual.length; i++) {
      if (actual[i] != expected[i]) {
        System.out.println(msg);
      }
      assertTrue("Mismatch at index " + i, actual[i] == expected[i]);
    }
  }

  @Test
  public void testLong() throws IOException, ClassNotFoundException {
    LongHolder v = (LongHolder) serializeAndDeserialize(new LongHolder(0x69));
    assertEquals(0x69, v.getLong());
  }

  // this method adds coverage for bug 43236
  @Test
  public void testObjectPdxInstance() throws IOException, ClassNotFoundException {
    Boolean previousPdxReadSerializedFlag = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    PdxReaderImpl.TESTHOOK_TRACKREADS = true;
    try {
      PdxInstance pi = (PdxInstance) serializeAndDeserialize(new ObjectHolder("hello"));
      ObjectHolder v3 = (ObjectHolder) pi.getObject();
      WritablePdxInstance wpi = pi.createWriter();
      wpi.setField("f1", "goodbye");
      assertEquals("goodbye", wpi.getField("f1"));
      ObjectHolder v = (ObjectHolder) wpi.getObject();
      ObjectHolder v2 = (ObjectHolder) wpi.getObject();
      assertEquals("goodbye", v.getObject());
      assertEquals("goodbye", v2.getObject());
      assertEquals("hello", v3.getObject());
      assertEquals("goodbye", wpi.getField("f1"));
    } finally {
      cache.setPdxReadSerializedOverride(previousPdxReadSerializedFlag);
      PdxReaderImpl.TESTHOOK_TRACKREADS = false;
    }
  }

  @Test
  public void testObjectArrayPdxInstance() throws IOException, ClassNotFoundException {
    Boolean previousPdxReadSerializedFlag = cache.getPdxReadSerializedOverride();
    cache.setPdxReadSerializedOverride(true);
    PdxReaderImpl.TESTHOOK_TRACKREADS = true;
    try {
      LongFieldHolder[] v = new LongFieldHolder[] {new LongFieldHolder(1), new LongFieldHolder(2)};
      PdxInstance pi = (PdxInstance) serializeAndDeserialize(new ObjectArrayHolder(v));
      ObjectArrayHolder oah = (ObjectArrayHolder) pi.getObject();
      LongFieldHolder[] nv = (LongFieldHolder[]) oah.getObjectArray();
      if (!Arrays.equals(v, nv)) {
        throw new RuntimeException(
            "expected " + Arrays.toString(v) + " but had " + Arrays.toString(nv));
      }
      Object[] oa = (Object[]) pi.getField("f1");
      assertTrue(oa[0] instanceof PdxInstance);
      assertTrue(oa[1] instanceof PdxInstance);
      LongFieldHolder[] nv2 = new LongFieldHolder[2];
      nv2[0] = (LongFieldHolder) ((PdxInstance) oa[0]).getObject();
      nv2[1] = (LongFieldHolder) ((PdxInstance) oa[1]).getObject();
      if (!Arrays.equals(v, nv2)) {
        throw new RuntimeException(
            "expected " + Arrays.toString(v) + " but had " + Arrays.toString(nv2));
      }
    } finally {
      cache.setPdxReadSerializedOverride(previousPdxReadSerializedFlag);
      PdxReaderImpl.TESTHOOK_TRACKREADS = false;
    }
  }

  @Test
  public void testLongField() throws IOException, ClassNotFoundException {
    LongFieldHolder v =
        (LongFieldHolder) serializeAndDeserialize(new LongFieldHolder(0x1020304050607080L));
    assertEquals(0x1020304050607080L, v.getLong());
  }

  @Test
  public void testAll() throws IOException, ClassNotFoundException {
    AllFieldTypes v1 = new AllFieldTypes(0x1020304050607080L, false);
    AllFieldTypes v2 = (AllFieldTypes) serializeAndDeserialize(v1);
    assertEquals(v1, v2);
  }

  @Test
  public void testBasicAll() throws IOException, ClassNotFoundException {
    BasicAllFieldTypes v1 = new BasicAllFieldTypes(0x1020304050607080L, false);
    try {
      serializeAndDeserialize(v1);
      throw new RuntimeException("expected NotSerializableException");
    } catch (NotSerializableException expected) {

    }
    cache.setPdxSerializer(new BasicAllFieldTypesPdxSerializer());
    try {
      BasicAllFieldTypes v2 = (BasicAllFieldTypes) serializeAndDeserialize(v1);
      assertEquals(v1, v2);
    } finally {
      cache.setPdxSerializer(null);
    }
  }

  @Test
  public void testPdxSerializerFalse() throws IOException, ClassNotFoundException {
    cache.setPdxSerializer(new BasicAllFieldTypesPdxSerializer());
    try {
      POS v1 = new POS(3);
      POS v2 = (POS) serializeAndDeserialize(v1);
      assertEquals(v1, v2);
    } finally {
      cache.setPdxSerializer(null);
    }
  }

  @Test
  public void testAllWithNulls() throws IOException, ClassNotFoundException {
    AllFieldTypes v1 = new AllFieldTypes(0x102030405060708L, true);
    AllFieldTypes v2 = (AllFieldTypes) serializeAndDeserialize(v1);
    assertEquals(v1, v2);
  }

  @Test
  public void testBasicAllWithNulls() throws IOException, ClassNotFoundException {
    BasicAllFieldTypes v1 = new BasicAllFieldTypes(0x1020304050607080L, true);
    try {
      serializeAndDeserialize(v1);
      throw new RuntimeException("expected NotSerializableException");
    } catch (NotSerializableException expected) {

    }
    cache.setPdxSerializer(new BasicAllFieldTypesPdxSerializer());
    try {
      BasicAllFieldTypes v2 = (BasicAllFieldTypes) serializeAndDeserialize(v1);
      assertEquals(v1, v2);
    } finally {
      cache.setPdxSerializer(null);
    }
  }

  @Test
  public void testAllRF() throws IOException, ClassNotFoundException {
    AllFieldTypesRF v1 = new AllFieldTypesRF(0x1020304050607080L, false);
    AllFieldTypesRF v2 = (AllFieldTypesRF) serializeAndDeserialize(v1);
    assertEquals(v1, v2);
  }

  @Test
  public void testAllWithNullsRF() throws IOException, ClassNotFoundException {
    AllFieldTypesRF v1 = new AllFieldTypesRF(0x102030405060708L, true);
    AllFieldTypesRF v2 = (AllFieldTypesRF) serializeAndDeserialize(v1);
    assertEquals(v1, v2);
  }

  @Test
  public void testAllWF() throws IOException, ClassNotFoundException {
    AllFieldTypesWF v1 = new AllFieldTypesWF(0x1020304050607080L, false);
    AllFieldTypesWF v2 = (AllFieldTypesWF) serializeAndDeserialize(v1);
    assertEquals(v1, v2);
  }

  @Test
  public void testAllWithNullsWF() throws IOException, ClassNotFoundException {
    AllFieldTypesWF v1 = new AllFieldTypesWF(0x102030405060708L, true);
    AllFieldTypesWF v2 = (AllFieldTypesWF) serializeAndDeserialize(v1);
    assertEquals(v1, v2);
  }

  private Object serializeAndDeserialize(Object in) throws IOException, ClassNotFoundException {
    HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(in, hdos);
    byte[] bytes = hdos.toByteArray();
    System.out.println("Serialized bytes = " + Arrays.toString(bytes));
    return DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(bytes)));
  }

  public static class LongHolder implements PdxSerializable {
    private long v;

    public LongHolder(long v) {
      this.v = v;
    }

    public LongHolder() {

    }

    public long getLong() {
      return v;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeLong("f1", v);
    }

    @Override
    public void fromData(PdxReader reader) {
      v = reader.readLong("f1");
    }
  }
  public static class ObjectHolder implements PdxSerializable {
    private Object v;

    public ObjectHolder(Object v) {
      this.v = v;
    }

    public ObjectHolder() {

    }

    public Object getObject() {
      return v;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeObject("f1", v);
    }

    @Override
    public void fromData(PdxReader reader) {
      v = reader.readObject("f1");
    }
  }
  public static class ObjectArrayHolder implements PdxSerializable {
    private Object[] v;

    public ObjectArrayHolder(Object[] v) {
      this.v = v;
    }

    public ObjectArrayHolder() {

    }

    public Object[] getObjectArray() {
      return v;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeObjectArray("f1", v);
    }

    @Override
    public void fromData(PdxReader reader) {
      v = reader.readObjectArray("f1");
    }
  }

  public static class LongFieldHolder implements PdxSerializable {
    private long v;

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + (int) (v ^ (v >>> 32));
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
      LongFieldHolder other = (LongFieldHolder) obj;
      return v == other.v;
    }

    public LongFieldHolder(long v) {
      this.v = v;
    }

    public LongFieldHolder() {

    }

    public long getLong() {
      return v;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeLong("f1", v);
    }

    @Override
    public void fromData(PdxReader reader) {
      v = (Long) reader.readField("f1");
    }
  }

  public enum Day {
    SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY
  }

  public static class BasicAllFieldTypes implements PdxSerializerObject {

    public static String FILE_NAME = "testFile.txt";

    // additional fields from tests.util.ValueHolder
    public char aChar;
    public boolean aBoolean;
    public byte aByte;
    public short aShort;
    public int anInt;
    public long aLong;
    public float aFloat;
    public double aDouble;
    public Date aDate;
    public String aString;
    public Object anObject;
    public Map aMap;
    public Collection aCollection;
    public boolean[] aBooleanArray;
    public char[] aCharArray;
    public byte[] aByteArray;
    public short[] aShortArray;
    public int[] anIntArray;
    public long[] aLongArray;
    public float[] aFloatArray;
    public double[] aDoubleArray;
    public String[] aStringArray;
    public Object[] anObjectArray;
    public byte[][] anArrayOfByteArray;

    public BasicAllFieldTypes() {}

    public BasicAllFieldTypes(long base, boolean nulls) {
      fillInBaseValues(base, nulls);
    }


    /**
     * Init fields using base as appropriate, but does not initialize aQueryObject field.
     *
     * @param base The base value for all fields. Fields are all set to a value using base as
     *        appropriate.
     * @param useNulls If true, fill in nulls for those fields that can be set to null (to test null
     *        values), if false, then use a valid value (to test non-null values).
     */
    public void fillInBaseValues(long base, boolean useNulls) {
      final int maxCollectionSize = 100;
      aChar = (char) base;
      aBoolean = true;
      aByte = (byte) base;
      aShort = (short) base;
      anInt = (int) base;
      aLong = base;
      aFloat = base;
      aDouble = base;
      aDate = new Date(base);
      aString = "" + base;
      if (useNulls) {
        aDate = null;
        aString = null;
        anObject = null;
        aMap = null;
        aCollection = null;
        // aRegion = null;
        aBooleanArray = null;
        aCharArray = null;
        aByteArray = null;
        aShortArray = null;
        anIntArray = null;
        aLongArray = null;
        aFloatArray = null;
        aDoubleArray = null;
        anObjectArray = null;
        anArrayOfByteArray = null;
      } else {
        int desiredCollectionSize = (int) Math.min(base, maxCollectionSize);
        anObject = new SimpleClass((int) base, (byte) base);
        aMap = new HashMap();
        aCollection = new ArrayList();
        aBooleanArray = new boolean[desiredCollectionSize];
        aCharArray = new char[desiredCollectionSize];
        aByteArray = new byte[desiredCollectionSize];
        aShortArray = new short[desiredCollectionSize];
        anIntArray = new int[desiredCollectionSize];
        aLongArray = new long[desiredCollectionSize];
        aFloatArray = new float[desiredCollectionSize];
        aDoubleArray = new double[desiredCollectionSize];
        aStringArray = new String[desiredCollectionSize];
        anObjectArray = new Object[desiredCollectionSize];
        anArrayOfByteArray = new byte[desiredCollectionSize][desiredCollectionSize];
        if (desiredCollectionSize > 0) {
          for (int i = 0; i < desiredCollectionSize - 1; i++) {
            aMap.put(i, i);
            aCollection.add(i);
            aBooleanArray[i] = (i % 2) == 0;
            aCharArray[i] = (char) (i);
            aByteArray[i] = (byte) (i);
            aShortArray[i] = (short) i;
            anIntArray[i] = i;
            aLongArray[i] = i;
            aFloatArray[i] = i;
            aDoubleArray[i] = i;
            aStringArray[i] = "" + i;
            anObjectArray[i] = anObject;
            anArrayOfByteArray[i] = aByteArray;
          }
          // now add the last value, using base if possible
          aMap.put(base, base);
          aCollection.add(base);
          aBooleanArray[desiredCollectionSize - 1] = true;
          aCharArray[desiredCollectionSize - 1] = (char) base;
          aByteArray[desiredCollectionSize - 1] = (byte) base;
          aShortArray[desiredCollectionSize - 1] = (short) base;
          anIntArray[desiredCollectionSize - 1] = (int) base;
          aLongArray[desiredCollectionSize - 1] = base;
          aFloatArray[desiredCollectionSize - 1] = base;
          aDoubleArray[desiredCollectionSize - 1] = base;
          aStringArray[desiredCollectionSize - 1] = "" + base;
          anObjectArray[desiredCollectionSize - 1] = anObject;
          // aRegion = CacheHelper.getCache().rootRegions().iterator().next();
        }
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return getClass().getName() + " [aChar=" + aChar + ", aBoolean=" + aBoolean
          + ", aByte=" + aByte + ", aShort=" + aShort + ", anInt=" + anInt
          + ", aLong=" + aLong + ", aFloat=" + aFloat + ", aDouble=" + aDouble
          + ", aDate=" + aDate + ", aString=" + aString + ", anObject=" + anObject
          + ", aMap=" + aMap + ", aCollection=" + aCollection + ", aBooleanArray="
          + Arrays.toString(aBooleanArray) + ", aCharArray=" + Arrays.toString(aCharArray)
          + ", aByteArray=" + Arrays.toString(aByteArray) + ", aShortArray="
          + Arrays.toString(aShortArray) + ", anIntArray=" + Arrays.toString(anIntArray)
          + ", aLongArray=" + Arrays.toString(aLongArray) + ", aFloatArray="
          + Arrays.toString(aFloatArray) + ", aDoubleArray="
          + Arrays.toString(aDoubleArray) + ", aStringArray="
          + Arrays.toString(aStringArray) + ", anObjectArray="
          + Arrays.toString(anObjectArray) + ", anArrayOfByteArray="
          + Arrays.toString(anArrayOfByteArray) + "]";
    }

    @Override
    public int hashCode() {
      return 37;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof BasicAllFieldTypes)) {
        return false;
      }
      BasicAllFieldTypes other = (BasicAllFieldTypes) o;
      if (aChar != other.aChar) {
        System.out.println("!= aChar");
        return false;
      }
      if (aByte != other.aByte) {
        System.out.println("!= aByte");
        return false;
      }
      if (aBoolean != other.aBoolean) {
        System.out.println("!= aBoolean");
        return false;
      }
      if (aShort != other.aShort) {
        System.out.println("!= aShort");
        return false;
      }
      if (anInt != other.anInt) {
        System.out.println("!= anInt");
        return false;
      }
      if (aLong != other.aLong) {
        System.out.println("!= aLong");
        return false;
      }
      if (aFloat != other.aFloat) {
        System.out.println("!= aFloat");
        return false;
      }
      if (aDouble != other.aDouble) {
        System.out.println("!= aDouble");
        return false;
      }
      if (!basicEquals(aDate, other.aDate)) {
        System.out.println("!= aDate");
        return false;
      }
      if (!basicEquals(aString, other.aString)) {
        System.out.println("!= aString");
        return false;
      }
      if (!basicEquals(anObject, other.anObject)) {
        System.out.println("!= nObject");
        return false;
      }
      if (!basicEquals(aMap, other.aMap)) {
        System.out.println("!= aMap");
        return false;
      }
      if (!basicEquals(aCollection, other.aCollection)) {
        System.out.println("!= aCollection");
        return false;
      }
      // if (!basicEquals(this.aRegion, other.aRegion)) {
      // return false;
      // }
      if (!Arrays.equals(aBooleanArray, other.aBooleanArray)) {
        System.out.println("!= boolean[]");
        return false;
      }
      if (!Arrays.equals(aCharArray, other.aCharArray)) {
        System.out.println("!= char[]");
        return false;
      }
      if (!Arrays.equals(aByteArray, other.aByteArray)) {
        System.out.println("!= byte[]");
        return false;
      }
      if (!Arrays.equals(aShortArray, other.aShortArray)) {
        System.out.println("!= short[]");
        return false;
      }
      if (!Arrays.equals(anIntArray, other.anIntArray)) {
        System.out.println("!= int[]");
        return false;
      }
      if (!Arrays.equals(aLongArray, other.aLongArray)) {
        System.out.println("!= long[]");
        return false;
      }
      if (!Arrays.equals(aFloatArray, other.aFloatArray)) {
        System.out.println("!= float[]");
        return false;
      }
      if (!Arrays.equals(aDoubleArray, other.aDoubleArray)) {
        System.out.println("!= double[]");
        return false;
      }
      if (!Arrays.equals(aStringArray, other.aStringArray)) {
        System.out.println("!= String[]");
        return false;
      }
      if (!Arrays.equals(anObjectArray, other.anObjectArray)) {
        System.out.println("!= Object[]");
        return false;
      }
      if (!byteArrayofArraysEqual(anArrayOfByteArray, other.anArrayOfByteArray)) {
        System.out.println("!= byte[][]");
        return false;
      }
      return true;
    }

    private boolean byteArrayofArraysEqual(byte[][] o1, byte[][] o2) {
      if (o1 == null) {
        return o2 == null;
      } else if (o2 == null) {
        return false;
      } else {
        if (o1.length != o2.length) {
          return false;
        }
        for (int i = 0; i < o1.length; i++) {
          if (!Arrays.equals(o1[i], o2[i])) {
            return false;
          }
        }
      }
      return true;
    }

    private boolean basicEquals(Object o1, Object o2) {
      if (o1 == null) {
        return o2 == null;
      } else if (o2 == null) {
        return false;
      } else {
        return o1.equals(o2);
      }
    }

  }

  @Test
  public void testVariableFields() throws Exception {
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(new VariableFields(1), out);
    try {
      DataSerializer.writeObject(new VariableFields(2), out);
      throw new RuntimeException("expected ToDataException");
    } catch (ToDataException expected) {
      if (!(expected.getCause() instanceof PdxSerializationException)) {
        throw new RuntimeException("expected cause to be PdxSerializationException");
      }
    }
    try {
      DataSerializer.writeObject(new VariableFields(0), out);
      throw new RuntimeException("expected PdxSerializationException");
    } catch (PdxSerializationException expected) {
    }
    try {
      DataSerializer.writeObject(new VariableFields(1, Object.class), out);
      throw new RuntimeException("expected ToDataException");
    } catch (ToDataException expected) {
      if (!(expected.getCause() instanceof PdxSerializationException)) {
        throw new RuntimeException("expected cause to be PdxSerializationException");
      }
    }
    // TODO also test marking different identity fields.
  }

  public static class VariableFields implements PdxSerializable {

    private Class fieldType = String.class;
    private final int fieldCount;

    public VariableFields(int fieldCount) {
      this.fieldCount = fieldCount;
    }

    public VariableFields(int fieldCount, Class fieldType) {
      this.fieldCount = fieldCount;
      this.fieldType = fieldType;
    }

    @Override
    public void toData(PdxWriter writer) {
      ((PdxWriterImpl) writer).setDoExtraValidation(true);
      writer.writeInt("fieldCount", fieldCount);
      for (int i = 0; i < fieldCount; i++) {
        writer.writeField("f" + i, null, fieldType);
      }
    }

    @Override
    public void fromData(PdxReader reader) {
      throw new IllegalStateException("should never be called");
    }

  }
  public static class AllFieldTypes extends BasicAllFieldTypes implements PdxSerializable {

    public AllFieldTypes() {
      super();
    }

    public AllFieldTypes(long base, boolean nulls) {
      super(base, nulls);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.pdx.PdxSerializable#toData(org.apache.geode. pdx.PdxWriter)
     */
    @Override
    public void toData(PdxWriter out) {
      new BasicAllFieldTypesPdxSerializer().toData(this, out);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.pdx.PdxSerializable#fromData(org.apache.geode .pdx.PdxReader)
     */
    @Override
    public void fromData(PdxReader in) {
      new BasicAllFieldTypesPdxSerializer().fillInData(in, this);
    }
  }

  public static class BasicAllFieldTypesPdxSerializer implements PdxSerializer {

    @Override
    public boolean toData(Object o, PdxWriter out) {
      if (o instanceof BasicAllFieldTypes) {
        BasicAllFieldTypes pdx = (BasicAllFieldTypes) o;
        // extra fields for this version
        out.writeChar("aChar", pdx.aChar);
        out.writeBoolean("aBoolean", pdx.aBoolean);
        out.writeByte("aByte", pdx.aByte);
        out.writeShort("aShort", pdx.aShort);
        out.writeInt("anInt", pdx.anInt);
        out.writeLong("aLong", pdx.aLong);
        out.writeFloat("aFloat", pdx.aFloat);
        out.writeDouble("aDouble", pdx.aDouble);
        out.writeDate("aDate", pdx.aDate);
        out.writeString("aString", pdx.aString);
        out.writeObject("anObject", pdx.anObject);
        out.writeObject("aMap", pdx.aMap);
        out.writeObject("aCollection", pdx.aCollection);
        out.writeBooleanArray("aBooleanArray", pdx.aBooleanArray);
        out.writeCharArray("aCharArray", pdx.aCharArray);
        out.writeByteArray("aByteArray", pdx.aByteArray);
        out.writeShortArray("aShortArray", pdx.aShortArray);
        out.writeIntArray("anIntArray", pdx.anIntArray);
        out.writeLongArray("aLongArray", pdx.aLongArray);
        out.writeFloatArray("aFloatArray", pdx.aFloatArray);
        out.writeDoubleArray("aDoubleArray", pdx.aDoubleArray);
        out.writeStringArray("aStringArray", pdx.aStringArray);
        out.writeObjectArray("anObjectArray", pdx.anObjectArray);
        out.writeArrayOfByteArrays("anArrayOfByteArray", pdx.anArrayOfByteArray);
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Object fromData(Class<?> clazz, PdxReader in) {
      if (BasicAllFieldTypes.class.isAssignableFrom(clazz)) {
        BasicAllFieldTypes pdx = new BasicAllFieldTypes();
        fillInData(in, pdx);
        return pdx;
      } else {
        return null;
      }
    }

    public void fillInData(PdxReader in, BasicAllFieldTypes pdx) {
      // extra fields for this version
      try {
        in.readBoolean("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readByte("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readShort("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readInt("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readLong("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readFloat("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readDouble("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readDate("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readString("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readObject("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readBooleanArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readCharArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readByteArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readShortArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readIntArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readLongArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readFloatArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readDoubleArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readStringArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readObjectArray("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readArrayOfByteArrays("aChar");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      try {
        in.readChar("aBoolean");
        throw new RuntimeException("expected PdxFieldTypeMismatchException");
      } catch (PdxFieldTypeMismatchException expected) {
        // expected
      }
      pdx.aChar = in.readChar("aChar");
      pdx.aBoolean = in.readBoolean("aBoolean");
      pdx.aByte = in.readByte("aByte");
      pdx.aShort = in.readShort("aShort");
      pdx.anInt = in.readInt("anInt");
      pdx.aLong = in.readLong("aLong");
      pdx.aFloat = in.readFloat("aFloat");
      pdx.aDouble = in.readDouble("aDouble");
      pdx.aDate = in.readDate("aDate");
      pdx.aString = in.readString("aString");
      pdx.anObject = in.readObject("anObject");
      pdx.aMap = (Map) in.readObject("aMap");
      pdx.aCollection = (Collection) in.readObject("aCollection");
      pdx.aBooleanArray = in.readBooleanArray("aBooleanArray");
      pdx.aCharArray = in.readCharArray("aCharArray");
      pdx.aByteArray = in.readByteArray("aByteArray");
      pdx.aShortArray = in.readShortArray("aShortArray");
      pdx.anIntArray = in.readIntArray("anIntArray");
      pdx.aLongArray = in.readLongArray("aLongArray");
      pdx.aFloatArray = in.readFloatArray("aFloatArray");
      pdx.aDoubleArray = in.readDoubleArray("aDoubleArray");
      pdx.aStringArray = in.readStringArray("aStringArray");
      pdx.anObjectArray = in.readObjectArray("anObjectArray");
      pdx.anArrayOfByteArray = in.readArrayOfByteArrays("anArrayOfByteArray");
    }

  }

  public static class AllFieldTypesRF extends AllFieldTypes {
    public AllFieldTypesRF(long l, boolean b) {
      super(l, b);
    }

    public AllFieldTypesRF() {
      super();
    }

    @Override
    public void fromData(PdxReader in) {
      aChar = (Character) in.readField("aChar");
      aBoolean = (Boolean) in.readField("aBoolean");
      aByte = (Byte) in.readField("aByte");
      aShort = (Short) in.readField("aShort");
      anInt = (Integer) in.readField("anInt");
      aLong = (Long) in.readField("aLong");
      aFloat = (Float) in.readField("aFloat");
      aDouble = (Double) in.readField("aDouble");
      aDate = (Date) in.readField("aDate");
      aString = (String) in.readField("aString");
      anObject = in.readField("anObject");
      aMap = (Map) in.readField("aMap");
      aCollection = (Collection) in.readField("aCollection");
      aBooleanArray = (boolean[]) in.readField("aBooleanArray");
      aCharArray = (char[]) in.readField("aCharArray");
      aByteArray = (byte[]) in.readField("aByteArray");
      aShortArray = (short[]) in.readField("aShortArray");
      anIntArray = (int[]) in.readField("anIntArray");
      aLongArray = (long[]) in.readField("aLongArray");
      aFloatArray = (float[]) in.readField("aFloatArray");
      aDoubleArray = (double[]) in.readField("aDoubleArray");
      aStringArray = (String[]) in.readField("aStringArray");
      anObjectArray = (Object[]) in.readField("anObjectArray");
      anArrayOfByteArray = (byte[][]) in.readField("anArrayOfByteArray");
    }

  }

  public static class AllFieldTypesWF extends AllFieldTypes {
    public AllFieldTypesWF(long l, boolean b) {
      super(l, b);
    }

    public AllFieldTypesWF() {
      super();
    }

    @Override
    public void toData(PdxWriter out) {
      out.writeField("aChar", aChar, char.class);
      out.writeField("aBoolean", aBoolean, boolean.class);
      out.writeField("aByte", aByte, byte.class);
      out.writeField("aShort", aShort, short.class);
      out.writeField("anInt", anInt, int.class);
      out.writeField("aLong", aLong, long.class);
      out.writeField("aFloat", aFloat, float.class);
      out.writeField("aDouble", aDouble, double.class);
      out.writeField("aDate", aDate, Date.class);
      out.writeField("aString", aString, String.class);
      out.writeField("anObject", anObject, Object.class);
      out.writeField("aMap", aMap, Map.class);
      out.writeField("aCollection", aCollection, Collection.class);
      out.writeField("aBooleanArray", aBooleanArray, boolean[].class);
      out.writeField("aCharArray", aCharArray, char[].class);
      out.writeField("aByteArray", aByteArray, byte[].class);
      out.writeField("aShortArray", aShortArray, short[].class);
      out.writeField("anIntArray", anIntArray, int[].class);
      out.writeField("aLongArray", aLongArray, long[].class);
      out.writeField("aFloatArray", aFloatArray, float[].class);
      out.writeField("aDoubleArray", aDoubleArray, double[].class);
      out.writeField("aStringArray", aStringArray, String[].class);
      out.writeField("anObjectArray", anObjectArray, Object[].class);
      // TODO test other types of Object[] like SimpleClass[].
      out.writeField("anArrayOfByteArray", anArrayOfByteArray, byte[][].class);
    }
  }

  private byte[] createBlob(Object o) throws IOException {
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    DataSerializer.writeObject(o, out);
    return out.toByteArray();
  }

  private <T> T deblob(byte[] blob) throws IOException, ClassNotFoundException {
    return DataSerializer.readObject(new DataInputStream(new ByteArrayInputStream(blob)));
  }

  /**
   * Make sure that if a class adds a field that pdx serialization will preserve the extra field.
   */
  @Test
  public void testFieldAppend() throws Exception {
    MyEvolvablePdx.setVersion(1);
    MyEvolvablePdx pdx = new MyEvolvablePdx(7);
    assertEquals(7, pdx.f1);
    assertEquals(0, pdx.f2);

    MyEvolvablePdx.setVersion(2);
    pdx = new MyEvolvablePdx(7);
    assertEquals(7, pdx.f1);
    assertEquals(8, pdx.f2);
    byte[] v2actual = createBlob(pdx);
    int v2typeId = getBlobPdxTypeId(v2actual);

    cache.getPdxRegistry().removeLocal(pdx);
    MyEvolvablePdx.setVersion(1);
    MyEvolvablePdx pdxv1 = deblob(v2actual);
    assertEquals(7, pdxv1.f1);
    assertEquals(0, pdxv1.f2);

    // now reserialize and make sure f2 is preserved
    byte[] v1actual = createBlob(pdxv1);
    assertEquals(v2typeId, getBlobPdxTypeId(v1actual));
    checkBytes(v2actual, v1actual);
    // serialize one more time to make sure internal cache works ok
    v1actual = createBlob(pdxv1);
    assertEquals(v2typeId, getBlobPdxTypeId(v1actual));
    checkBytes(v2actual, v1actual);
    // make sure CopyHelper preserves extra fields
    MyEvolvablePdx pdxv1copy = CopyHelper.copy(pdxv1);
    v1actual = createBlob(pdxv1copy);
    assertEquals(v2typeId, getBlobPdxTypeId(v1actual));
    checkBytes(v2actual, v1actual);

    MyEvolvablePdx.setVersion(2);
    cache.getPdxRegistry().removeLocal(pdx);
    MyEvolvablePdx pdxv2 = deblob(v1actual);
    assertEquals(7, pdxv2.f1);
    assertEquals(8, pdxv2.f2);
    // make sure we can dserialize a second time with the same results
    pdxv2 = deblob(v1actual);
    assertEquals(7, pdxv2.f1);
    assertEquals(8, pdxv2.f2);
  }

  @Test
  public void testFieldInsert() throws Exception {
    MyEvolvablePdx.setVersion(1);
    MyEvolvablePdx pdx = new MyEvolvablePdx(7);
    assertEquals(7, pdx.f1);
    assertEquals(0, pdx.f2);

    MyEvolvablePdx.setVersion(3);
    pdx = new MyEvolvablePdx(7);
    assertEquals(7, pdx.f1);
    assertEquals(8, pdx.f2);
    byte[] v3actual = createBlob(pdx);
    int v3typeId = getBlobPdxTypeId(v3actual);
    cache.getPdxRegistry().removeLocal(pdx);
    MyEvolvablePdx.setVersion(1);
    MyEvolvablePdx pdxv1 = deblob(v3actual);
    assertEquals(7, pdxv1.f1);
    assertEquals(0, pdxv1.f2);

    int numPdxTypes = getNumPdxTypes();
    // now reserialize and make sure f2 is preserved
    byte[] v1actual = createBlob(pdxv1);

    int mergedTypeId = getBlobPdxTypeId(v1actual);
    assertEquals(numPdxTypes + 1, getNumPdxTypes());
    TypeRegistry tr = cache.getPdxRegistry();
    PdxType v3Type = tr.getType(v3typeId);
    PdxType mergedType = tr.getType(mergedTypeId);
    assertFalse(mergedType.equals(v3Type));
    assertTrue(mergedType.compatible(v3Type));

    MyEvolvablePdx.setVersion(3);
    cache.getPdxRegistry().removeLocal(pdxv1);
    MyEvolvablePdx pdxv3 = deblob(v1actual);
    assertEquals(7, pdxv3.f1);
    assertEquals(8, pdxv3.f2);
  }

  @Test
  public void testFieldRemove() throws Exception {
    // this test pretends that version 1 is newer than version2
    // so it look like a field was removed.
    MyEvolvablePdx.setVersion(1);
    MyEvolvablePdx pdx = new MyEvolvablePdx(7);
    assertEquals(7, pdx.f1);
    assertEquals(0, pdx.f2);

    byte[] v1actual = createBlob(pdx);
    int v1typeId = getBlobPdxTypeId(v1actual);

    cache.getPdxRegistry().removeLocal(pdx);
    MyEvolvablePdx.setVersion(2);
    MyEvolvablePdx pdxv2 = deblob(v1actual);
    assertEquals(7, pdxv2.f1);
    assertEquals(0, pdxv2.f2);
    pdxv2.f2 = 23;

    int numPdxTypes = getNumPdxTypes();
    // now reserialize and make sure it is version2 and not version1
    byte[] v2actual = createBlob(pdxv2);
    int v2typeId = getBlobPdxTypeId(v2actual);
    assertEquals(numPdxTypes + 1, getNumPdxTypes());

    TypeRegistry tr = cache.getPdxRegistry();
    PdxType v2Type = tr.getType(v2typeId);
    PdxType v1Type = tr.getType(v1typeId);
    assertFalse(v1Type.equals(v2Type));
    assertFalse(v1Type.compatible(v2Type));
    assertNotNull(v1Type.getPdxField("f1"));
    assertNull(v1Type.getPdxField("f2"));
    assertNotNull(v2Type.getPdxField("f1"));
    assertNotNull(v2Type.getPdxField("f2"));

    MyEvolvablePdx.setVersion(1);
    cache.getPdxRegistry().removeLocal(pdx);
    MyEvolvablePdx pdxv3 = deblob(v2actual);
    assertEquals(7, pdxv3.f1);
    assertEquals(0, pdxv3.f2);
  }

  private int getBlobPdxTypeId(byte[] blob) {
    ByteBuffer bb = ByteBuffer.wrap(blob);
    // skip byte for PDX dscode and integer for pdx length
    return bb.getInt(DataSize.BYTE_SIZE + DataSize.INTEGER_SIZE);
  }

  public static class MyEvolvablePdx implements PdxSerializable {
    private static int version = 1;

    private static int getVersion() {
      return version;
    }

    private static void setVersion(int v) {
      version = v;
    }

    public int f1;
    public int f2;

    public MyEvolvablePdx(int base) {
      f1 = base;
      base++;
      if (getVersion() >= 2) {
        f2 = base;
        base++;
      }
    }

    public MyEvolvablePdx() {
      // need for pdx deserialization
    }

    @Override
    public void toData(PdxWriter writer) {
      if (getVersion() == 3) {
        writer.writeInt("f2", f2);
      }
      writer.writeInt("f1", f1);
      if (getVersion() == 2) {
        writer.writeInt("f2", f2);
      }
    }

    @Override
    public void fromData(PdxReader reader) {
      if (getVersion() == 3) {
        f2 = reader.readInt("f2");
      }
      f1 = reader.readInt("f1");
      if (getVersion() == 2) {
        f2 = reader.readInt("f2");
      }
    }
  }


  /** PlainOldSerializable */
  public static class POS implements java.io.Serializable {
    int f;

    public POS(int i) {
      f = i;
    }

    public int hashCode() {
      return f;
    }

    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof POS)) {
        return false;
      }
      POS other = (POS) obj;
      return f == other.f;
    }
  }
}
