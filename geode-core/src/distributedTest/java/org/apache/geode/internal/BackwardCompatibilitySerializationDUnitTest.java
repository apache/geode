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
package org.apache.geode.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.SerializationVersions;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.serialization.internal.DSFIDSerializerImpl;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

/**
 * Test the DSFID serialization framework added for rolling upgrades
 */
@Category({SerializationTest.class})
public class BackwardCompatibilitySerializationDUnitTest extends JUnit4CacheTestCase {

  private transient ByteArrayOutputStream baos;
  private transient ByteArrayInputStream bais;

  public static boolean toDataCalled = false;
  public static boolean toDataPre11Called = false;
  public static boolean toDataPre15called = false;
  public static boolean fromDataCalled = false;
  public static boolean fromDataPre11Called = false;
  public static boolean fromDataPre15Called = false;

  public TestMessage msg = new TestMessage();

  public BackwardCompatibilitySerializationDUnitTest() {
    super();
  }

  static {
    InternalDataSerializer.getDSFIDSerializer()
        .register(TestMessage.TEST_MESSAGE_DSFID, TestMessage.class);
  }

  @Override
  public final void postSetUp() {
    baos = new ByteArrayOutputStream();
  }

  @Override
  public final void preTearDownCacheTestCase() {
    resetFlags();
    baos = null;
    bais = null;
  }

  /**
   * Test if correct toData/toDataPreXXX is called when changes are made to the TestMessage
   */
  @Test
  public void testToDataFromHigherVersionToLower() throws Exception {
    DataOutputStream dos =
        new VersionedDataOutputStream(new DataOutputStream(baos), KnownVersion.OLDEST);
    InternalDataSerializer.writeDSFID(msg, dos);
    assertTrue(toDataPre11Called);
    assertFalse(toDataCalled);
  }

  /**
   * Test if correct toData/toDataXXX is called when changes are made to the TestMessage
   */
  @Test
  public void testToDataFromLowerVersionToHigher() throws Exception {
    DataOutputStream dos =
        new VersionedDataOutputStream(new DataOutputStream(baos), KnownVersion.GEODE_1_5_0);
    InternalDataSerializer.writeDSFID(msg, dos);
    assertTrue(toDataCalled);
  }

  /**
   * Test if correct fromData/fromDataXXX is called when changes are made to the TestMessage
   */
  @Test
  public void testFromDataFromHigherVersionToLower() throws Exception {
    InternalDataSerializer.writeDSFID(msg, new DataOutputStream(baos));
    bais = new ByteArrayInputStream(baos.toByteArray());

    DataInputStream dis =
        new VersionedDataInputStream(new DataInputStream(bais), KnownVersion.GEODE_1_5_0);
    Object o = InternalDataSerializer.basicReadObject(dis);
    assertTrue(o instanceof TestMessage);
    assertTrue(fromDataCalled);
  }

  /**
   * Test if correct fromData/fromDataXXX is called when changes are made to the TestMessage
   */
  @Test
  public void testFromDataFromLowerVersionToHigher() throws Exception {
    InternalDataSerializer.writeDSFID(msg, new DataOutputStream(baos));
    bais = new ByteArrayInputStream(baos.toByteArray());

    DataInputStream dis =
        new VersionedDataInputStream(new DataInputStream(bais), KnownVersion.OLDEST);
    Object o = InternalDataSerializer.basicReadObject(dis);
    assertTrue(o instanceof TestMessage);
    assertTrue(fromDataPre11Called);
  }

  /**
   * Test if all messages implement toDataPreXXX and fromDataPreXXX if the message has been upgraded
   * in any of the versions
   *
   */
  @Test
  public void testAllMessages() throws Exception {
    // list of msgs not created using reflection
    // taken from DSFIDFactory.create()
    ArrayList<Integer> constdsfids = new ArrayList<>();
    constdsfids.add(new Byte(DataSerializableFixedID.REGION).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.END_OF_STREAM_TOKEN).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.DLOCK_REMOTE_TOKEN).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.TRANSACTION_ID).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.INTEREST_RESULT_POLICY).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.UNDEFINED).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.RESULTS_BAG).intValue());
    constdsfids.add(new Byte(DataSerializableFixedID.GATEWAY_EVENT_IMPL_66).intValue());
    constdsfids.add(new Short(DataSerializableFixedID.TOKEN_INVALID).intValue());
    constdsfids.add(new Short(DataSerializableFixedID.TOKEN_LOCAL_INVALID).intValue());
    constdsfids.add(new Short(DataSerializableFixedID.TOKEN_DESTROYED).intValue());
    constdsfids.add(new Short(DataSerializableFixedID.TOKEN_REMOVED).intValue());
    constdsfids.add(new Short(DataSerializableFixedID.TOKEN_REMOVED2).intValue());
    constdsfids.add(new Short(DataSerializableFixedID.TOKEN_TOMBSTONE).intValue());

    for (int i = 0; i < 256; i++) {
      Constructor<?> cons =
          ((DSFIDSerializerImpl) InternalDataSerializer.getDSFIDSerializer())
              .getDsfidmap()[i];
      if (!constdsfids.contains(i - Byte.MAX_VALUE - 1) && cons != null) {
        Object ds = cons.newInstance((Object[]) null);
        checkSupportForRollingUpgrade(ds);
      }
    }

    // some msgs require distributed system
    Cache c = getCache();
    for (Object o : ((DSFIDSerializerImpl) InternalDataSerializer
        .getDSFIDSerializer())
            .getDsfidmap2().values()) {
      Constructor<?> cons = (Constructor<?>) o;
      if (cons != null) {
        DataSerializableFixedID ds = (DataSerializableFixedID) cons.newInstance((Object[]) null);
        checkSupportForRollingUpgrade(ds);
      }
    }
    c.close();
  }

  private void checkSupportForRollingUpgrade(Object ds) {
    KnownVersion[] versions = null;
    if (ds instanceof SerializationVersions) {
      versions = ((SerializationVersions) ds).getSerializationVersions();
    }
    if (versions != null && versions.length > 0) {
      for (final KnownVersion version : versions) {
        if (ds instanceof DataSerializableFixedID) {
          try {
            ds.getClass().getMethod("toDataPre_" + version.getMethodSuffix(),
                DataOutput.class, SerializationContext.class);

            ds.getClass().getMethod("fromDataPre_" + version.getMethodSuffix(),
                DataInput.class, DeserializationContext.class);
          } catch (NoSuchMethodException e) {
            fail(
                "toDataPreXXX or fromDataPreXXX for previous versions not found " + e.getMessage());
          }
        }
        if (ds instanceof DataSerializable) {
          try {
            ds.getClass().getMethod("toDataPre_" + version.getMethodSuffix(),
                DataOutput.class);

            ds.getClass().getMethod("fromDataPre_" + version.getMethodSuffix(),
                DataInput.class);
          } catch (NoSuchMethodException e) {
            fail(
                "toDataPreXXX or fromDataPreXXX for previous versions not found " + e.getMessage());
          }
        }
      }
    } else {
      for (Method method : ds.getClass().getMethods()) {
        if (method.getName().startsWith("toDataPre")) {
          fail(
              "Found backwards compatible toData, but class does not implement getSerializationVersions()"
                  + method);
        } else if (method.getName().startsWith("fromDataPre")) {
          fail(
              "Found backwards compatible fromData, but class does not implement getSerializationVersions()"
                  + method);
        }
      }

    }
  }

  private void resetFlags() {
    toDataCalled = false;
    toDataPre11Called = false;
    toDataPre15called = false;
    fromDataCalled = false;
    fromDataPre11Called = false;
    fromDataPre15Called = false;
  }

  public static class TestMessage implements DataSerializableFixedID {
    /** The versions in which this message was modified */
    private static final KnownVersion[] dsfidVersions =
        new KnownVersion[] {KnownVersion.GEODE_1_1_0, KnownVersion.GEODE_1_5_0};

    static final int TEST_MESSAGE_DSFID = 12345;

    public TestMessage() {}

    @Override
    public KnownVersion[] getSerializationVersions() {
      return dsfidVersions;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      toDataCalled = true;
    }

    @SuppressWarnings("unused")
    public void toDataPre_GEODE_1_1_0_0(@SuppressWarnings("unused") DataOutput out,
        @SuppressWarnings("unused") SerializationContext context) {
      toDataPre11Called = true;
    }

    @SuppressWarnings("unused")
    public void toDataPre_GEODE_1_5_0_0(@SuppressWarnings("unused") DataOutput out,
        @SuppressWarnings("unused") SerializationContext context) {
      toDataPre15called = true;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException {
      fromDataCalled = true;
    }

    @SuppressWarnings("unused")
    public void fromDataPre_GEODE_1_1_0_0(@SuppressWarnings("unused") DataInput out,
        @SuppressWarnings("unused") DeserializationContext context) {
      fromDataPre11Called = true;
    }

    @SuppressWarnings("unused")
    public void fromDataPre_GEODE_1_5_0_0(@SuppressWarnings("unused") DataInput out,
        @SuppressWarnings("unused") DeserializationContext context) {
      fromDataPre15Called = true;
    }

    @Override
    public int getDSFID() {
      return TEST_MESSAGE_DSFID;
    }

  }
}
