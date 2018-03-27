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

import static org.apache.geode.pdx.PdxSerializerRegressionTest.TestSerializer;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.test.junit.categories.SerializationTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, SerializationTest.class})
public class PdxSerializerJUnitTest {

  Cache cache;

  @Before
  public void setup() {
    // create a cache so we can work with PDX
    cache = (new CacheFactory()).setPdxSerializer(new TestSerializer()).setPdxReadSerialized(true)
        .set("mcast-port", "0").create();
  }

  @After
  public void teardown() {
    cache.close();
    cache = null;
  }

  /**
   * When deserializing DataSerializableFixedID objects DataSerializer disables
   * pdx-read-serialized to protect Geode message classes from inadvertently reading
   * PdxInstances instead of expected deserialized values in their fromData methods.
   * This test ensures that readObject() fully deserializes a Set that is pdx-serialized
   * and that other methods that respect the pdx-read-serialized setting return
   * PdxInstances.
   */
  @Test
  public void deserializingDSFIDDoesNotReturnPdxInstance()
      throws IOException, ClassNotFoundException {

    HeapDataOutputStream hdos = new HeapDataOutputStream(200, Version.CURRENT);
    TestDSFID testObject = new TestDSFID();
    Set setOfInts = new HashSet();
    setOfInts.add(1);
    setOfInts.add(2);
    // The TestSerializer will PDX-serialize a Collections.UnmodifiableSet
    testObject.mySet = Collections.unmodifiableSet(setOfInts);
    DataSerializer.writeObject(testObject, hdos);
    byte[] serializedForm = hdos.toByteArray();
    ByteArrayInputStream bytesIn = new ByteArrayInputStream(serializedForm);
    DataInputStream inputStream = new DataInputStream(bytesIn);

    testObject = DataSerializer.readObject(inputStream);
    assertThat(testObject.mySet.getClass().getName())
        .isEqualTo("java.util.Collections$UnmodifiableSet");
    assertThat(testObject.readUserObjectResult).isInstanceOf(PdxInstance.class);
    assertThat(testObject.doWithPdxSerializedResult).isInstanceOf(PdxInstance.class);
  }


  static class TestDSFID implements DataSerializableFixedID {
    Object mySet;
    Object readUserObjectResult;
    Object doWithPdxSerializedResult;

    public TestDSFID() {}

    @Override
    public int getDSFID() {
      // NO_FIXED_ID tells DataSerializer to write the class name on the output stream.
      // It is only used for testing.
      return NO_FIXED_ID;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(mySet, out);
      DataSerializer.writeObject(mySet, out);
      DataSerializer.writeObject(mySet, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      // toData serialized a Set three times. TestSerializer has pdx-serialized
      // these sets.

      // readObject should return a Set
      mySet = DataSerializer.readObject(in);

      // readUserObject should return a PdxInstance
      readUserObjectResult = InternalDataSerializer.readUserObject(in);

      // readObject in the context of doWithPdxReadSerialized should return a PdxInstance
      InternalDataSerializer.doWithPdxReadSerialized(() -> {
        doWithPdxSerializedResult = DataSerializer.readObject(in);
      });
    }

    @Override
    public Version[] getSerializationVersions() {
      return new Version[0];
    }
  }

}
