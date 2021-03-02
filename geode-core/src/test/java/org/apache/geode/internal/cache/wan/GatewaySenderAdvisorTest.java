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
package org.apache.geode.internal.cache.wan;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.VersionedDataInputStream;
import org.apache.geode.internal.serialization.VersionedDataOutputStream;
import org.apache.geode.internal.tcp.ByteBufferInputStream;

public class GatewaySenderAdvisorTest {

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}


  @Test
  public void testGatewaySenderProfileSerializeAndDeserializeCurrent()
      throws IOException, ClassNotFoundException {
    InternalDistributedMember internalDistributedMember =
        new InternalDistributedMember("localhost", 8888);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    GatewaySenderAdvisor.GatewaySenderProfile gatewaySenderProfile =
        new GatewaySenderAdvisor.GatewaySenderProfile(internalDistributedMember, 1);


    VersionedDataOutputStream versionedDataOutputStream =
        new VersionedDataOutputStream(byteArrayOutputStream, KnownVersion.CURRENT);
    DataSerializer.writeObject(gatewaySenderProfile, versionedDataOutputStream);
    versionedDataOutputStream.flush();

    ByteBuffer bb = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(bb);

    VersionedDataInputStream versionedDataInputStream =
        new VersionedDataInputStream(byteBufferInputStream, KnownVersion.CURRENT);
    GatewaySenderAdvisor.GatewaySenderProfile gatewaySenderProfile2 =
        DataSerializer.readObject(versionedDataInputStream);
    assertThat(gatewaySenderProfile).isEqualTo(gatewaySenderProfile2);
  }


  @Test
  public void testGatewaySenderProfileSerializeAndDeserialize113()
      throws IOException, ClassNotFoundException {
    InternalDistributedMember internalDistributedMember =
        new InternalDistributedMember("localhost", 8888);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    GatewaySenderAdvisor.GatewaySenderProfile gatewaySenderProfile =
        new GatewaySenderAdvisor.GatewaySenderProfile(internalDistributedMember, 1);


    VersionedDataOutputStream versionedDataOutputStream =
        new VersionedDataOutputStream(byteArrayOutputStream, KnownVersion.GEODE_1_13_0);
    DataSerializer.writeObject(gatewaySenderProfile, versionedDataOutputStream);
    versionedDataOutputStream.flush();

    ByteBuffer bb = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(bb);

    VersionedDataInputStream versionedDataInputStream =
        new VersionedDataInputStream(byteBufferInputStream, KnownVersion.GEODE_1_13_0);
    GatewaySenderAdvisor.GatewaySenderProfile gatewaySenderProfile2 =
        DataSerializer.readObject(versionedDataInputStream);
    assertThat(gatewaySenderProfile).isEqualTo(gatewaySenderProfile2);
  }
}
