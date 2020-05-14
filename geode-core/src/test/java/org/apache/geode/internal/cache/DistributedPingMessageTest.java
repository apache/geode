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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.serialization.DataSerializableFixedID.DISTRIBUTED_PING_MESSAGE;
import static org.hamcrest.text.MatchesPattern.matchesPattern;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;

public class DistributedPingMessageTest {

  @Test
  public void testToString() {
    final InternalDistributedMember dummyDistributedMember =
        new InternalDistributedMember("localhost", 1);
    final InternalDistributedMember clientDistributedMember =
        new InternalDistributedMember("localhost", 2);
    final ClientProxyMembershipID proxyID = new ClientProxyMembershipID(clientDistributedMember);
    final DistributedPingMessage message =
        new DistributedPingMessage(dummyDistributedMember, proxyID);
    assertThat(message.toString(),
        matchesPattern("DistributedPingMessage@.*; proxyId=identity\\(localhost<.*>:2.*"));
  }

  @Test
  public void testGetDSFID() {
    assertEquals(new DistributedPingMessage().getDSFID(), DISTRIBUTED_PING_MESSAGE);
  }

  @Test
  public void testGetSerializationVersions() {
    assertNull(new DistributedPingMessage().getSerializationVersions());
  }

  @Test
  public void testToDataAndFromData() throws IOException, ClassNotFoundException {
    final InternalDistributedMember dummyDistributedMember =
        new InternalDistributedMember("localhost", 1);
    final InternalDistributedMember clientDistributedMember =
        new InternalDistributedMember("localhost", 2);
    final ClientProxyMembershipID proxyID = new ClientProxyMembershipID(clientDistributedMember);
    final DistributedPingMessage before =
        new DistributedPingMessage(dummyDistributedMember, proxyID);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(1024);
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    before.toData(dataOutputStream,
        InternalDataSerializer.createSerializationContext(dataOutputStream));
    dataOutputStream.close();

    final DistributedPingMessage after = new DistributedPingMessage();
    ByteArrayInputStream byteArrayInputStream =
        new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
    DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream);
    after.fromData(dataInputStream,
        InternalDataSerializer.createDeserializationContext(dataInputStream));

    assertEquals(before.getProxyID(), after.getProxyID());
  }

}
