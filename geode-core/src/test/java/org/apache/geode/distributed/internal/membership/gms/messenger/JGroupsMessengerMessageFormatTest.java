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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactory;
import org.apache.geode.distributed.internal.membership.api.MembershipConfig;
import org.apache.geode.distributed.internal.membership.api.Message;
import org.apache.geode.distributed.internal.membership.gms.GMSMembershipView;
import org.apache.geode.distributed.internal.membership.gms.Services;
import org.apache.geode.distributed.internal.membership.gms.interfaces.JoinLeave;
import org.apache.geode.distributed.internal.membership.gms.messages.InstallViewMessage;
import org.apache.geode.distributed.internal.membership.gms.messages.JoinRequestMessage;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.KnownVersion;

public class JGroupsMessengerMessageFormatTest {

  private JGroupsMessenger<InternalDistributedMember> messenger;
  private InternalDistributedMember member;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    ProbeObject.deserialized = false;
    member = new InternalDistributedMember("localhost", 1234);

    Services<InternalDistributedMember> services = mock(Services.class);
    when(services.getConfig()).thenReturn(new MembershipConfig() {});
    when(services.getSerializer()).thenReturn(InternalDataSerializer.getDSFIDSerializer());
    MemberIdentifierFactory<InternalDistributedMember> memberFactory =
        mock(MemberIdentifierFactory.class);
    when(memberFactory.create(any())).thenReturn(member);
    when(services.getMemberFactory()).thenReturn(memberFactory);
    JoinLeave<InternalDistributedMember> joinLeave = mock(JoinLeave.class);
    when(joinLeave.getMemberID(any())).thenReturn(member);
    when(services.getJoinLeave()).thenReturn(joinLeave);

    messenger = new JGroupsMessenger<>();
    messenger.init(services);
  }

  @Test
  public void deserializeMessageRejectsNonFixedIdPayload() throws Exception {
    HeapDataOutputStream out = startPayload();
    out.writeByte(DSCODE.SERIALIZABLE.toByte());
    ObjectOutputStream objectOut = new ObjectOutputStream(out);
    objectOut.writeObject(new ProbeObject());
    objectOut.flush();

    assertThatThrownBy(() -> deserialize(out)).isInstanceOf(IOException.class);
    assertThat(ProbeObject.deserialized).isFalse();
  }

  @Test
  public void deserializeMessageRejectsJoinRequestWithUnexpectedCredentialsFormat()
      throws Exception {
    HeapDataOutputStream out = startPayload();
    InternalDataSerializer.writeObject(
        new JoinRequestMessage<>(member, member, new ProbeObject(), 0, 0), out);

    assertThatThrownBy(() -> deserialize(out)).isInstanceOf(IOException.class);
    assertThat(ProbeObject.deserialized).isFalse();
  }

  @Test
  public void deserializeMessageRejectsInstallViewWithUnexpectedCredentialsFormat()
      throws Exception {
    HeapDataOutputStream out = startPayload();
    InternalDataSerializer.writeObject(
        new InstallViewMessage<>(new GMSMembershipView<>(member), new ProbeObject(), false), out);

    assertThatThrownBy(() -> deserialize(out)).isInstanceOf(IOException.class);
    assertThat(ProbeObject.deserialized).isFalse();
  }

  @Test
  public void deserializeMessageReadsJoinRequestWithPropertiesCredentials() throws Exception {
    Properties credentials = new Properties();
    credentials.setProperty("security-username", "user");
    credentials.setProperty("security-password", "secret");
    HeapDataOutputStream out = startPayload();
    InternalDataSerializer.writeObject(
        new JoinRequestMessage<>(member, member, credentials, 0, 0), out);

    Message<InternalDistributedMember> result = deserialize(out);

    assertThat(result).isInstanceOf(JoinRequestMessage.class);
    assertThat(((JoinRequestMessage<InternalDistributedMember>) result).getCredentials())
        .isEqualTo(credentials);
  }

  @Test
  public void deserializeMessageReadsInstallViewWithoutCredentials() throws Exception {
    HeapDataOutputStream out = startPayload();
    InternalDataSerializer.writeObject(
        new InstallViewMessage<>(new GMSMembershipView<>(member), null, false), out);

    Message<InternalDistributedMember> result = deserialize(out);

    assertThat(result).isInstanceOf(InstallViewMessage.class);
    assertThat(((InstallViewMessage<InternalDistributedMember>) result).getCredentials())
        .isNull();
  }

  private HeapDataOutputStream startPayload() throws IOException {
    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    member.getMemberData().writeEssentialData(out,
        InternalDataSerializer.createSerializationContext(out));
    return out;
  }

  private Message<InternalDistributedMember> deserialize(HeapDataOutputStream out)
      throws Exception {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
    return messenger.deserializeMessage(in, KnownVersion.CURRENT_ORDINAL);
  }

  public static class ProbeObject implements Serializable {
    static boolean deserialized;

    private void readObject(java.io.ObjectInputStream in)
        throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      deserialized = true;
    }
  }
}
