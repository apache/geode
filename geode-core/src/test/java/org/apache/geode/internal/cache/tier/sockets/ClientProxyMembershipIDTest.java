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

package org.apache.geode.internal.cache.tier.sockets;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.NullDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;
import org.apache.geode.internal.serialization.DSCODE;

public class ClientProxyMembershipIDTest {

  @Test
  public void writeExternalThrowsIfIdentityIsTooLong() throws IOException {
    final byte[] identity = new byte[Short.MAX_VALUE + 1];
    final ClientProxyMembershipID clientProxyMembershipID =
        new ClientProxyMembershipID(identity, 0, null);
    final ObjectOutput out = new ObjectOutputStream(new NullDataOutputStream());

    assertThatThrownBy(() -> clientProxyMembershipID.writeExternal(out))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("identity length is too big");
  }

  @Test
  public void readExternalThrowsIfIdentityIsTooShort() throws IOException {
    final ClientProxyMembershipID clientProxyMembershipID = new ClientProxyMembershipID();
    final ByteArrayOutputStream dos = new ByteArrayOutputStream();
    final ObjectOutputStream out = new ObjectOutputStream(dos);
    out.writeShort(-1);
    out.write(new byte[0]);
    out.writeInt(0);
    out.close();
    final byte[] bytes = dos.toByteArray();
    final ObjectInput in = new ObjectInputStream(new ByteArrayDataInput(bytes));

    assertThatThrownBy(() -> clientProxyMembershipID.readExternal(in))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("identity length is too small");
  }


  @Test
  public void readCanonicalizedRejectsUnexpectedHeader() throws Exception {
    ProbeObject.deserialized = false;

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    bytes.write(DSCODE.SERIALIZABLE.toByte());
    final ObjectOutputStream objectOut = new ObjectOutputStream(bytes);
    objectOut.writeObject(new ProbeObject());
    objectOut.close();
    final ByteArrayDataInput in = new ByteArrayDataInput(bytes.toByteArray());

    assertThatThrownBy(() -> ClientProxyMembershipID.readCanonicalized(in))
        .isInstanceOf(IOException.class);
    assertThat(ProbeObject.deserialized).isFalse();
  }

  @Test
  public void readCanonicalizedReadsNull() throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataSerializer.writeObject(null, new DataOutputStream(bytes));
    final ByteArrayDataInput in = new ByteArrayDataInput(bytes.toByteArray());

    assertThat(ClientProxyMembershipID.readCanonicalized(in)).isNull();
  }

  @Test
  public void readCanonicalizedReadsFixedIdForm() throws Exception {
    final ClientProxyMembershipID written = new ClientProxyMembershipID();
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataSerializer.writeObject(written, new DataOutputStream(bytes));
    final ByteArrayDataInput in = new ByteArrayDataInput(bytes.toByteArray());

    assertThat(ClientProxyMembershipID.readCanonicalized(in)).isNotNull();
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
