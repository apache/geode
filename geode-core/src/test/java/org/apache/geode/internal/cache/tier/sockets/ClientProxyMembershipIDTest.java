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


import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import org.junit.Test;

import org.apache.geode.internal.NullDataOutputStream;
import org.apache.geode.internal.serialization.ByteArrayDataInput;

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

}
