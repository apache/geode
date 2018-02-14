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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataStream;
import org.apache.geode.internal.cache.tier.Acceptor;

/**
 * A <code>ServerHandShakeProcessor</code> verifies the client's version compatibility with server.
 *
 * @since GemFire 5.7
 */


public class ServerHandshakeProcessor {
  protected static final byte REPLY_REFUSED = (byte) 60;

  protected static final byte REPLY_INVALID = (byte) 61;

  public static final Version currentServerVersion = Acceptor.VERSION;

  /**
   * Refuse a received handshake.
   *
   * @param out the Stream to the waiting greeter.
   * @param message providing details about the refusal reception, mainly for client logging.
   * @throws IOException
   */
  public static void refuse(OutputStream out, String message) throws IOException {
    refuse(out, message, REPLY_REFUSED);
  }

  /**
   * Refuse a received handshake.
   *
   * @param out the Stream to the waiting greeter.
   * @param message providing details about the refusal reception, mainly for client logging.
   * @param exception providing details about exception occurred.
   * @throws IOException
   */
  public static void refuse(OutputStream out, String message, byte exception) throws IOException {

    HeapDataOutputStream hdos = new HeapDataOutputStream(32, Version.CURRENT);
    DataOutputStream dos = new DataOutputStream(hdos);
    // Write refused reply
    dos.writeByte(exception);

    // write dummy endpointType
    dos.writeByte(0);
    // write dummy queueSize
    dos.writeInt(0);

    // Write the server's member
    DistributedMember member = InternalDistributedSystem.getAnyInstance().getDistributedMember();
    HeapDataOutputStream memberDos = new HeapDataOutputStream(Version.CURRENT);
    DataSerializer.writeObject(member, memberDos);
    DataSerializer.writeByteArray(memberDos.toByteArray(), dos);
    memberDos.close();

    // Write the refusal message
    if (message == null) {
      message = "";
    }
    dos.writeUTF(message);

    // Write dummy delta-propagation property value. This will never be read at
    // receiver because the exception byte above will cause the receiver code
    // throw an exception before the below byte could be read.
    dos.writeBoolean(Boolean.TRUE);

    out.write(hdos.toByteArray());
    out.flush();
  }

}
