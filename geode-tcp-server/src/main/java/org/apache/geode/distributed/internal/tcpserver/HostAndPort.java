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
package org.apache.geode.distributed.internal.tcpserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * HostAndPort is a holder of a host name/address and a port. It is the primary
 * way to specify a connection endpoint in the socket-creator methods.
 * <p>
 * This class preserves the hostName string passed in to its constructor that takes a
 * hostName string and will respond with that string when asked for a hostName.
 * <p>
 * Note: This class is serializable for testing. A number of client/server and WAN tests
 * transmit PoolAttributes between unit test JVMs using RMI. PoolAttributes are
 * Externalizable for this purpose and use Geode serialization to transmit HostAndPort
 * objects along with other attributes.
 *
 * @see TcpSocketCreator
 * @see ClusterSocketCreator
 * @see ClientSocketCreator
 * @see AdvancedSocketCreator
 * @see TcpClient
 * @see HostAddress
 */
public class HostAndPort extends InetSocketWrapper implements DataSerializableFixedID {

  public HostAndPort() {
    // serialization constructor
  }

  public HostAndPort(String hostName, int port) {
    super(hostName, port);
  }

  public int getPort() {
    return inetSocketAddress.getPort();
  }

  @Override
  public int getDSFID() {
    return HOST_AND_PORT;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    if (inetSocketAddress.isUnresolved()) {
      out.writeByte(0);
      StaticSerialization.writeString(getHostName(), out);
      out.writeInt(getPort());
    } else {
      out.writeByte(1);
      StaticSerialization.writeInetAddress(inetSocketAddress.getAddress(), out);
      out.writeInt(getPort());
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    InetAddress address;
    byte flags = in.readByte();
    if ((flags & 1) == 0) {
      String hostName = StaticSerialization.readString(in);
      int port = in.readInt();
      if (hostName == null || hostName.isEmpty()) {
        inetSocketAddress = new InetSocketAddress(port);
      } else {
        inetSocketAddress = InetSocketAddress.createUnresolved(hostName, port);
      }
    } else {
      address = StaticSerialization.readInetAddress(in);
      int port = in.readInt();
      inetSocketAddress = new InetSocketAddress(address, port);
    }
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }
}
