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
import java.util.Objects;

import org.apache.commons.validator.routines.InetAddressValidator;

import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;

/**
 * HostAndPort is a holder of a host name/address and a port. It is the primary
 * way to specify a connection endpoint in the socket-creator methods.
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
 */
public class HostAndPort implements DataSerializableFixedID {

  private InetSocketAddress socketInetAddress;

  public HostAndPort() {
    // serialization constructor
  }

  public HostAndPort(String hostName, int port) {
    if (hostName == null) {
      socketInetAddress = new InetSocketAddress(port);
    } else if (InetAddressValidator.getInstance().isValid(hostName)) {
      // numeric address - use as-is
      socketInetAddress = new InetSocketAddress(hostName, port);
    } else {
      // non-numeric address - resolve hostname when needed
      socketInetAddress = InetSocketAddress.createUnresolved(hostName, port);
    }
  }

  /**
   * Returns an InetSocketAddress for this host and port. An attempt is made to resolve the
   * host name but if resolution fails an unresolved InetSocketAddress is returned. This return
   * value will not hold an InetAddress, so calling getAddress() on it will return null.
   */
  public InetSocketAddress getSocketInetAddress() {
    if (socketInetAddress.isUnresolved()) {
      // note that this leaves the InetAddress null if the hostname isn't resolvable
      return new InetSocketAddress(socketInetAddress.getHostString(), socketInetAddress.getPort());
    } else {
      return this.socketInetAddress;
    }
  }

  public String getHostName() {
    return socketInetAddress.getHostString();
  }

  public int getPort() {
    return socketInetAddress.getPort();
  }

  @Override
  public int hashCode() {
    return socketInetAddress.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HostAndPort that = (HostAndPort) o;
    return Objects.equals(socketInetAddress, that.socketInetAddress);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [socketInetAddress=" + socketInetAddress + "]";
  }

  public InetAddress getAddress() {
    return getSocketInetAddress().getAddress();
  }

  @Override
  public int getDSFID() {
    return HOST_AND_PORT;
  }

  @Override
  public void toData(DataOutput out, SerializationContext context) throws IOException {
    if (socketInetAddress.isUnresolved()) {
      out.writeByte(0);
      StaticSerialization.writeString(getHostName(), out);
      out.writeInt(getPort());
    } else {
      out.writeByte(1);
      StaticSerialization.writeInetAddress(socketInetAddress.getAddress(), out);
      out.writeInt(getPort());
    }
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    InetAddress address = null;
    byte flags = in.readByte();
    if ((flags & 1) == 0) {
      String hostName = StaticSerialization.readString(in);
      int port = in.readInt();
      if (hostName == null || hostName.isEmpty()) {
        socketInetAddress = new InetSocketAddress(port);
      } else {
        socketInetAddress = InetSocketAddress.createUnresolved(hostName, port);
      }
    } else {
      address = StaticSerialization.readInetAddress(in);
      int port = in.readInt();
      socketInetAddress = new InetSocketAddress(address, port);
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }


}
