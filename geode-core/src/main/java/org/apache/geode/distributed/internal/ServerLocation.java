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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.internal.inet.LocalHostUtil;

/**
 * Represents the location of a cache server. This class is preferable to InetSocketAddress because
 * it doesn't do any name resolution.
 *
 *
 */
public class ServerLocation implements DataSerializable, Comparable<ServerLocation> {
  private static final long serialVersionUID = -5850116974987640560L;

  private String hostName;
  private int port;
  /**
   * Used exclusively in case of single user authentication mode. Client sends this userId to the
   * server with each operation to identify itself at the server.
   */
  private long userId = -1;

  /**
   * If its value is two, it lets the client know that it need not send the security part
   * (connection-id, user-id) with each operation to the server. Also, that the client should not
   * expect the security part in the server's response.
   */
  private final AtomicInteger requiresCredentials = new AtomicInteger(INITIAL_REQUIRES_CREDENTIALS);

  public static final int INITIAL_REQUIRES_CREDENTIALS = 0;

  public static final int REQUIRES_CREDENTIALS = 1;

  public static final int REQUIRES_NO_CREDENTIALS = 2;

  /**
   * For DataSerializer
   */
  public ServerLocation() {

  }

  public ServerLocation(String hostName, int port) {
    this.hostName = hostName;
    this.port = port;
  }

  public String getHostName() {
    return hostName;
  }

  public int getPort() {
    return port;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    hostName = DataSerializer.readString(in);
    port = in.readInt();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(hostName, out);
    out.writeInt(port);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    // result = prime * result + ((hostName == null) ? 0 : hostName.hashCode());
    result = prime * result + port;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof ServerLocation))
      return false;
    final ServerLocation other = (ServerLocation) obj;
    if (hostName == null) {
      if (other.hostName != null)
        return false;
    } else if (other.hostName == null) {
      return false;
    } else if (!hostName.equals(other.hostName)) {
      String canonicalHostName;
      try {
        canonicalHostName = LocalHostUtil.getCanonicalLocalHostName();
      } catch (UnknownHostException e) {
        throw new IllegalStateException("getLocalHost failed with " + e);
      }
      if ("localhost".equals(hostName)) {
        if (!canonicalHostName.equals(other.hostName)) {
          return false;
        }
      } else if ("localhost".equals(other.hostName)) {
        if (!canonicalHostName.equals(hostName)) {
          return false;
        }
      } else {
        return false; // fix for bug 42040
      }
    }
    if (port != other.port)
      return false;
    return true;
  }

  @Override
  public String toString() {
    return hostName + ":" + port;
  }

  @Override
  public int compareTo(ServerLocation other) {
    int difference = hostName.compareTo(other.hostName);
    if (difference != 0) {
      return difference;
    }
    return port - other.getPort();
  }

  public void setUserId(long id) {
    this.userId = id;
  }

  public long getUserId() {
    return this.userId;
  }

  public void compareAndSetRequiresCredentials(boolean bool) {
    int val = bool ? REQUIRES_CREDENTIALS : REQUIRES_NO_CREDENTIALS;
    this.requiresCredentials.compareAndSet(INITIAL_REQUIRES_CREDENTIALS, val);
  }

  public void setRequiresCredentials(boolean bool) {
    int val = bool ? REQUIRES_CREDENTIALS : REQUIRES_NO_CREDENTIALS;
    this.requiresCredentials.set(val);
  }

  public boolean getRequiresCredentials() {
    return this.requiresCredentials.get() == REQUIRES_CREDENTIALS ? true : false;
  }

}
