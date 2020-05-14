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
package org.apache.geode.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpSocketFactory;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * Sent to a locator to request it to find (and possibly start) a jmx manager for us. It returns a
 * JmxManagerLocatorResponse.
 *
 * @since GemFire 7.0
 *
 */
public class JmxManagerLocatorRequest implements DataSerializableFixedID {

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {}

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {}

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.JMX_MANAGER_LOCATOR_REQUEST;
  }

  @Override
  public String toString() {
    return "JmxManagerLocatorRequest";
  }

  @Immutable
  private static final JmxManagerLocatorRequest SINGLETON = new JmxManagerLocatorRequest();

  /**
   * Send a request to the specified locator asking it to find (and start if needed) a jmx manager.
   * A jmx manager will only be started
   *
   * @param locatorHost the name of the host the locator is on
   * @param locatorPort the port the locator is listening on
   * @param msTimeout how long in milliseconds to wait for a response from the locator
   * @param sslConfigProps Map carrying SSL configuration that can be used by SocketCreator
   * @return a response object that describes the jmx manager.
   * @throws IOException if we can not connect to the locator, timeout waiting for a response, or
   *         have trouble communicating with it.
   */
  public static JmxManagerLocatorResponse send(String locatorHost, int locatorPort, int msTimeout,
      Properties sslConfigProps) throws IOException, ClassNotFoundException {

    // simply need to turn sslConfigProps into sslConfig for locator
    SSLConfig sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(sslConfigProps,
        SecurableCommunicationChannel.LOCATOR);
    SocketCreator socketCreator = new SocketCreator(sslConfig);
    TcpClient client = new TcpClient(socketCreator,
        InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
        InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer(),
        TcpSocketFactory.DEFAULT);
    Object responseFromServer = client.requestToServer(new HostAndPort(locatorHost, locatorPort),
        SINGLETON, msTimeout, true);

    if (responseFromServer instanceof JmxManagerLocatorResponse)
      return (JmxManagerLocatorResponse) responseFromServer;
    else {
      throw new IllegalStateException(
          "Unrecognisable response received: This could be the result of trying to connect a non-SSL-enabled client to an SSL-enabled locator.");
    }
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
