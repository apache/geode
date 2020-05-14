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
package org.apache.geode.admin.internal;

import static org.apache.geode.admin.internal.InetAddressUtilsWithLogging.validateHost;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.net.InetAddress;
import java.util.Properties;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.admin.DistributionLocator;
import org.apache.geode.admin.DistributionLocatorConfig;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

/**
 * Provides an implementation of <code>DistributionLocatorConfig</code>.
 *
 * @since GemFire 4.0
 */
public class DistributionLocatorConfigImpl extends ManagedEntityConfigImpl
    implements DistributionLocatorConfig {

  /** The minimum networking port (0) */
  public static final int MIN_PORT = 0;

  /** The maximum networking port (65535) */
  public static final int MAX_PORT = 65535;

  ////////////////////// Instance Fields //////////////////////

  /** The port on which this locator listens */
  private int port;

  /** The address to bind to on a multi-homed host */
  private String bindAddress;

  /**
   * The properties used to configure the DistributionLocator's DistributedSystem
   */
  private Properties dsProperties;

  /** The DistributionLocator that was created with this config */
  private DistributionLocator locator;

  ////////////////////// Static Methods //////////////////////

  /**
   * Contacts a distribution locator on the given host and port and creates a
   * <code>DistributionLocatorConfig</code> for it.
   *
   * @return <code>null</code> if the locator cannot be contacted
   */
  static DistributionLocatorConfig createConfigFor(String host, int port, InetAddress bindAddress) {
    String[] info = new String[] {"unknown", "unknown"};

    try {
      TcpClient client = new TcpClient(SocketCreatorFactory
          .getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR),
          InternalDataSerializer.getDSFIDSerializer().getObjectSerializer(),
          InternalDataSerializer.getDSFIDSerializer().getObjectDeserializer());
      if (bindAddress != null) {
        info = client.getInfo(new HostAndPort(bindAddress.getHostAddress(), port));
      } else {
        info = client.getInfo(new HostAndPort(host, port));
      }
      if (info == null) {
        return null;
      }
    } catch (GemFireConfigException e) {
      // communications are not initialized at this point
    }

    DistributionLocatorConfigImpl config = new DistributionLocatorConfigImpl();
    config.setHost(host);
    config.setPort(port);
    if (bindAddress != null) {
      config.setBindAddress(bindAddress.getHostAddress());
    }
    config.setWorkingDirectory(info[0]);
    config.setProductDirectory(info[1]);

    return config;
  }

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>DistributionLocatorConfigImpl</code> with the default settings.
   */
  public DistributionLocatorConfigImpl() {
    this.port = 0;
    this.bindAddress = null;
    this.locator = null;
    this.dsProperties = new java.util.Properties();
    this.dsProperties.setProperty(MCAST_PORT, "0");
  }

  ///////////////////// Instance Methods /////////////////////

  /**
   * Sets the locator that was configured with this <Code>DistributionLocatorConfigImpl</code>.
   */
  void setLocator(DistributionLocator locator) {
    this.locator = locator;
  }

  @Override
  protected boolean isReadOnly() {
    return this.locator != null && this.locator.isRunning();
  }

  @Override
  public int getPort() {
    return this.port;
  }

  @Override
  public void setPort(int port) {
    checkReadOnly();
    this.port = port;
    configChanged();
  }

  @Override
  public String getBindAddress() {
    return this.bindAddress;
  }

  @Override
  public void setBindAddress(String bindAddress) {
    checkReadOnly();
    this.bindAddress = bindAddress;
    configChanged();
  }

  @Override
  public void setDistributedSystemProperties(Properties props) {
    this.dsProperties = props;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    return this.dsProperties;
  }

  @Override
  public void validate() {
    super.validate();

    if (port < MIN_PORT || port > MAX_PORT) {
      throw new IllegalArgumentException(
          String.format("Port ( %s ) must be an integer between %s and %s",
              new Object[] {Integer.valueOf(port), Integer.valueOf(MIN_PORT),
                  Integer.valueOf(MAX_PORT)}));
    }

    if (this.bindAddress != null && validateHost(this.bindAddress) == null) {
      throw new IllegalArgumentException(
          String.format("Invalid host %s",
              this.bindAddress));
    }
  }

  /**
   * Currently, listeners are not supported on the locator config.
   */
  @Override
  protected void configChanged() {

  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    DistributionLocatorConfigImpl clone = (DistributionLocatorConfigImpl) super.clone();
    clone.locator = null;
    return clone;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DistributionLocatorConfig: host=").append(getHost());
    sb.append(", bindAddress=").append(getBindAddress());
    sb.append(", port=").append(getPort());
    return sb.toString();
  }
}
