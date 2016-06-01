/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.DistributionLocator;
import com.gemstone.gemfire.admin.DistributionLocatorConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.net.InetAddress;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;

/**
 * Provides an implementation of
 * <code>DistributionLocatorConfig</code>.
 *
 * @since GemFire 4.0
 */
public class DistributionLocatorConfigImpl 
  extends ManagedEntityConfigImpl 
  implements DistributionLocatorConfig {

  /** The minimum networking port (0) */
  public static final int MIN_PORT = 0;

  /** The maximum networking port (65535) */
  public static final int MAX_PORT = 65535;
  
  //////////////////////  Instance Fields  //////////////////////

  /** The port on which this locator listens */
  private int port;

  /** The address to bind to on a multi-homed host */
  private String bindAddress;
  
  /** The properties used to configure the DistributionLocator's
      DistributedSystem */
  private Properties dsProperties;

  /** The DistributionLocator that was created with this config */
  private DistributionLocator locator;

  //////////////////////  Static Methods  //////////////////////

  /**
   * Contacts a distribution locator on the given host and port and
   * creates a <code>DistributionLocatorConfig</code> for it.
   *
   * @see InternalLocator#getLocatorInfo
   *
   * @return <code>null</code> if the locator cannot be contacted
   */
  static DistributionLocatorConfig
    createConfigFor(String host, int port, InetAddress bindAddress) {

    String[] info = null;
    if (bindAddress != null) {
      info = InternalLocator.getLocatorInfo(bindAddress, port);
    }
    else {
      info = InternalLocator.getLocatorInfo(InetAddressUtil.toInetAddress(host), port);
    }
    if (info == null) {
      return null;
    }

    DistributionLocatorConfigImpl config =
      new DistributionLocatorConfigImpl();
    config.setHost(host);
    config.setPort(port);
    if (bindAddress != null) {
      config.setBindAddress(bindAddress.getHostAddress());
    }
    config.setWorkingDirectory(info[0]);
    config.setProductDirectory(info[1]);

    return config;
  }

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>DistributionLocatorConfigImpl</code> with the
   * default settings.
   */
  public DistributionLocatorConfigImpl() {
    this.port = 0;
    this.bindAddress = null;
    this.locator = null;
    this.dsProperties = new java.util.Properties();
    this.dsProperties.setProperty(MCAST_PORT, "0");
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   * Sets the locator that was configured with this
   * <Code>DistributionLocatorConfigImpl</code>. 
   */
  void setLocator(DistributionLocator locator) {
    this.locator = locator;
  }

  @Override
  protected boolean isReadOnly() {
    return this.locator != null && this.locator.isRunning();
  }

  public int getPort() {
    return this.port;
  }

  public void setPort(int port) {
    checkReadOnly();
    this.port = port;
    configChanged();
  }

  public String getBindAddress() {
    return this.bindAddress;
  }

  public void setBindAddress(String bindAddress) {
    checkReadOnly();
    this.bindAddress = bindAddress;
    configChanged();
  }
  
  public void setDistributedSystemProperties(Properties props) {
    this.dsProperties = props;
  }
  
  public Properties getDistributedSystemProperties() {
    return this.dsProperties;
  }

  @Override
  public void validate() {
    super.validate();

    if (port < MIN_PORT || port > MAX_PORT) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionLocatorConfigImpl_PORT_0_MUST_BE_AN_INTEGER_BETWEEN_1_AND_2.toLocalizedString(new Object[] {Integer.valueOf(port), Integer.valueOf(MIN_PORT), Integer.valueOf(MAX_PORT)}));
    }

    if (this.bindAddress != null &&
        InetAddressUtil.validateHost(this.bindAddress) == null) {
      throw new IllegalArgumentException(LocalizedStrings.DistributionLocatorConfigImpl_INVALID_HOST_0.toLocalizedString(this.bindAddress));
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
    DistributionLocatorConfigImpl clone =
      (DistributionLocatorConfigImpl) super.clone();
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
