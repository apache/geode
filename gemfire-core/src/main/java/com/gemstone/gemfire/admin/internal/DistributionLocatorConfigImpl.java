/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.DistributionLocator;
import com.gemstone.gemfire.admin.DistributionLocatorConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.net.InetAddress;
import java.util.Properties;

/**
 * Provides an implementation of
 * <code>DistributionLocatorConfig</code>.
 *
 * @author David Whitlock
 * @since 4.0
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
    this.dsProperties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
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
