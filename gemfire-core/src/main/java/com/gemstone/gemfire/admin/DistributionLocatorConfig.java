/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;
import java.util.Properties;

/**
 * Describes the configuration of a {@link DistributionLocator}
 * managed by the GemFire administration APIs.  
 *
 * <P>
 *
 * A <code>DistributionLocatorConfig</code> can be modified using a
 * number of mutator methods until the
 * <code>DistributionLocator</code> configured by this object is
 * {@linkplain ManagedEntity#start started}.  After that,
 * attempts to modify most attributes in the
 * <code>DistributionLocatorConfig</code> will result in an {@link
 * IllegalStateException} being thrown.  If you wish to use the same
 * <code>DistributionLocatorConfig</code> to configure another
 * <code>DistributionLocator</code>s, a copy of the
 * <code>DistributionLocatorConfig</code> object can be made by
 * invoking the {@link Object#clone} method.
 *
 * @see AdminDistributedSystem#addDistributionLocator
 * @see com.gemstone.gemfire.distributed.Locator
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface DistributionLocatorConfig
  extends ManagedEntityConfig {

  /**
   * Returns the port on which ths distribution locator listens for
   * members to connect.  There is no default locator port, so a
   * non-default port must be specified.
   */
  public int getPort();

  /**
   * Sets the port on which the distribution locator listens for
   * members to connect.
   */
  public void setPort(int port);

  /**
   * Returns the address to which the distribution locator's port is
   * (or will be) bound.  By default, the bind address is
   * <code>null</code> meaning that the port will be bound to all
   * network addresses on the host.
   */
  public String getBindAddress();

  /**
   * Sets the address to which the distribution locator's port is
   * (or will be) bound.
   */
  public void setBindAddress(String bindAddress);

  /**
   * Sets the properties used to configure the locator's
   * DistributedSystem.
   * @since 5.0
   */
  public void setDistributedSystemProperties(Properties props);
  
  /**
   * Retrieves the properties used to configure the locator's
   * DistributedSystem.
   * @since 5.0
   */
  public Properties getDistributedSystemProperties();


}
