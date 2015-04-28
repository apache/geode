/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.admin;

/**
 * Represents a single distribution locator server, of which a
 * distributed system may use zero or many.  The distributed system
 * will be configured to use either multicast discovery or locator
 * service.
 *
 * @see DistributionLocatorConfig
 *
 * @author    Kirk Lund
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface DistributionLocator extends ManagedEntity {
  
  /** 
   * Returns the identity name for this locator.
   */
  public String getId();
  
  /**
   * Returns the configuration object for this distribution locator.
   *
   * @since 4.0
   */
  public DistributionLocatorConfig getConfig();

}

