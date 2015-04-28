/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management;

import com.gemstone.gemfire.distributed.Locator;

/**
 * MBean that provides access to information and management functionality for a
 * {@link Locator}.
 * 
 * @author rishim
 * @since 7.0
 */
public interface LocatorMXBean {

  /**
   * Returns the port on which this Locator listens for connections.
   */
  public int getPort();

  /**
   * Returns a string representing the IP address or host name that this Locator
   * will listen on.
   */
  public String getBindAddress();

  /**
   * Returns the name or IP address to pass to the client as the location
   * where the Locator is listening.
   */
  public String getHostnameForClients();

  /**
   * Returns whether the Locator provides peer location services to members.
   * 
   * @return True if the Locator provides peer locations services, false otherwise.
   */
  public boolean isPeerLocator();

  /**
   * Returns whether the Locator provides server location services To clients.
   * 
   * @return True if the Locator provides server location services, false otherwise.
   */
  public boolean isServerLocator();

  /**
   * Returns the most recent log entries for the Locator.
   */
  public String viewLog();

  /**
   * Returns a list of servers on which the manager service may be started
   * either by a Locator or users.
   */
  public String[] listPotentialManagers();

  /**
   * Returns the list of current managers.
   */
  public String[] listManagers();
}
