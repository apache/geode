/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.admin.jmx.internal;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * This interface is similar to {@link mx4j.tools.naming.NamingServiceMBean}.
 * Features that differ are:
 * 1. This MBean interface additionally provides a way to specify the host that 
 * the RMI Registry should get bound to.
 * 2. Port property can not be changed once set.
 * 
 * @author abhishek
 */
public interface RMIRegistryServiceMBean {

  /**
   * Returns the host on which rmiregistry listens for incoming connections
   *
   * @return the host on which rmiregistry listens for incoming connections
   */
  public String getHost();

  /**
   * Returns the port on which rmiregistry listens for incoming connections
   * 
   * @return the port on which rmiregistry listens for incoming connections
   */
  public int getPort();

  /**
   * Returns whether this MBean has been started and not yet stopped.
   * 
   * @return whether this MBean has been started and not yet stopped.
   * @see #start
   */
  public boolean isRunning();

  /**
   * Starts this MBean: rmiregistry can now accept incoming calls
   * 
   * @see #stop
   * @see #isRunning
   */
  public void start() throws RemoteException;

  /**
   * Stops this MBean: rmiregistry cannot accept anymore incoming calls
   * 
   * @see #start
   */
  public void stop() throws NoSuchObjectException;

  /**
   * Returns an array of the names bound in the rmiregistry
   * 
   * @return an array of the names bound in the rmiregistry
   * @see java.rmi.registry.Registry#list()
   */
  public String[] list() throws RemoteException;

  /**
   * Removes the binding for the specified <code>name</code> in the rmiregistry
   * 
   * @see java.rmi.registry.Registry#unbind(String)
   */
  public void unbind(String name) throws RemoteException, NotBoundException;
}
