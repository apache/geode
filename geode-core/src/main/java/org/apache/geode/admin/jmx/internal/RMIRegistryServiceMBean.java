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
package org.apache.geode.admin.jmx.internal;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * This interface is similar to mx4j.tools.naming.NamingServiceMBean.
 * Features that differ are:
 * 1. This MBean interface additionally provides a way to specify the host that 
 * the RMI Registry should get bound to.
 * 2. Port property can not be changed once set.
 * 
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
