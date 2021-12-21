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
package org.apache.geode.admin.jmx.internal;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

/**
 * This MBean is an implementation of {@link RMIRegistryServiceMBean}.
 *
 */
public class RMIRegistryService implements RMIRegistryServiceMBean {
  /* RMI Registry host */
  private String host;
  /* RMI Registry port */
  private int port;
  /* RMI Registry */
  private Registry registry;
  /* RMI Server Socket Factory */
  private RMIServerSocketFactory ssf;
  /* Whether RMI Registry is started & running */
  private boolean isRunning;

  /**
   * Constructor to configure RMI Registry to start using default RMI Registry port:
   * {@link Registry#REGISTRY_PORT}
   */
  public RMIRegistryService() {
    this(Registry.REGISTRY_PORT);
  }

  /**
   * Constructor to configure RMI Registry to start using given RMI Registry port.
   *
   * @param port to run RMI Registry on
   */
  public RMIRegistryService(int port) {
    setPort(port);
  }

  /**
   * Constructor to configure RMI Registry to start using given RMI Registry port & host bind
   * address.
   *
   * @param host to bind RMI Registry to
   * @param port to run RMI Registry on
   *
   * @throws UnknownHostException if IP Address can not be resolved for the given host string while
   *         creating the RMIServerSocketFactory
   */
  public RMIRegistryService(String host, int port) throws UnknownHostException {
    setPort(port);
    setHost(host);
    if (host != null && !host.trim().equals("")) {
      ssf = new RMIServerSocketFactoryImpl(host);
    }
  }

  /**
   * Returns the host on which rmiregistry listens for incoming connections
   *
   * @return the host on which rmiregistry listens for incoming connections
   */
  @Override
  public String getHost() {
    return host;
  }

  /**
   * Sets the host on which rmiregistry listens for incoming connections
   *
   * @param host the host on which rmiregistry listens for incoming connections
   */
  protected void setHost(String host) {
    if (isRunning()) {
      throw new IllegalStateException("RMIRegistryService is running, cannot change the host");
    }
    this.host = host;
  }

  /**
   * Returns the port on which rmiregistry listens for incoming connections
   *
   * @return the port on which rmiregistry listens for incoming connections
   */
  @Override
  public int getPort() {
    return port;
  }

  /**
   * Sets the port on which rmiregistry listens for incoming connections
   *
   * @param port the port on which rmiregistry listens for incoming connections
   */
  protected void setPort(int port) {
    if (isRunning()) {
      throw new IllegalStateException("RMIRegistryService is running, cannot change the port");
    }
    this.port = port;
  }

  /**
   * Starts this MBean: rmiregistry can now accept incoming calls
   *
   * @see #stop
   * @see #isRunning
   */
  @Override
  public synchronized void start() throws RemoteException {
    if (!isRunning()) {
      if (ssf != null) {
        registry = LocateRegistry.createRegistry(port, null, // RMIClientSocketFactory
            ssf); // RMIServerSocketFactory
      } else {
        registry = LocateRegistry.createRegistry(port);
      }

      isRunning = true;
    }
  }

  /**
   * Returns whether this MBean has been started and not yet stopped.
   *
   * @return whether this MBean has been started and not yet stopped.
   * @see #start
   */
  @Override
  public synchronized boolean isRunning() {
    return isRunning;
  }

  /**
   * Stops this MBean: rmiregistry cannot accept anymore incoming calls
   *
   * @see #start
   */
  @Override
  public synchronized void stop() throws NoSuchObjectException {
    if (isRunning()) {
      isRunning = !UnicastRemoteObject.unexportObject(registry, true);
    }
  }

  /**
   * Returns an array of the names bound in the rmiregistry
   *
   * @return an array of the names bound in the rmiregistry
   * @see java.rmi.registry.Registry#list()
   */
  @Override
  public String[] list() throws RemoteException {
    if (!isRunning()) {
      throw new IllegalStateException("RMIRegistryService is not running");
    }
    return registry.list();
  }

  /**
   * Removes the binding for the specified <code>name</code> in the rmiregistry
   *
   * @see java.rmi.registry.Registry#unbind(String)
   */
  @Override
  public void unbind(String name) throws RemoteException, NotBoundException {
    if (!isRunning()) {
      throw new IllegalStateException("RMIRegistryService is not running");
    }
    registry.unbind(name);
  }
}


/**
 * Custom implementation of the {@link RMIServerSocketFactory}
 *
 */
class RMIServerSocketFactoryImpl implements RMIServerSocketFactory {
  /* IP address to use for creating ServerSocket */
  private final InetAddress bindAddress;

  /**
   * Constructs a RMIServerSocketFactory. The given rmiBindAddress is used to bind the ServerSockets
   * created from this factory.
   *
   * @param rmiBindAddress String representation of the address to bind the ServerSockets to
   *
   * @throws UnknownHostException if IP Address can not be resolved for the given host string
   */
  /* default */ RMIServerSocketFactoryImpl(String rmiBindAddress) throws UnknownHostException {
    bindAddress = InetAddress.getByName(rmiBindAddress);
  }

  /**
   * Create a server socket on the specified port (port 0 indicates an anonymous port).
   *
   * @param port the port number
   * @return the server socket on the specified port
   * @exception IOException if an I/O error occurs during server socket creation
   */
  @Override
  public ServerSocket createServerSocket(int port) throws IOException {
    return new ServerSocket(port, 0/* backlog - for '0' internally uses the default */,
        bindAddress);
  }
}
