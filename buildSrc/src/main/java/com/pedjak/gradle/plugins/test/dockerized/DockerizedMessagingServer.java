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
package com.pedjak.gradle.plugins.test.dockerized;

import static java.util.stream.Collectors.toList;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.List;

import org.gradle.api.Action;
import org.gradle.api.UncheckedIOException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.concurrent.ExecutorFactory;
import org.gradle.internal.remote.Address;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.MessagingServer;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.internal.remote.internal.ConnectCompletion;
import org.gradle.internal.remote.internal.IncomingConnector;
import org.gradle.internal.remote.internal.hub.MessageHubBackedObjectConnection;
import org.gradle.internal.remote.internal.inet.MultiChoiceAddress;

/**
 * A copy of Gradle's MessageHubBackedServer, modified to insist on non-loopback
 * addresses (where Gradle's implementation prefers loopback addresses).
 */
public class DockerizedMessagingServer implements MessagingServer {
  private final IncomingConnector connector;
  private final ExecutorFactory executorFactory;

  public DockerizedMessagingServer(IncomingConnector connector, ExecutorFactory executorFactory) {
    this.connector = connector;
    this.executorFactory = executorFactory;
  }

  @Override
  public ConnectionAcceptor accept(Action<ObjectConnection> action) {
    return new RemoteConnectionAcceptor(
        connector.accept(new ConnectEventAction(action, executorFactory), true));
  }

  static class RemoteConnectionAcceptor implements ConnectionAcceptor {
    private static final Logger LOGGER = Logging.getLogger(RemoteConnectionAcceptor.class);

    private MultiChoiceAddress address;
    private final ConnectionAcceptor delegate;

    RemoteConnectionAcceptor(ConnectionAcceptor delegate) {
      this.delegate = delegate;
    }

    @Override
    public Address getAddress() {
      synchronized (delegate) {
        if (address == null) {
          MultiChoiceAddress original = (MultiChoiceAddress) delegate.getAddress();
          address = new MultiChoiceAddress(
              original.getCanonicalAddress(), original.getPort(), remoteAddresses());
        }
      }
      return address;
    }

    @Override
    public void requestStop() {
      delegate.requestStop();
    }

    @Override
    public void stop() {
      delegate.stop();
    }

    private static List<InetAddress> remoteAddresses() {
      try {
        return Collections.list(NetworkInterface.getNetworkInterfaces()).stream()
            .filter(RemoteConnectionAcceptor::isRemoteInterface)
            .flatMap(i -> Collections.list(i.getInetAddresses()).stream())
            .collect(toList());
      } catch (SocketException e) {
        throw new UncheckedIOException(e);
      }
    }

    private static boolean isRemoteInterface(NetworkInterface networkInterface) {
      try {
        return networkInterface.isUp() && !networkInterface.isLoopback();
      } catch (SocketException ex) {
        LOGGER.warn("Unable to inspect interface " + networkInterface);
        return false;
      }
    }
  }

  static class ConnectEventAction implements Action<ConnectCompletion> {
    private final Action<ObjectConnection> action;
    private final ExecutorFactory executorFactory;

    public ConnectEventAction(Action<ObjectConnection> action, ExecutorFactory executorFactory) {
      this.executorFactory = executorFactory;
      this.action = action;
    }

    @Override
    public void execute(ConnectCompletion completion) {
      action.execute(new MessageHubBackedObjectConnection(executorFactory, completion));
    }
  }
}
