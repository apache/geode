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
package org.apache.geode.gradle.test.dockerized;

import org.gradle.api.Action;
import org.gradle.internal.concurrent.ExecutorFactory;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.MessagingServer;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.internal.remote.internal.ConnectCompletion;
import org.gradle.internal.remote.internal.IncomingConnector;
import org.gradle.internal.remote.internal.hub.MessageHubBackedObjectConnection;

/**
 * A copy of Gradle's MessageHubBackedServer, modified to insist on non-loopback addresses. Gradle's
 * implementation prefers loopback addresses, which we cannot use to communicate with a process
 * running in a Docker container.
 */
public class DockerMessagingServer implements MessagingServer {
  private final IncomingConnector connector;
  private final ExecutorFactory executorFactory;

  public DockerMessagingServer(IncomingConnector connector, ExecutorFactory executorFactory) {
    this.connector = connector;
    this.executorFactory = executorFactory;
  }

  /**
   * Transforms the connection acceptor produced by the connector into one that uses addresses from
   * only non-loopback interfaces.
   */
  @Override
  public ConnectionAcceptor accept(Action<ObjectConnection> action) {
    ConnectEventAction connectEventAction = new ConnectEventAction(action, executorFactory);
    ConnectionAcceptor originalConnectionAcceptor = connector.accept(connectEventAction, true);
    return new NonLoopbackConnectionAcceptor(originalConnectionAcceptor);
  }

  /**
   * A copy of Gradle's MessageHubBackedServer.ConnectionEvent.
   */
  private static class ConnectEventAction implements Action<ConnectCompletion> {
    private final Action<ObjectConnection> action;
    private final ExecutorFactory executorFactory;

    public ConnectEventAction(Action<ObjectConnection> action, ExecutorFactory executorFactory) {
      this.action = action;
      this.executorFactory = executorFactory;
    }

    @Override
    public void execute(ConnectCompletion completion) {
      action.execute(new MessageHubBackedObjectConnection(executorFactory, completion));
    }
  }
}
