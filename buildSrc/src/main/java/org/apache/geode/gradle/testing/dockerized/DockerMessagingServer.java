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
package org.apache.geode.gradle.testing.dockerized;

import org.gradle.api.Action;
import org.gradle.internal.concurrent.ExecutorFactory;
import org.gradle.internal.remote.ConnectionAcceptor;
import org.gradle.internal.remote.MessagingServer;
import org.gradle.internal.remote.ObjectConnection;
import org.gradle.internal.remote.internal.ConnectCompletion;
import org.gradle.internal.remote.internal.IncomingConnector;
import org.gradle.internal.remote.internal.hub.MessageHubBackedObjectConnection;

/**
 * A copy of MessageHubBackedServer from Gradle v6.8.3, modified to accept connections from
 * processes running in Docker containers.
 */
public class DockerMessagingServer implements MessagingServer {
  private final IncomingConnector connector;
  private final ExecutorFactory executorFactory;

  public DockerMessagingServer(IncomingConnector connector, ExecutorFactory executorFactory) {
    this.connector = connector;
    this.executorFactory = executorFactory;
  }

  /**
   * Transforms Gradle's standard connection acceptor into one that will accept connections from
   * worker processes in Docker containers. A connection acceptor reports a list of candidate
   * addresses for worker processes to try to connect to. Gradle's standard acceptor reports the
   * host's loopback addresses, which processes in Docker containers cannot use. The transformed
   * acceptor instead reports a list of non-loopback addresses, and the Dockerized process will be
   * able to use at least one of those to connect to this server.
   */
  @Override
  public ConnectionAcceptor accept(Action<ObjectConnection> action) {
    ConnectEventAction connectEventAction = new ConnectEventAction(action);
    ConnectionAcceptor originalConnectionAcceptor = connector.accept(connectEventAction, true);
    return new DockerConnectionAcceptor(originalConnectionAcceptor);
  }

  /**
   * An unmodified copy of MessageHubBackedServer.ConnectionEvent from Gradle v6.8.3.
   */
  private class ConnectEventAction implements Action<ConnectCompletion> {
    private final Action<ObjectConnection> action;

    public ConnectEventAction(Action<ObjectConnection> action) {
      this.action = action;
    }

    @Override
    public void execute(ConnectCompletion completion) {
      action.execute(new MessageHubBackedObjectConnection(executorFactory, completion));
    }
  }
}
