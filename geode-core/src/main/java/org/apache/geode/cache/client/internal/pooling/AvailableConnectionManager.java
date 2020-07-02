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
package org.apache.geode.cache.client.internal.pooling;

import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Predicate;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.client.internal.ClientCacheConnection;

/**
 * This manager maintains a collection of PooledConnection instances.
 * PooledConnections can be added to this manager using one of the add* methods.
 * PooledConnections can be taken out of this manager to be used by calling one of the use* methods.
 * Once a PooledConnection has reached the end of its life it can be permanently removed from this
 * manager using the remove method.
 * The manager has a concept of "first" which identifies the PooledConnections that should be
 * preferred to be used by the next use* method, and "last" which identifies PooledConnections that
 * should only be used as a last resort.
 *
 */
public class AvailableConnectionManager {
  private final Deque<ClientCacheConnection> connections =
      new ConcurrentLinkedDeque<>();

  /**
   * Remove, activate, and return the first connection.
   * Connections that can not be activated will be removed from the manager but not returned.
   *
   * @return the activated connection or null if none found
   */
  public ClientCacheConnection useFirst() {
    ClientCacheConnection connection;
    while (null != (connection = connections.pollFirst())) {
      if (connection.activate()) {
        return connection;
      }
    }
    return null;
  }

  /**
   * Removes the first connection equal to the given connection from this manager.
   *
   * @param connection the connection to remove
   * @return true if a connection was removed; otherwise false
   */
  public boolean remove(ClientCacheConnection connection) {
    return connections.remove(connection);
  }

  /**
   * Remove, activate, and return the first connection that matches the predicate.
   * Connections that can not be activated will be removed from the manager but not returned.
   *
   * @param predicate that the connections are matched against
   * @return the activated connection or null if none found
   */
  public ClientCacheConnection useFirst(Predicate<ClientCacheConnection> predicate) {
    final EqualsWithPredicate equalsWithPredicate = new EqualsWithPredicate(predicate);
    while (connections.removeFirstOccurrence(equalsWithPredicate)) {
      ClientCacheConnection connection = equalsWithPredicate.getConnectionThatMatched();
      if (connection.activate()) {
        // Need to recheck the predicate after we have activated.
        // Until activated load conditioning can change the server
        // we are connected to.
        if (predicate.test(connection)) {
          return connection;
        } else {
          addLast(connection, false);
        }
      }
    }
    return null;
  }

  /**
   * Passivate and add the given connection to this manager as its first, highest priority
   * connection.
   *
   * @param connection the connection to passivate and add
   * @param accessed true if the connection was used by the caller, false otherwise
   */
  public void addFirst(ClientCacheConnection connection, boolean accessed) {
    passivate(connection, accessed);
    connections.addFirst(connection);
  }

  /**
   * Passivate and add the given connection to this manager as its last, lowest priority connection.
   *
   * @param connection the connection to passivate and add
   * @param accessed true if the connection was used by the caller, false otherwise
   */
  public void addLast(ClientCacheConnection connection, boolean accessed) {
    passivate(connection, accessed);
    connections.addLast(connection);
  }

  private void passivate(ClientCacheConnection connection, boolean accessed) {
    // thread local connections are already passive at this point
    if (connection.isActive()) {
      connection.passivate(accessed);
    }
  }

  // used by unit tests
  @VisibleForTesting
  Deque<ClientCacheConnection> getDeque() {
    return connections;
  }

  /**
   * This class exists so that we can use ConcurrentLinkedDeque removeFirstOccurrence.
   * We want to efficiently scan the Deque for an item that matches the predicate without changing
   * the position of items that do not match. We also need to know the identity of the first item
   * that did match.
   */
  private static class EqualsWithPredicate {
    private final Predicate<ClientCacheConnection> predicate;
    private ClientCacheConnection connectionThatMatched;

    EqualsWithPredicate(Predicate<ClientCacheConnection> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ClientCacheConnection)) {
        return false;
      }
      ClientCacheConnection pooledConnection = (ClientCacheConnection) o;
      if (predicate.test(pooledConnection)) {
        this.connectionThatMatched = pooledConnection;
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(predicate, connectionThatMatched);
    }

    public ClientCacheConnection getConnectionThatMatched() {
      return this.connectionThatMatched;
    }
  }
}
