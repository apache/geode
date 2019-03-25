package org.apache.geode.cache.client.internal.pooling;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Predicate;

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
  private final ConcurrentLinkedDeque<PooledConnection> connections =
      new ConcurrentLinkedDeque<>();

  /**
   * Remove, activate, and return the first connection.
   * Connections that can not be activated will be removed
   * from the manager but not returned.
   *
   * @return the activated connection or null if none found
   */
  public PooledConnection useFirst() {
    PooledConnection connection;
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
  public boolean remove(PooledConnection connection) {
    return connections.remove(connection);
  }

  /**
   * This class exists so that we can use ConcurrentLinkedDeque removeFirstOccurrence.
   * We want to efficiently scan the deque for an item that matches the predicate
   * without changing the position of items that do not match. We also need
   * to know the identity of the first item that did match.
   */
  private static class EqualsWithPredicate {
    private final Predicate<PooledConnection> predicate;
    private PooledConnection connectionThatMatched;

    EqualsWithPredicate(Predicate<PooledConnection> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean equals(Object o) {
      PooledConnection pooledConnection = (PooledConnection) o;
      if (predicate.test(pooledConnection)) {
        this.connectionThatMatched = pooledConnection;
        return true;
      }
      return false;
    }

    public PooledConnection getConnectionThatMatched() {
      return this.connectionThatMatched;
    }
  }

  /**
   * Remove, activate, and return the first connection that matches the predicate.
   * Connections that can not be activated will be removed
   * from the manager but not returned.
   *
   * @param predicate that the connections are matched against
   * @return the activated connection or null if none found
   */
  public PooledConnection useFirst(Predicate<PooledConnection> predicate) {
    final EqualsWithPredicate equalsWithPredicate = new EqualsWithPredicate(predicate);
    while (connections.removeFirstOccurrence(equalsWithPredicate)) {
      PooledConnection connection = equalsWithPredicate.getConnectionThatMatched();
      if (connection.activate()) {
        return connection;
      }
    }
    return null;
  }

  /**
   * Passivate and add the given connection to this manager as its first,
   * highest priority, connection.
   *
   * @param connection the connection to passivate and add
   * @param accessed true if the connection was used by the caller, false otherwise
   */
  public void addFirst(PooledConnection connection, boolean accessed) {
    passivate(connection, accessed);
    connections.addFirst(connection);
  }

  /**
   * Passivate and add the given connection to this manager as its last,
   * lowest priority, connection.
   *
   * @param connection the connection to passivate and add
   * @param accessed true if the connection was used by the caller, false otherwise
   */
  public void addLast(PooledConnection connection, boolean accessed) {
    passivate(connection, accessed);
    connections.addLast(connection);
  }

  private void passivate(PooledConnection connection, boolean accessed) {
    // thread local connections are already passive at this point
    if (connection.isActive()) {
      connection.passivate(accessed);
    }
  }
}
