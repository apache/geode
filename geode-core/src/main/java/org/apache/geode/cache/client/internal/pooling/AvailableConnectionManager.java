package org.apache.geode.cache.client.internal.pooling;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.function.Predicate;


public class AvailableConnectionManager {
  private final ConcurrentLinkedDeque<PooledConnection> availableConnections =
      new ConcurrentLinkedDeque<>();

  public PooledConnection getConnection() {
    while (true) {
      PooledConnection connection = availableConnections.pollFirst();
      if (null == connection) {
        return null;
      }
      try {
        connection.activate();
        return connection;
      } catch (ConnectionDestroyedException ignored) {
        // loop around and try the next one
      }
    }
  }

  public boolean removeConnection(PooledConnection connection) {
    return availableConnections.remove(connection);
  }

  /**
   * @param predicate to test connection against
   * @return A PooledConnection if one matches the predicate, otherwise null.
   */
  public PooledConnection findConnection(Predicate<PooledConnection> predicate) {
    for (int i = availableConnections.size(); i > 0; --i) {
      PooledConnection connection = availableConnections.pollFirst();
      if (null == connection) {
        break;
      }

      if (predicate.test(connection)) {
        try {
          connection.activate();
          return connection;
        } catch (ConnectionDestroyedException ignored) {
          continue;
        }
      }

      availableConnections.offerLast(connection);
    }

    return null;
  }

  public void returnConnection(PooledConnection connection, boolean accessed, boolean addToTail) {
    // thread local connections are already passive at this point
    if (connection.isActive()) {
      connection.passivate(accessed);
    }

    if (addToTail) {
      availableConnections.addLast(connection);
    } else {
      availableConnections.addFirst(connection);
    }
  }
}
