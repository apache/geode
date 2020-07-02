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
package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManager;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category(ClientServerTest.class)
public class OpExecutorImplJUnitTest {

  private DummyManager manager;
  private DummyEndpointManager endpointManager;
  private DummyQueueManager queueManager;
  private RegisterInterestTracker riTracker;

  private int borrows;
  private int returns;
  private int invalidateConnections;
  private int exchanges;
  private int serverCrashes;
  private int getPrimary;
  private int getBackups;
  private CancelCriterion cancelCriterion;

  @Before
  public void setUp() {
    endpointManager = new DummyEndpointManager();
    queueManager = new DummyQueueManager();
    manager = new DummyManager();
    riTracker = new RegisterInterestTracker();
    cancelCriterion = new CancelCriterion() {

      @Override
      public String cancelInProgress() {
        return null;
      }

      @Override
      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    };
  }

  @Test
  public void testExecute() {
    ExecutablePool exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 3,
        10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion, null);
    Object result = exec.execute(cnx -> "hello");

    assertThat(result).isEqualTo("hello");
    assertThat(borrows).isEqualTo(1);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(0);
    assertThat(serverCrashes).isEqualTo(0);

    reset();

    Throwable thrown = catchThrowable(() -> {
      exec.execute(cnx -> {
        throw new SocketTimeoutException("test");
      });
    });
    assertThat(thrown).isInstanceOf(ServerConnectivityException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(exchanges).isEqualTo(3);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(4);
    assertThat(serverCrashes).isEqualTo(0);

    reset();

    thrown = catchThrowable(() -> {
      exec.execute(cnx -> {
        throw new ServerOperationException("Something didn't work");
      });
    });
    assertThat(thrown).isInstanceOf(ServerOperationException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(0);
    assertThat(serverCrashes).isEqualTo(0);

    reset();

    thrown = catchThrowable(() -> {
      exec.execute(cnx -> {
        throw new IOException("Something didn't work");
      });
    });
    assertThat(thrown).isInstanceOf(ServerConnectivityException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(exchanges).isEqualTo(3);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(4);
    assertThat(serverCrashes).isEqualTo(4);
  }

  @Test
  public void testExecuteOncePerServer() {
    ExecutablePool exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, -1,
        10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion, null);

    manager.numServers = 5;

    Throwable thrown = catchThrowable(() -> {
      exec.execute(cnx -> {
        throw new IOException("Something didn't work");
      });
    });
    assertThat(thrown).isInstanceOf(ServerConnectivityException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(exchanges).isEqualTo(4);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(6);
    assertThat(serverCrashes).isEqualTo(6);
  }

  @Test
  public void testRetryFailedServers() {
    ExecutablePool exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 10,
        10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion, null);

    manager.numServers = 5;

    Throwable thrown = catchThrowable(() -> {
      exec.execute(cnx -> {
        throw new IOException("Something didn't work");
      });
    });
    assertThat(thrown).isInstanceOf(ServerConnectivityException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(exchanges).isEqualTo(10);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(11);
    assertThat(serverCrashes).isEqualTo(11);
  }

  @Test
  public void testExecuteOn() {
    ExecutablePool exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 3,
        10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion, null);
    ServerLocation server = new ServerLocation("localhost", -1);
    Object result = exec.executeOn(server, cnx -> "hello");

    assertThat(result).isEqualTo("hello");
    assertThat(borrows).isEqualTo(1);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(0);
    assertThat(serverCrashes).isEqualTo(0);

    reset();

    Throwable thrown = catchThrowable(() -> {
      exec.executeOn(server, cnx -> {
        throw new SocketTimeoutException("test");
      });
    });
    assertThat(thrown).isInstanceOf(ServerConnectivityException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(1);
    assertThat(serverCrashes).isEqualTo(0);

    reset();

    thrown = catchThrowable(() -> {
      exec.executeOn(server, cnx -> {
        throw new ServerOperationException("Something didn't work");
      });
    });
    assertThat(thrown).isInstanceOf(ServerOperationException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(0);
    assertThat(serverCrashes).isEqualTo(0);

    reset();

    thrown = catchThrowable(() -> {
      exec.executeOn(server, cnx -> {
        throw new Exception("Something didn't work");
      });
    });
    assertThat(thrown).isInstanceOf(ServerConnectivityException.class);

    assertThat(borrows).isEqualTo(1);
    assertThat(returns).isEqualTo(1);
    assertThat(invalidateConnections).isEqualTo(1);
    assertThat(serverCrashes).isEqualTo(1);
  }

  @Test
  public void testExecuteOnAllQueueServers() {
    ExecutablePool exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 3,
        10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion, null);
    exec.executeOnAllQueueServers(cnx -> "hello");

    assertThat(invalidateConnections).isEqualTo(0);
    assertThat(serverCrashes).isEqualTo(0);
    assertThat(getPrimary).isEqualTo(1);
    assertThat(getBackups).isEqualTo(1);

    reset();

    queueManager.backups = 3;
    exec.executeOnAllQueueServers(cnx -> {
      throw new SocketTimeoutException("test");
    });

    assertThat(invalidateConnections).isEqualTo(4);
    assertThat(serverCrashes).isEqualTo(0);
    assertThat(getPrimary).isEqualTo(1);
    assertThat(getBackups).isEqualTo(1);

    reset();

    queueManager.backups = 3;
    Object result = exec.executeOnQueuesAndReturnPrimaryResult(new Op() {
      private int i;

      @Override
      public Object attempt(ClientCacheConnection cnx) throws Exception {
        i++;
        if (i < 15) {
          throw new IOException("test");
        }
        return "hello";
      }
    });

    assertThat(result).isEqualTo("hello");
    assertThat(serverCrashes).isEqualTo(14);
    assertThat(invalidateConnections).isEqualTo(14);
    assertThat(getPrimary).isEqualTo(12);
    assertThat(getBackups).isEqualTo(1);
  }

  @Test
  public void executeWithServerAffinityDoesNotChangeInitialRetryCountOfZero() {
    OpExecutorImpl opExecutor =
        new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, -1,
            10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion,
            mock(PoolImpl.class));
    Op txSynchronizationOp = mock(TXSynchronizationOp.Impl.class);
    ServerLocation serverLocation = mock(ServerLocation.class);
    opExecutor.setAffinityRetryCount(0);

    opExecutor.executeWithServerAffinity(serverLocation, txSynchronizationOp);

    assertThat(opExecutor.getAffinityRetryCount()).isEqualTo(0);
  }

  @Test
  public void executeWithServerAffinityWithNonZeroAffinityRetryCountWillNotSetToZero() {
    OpExecutorImpl opExecutor =
        new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, -1,
            10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion,
            mock(PoolImpl.class));
    Op txSynchronizationOp = mock(TXSynchronizationOp.Impl.class);
    ServerLocation serverLocation = mock(ServerLocation.class);
    opExecutor.setAffinityRetryCount(1);

    opExecutor.executeWithServerAffinity(serverLocation, txSynchronizationOp);

    assertThat(opExecutor.getAffinityRetryCount()).isNotEqualTo(0);
  }

  @Test
  public void executeWithServerAffinityWithServerConnectivityExceptionIncrementsRetryCountAndResetsToZero() {
    OpExecutorImpl opExecutor =
        spy(new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, -1,
            10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion,
            mock(PoolImpl.class)));
    AbstractOp txSynchronizationOp = mock(TXSynchronizationOp.Impl.class);
    ServerLocation serverLocation = mock(ServerLocation.class);
    ServerConnectivityException serverConnectivityException =
        new ServerConnectivityException("test");
    doThrow(serverConnectivityException)
        .when(opExecutor)
        .executeOnServer(serverLocation, txSynchronizationOp, true, false);
    when(txSynchronizationOp.getMessage())
        .thenReturn(mock(Message.class));
    opExecutor.setupServerAffinity(true);
    opExecutor.setAffinityRetryCount(0);

    opExecutor.executeWithServerAffinity(serverLocation, txSynchronizationOp);

    verify(opExecutor, times(1)).setAffinityRetryCount(1);
    assertThat(opExecutor.getAffinityRetryCount()).isEqualTo(0);
  }

  @Test
  public void executeWithServerAffinityAndRetryCountGreaterThansTxRetryAttemptThrowsServerConnectivityException() {
    OpExecutorImpl opExecutor =
        spy(new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, -1,
            10, PoolFactory.DEFAULT_SERVER_CONNECTION_TIMEOUT, cancelCriterion,
            mock(PoolImpl.class)));
    AbstractOp txSynchronizationOp = mock(TXSynchronizationOp.Impl.class);
    ServerLocation serverLocation = mock(ServerLocation.class);
    ServerConnectivityException serverConnectivityException =
        new ServerConnectivityException("test");
    doThrow(serverConnectivityException)
        .when(opExecutor)
        .executeOnServer(serverLocation, txSynchronizationOp, true, false);
    when(txSynchronizationOp.getMessage()).thenReturn(mock(Message.class));
    opExecutor.setupServerAffinity(true);
    opExecutor.setAffinityRetryCount(OpExecutorImpl.TX_RETRY_ATTEMPT + 1);

    Throwable thrown = catchThrowable(() -> {
      opExecutor.executeWithServerAffinity(serverLocation, txSynchronizationOp);
    });
    assertThat(thrown).isSameAs(serverConnectivityException);
  }

  private void reset() {
    borrows = 0;
    returns = 0;
    invalidateConnections = 0;
    exchanges = 0;
    serverCrashes = 0;
    getPrimary = 0;
    getBackups = 0;
  }

  private class DummyManager implements ConnectionManager {

    private int numServers = Integer.MAX_VALUE;
    private int currentServer;

    @Override
    public void emergencyClose() {
      // nothing
    }

    @Override
    public ClientCacheConnection borrowConnection(long acquireTimeout) {
      borrows++;
      return new DummyConnection(new ServerLocation("localhost", currentServer++ % numServers));
    }

    @Override
    public ClientCacheConnection borrowConnection(ServerLocation server, long acquireTimeout,
        boolean onlyUseExistingCnx) {
      borrows++;
      return new DummyConnection(server);
    }

    @Override
    public void close(boolean keepAlive) {
      // nothing
    }

    @Override
    public void returnConnection(ClientCacheConnection connection) {
      returns++;

    }

    @Override
    public void returnConnection(ClientCacheConnection connection, boolean accessed) {
      returns++;

    }

    @Override
    public void start(ScheduledExecutorService backgroundProcessor) {
      // nothing
    }

    @Override
    public ClientCacheConnection exchangeConnection(ClientCacheConnection conn,
        Set<ServerLocation> excludedServers) {
      if (excludedServers.size() >= numServers) {
        throw new NoAvailableServersException("test");
      }
      exchanges++;
      return new DummyConnection(new ServerLocation("localhost", currentServer++ % numServers));
    }

    @Override
    public int getConnectionCount() {
      return 0;
    }
  }

  private class DummyConnection implements ClientCacheConnection {

    private final ServerLocation server;

    DummyConnection(ServerLocation serverLocation) {
      server = serverLocation;
    }

    @Override
    public void close(boolean keepAlive) {
      // nothing
    }

    @Override
    public void destroy() {
      invalidateConnections++;
    }

    @Override
    public boolean isDestroyed() {
      return false;
    }

    @Override
    public ByteBuffer getCommBuffer() {
      return null;
    }

    @Override
    public ServerLocation getServer() {
      return server;
    }

    @Override
    public Socket getSocket() {
      return null;
    }

    @Override
    public long getBirthDate() {
      return 0;
    }

    @Override
    public void setBirthDate(long ts) {
      // nothing
    }

    @Override
    public ConnectionStats getStats() {
      return null;
    }

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public int getDistributedSystemId() {
      return 0;
    }

    @Override
    public Endpoint getEndpoint() {
      return new Endpoint(null, null, null, null, null);
    }

    @Override
    public ServerQueueStatus getQueueStatus() {
      return null;
    }

    @Override
    public Object execute(Op op) throws Exception {
      return op.attempt(this);
    }

    @Override
    public void emergencyClose() {
      // nothing
    }

    @Override
    public short getWanSiteVersion() {
      return -1;
    }

    @Override
    public void setWanSiteVersion(short wanSiteVersion) {
      // nothing
    }

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public OutputStream getOutputStream() {
      return null;
    }

    @Override
    public void setConnectionID(long id) {
      // nothing
    }

    @Override
    public long getConnectionID() {
      return 0;
    }
  }

  private class DummyEndpointManager implements EndpointManager {

    @Override
    public void addListener(EndpointListener listener) {
      // nothing
    }

    @Override
    public void close() {
      // nothing
    }

    @Override
    public Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId) {
      return null;
    }

    @Override
    public Map<ServerLocation, Endpoint> getEndpointMap() {
      return null;
    }

    @Override
    public void removeListener(EndpointListener listener) {
      // nothing
    }

    @Override
    public void serverCrashed(Endpoint endpoint) {
      serverCrashes++;
    }

    @Override
    public int getConnectedServerCount() {
      return 0;
    }

    @Override
    public Map<ServerLocation, ConnectionStats> getAllStats() {
      return null;
    }

    @Override
    public String getPoolName() {
      return null;
    }
  }

  private class DummyQueueManager implements QueueManager {

    private int backups;
    private int currentServer;

    @Override
    public QueueConnections getAllConnectionsNoWait() {
      return getAllConnections();
    }

    @Override
    public void emergencyClose() {
      // nothing
    }

    @Override
    public QueueConnections getAllConnections() {
      return new QueueConnections() {
        @Override
        public List<ClientCacheConnection> getBackups() {
          getBackups++;
          List<ClientCacheConnection> result = new ArrayList<>(backups);
          for (int i = 0; i < backups; i++) {
            result.add(new DummyConnection(new ServerLocation("localhost", currentServer++)));
          }
          return result;
        }

        @Override
        public ClientCacheConnection getPrimary() {
          getPrimary++;
          return new DummyConnection(new ServerLocation("localhost", currentServer++));
        }

        @Override
        public QueueConnectionImpl getConnection(Endpoint ep) {
          return null;
        }
      };
    }

    @Override
    public void close(boolean keepAlive) {
      // nothing
    }

    @Override
    public void start(ScheduledExecutorService background) {
      // nothing
    }

    @Override
    public QueueState getState() {
      return null;
    }

    @Override
    public InternalPool getPool() {
      return null;
    }

    @Override
    public void readyForEvents(InternalDistributedSystem system) {
      // nothing
    }

    @Override
    public InternalLogWriter getSecurityLogger() {
      return null;
    }

    @Override
    public void checkEndpoint(ClientUpdater qc, Endpoint endpoint) {
      // nothing
    }
  }
}
