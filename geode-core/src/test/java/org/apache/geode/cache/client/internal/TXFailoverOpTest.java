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

import static org.mockito.Mockito.*;

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManager;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest({TXManagerImpl.class})
public class TXFailoverOpTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  DummyManager manager;
  private EndpointManager endpointManager;
  private DummyQueueManager queueManager;
  private RegisterInterestTracker riTracker;

  protected int borrows;
  protected int returns;
  protected int invalidateConnections;
  protected int exchanges;
  protected int serverCrashes;
  protected int getPrimary;
  protected int getBackups;
  private CancelCriterion cancelCriterion;

  @Before
  public void setUp() {
    this.endpointManager = new DummyEndpointManager();
    this.queueManager = new DummyQueueManager();
    this.manager = new DummyManager();
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
  public void txFailoverThrowsTransactionExceptionBack() throws Exception {
    PoolImpl mockPool = mock(PoolImpl.class);
    when(mockPool.execute(any())).thenThrow(new TransactionException()).thenReturn(true);

    OpExecutorImpl exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 3,
        10, false, cancelCriterion, mockPool);
    exec.setupServerAffinity(Boolean.TRUE);

    TXStateProxy mockTXStateProxy = mock(TXStateProxy.class);
    PowerMockito.mockStatic(TXManagerImpl.class);
    PowerMockito.when(TXManagerImpl.getCurrentTXState()).thenReturn(mockTXStateProxy);

    expectedException.expect(TransactionException.class);
    TXFailoverOp.execute(exec, 1);
  }

  protected class DummyManager implements ConnectionManager {

    protected int numServers = Integer.MAX_VALUE;
    private int currentServer = 0;

    public DummyManager() {}

    @Override
    public void emergencyClose() {}

    @Override
    public Connection borrowConnection(long aquireTimeout) {
      borrows++;
      return new DummyConnection(new ServerLocation("localhost", currentServer++ % numServers));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.cache.client.internal.pooling.ConnectionManager#borrowConnection(org.apache.
     * geode.distributed.internal.ServerLocation, long)
     */
    @Override
    public Connection borrowConnection(ServerLocation server, long aquireTimeout,
        boolean onlyUseExistingCnx) {
      borrows++;
      return new DummyConnection(server);
    }

    @Override
    public void close(boolean keepAlive) {}

    @Override
    public void returnConnection(Connection connection) {
      returns++;

    }

    @Override
    public void returnConnection(Connection connection, boolean accessed) {
      returns++;

    }

    @Override
    public void start(ScheduledExecutorService backgroundProcessor) {}

    @Override
    public Connection exchangeConnection(Connection conn, Set excludedServers, long aquireTimeout) {
      if (excludedServers.size() >= numServers) {
        throw new NoAvailableServersException();
      }
      exchanges++;
      return new DummyConnection(new ServerLocation("localhost", currentServer++ % numServers));
    }

    @Override
    public int getConnectionCount() {
      return 0;
    }

    @Override
    public Connection getConnection(Connection conn) {
      return conn;
    }

    @Override
    public void activate(Connection conn) {}

    @Override
    public void passivate(Connection conn, boolean accessed) {}
  }

  protected class DummyConnection implements Connection {

    private ServerLocation server;

    public DummyConnection(ServerLocation serverLocation) {
      this.server = serverLocation;
    }

    @Override
    public void close(boolean keepAlive) throws Exception {}

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
    public ConnectionStats getStats() {
      return null;
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
      throw new SocketTimeoutException();
    }

    @Override
    public void emergencyClose() {}

    @Override
    public short getWanSiteVersion() {
      return -1;
    }

    @Override
    public void setWanSiteVersion(short wanSiteVersion) {}

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public OutputStream getOutputStream() {
      return null;
    }

    @Override
    public void setConnectionID(long id) {}

    @Override
    public long getConnectionID() {
      return 0;
    }
  }

  protected class DummyEndpointManager implements EndpointManager {

    @Override
    public void addListener(EndpointListener listener) {}

    @Override
    public void close() {}

    @Override
    public Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId) {
      return null;
    }

    @Override
    public Map getEndpointMap() {
      return null;
    }

    @Override
    public void removeListener(EndpointListener listener) {}

    @Override
    public void serverCrashed(Endpoint endpoint) {
      serverCrashes++;
    }

    @Override
    public int getConnectedServerCount() {
      return 0;
    }

    @Override
    public Map getAllStats() {
      return null;
    }

    @Override
    public String getPoolName() {
      return null;
    }
  }

  protected class DummyQueueManager implements QueueManager {

    int backups = 0;
    int currentServer = 0;

    public QueueConnections getAllConnectionsNoWait() {
      return getAllConnections();
    }

    @Override
    public void emergencyClose() {}

    @Override
    public QueueConnections getAllConnections() {
      return new QueueConnections() {
        @Override
        public List getBackups() {
          getBackups++;
          ArrayList result = new ArrayList(backups);
          for (int i = 0; i < backups; i++) {
            result.add(new DummyConnection(new ServerLocation("localhost", currentServer++)));
          }
          return result;
        }

        @Override
        public Connection getPrimary() {
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
    public void close(boolean keepAlive) {}

    @Override
    public void start(ScheduledExecutorService background) {}

    @Override
    public QueueState getState() {
      return null;
    }

    @Override
    public InternalPool getPool() {
      return null;
    }

    @Override
    public void readyForEvents(InternalDistributedSystem system) {}

    @Override
    public InternalLogWriter getSecurityLogger() {
      return null;
    }

    @Override
    public void checkEndpoint(ClientUpdater qc, Endpoint endpoint) {}
  }

}
