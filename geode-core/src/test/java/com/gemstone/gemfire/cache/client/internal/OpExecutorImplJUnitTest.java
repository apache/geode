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
package com.gemstone.gemfire.cache.client.internal;

import static org.junit.Assert.*;

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

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OpExecutorImplJUnitTest {

  DummyManager manager;
  private LogWriter logger;
  private DummyEndpointManager endpointManager;
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
    this.logger = new LocalLogWriter(InternalLogWriter.FINEST_LEVEL, System.out);
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
  public void testExecute() throws Exception {
    OpExecutorImpl exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 3, 10, false, cancelCriterion, null);
    Object result = exec.execute(new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        return "hello";
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    assertEquals("hello", result);
    assertEquals(1, borrows);
    assertEquals(1, returns);
    assertEquals(0, invalidateConnections);
    assertEquals(0, serverCrashes);

    reset();
    
    try {
    result = exec.execute(new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        throw new SocketTimeoutException();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    fail("Should have got an exception");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(3, exchanges);
    assertEquals(1, returns);
    assertEquals(4, invalidateConnections);
    assertEquals(0, serverCrashes);
    
    reset();
    
    try {
      result = exec.execute(new Op() {
        @Override
        public Object attempt(Connection cnx) throws Exception {
          throw new ServerOperationException("Something didn't work");
        }
        @Override
        public boolean useThreadLocalConnection() {
          return true;
        }
      });
      fail("Should have got an exception");
    } catch(ServerOperationException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(1, returns);
    assertEquals(0, invalidateConnections);
    assertEquals(0, serverCrashes);
    
    reset();
    
    try {
      result = exec.execute(new Op() {
        @Override
        public Object attempt(Connection cnx) throws Exception {
          throw new IOException("Something didn't work");
        }
        @Override
        public boolean useThreadLocalConnection() {
          return true;
        }
      });
      fail("Should have got an exception");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(3, exchanges);
    assertEquals(1, returns);
    assertEquals(4, invalidateConnections);
    assertEquals(4, serverCrashes);
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

  @Test
  public void testExecuteOncePerServer() throws Exception {
    OpExecutorImpl exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, -1, 10, false, cancelCriterion, null);
    
    manager.numServers = 5;
    try {
      exec.execute(new Op() {
        @Override
        public Object attempt(Connection cnx) throws Exception {
          throw new IOException("Something didn't work");
        }
        @Override
        public boolean useThreadLocalConnection() {
          return true;
        }
      });
      fail("Should have got an exception");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(4, exchanges);
    assertEquals(1, returns);
    assertEquals(6, invalidateConnections);
    assertEquals(6, serverCrashes);
  }

  @Test
  public void testRetryFailedServers() throws Exception {
    OpExecutorImpl exec = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, 10, 10, false, cancelCriterion, null);
    
    manager.numServers = 5;
    try {
      exec.execute(new Op() {
        @Override
        public Object attempt(Connection cnx) throws Exception {
          throw new IOException("Something didn't work");
        }
        @Override
        public boolean useThreadLocalConnection() {
          return true;
        }
      });
      fail("Should have got an exception");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(10, exchanges);
    assertEquals(1, returns);
    assertEquals(11, invalidateConnections);
    assertEquals(11, serverCrashes);
  }

  @Test
  public void testExecuteOn() throws Exception {
    OpExecutorImpl exec = new OpExecutorImpl(manager,queueManager, endpointManager, riTracker, 3, 10, false, cancelCriterion, null);
    ServerLocation server = new ServerLocation("localhost", -1);
    Object result = exec.executeOn(server, new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        return "hello";
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    assertEquals("hello", result);
    assertEquals(1, borrows);
    assertEquals(1, returns);
    assertEquals(0, invalidateConnections);
    assertEquals(0, serverCrashes);

    reset();
    
    try {
    result = exec.executeOn(server, new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        throw new SocketTimeoutException();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    fail("Should have got an exception");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(1, returns);
    assertEquals(1, invalidateConnections);
    assertEquals(0, serverCrashes);
    
    reset();
    
    try {
      result = exec.executeOn(server,new Op() {
        @Override
        public Object attempt(Connection cnx) throws Exception {
          throw new ServerOperationException("Something didn't work");
        }
        @Override
        public boolean useThreadLocalConnection() {
          return true;
        }
      });
      fail("Should have got an exception");
    } catch(ServerOperationException expected) {
      //do nothing
    }
    assertEquals(1, borrows);
    assertEquals(1, returns);
    assertEquals(0, invalidateConnections);
    assertEquals(0, serverCrashes);
    
    reset();

    {
      final String expectedEx = "java.lang.Exception";
      final String addExpected =
        "<ExpectedException action=add>" + expectedEx + "</ExpectedException>";
      final String removeExpected =
        "<ExpectedException action=remove>" + expectedEx + "</ExpectedException>";
      logger.info(addExpected);
      try {
        result = exec.executeOn(server,new Op() {
          @Override
            public Object attempt(Connection cnx) throws Exception {
              throw new Exception("Something didn't work");
            }
            @Override
            public boolean useThreadLocalConnection() {
              return true;
            }
          });
        fail("Should have got an exception");
      } catch(ServerConnectivityException expected) {
        //do nothing
      } finally {
        logger.info(removeExpected);
      }
    }
    assertEquals(1, borrows);
    assertEquals(1, returns);
    assertEquals(1, invalidateConnections);
    assertEquals(1, serverCrashes);
  }

  @Test
  public void testExecuteOnAllQueueServers() {
    OpExecutorImpl exec = new OpExecutorImpl(manager,queueManager, endpointManager, riTracker, 3, 10, false, cancelCriterion, null);
    exec.executeOnAllQueueServers(new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        return "hello";
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    assertEquals(0, invalidateConnections);
    assertEquals(0, serverCrashes);
    assertEquals(1, getPrimary);
    assertEquals(1, getBackups);
    
    reset();
    
    queueManager.backups = 3;
    exec.executeOnAllQueueServers(new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        throw new SocketTimeoutException();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    
    assertEquals(4, invalidateConnections);
    assertEquals(0, serverCrashes);
    assertEquals(1, getPrimary);
    assertEquals(1, getBackups);
    
    reset();
    
    queueManager.backups = 3;
    Object result = exec.executeOnQueuesAndReturnPrimaryResult(new Op() {
      int i = 0;
      @Override
      public Object attempt(Connection cnx) throws Exception {
        i++;
        if(i < 15) {
          throw new IOException();
        }
        return "hello";
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    });
    
    assertEquals("hello", result);
    assertEquals(14, serverCrashes);
    assertEquals(14, invalidateConnections);
    assertEquals(12, getPrimary);
    assertEquals(1, getBackups);
    
  }

  @Test
  public void testThreadLocalConnection() {
    OpExecutorImpl exec = new OpExecutorImpl(manager,queueManager, endpointManager, riTracker, 3, 10, true, cancelCriterion, null);
    ServerLocation server = new ServerLocation("localhost", -1);
    Op op = new Op() {
      @Override
      public Object attempt(Connection cnx) throws Exception {
        //do nothing
        return cnx;
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    exec.execute(op);
    assertEquals(1, borrows);
    assertEquals(0, returns);
    reset();
    exec.execute(op);
    assertEquals(0, borrows);
    assertEquals(0, returns);
    reset();
    exec.executeOn(server, op);
    assertEquals(1, borrows);
    assertEquals(0, returns);
    reset();
    exec.executeOn(server, op);
    assertEquals(0, borrows);
    assertEquals(0, returns);
    exec.execute(op);
    reset();
    assertEquals(0, borrows);
    assertEquals(0, returns);
  }
  
  private class DummyManager implements ConnectionManager {

    protected int numServers  = Integer.MAX_VALUE;
    private int currentServer = 0;

    public DummyManager() {
    }

    @Override
    public void emergencyClose() {
    }

    @Override
    public Connection borrowConnection(long aquireTimeout) {
      borrows++;
      return new DummyConnection(new ServerLocation("localhost", currentServer++ % numServers));
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager#borrowConnection(com.gemstone.gemfire.distributed.internal.ServerLocation, long)
     */
    @Override
    public Connection borrowConnection(ServerLocation server, long aquireTimeout,boolean onlyUseExistingCnx) {
      borrows++;
      return new DummyConnection(server);
    }

    @Override
    public void close(boolean keepAlive) {
    }

    @Override
    public void returnConnection(Connection connection) {
      returns++;
      
    }

    @Override
    public void returnConnection(Connection connection, boolean accessed) {
      returns++;
      
    }

    @Override
    public void start(ScheduledExecutorService backgroundProcessor) {
    }

    @Override
    public Connection exchangeConnection(Connection conn, Set excludedServers, long aquireTimeout) {
      if(excludedServers.size() >= numServers) {
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
    public void activate(Connection conn) {
    }

    @Override
    public void passivate(Connection conn, boolean accessed) {
    }
  }
  
  private class DummyConnection implements Connection {
    
    private ServerLocation server;

    public DummyConnection(ServerLocation serverLocation) {
      this.server = serverLocation;
    }

    @Override
    public void close(boolean keepAlive) throws Exception {
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
    public ConnectionStats getStats() {
      return null;
    }

    @Override
    public int getDistributedSystemId() {
      return 0;
    }

    @Override
    public Endpoint getEndpoint() {
      return new Endpoint(null,null,null,null, null);
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
    }

    @Override
    public short getWanSiteVersion(){
      return -1;
    }

    @Override
    public void setWanSiteVersion(short wanSiteVersion){
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
    }

    @Override
    public long getConnectionID() {
      return 0;
    }
  }
  
  private class DummyEndpointManager implements EndpointManager {

    @Override
    public void addListener(EndpointListener listener) {
    }

    @Override
    public void close() {
    }

    @Override
    public Endpoint referenceEndpoint(ServerLocation server, DistributedMember memberId) {
      return null;
    }

    @Override
    public Map getEndpointMap() {
      return null;
    }

    @Override
    public void removeListener(EndpointListener listener) {
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
    public Map getAllStats() {
      return null;
    }

    @Override
    public String getPoolName() {
      return null;
    }
  }
  
  private class DummyQueueManager implements QueueManager {

    int backups = 0;
    int currentServer = 0;

    public QueueConnections getAllConnectionsNoWait() {
      return getAllConnections();
    }

    @Override
    public void emergencyClose() {
    }

    @Override
    public QueueConnections getAllConnections() {
      return new QueueConnections() {
        @Override
        public List getBackups() {
          getBackups++;
          ArrayList result = new ArrayList(backups);
          for(int i = 0; i < backups; i++) {
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
    public void close(boolean keepAlive) {
    }

    @Override
    public void start(ScheduledExecutorService background) {
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
    }

    @Override
    public InternalLogWriter getSecurityLogger() {
      return null;
    }

    @Override
    public void checkEndpoint(ClientUpdater qc, Endpoint endpoint) {
    }
  }

}
