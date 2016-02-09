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
package com.gemstone.gemfire.cache.client.internal.pooling;

import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.client.AllConnectionsInUseException;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.internal.ClientUpdater;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.ConnectionFactory;
import com.gemstone.gemfire.cache.client.internal.ConnectionStats;
import com.gemstone.gemfire.cache.client.internal.Endpoint;
import com.gemstone.gemfire.cache.client.internal.EndpointManager;
import com.gemstone.gemfire.cache.client.internal.EndpointManagerImpl;
import com.gemstone.gemfire.cache.client.internal.Op;
import com.gemstone.gemfire.cache.client.internal.QueueManager;
import com.gemstone.gemfire.cache.client.internal.ServerBlackList;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PoolStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerQueueStatus;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author dsmith
 *
 */
@Category(IntegrationTest.class)
public class ConnectionManagerJUnitTest {
  private static final long TIMEOUT = 30 * 1000;
  //This is added for some windows machines which think the connection expired
  //before the idle timeout due to precision issues.
  private static final long ALLOWABLE_ERROR_IN_EXPIRATION= 20; //milliseconds
  ConnectionManager manager;
  private InternalLogWriter logger;
  protected DummyFactory factory;
  private DistributedSystem ds;
  private ScheduledExecutorService background;
  protected EndpointManager endpointManager;
  private CancelCriterion cancelCriterion;
  private PoolStats poolStats;
  
  @Before
  public void setUp() {
    this.logger = new LocalLogWriter(InternalLogWriter.FINEST_LEVEL, System.out);
    factory = new DummyFactory();
    
    Properties properties = new Properties();
    properties.put(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.put(DistributionConfig.LOCATORS_NAME, "");
    ds = DistributedSystem.connect(properties);
    background = Executors.newSingleThreadScheduledExecutor();
    poolStats = new PoolStats(ds, "connectionManagerJUnitTest");
    endpointManager = new EndpointManagerImpl("pool", ds, ds.getCancelCriterion(), poolStats);
    cancelCriterion = new CancelCriterion() {

      public String cancelInProgress() {
        return null;
      }

      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    };
  }
  
  @After
  public void tearDown() throws InterruptedException {
    ds.disconnect();
    if(manager != null) {
      manager.close(false);
    }
    background.shutdownNow();
  }
  
  @Test
  public void testGet() throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    manager = new ConnectionManagerImpl("pool", factory,endpointManager, 3, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    Connection conn[] = new Connection[4];
    
    conn[0] = manager.borrowConnection(0);
    Assert.assertEquals(1,factory.creates);
    
    manager.returnConnection(conn[0]);
    conn[0] = manager.borrowConnection(0);
    Assert.assertEquals(1,factory.creates);
    conn[1] = manager.borrowConnection(0);
    manager.returnConnection(conn[0]);
    manager.returnConnection(conn[1]);
    Assert.assertEquals(2,factory.creates);
    
    conn[0] = manager.borrowConnection(0);
    conn[1] = manager.borrowConnection(0);
    conn[2] = manager.borrowConnection(0);
    Assert.assertEquals(3,factory.creates);
  
    try {
      conn[4] = manager.borrowConnection(10);
      fail("Should have received an all connections in use exception");
    } catch(AllConnectionsInUseException e) {
      //expected exception
    }
  }
  
  @Test
  public void testPrefill() throws InterruptedException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 2, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    final String descrip = manager.toString();
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return factory.creates == 2 && factory.destroys == 0;
      }
      public String description() {
        return "waiting for manager " + descrip; 
      }
    };
    Wait.waitForCriterion(ev, 200, 200, true);
  }
  
  @Test
  public void testInvalidateConnection() throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, 0L, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    Connection conn = manager.borrowConnection(0);
    Assert.assertEquals(1,factory.creates);
    Assert.assertEquals(0,factory.destroys);
    conn.destroy();
    manager.returnConnection(conn);
    Assert.assertEquals(1,factory.creates);
    Assert.assertEquals(1,factory.destroys);
    conn = manager.borrowConnection(0);
    Assert.assertEquals(2,factory.creates);
    Assert.assertEquals(1,factory.destroys);
  }
  
  @Test
  public void testInvalidateServer() throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    ServerLocation server1 = new ServerLocation("localhost", 1);
    ServerLocation server2 = new ServerLocation("localhost", 2);
    factory.nextServer = server1;
    Connection conn1 = manager.borrowConnection(0);
    Connection conn2 = manager.borrowConnection(0);
    Connection conn3 = manager.borrowConnection(0);
    factory.nextServer = server2;
    Connection conn4 = manager.borrowConnection(0);
    
    Assert.assertEquals(4,factory.creates);
    Assert.assertEquals(0,factory.destroys);
    
    manager.returnConnection(conn2);
    endpointManager.serverCrashed(conn2.getEndpoint());
    Assert.assertEquals(3,factory.destroys);
    conn1.destroy();
    manager.returnConnection(conn1);
    Assert.assertEquals(3,factory.destroys);
    manager.returnConnection(conn3);
    manager.returnConnection(conn4);
    Assert.assertEquals(3,factory.destroys);
    
    manager.borrowConnection(0);
    Assert.assertEquals(4,factory.creates);
    Assert.assertEquals(3,factory.destroys);
  }
  
//  public void testGetConnectionToSpecificServer() throws AllConnectionsInUseException, NoAvailableServersException, InterruptedException {
//    DummySource source = new DummySource();
//    manager = new ConnectionManager(source, factory, 10, -1, 10, logger, logger);
//    manager.start();
//    
//    source.nextServer = new ServerLocation("localhost", 10);
//    Connection conn1 = manager.borrowConnection(10);
//    manager.returnConnection(conn1);
//    
//    try {
//      Connection conn2 = manager.borrowConnection(new ServerLocation("locahost", 20), 10);
//      Assert.fail("Should have received no servers available, because we asked for a server we can't get");
//    } catch(NoAvailableServersException expected) {
//      
//    }
//    
//  }
  
  @Test
  public void testIdleExpiration() throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    final long idleTimeout = 300;
    manager = new ConnectionManagerImpl("pool",  factory, endpointManager, 5, 2, idleTimeout, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    {
      long start = System.currentTimeMillis();
      synchronized(factory) {
        long remaining = TIMEOUT;
        while(factory.creates < 2  && remaining > 0) {
          factory.wait(remaining); 
          remaining = TIMEOUT - (System.currentTimeMillis() - start);
        }
      }
      Assert.assertEquals(2,factory.creates);
      Assert.assertEquals(0,factory.destroys);
      Assert.assertEquals(0,factory.closes);
      Assert.assertEquals(0,poolStats.getIdleExpire());
      // no need to wait; dangerous because it gives connections a chance to expire
      //     //wait for prefill task to finish. 
      //     Thread.sleep(100);
    }
    
    Connection conn1 = manager.borrowConnection(500);
    Connection conn2 = manager.borrowConnection(500);
    Connection conn3 = manager.borrowConnection(500);
    Connection conn4 = manager.borrowConnection(500);
    Connection conn5 = manager.borrowConnection(500);
    
    //wait to make sure checked out connections aren't timed out
    Thread.sleep(idleTimeout*2);
    Assert.assertEquals(5,factory.creates);
    Assert.assertEquals(0,factory.destroys);
    Assert.assertEquals(0,factory.closes);
    Assert.assertEquals(0,poolStats.getIdleExpire());

    // make sure a thread local connection that has been passivated can idle-expire
    manager.passivate(conn1, true);
    
    {
      long start = System.currentTimeMillis();
      synchronized(factory) {
        long remaining = TIMEOUT;
        while(factory.destroys < 1 && remaining > 0) {
          factory.wait(remaining); 
          remaining = TIMEOUT - (System.currentTimeMillis() - start);
        }
      }
      long elapsed = System.currentTimeMillis() - start;
      Assert.assertTrue("Elapsed " + elapsed + " is less than idle timeout "
          + idleTimeout,
          elapsed + ALLOWABLE_ERROR_IN_EXPIRATION >= idleTimeout);
      Assert.assertEquals(5,factory.creates);
      Assert.assertEquals(1,factory.destroys);
      Assert.assertEquals(1,factory.closes);
      Assert.assertEquals(1,poolStats.getIdleExpire());
    }

    // now return all other connections to pool and verify that just 2 expire
    manager.returnConnection(conn2);
    manager.returnConnection(conn3);
    manager.returnConnection(conn4);
    manager.returnConnection(conn5);

    {
      long start = System.currentTimeMillis();
      synchronized(factory) {
        long remaining = TIMEOUT;
        while(factory.destroys < 3 && remaining > 0) {
          factory.wait(remaining); 
          remaining = TIMEOUT - (System.currentTimeMillis() - start);
        }
      }
      long elapsed = System.currentTimeMillis() - start;
      Assert.assertTrue("Elapsed " + elapsed + " is less than idle timeout "
              + idleTimeout,
              elapsed + ALLOWABLE_ERROR_IN_EXPIRATION >= idleTimeout);
      Assert.assertEquals(5,factory.creates);
      Assert.assertEquals(3,factory.destroys);
      Assert.assertEquals(3,factory.closes);
      Assert.assertEquals(3,poolStats.getIdleExpire());
    }

    //wait to make sure min-connections don't time out
    Thread.sleep(idleTimeout*2);
    Assert.assertEquals(5,factory.creates);
    Assert.assertEquals(3,factory.destroys);
    Assert.assertEquals(3,factory.closes);
    Assert.assertEquals(3,poolStats.getIdleExpire());
  }
  @Test
  public void testBug41516() throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    final long idleTimeout = 300;
    manager = new ConnectionManagerImpl("pool",  factory, endpointManager, 2, 1, idleTimeout, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn1 = manager.borrowConnection(500);
    Connection conn2 = manager.borrowConnection(500);
    
    //Return some connections, let them idle expire
    manager.returnConnection(conn1);
    manager.returnConnection(conn2);
    
    {
      long start = System.currentTimeMillis();
      synchronized(factory) {
        long remaining = TIMEOUT;
        while(factory.destroys < 1 && remaining > 0) {
          factory.wait(remaining); 
          remaining = TIMEOUT - (System.currentTimeMillis() - start);
        }
      }
      long elapsed = System.currentTimeMillis() - start;
      Assert.assertTrue("Elapsed " + elapsed + " is less than idle timeout "
          + idleTimeout,
          elapsed + ALLOWABLE_ERROR_IN_EXPIRATION >= idleTimeout);
      Assert.assertEquals(1,factory.destroys);
      Assert.assertEquals(1,factory.closes);
      Assert.assertEquals(1,poolStats.getIdleExpire());
    }
    
    //Ok, now get some connections that fill our queue
    Connection ping1 = manager.borrowConnection(new ServerLocation("localhost", 5), 500, false);
    Connection ping2 = manager.borrowConnection(new ServerLocation("localhost", 5), 500, false);
    manager.returnConnection(ping1);
    manager.returnConnection(ping2);
    
    Connection conn3 = manager.borrowConnection(500);
    Connection conn4 = manager.borrowConnection(500);
    long start = System.currentTimeMillis();
    try {
      Connection conn5 = manager.borrowConnection(500);
      fail("Didn't get an exception");
    } catch(AllConnectionsInUseException e) {
      //expected
    }
    long elapsed = System.currentTimeMillis() - start;
    Assert.assertTrue("Elapsed = " + elapsed, elapsed >= 500);
  }
  
  @Test
  public void testLifetimeExpiration() throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException, Throwable {
    int lifetimeTimeout = 500;
    manager = new ConnectionManagerImpl("pool",  factory, endpointManager, 2, 2, -1, lifetimeTimeout, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    {
      long start = System.currentTimeMillis();
      synchronized(factory) {
        long remaining = TIMEOUT;
        while(factory.creates < 2  && remaining > 0) {
          factory.wait(remaining); 
          remaining = TIMEOUT - (System.currentTimeMillis() - start);
        }
      }
      Assert.assertEquals(2,factory.creates);
      Assert.assertEquals(0,factory.destroys);
      Assert.assertEquals(0,factory.finds);
    }

    // need to start a thread that keeps the connections busy
    // so that their last access time keeps changing
    AtomicReference exception = new AtomicReference();
    int updaterCount = 2;
    UpdaterThread[] updaters = new UpdaterThread[updaterCount];
    
    for(int i = 0; i < updaterCount; i++) {
      updaters[i] = new UpdaterThread(null, exception, i, (lifetimeTimeout/10)*2, true);
    }
    
    for(int i = 0; i < updaterCount; i++) {
      updaters[i].start();
    }
    
    {
      long start = System.currentTimeMillis();
      synchronized(factory) {
        long remaining = TIMEOUT;
        while(factory.finds < 2  && remaining > 0) {
          factory.wait(remaining); 
          remaining = TIMEOUT - (System.currentTimeMillis() - start);
        }
      }
      long end = System.currentTimeMillis();
      Assert.assertEquals(2,factory.finds);
      // server shouldn't have changed so no increase in creates or destroys
      Assert.assertEquals(2,factory.creates);
      Assert.assertEquals(0,factory.destroys);
      Assert.assertEquals(0,factory.closes);

      Assert.assertTrue("took too long to expire lifetime; expected="
                        + lifetimeTimeout + " but took=" + (end-start),
                        (end-start) < lifetimeTimeout*2);
    }
    
    for(int i = 0; i < updaterCount; i++) {
      ThreadUtils.join(updaters[i], 30 * 1000);
    }

    if(exception.get() !=null) {
      throw (Throwable) exception.get();
    }
    
    for(int i = 0; i < updaterCount; i++) {
      Assert.assertFalse("Updater [" + i + "] is still running", updaters[i].isAlive());
    }
    
//     //wait for prefill task to finish.
//     Thread.sleep(100);
    
//     Connection conn1 = manager.borrowConnection(0);
//     Connection conn2 = manager.borrowConnection(0);
//     Connection conn3 = manager.borrowConnection(0);
//     Connection conn4 = manager.borrowConnection(0);
//     Connection conn5 = manager.borrowConnection(0);
    
//     //wait to make sure checked out connections aren't timed out
//     Thread.sleep(idleTimeout + 100);
//     Assert.assertEquals(5,factory.creates);
//     Assert.assertEquals(0,factory.destroys);
    
//     manager.returnConnection(conn1);
//     manager.returnConnection(conn2);
//     manager.returnConnection(conn3);
//     manager.returnConnection(conn4);
//     manager.returnConnection(conn5);
    
//     long start = System.currentTimeMillis();
//     synchronized(factory) {
//       while(factory.destroys < 3 && remaining > 0) {
//         factory.wait(remaining); 
//         remaining = TIMEOUT - (System.currentTimeMillis() - start);
//       }
//     }
    
//     long elapsed = System.currentTimeMillis() - start;
//     Assert.assertTrue(elapsed > idleTimeout);
    
//     Assert.assertEquals(5,factory.creates);
//     Assert.assertEquals(3,factory.destroys);
  }
  
  @Test
  public void testExclusiveConnectionAccess() throws Throwable {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    AtomicReference exception = new AtomicReference();
    AtomicBoolean haveConnection = new AtomicBoolean();
    int updaterCount = 10;
    UpdaterThread[] updaters = new UpdaterThread[updaterCount];
    
    for(int i = 0; i < updaterCount; i++) {
      updaters[i] = new UpdaterThread(haveConnection, exception, i);
    }
    
    for(int i = 0; i < updaterCount; i++) {
      updaters[i].start();
    }
    
    for(int i = 0; i < updaterCount; i++) {
      ThreadUtils.join(updaters[i], 30 * 1000);
    }

    if(exception.get() !=null) {
      throw (Throwable) exception.get();
    }
    
    for(int i = 0; i < updaterCount; i++) {
      Assert.assertFalse("Updater [" + i + "] is still running", updaters[i].isAlive());
    }
  }
  
  @Test
  public void testClose() throws AllConnectionsInUseException, NoAvailableServersException, InterruptedException {
    manager = new ConnectionManagerImpl("pool",  factory, endpointManager, 10, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    Connection conn1 = manager.borrowConnection(0);
    manager.borrowConnection(0);
    manager.returnConnection(conn1);
    Assert.assertEquals(2,factory.creates);
    Assert.assertEquals(0,factory.destroys);
    
    manager.close(false);
    
    Assert.assertEquals(2, factory.closes);
    Assert.assertEquals(2, factory.destroys);
    
  }
  
  @Test
  public void testExchangeConnection() throws Exception {
    manager = new ConnectionManagerImpl("pool",  factory, endpointManager, 2, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    Connection  conn1 = manager.borrowConnection(10);
    Connection  conn2 = manager.borrowConnection(10);
    try {
      manager.borrowConnection(10);
      fail("Exepected no servers available");
    } catch(AllConnectionsInUseException e) {
      //expected
    }
    
    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(0, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());
    
    Connection conn3  = manager.exchangeConnection(conn1, Collections.EMPTY_SET, 10);
    
    Assert.assertEquals(3, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());
    
    manager.returnConnection(conn2);
    
    Assert.assertEquals(3, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());
    
    Connection conn4  = manager.exchangeConnection(conn3, Collections.singleton(conn3.getServer()), 10);
    
    Assert.assertEquals(4, factory.creates);
    Assert.assertEquals(2, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());
    
    manager.returnConnection(conn4);
  }
  
  @Test
  public void testBlocking() throws Throwable {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    final Connection conn1 = manager.borrowConnection(10);
    
    long startTime = System.currentTimeMillis();
    try {
      manager.borrowConnection(100);
      fail("Should have received no servers available");
    } catch(AllConnectionsInUseException expected) {
      
    }
    long elapsed = System.currentTimeMillis() - startTime;
    Assert.assertTrue("Should have blocked for 100 millis for a connection", elapsed >= 100);
    
    Thread returnThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        manager.returnConnection(conn1);
      }
    };
    
    returnThread.start();
    startTime = System.currentTimeMillis();
    Connection conn2 = manager.borrowConnection(1000);
    elapsed = System.currentTimeMillis() - startTime;
    Assert.assertTrue("Should have blocked for less than 1 second", elapsed < 1000);
    manager.returnConnection(conn2);
    
    
    final Connection conn3 = manager.borrowConnection(10);
    Thread invalidateThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        conn3.destroy();
        manager.returnConnection(conn3);
      }
    };
    
    invalidateThread.start();
    startTime = System.currentTimeMillis();
    conn2 = manager.borrowConnection(1000);
    elapsed = System.currentTimeMillis() - startTime;
    Assert.assertTrue("Should have blocked for less than 1 second", elapsed < 1000);
    manager.returnConnection(conn2);
    
    final Connection conn4 = manager.borrowConnection(10);
    Thread invalidateThread2 = new Thread() {
      public void run() {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        endpointManager.serverCrashed(conn4.getEndpoint());
        manager.returnConnection(conn4);
      }
    };
    
    invalidateThread2.start();
    startTime = System.currentTimeMillis();
    conn2 = manager.borrowConnection(1000);
    elapsed = System.currentTimeMillis() - startTime;
    Assert.assertTrue("Should have blocked for less than 1 second", elapsed < 1000);
    manager.returnConnection(conn2);
  }
  
  @Test
  public void testExplicitServer() throws Exception {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    
    Connection conn1 = manager.borrowConnection(0);
    
    try {
      manager.borrowConnection(10);
      fail("Should have received an error");
    } catch(AllConnectionsInUseException expected) {
      //do nothing
    }
    
    Connection conn3 = manager.borrowConnection(new ServerLocation("localhost", -2), 10,false);
    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(0, factory.destroys);
    Assert.assertEquals(0, factory.closes);
    
    manager.returnConnection(conn3);
    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    Assert.assertEquals(1, factory.closes);
    
    manager.returnConnection(conn1);
    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    Assert.assertEquals(1, factory.closes);
  }
  
  private class UpdaterThread  extends Thread {
    
    private AtomicReference exception;
    
    private final AtomicBoolean haveConnection;

    private int id;
    private final int iterations;
    /**
     * If true then obtain the connection as if it is a thread local one
     */
    private final boolean threadLocal;
    
    public UpdaterThread(AtomicBoolean haveConnection, AtomicReference exception, int id) {
      this(haveConnection, exception, id, 10, false);
    }

    public UpdaterThread(AtomicBoolean haveConnection, AtomicReference exception, int id, int iterations, boolean threadLocal) {
      this.haveConnection = haveConnection;
      this.exception =exception;
      this.id = id;
      this.iterations = iterations;
      this.threadLocal = threadLocal;
    }

    private Connection borrow(int i) {
      long startTime = System.currentTimeMillis();
      Connection conn = manager.borrowConnection(2000);
      if (haveConnection != null) {
        Assert.assertTrue("Updater[" + id + "] loop[" + i + "] Someone else has the connection!", haveConnection.compareAndSet(false, true));
      }
      long elapsed = System.currentTimeMillis() - startTime;
      if (elapsed >= 2000) {
        Assert.assertTrue("Elapsed time (" + elapsed + ") >= 2000", false);
      }
      return conn;
    }
    
    public void run() {
      int i = 0;
      Connection conn = null;
      try {
        if (threadLocal) {
          conn = borrow(-1);
        }
        for(i = 0; i < iterations; i++) {
          if (!threadLocal) {
            conn = borrow(i);
          } else {
            if (i != 0) {
              manager.activate(conn);
            }
          }
          try {
            Thread.sleep(10);
            if (haveConnection != null) {
              Assert.assertTrue("Updater[" + id + "] loop[" + i + "] Someone else changed the connection flag", haveConnection.compareAndSet(true, false));
            }
          } finally {
            if (!threadLocal) {
              manager.returnConnection(conn);
            } else {
              manager.passivate(conn, true);
            }
          }
        }
      } catch(Throwable t) {
        this.exception.compareAndSet(null, new Exception("ERROR Updater[" + id + "] loop[" + i + "]" , t));
      } finally {
        if (threadLocal && conn != null ) {
          manager.returnConnection(conn);
        }
      }
      
    }
  }
  
  public class DummyFactory implements ConnectionFactory {
    public ServerLocation nextServer = new ServerLocation("localhost", -1);
    protected volatile int creates;
    protected volatile int destroys;
    protected volatile int closes;
    protected volatile int finds;

    public ServerBlackList getBlackList() {
      return new ServerBlackList(1);
    }


    public ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers) {
      synchronized(this) {
        finds++;
        this.notifyAll();
      }
      if (excludedServers != null) {
        if (excludedServers.contains(nextServer)) {
          return null;
        }
      }
      return nextServer;
    }

    public Connection createClientToServerConnection(Set excluded) {
      return createClientToServerConnection(nextServer, true);
    }


    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.client.internal.ConnectionFactory#createClientToServerConnection(com.gemstone.gemfire.distributed.internal.ServerLocation)
     */
    public Connection createClientToServerConnection(final ServerLocation location, boolean forQueue) {
      synchronized(this) {
        creates++;
        this.notifyAll();
      }
      DistributedMember fakeMember = null;
      try {
        fakeMember = new InternalDistributedMember("localhost", 555);
      } catch (UnknownHostException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      final DistributedMember member = fakeMember;
      
      return new Connection()  {

        private Endpoint endpoint = endpointManager.referenceEndpoint(location, member);

        public void destroy() {
          synchronized(DummyFactory.this) {
            destroys++;
            DummyFactory.this.notifyAll();
          }
        }

        public ServerLocation getServer() {
          return location;
        }

        public ByteBuffer getCommBuffer() {
          return null;
        }

        public Socket getSocket() {
          return null;
        }

        public ConnectionStats getStats() {
          return null;
        }

        public void close(boolean keepAlive) throws Exception {
          synchronized(DummyFactory.this) {
            closes++;
            DummyFactory.this.notifyAll();
          }
        }

        public Endpoint getEndpoint() {
          return endpoint;
        }

        public ServerQueueStatus getQueueStatus() {
          return null;
        }

        public Object execute(Op op) throws Exception {
          return op.attempt(this);
        }
        
        public boolean isDestroyed() {return false;}

        public void emergencyClose() {
        }
        
        public short getWanSiteVersion(){
          return -1;
        }
        
        public int getDistributedSystemId() {
          return -1;
        }
        
        public void setWanSiteVersion(short wanSiteVersion){
        }

        public InputStream getInputStream() {
          return null;
        }

        public OutputStream getOutputStream() {
          return null;
        }

        public void setConnectionID(long id) {
        }

        public long getConnectionID() {
          return 0;
        }
      };
    }

    public ClientUpdater createServerToClientConnection(Endpoint endpoint,
        QueueManager manager, boolean isPrimary, ClientUpdater failedUpdater) {
      return null;
    }
  }
}
