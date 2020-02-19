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

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.AllConnectionsInUseException;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.internal.ClientUpdater;
import org.apache.geode.cache.client.internal.Connection;
import org.apache.geode.cache.client.internal.ConnectionFactory;
import org.apache.geode.cache.client.internal.ConnectionStats;
import org.apache.geode.cache.client.internal.Endpoint;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.client.internal.EndpointManagerImpl;
import org.apache.geode.cache.client.internal.Op;
import org.apache.geode.cache.client.internal.QueueManager;
import org.apache.geode.cache.client.internal.ServerDenyList;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ConnectionManagerJUnitTest {

  private static final long TIMEOUT_MILLIS = 30 * 1000;
  // Some machines do not have a monotonic clock.
  private static final long ALLOWABLE_ERROR_IN_MILLIS = 100;
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
    this.logger = new LocalLogWriter(FINEST.intLevel(), System.out);
    factory = new DummyFactory();

    Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, "");
    ds = DistributedSystem.connect(properties);
    background = Executors.newSingleThreadScheduledExecutor();
    poolStats = new PoolStats(ds, "connectionManagerJUnitTest");
    endpointManager = new EndpointManagerImpl("pool", ds, ds.getCancelCriterion(), poolStats);
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

  @After
  public void tearDown() throws InterruptedException {
    ds.disconnect();
    if (manager != null) {
      manager.close(false);
    }
    background.shutdownNow();
  }

  @Test
  public void testAddVarianceToInterval() {
    assertThat(ConnectionManagerImpl.addVarianceToInterval(0)).as("Zero gets zero variance")
        .isEqualTo(0);
    assertThat(ConnectionManagerImpl.addVarianceToInterval(300000))
        .as("Large value gets +/-10% variance").isNotEqualTo(300000).isGreaterThanOrEqualTo(270000)
        .isLessThanOrEqualTo(330000);
    assertThat(ConnectionManagerImpl.addVarianceToInterval(9)).as("Small value gets +/-1 variance")
        .isNotEqualTo(9).isGreaterThanOrEqualTo(8).isLessThanOrEqualTo(10);
  }

  @Test
  public void testGet()
      throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 3, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn[] = new Connection[4];

    conn[0] = manager.borrowConnection(0);
    Assert.assertEquals(1, factory.creates);

    manager.returnConnection(conn[0]);
    conn[0] = manager.borrowConnection(0);
    Assert.assertEquals(1, factory.creates);
    conn[1] = manager.borrowConnection(0);
    manager.returnConnection(conn[0]);
    manager.returnConnection(conn[1]);
    Assert.assertEquals(2, factory.creates);

    conn[0] = manager.borrowConnection(0);
    conn[1] = manager.borrowConnection(0);
    conn[2] = manager.borrowConnection(0);
    Assert.assertEquals(3, factory.creates);

    try {
      conn[4] = manager.borrowConnection(10);
      fail("Should have received an all connections in use exception");
    } catch (AllConnectionsInUseException e) {
      // expected exception
    }
  }

  @Test
  public void testPrefill() throws InterruptedException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 2, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    final String descrip = manager.toString();
    WaitCriterion ev = new WaitCriterion() {
      @Override
      public boolean done() {
        return factory.creates == 2 && factory.destroys == 0;
      }

      @Override
      public String description() {
        return "waiting for manager " + descrip;
      }
    };
    GeodeAwaitility.await().untilAsserted(ev);
  }

  @Test
  public void testInvalidateConnection()
      throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, 0L, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn = manager.borrowConnection(0);
    Assert.assertEquals(1, factory.creates);
    Assert.assertEquals(0, factory.destroys);
    conn.destroy();
    manager.returnConnection(conn);
    Assert.assertEquals(1, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    conn = manager.borrowConnection(0);
    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(1, factory.destroys);
  }

  @Test
  public void testInvalidateServer()
      throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    ServerLocation server1 = new ServerLocation("localhost", 1);
    ServerLocation server2 = new ServerLocation("localhost", 2);
    factory.nextServer = server1;
    Connection conn1 = manager.borrowConnection(0);
    Connection conn2 = manager.borrowConnection(0);
    Connection conn3 = manager.borrowConnection(0);
    factory.nextServer = server2;
    Connection conn4 = manager.borrowConnection(0);

    Assert.assertEquals(4, factory.creates);
    Assert.assertEquals(0, factory.destroys);

    manager.returnConnection(conn2);
    endpointManager.serverCrashed(conn2.getEndpoint());
    Assert.assertEquals(3, factory.destroys);
    conn1.destroy();
    manager.returnConnection(conn1);
    Assert.assertEquals(3, factory.destroys);
    manager.returnConnection(conn3);
    manager.returnConnection(conn4);
    Assert.assertEquals(3, factory.destroys);

    manager.borrowConnection(0);
    Assert.assertEquals(4, factory.creates);
    Assert.assertEquals(3, factory.destroys);
  }

  @Test
  public void testIdleExpiration()
      throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    final long idleTimeoutMillis = 300;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 5, 2, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    {
      factory.waitWhile(() -> factory.creates < 2);
      Assert.assertEquals(2, factory.creates);
      Assert.assertEquals(0, factory.destroys);
      Assert.assertEquals(0, factory.closes);
      Assert.assertEquals(0, poolStats.getIdleExpire());
      // no need to wait; dangerous because it gives connections a chance to expire
      // //wait for prefill task to finish.
      // Thread.sleep(100);
    }

    Connection conn1 = manager.borrowConnection(500);
    Connection conn2 = manager.borrowConnection(500);
    Connection conn3 = manager.borrowConnection(500);
    Connection conn4 = manager.borrowConnection(500);
    Connection conn5 = manager.borrowConnection(500);

    // wait to make sure checked out connections aren't timed out
    Thread.sleep(idleTimeoutMillis * 2);
    Assert.assertEquals(5, factory.creates);
    Assert.assertEquals(0, factory.destroys);
    Assert.assertEquals(0, factory.closes);
    Assert.assertEquals(0, poolStats.getIdleExpire());

    {
      // make sure a connection that has been passivated can idle-expire
      conn1.passivate(true);

      long elapsedMillis = factory.waitWhile(() -> factory.destroys < 1);
      Assert.assertEquals(5, factory.creates);
      Assert.assertEquals(1, factory.destroys);
      Assert.assertEquals(1, factory.closes);
      Assert.assertEquals(1, poolStats.getIdleExpire());
      checkIdleTimeout(idleTimeoutMillis, elapsedMillis);
    }

    // now return all other connections to pool and verify that just 2 expire
    manager.returnConnection(conn2);
    manager.returnConnection(conn3);
    manager.returnConnection(conn4);
    manager.returnConnection(conn5);

    {
      long elapsedMillis = factory.waitWhile(() -> factory.destroys < 3);
      Assert.assertEquals(5, factory.creates);
      Assert.assertEquals(3, factory.destroys);
      Assert.assertEquals(3, factory.closes);
      Assert.assertEquals(3, poolStats.getIdleExpire());
      checkIdleTimeout(idleTimeoutMillis, elapsedMillis);
    }

    // wait to make sure min-connections don't time out
    Thread.sleep(idleTimeoutMillis * 2);
    Assert.assertEquals(5, factory.creates);
    Assert.assertEquals(3, factory.destroys);
    Assert.assertEquals(3, factory.closes);
    Assert.assertEquals(3, poolStats.getIdleExpire());
  }

  private void checkIdleTimeout(final long idleTimeoutMillis, long elapsedMillis) {
    Assert.assertTrue(
        "Elapsed " + elapsedMillis + " is less than idle timeout " + idleTimeoutMillis,
        elapsedMillis >= (idleTimeoutMillis - ALLOWABLE_ERROR_IN_MILLIS));
    Assert.assertTrue(
        "Elapsed " + elapsedMillis + " is greater than idle timeout " + idleTimeoutMillis,
        elapsedMillis <= (idleTimeoutMillis + ALLOWABLE_ERROR_IN_MILLIS));
  }

  @Test
  public void testBug41516()
      throws InterruptedException, AllConnectionsInUseException, NoAvailableServersException {
    final long idleTimeoutMillis = 300;
    final long BORROW_TIMEOUT_MILLIS = 500;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 2, 1, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn1 = manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
    Connection conn2 = manager.borrowConnection(BORROW_TIMEOUT_MILLIS);

    // Return some connections, let them idle expire
    manager.returnConnection(conn1);
    manager.returnConnection(conn2);

    {
      long elapsedMillis = factory.waitWhile(() -> factory.destroys < 1);
      Assert.assertEquals(1, factory.destroys);
      Assert.assertEquals(1, factory.closes);
      Assert.assertEquals(1, poolStats.getIdleExpire());
      Assert.assertTrue(
          "Elapsed " + elapsedMillis + " is less than idle timeout " + idleTimeoutMillis,
          elapsedMillis + ALLOWABLE_ERROR_IN_MILLIS >= idleTimeoutMillis);
    }

    // Ok, now get some connections that fill our queue
    Connection ping1 =
        manager.borrowConnection(new ServerLocation("localhost", 5), BORROW_TIMEOUT_MILLIS, false);
    Connection ping2 =
        manager.borrowConnection(new ServerLocation("localhost", 5), BORROW_TIMEOUT_MILLIS, false);
    manager.returnConnection(ping1);
    manager.returnConnection(ping2);

    manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
    manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
    long startNanos = nowNanos();
    try {
      manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
      fail("Didn't get an exception");
    } catch (AllConnectionsInUseException e) {
      // expected
    }
    long elapsedMillis = elapsedMillis(startNanos);
    Assert.assertTrue("Elapsed = " + elapsedMillis,
        elapsedMillis >= BORROW_TIMEOUT_MILLIS - ALLOWABLE_ERROR_IN_MILLIS);
  }

  @Test
  public void testLifetimeExpiration() throws InterruptedException, AllConnectionsInUseException,
      NoAvailableServersException, Throwable {
    int lifetimeTimeout = 500;
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 2, 2, -1, lifetimeTimeout,
        logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    {
      factory.waitWhile(() -> factory.creates < 2);
      Assert.assertEquals(2, factory.creates);
      Assert.assertEquals(0, factory.destroys);
      Assert.assertEquals(0, factory.finds);
    }

    // need to start a thread that keeps the connections busy
    // so that their last access time keeps changing
    AtomicReference exception = new AtomicReference();
    int updaterCount = 2;
    UpdaterThread[] updaters = new UpdaterThread[updaterCount];

    for (int i = 0; i < updaterCount; i++) {
      updaters[i] = new UpdaterThread(null, exception, i, (lifetimeTimeout / 10) * 2);
    }

    for (int i = 0; i < updaterCount; i++) {
      updaters[i].start();
    }

    {
      long durationMillis = factory.waitWhile(() -> factory.finds < 2);
      Assert.assertEquals(2, factory.finds);
      // server shouldn't have changed so no increase in creates or destroys
      Assert.assertEquals(2, factory.creates);
      Assert.assertEquals(0, factory.destroys);
      Assert.assertEquals(0, factory.closes);

      Assert.assertTrue("took too long to expire lifetime; expected=" + lifetimeTimeout
          + " but took=" + durationMillis, durationMillis < lifetimeTimeout * 5);
    }

    for (int i = 0; i < updaterCount; i++) {
      ThreadUtils.join(updaters[i], 30 * 1000);
    }

    if (exception.get() != null) {
      throw (Throwable) exception.get();
    }

    for (int i = 0; i < updaterCount; i++) {
      Assert.assertFalse("Updater [" + i + "] is still running", updaters[i].isAlive());
    }
  }

  @Test
  public void testExclusiveConnectionAccess() throws Throwable {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);
    AtomicReference exception = new AtomicReference();
    AtomicBoolean haveConnection = new AtomicBoolean();
    int updaterCount = 10;
    UpdaterThread[] updaters = new UpdaterThread[updaterCount];

    for (int i = 0; i < updaterCount; i++) {
      updaters[i] = new UpdaterThread(haveConnection, exception, i);
    }

    for (int i = 0; i < updaterCount; i++) {
      updaters[i].start();
    }

    for (int i = 0; i < updaterCount; i++) {
      ThreadUtils.join(updaters[i], 30 * 1000);
    }

    if (exception.get() != null) {
      throw (Throwable) exception.get();
    }

    for (int i = 0; i < updaterCount; i++) {
      Assert.assertFalse("Updater [" + i + "] is still running", updaters[i].isAlive());
    }
  }

  @Test
  public void testClose()
      throws AllConnectionsInUseException, NoAvailableServersException, InterruptedException {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn1 = manager.borrowConnection(0);
    manager.borrowConnection(0);
    manager.returnConnection(conn1);
    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(0, factory.destroys);

    manager.close(false);

    Assert.assertEquals(2, factory.closes);
    Assert.assertEquals(2, factory.destroys);

  }

  @Test
  public void testExchangeConnection() throws Exception {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 2, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn1 = manager.borrowConnection(10);
    Connection conn2 = manager.borrowConnection(10);
    try {
      manager.borrowConnection(10);
      fail("Exepected no servers available");
    } catch (AllConnectionsInUseException e) {
      // expected
    }

    Assert.assertEquals(2, factory.creates);
    Assert.assertEquals(0, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());

    Connection conn3 = manager.exchangeConnection(conn1, Collections.emptySet());

    Assert.assertEquals(3, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());

    manager.returnConnection(conn2);

    Assert.assertEquals(3, factory.creates);
    Assert.assertEquals(1, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());

    Connection conn4 =
        manager.exchangeConnection(conn3, Collections.singleton(conn3.getServer()));

    Assert.assertEquals(4, factory.creates);
    Assert.assertEquals(2, factory.destroys);
    Assert.assertEquals(2, manager.getConnectionCount());

    manager.returnConnection(conn4);
  }

  /**
   * This tests that a deadlock between connection formation and connection pool closing has been
   * fixed. See GEODE-4615
   */
  @Test
  public void testThatMapCloseCausesCacheClosedException() throws Exception {
    final ConnectionManagerImpl connectionManager = new ConnectionManagerImpl("pool", factory,
        endpointManager, 2, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager = connectionManager;
    connectionManager.start(background);
    final ConnectionManagerImpl.ConnectionMap connectionMap = connectionManager.allConnectionsMap;

    final int thread1 = 0;
    final int thread2 = 1;
    final boolean[] ready = new boolean[2];
    Thread thread = new Thread("ConnectionManagerJUnitTest thread") {
      @Override
      public void run() {
        setReady(ready, thread1);
        waitUntilReady(ready, thread2);
        connectionMap.close(false);
      }
    };
    thread.setDaemon(true);
    thread.start();
    try {
      Connection firstConnection = connectionManager.borrowConnection(0);
      synchronized (firstConnection) {
        setReady(ready, thread2);
        waitUntilReady(ready, thread1);
        // the other thread will now try to close the connection map but it will block
        // because this thread has locked one of the connections
        await().until(() -> connectionMap.closing);
        try {
          connectionManager.borrowConnection(0);
          fail("expected a CacheClosedException");
        } catch (CacheClosedException e) {
          // expected
        }
      }
    } finally {
      if (thread.isAlive()) {
        System.out.println("stopping background thread");
        thread.interrupt();
        thread.join();
      }
    }
  }

  private void setReady(boolean[] ready, int index) {
    System.out.println(
        Thread.currentThread().getName() + ": setting that thread" + (index + 1) + " is ready");
    synchronized (ready) {
      ready[index] = true;
    }
  }

  private void waitUntilReady(boolean[] ready, int index) {
    System.out.println(
        Thread.currentThread().getName() + ": waiting for thread" + (index + 1) + " to be ready");
    await().until(() -> {
      synchronized (ready) {
        return (ready[index]);
      }
    });
  }

  private long nowNanos() {
    return System.nanoTime();
  }

  private long elapsedNanos(long startNanos) {
    return nowNanos() - startNanos;
  }

  private long elapsedMillis(long startNanos) {
    return NANOSECONDS.toMillis(elapsedNanos(startNanos));
  }

  @Test
  public void testBlocking() throws Throwable {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    final Connection conn1 = manager.borrowConnection(10);

    long BORROW_TIMEOUT_MILLIS = 300;
    long startNonos = nowNanos();
    try {
      manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
      fail("Should have received no servers available");
    } catch (AllConnectionsInUseException expected) {

    }
    long elapsedMillis = elapsedMillis(startNonos);
    Assert.assertTrue(
        "Should have blocked for " + BORROW_TIMEOUT_MILLIS + " millis for a connection",
        elapsedMillis >= BORROW_TIMEOUT_MILLIS - ALLOWABLE_ERROR_IN_MILLIS);

    Thread returnThread = new Thread() {
      @Override
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
    BORROW_TIMEOUT_MILLIS = 5000;
    startNonos = nowNanos();
    Connection conn2 = manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
    elapsedMillis = elapsedMillis(startNonos);
    Assert.assertTrue(
        "Should have blocked for less than " + BORROW_TIMEOUT_MILLIS + " milliseconds",
        elapsedMillis < BORROW_TIMEOUT_MILLIS + ALLOWABLE_ERROR_IN_MILLIS);
    manager.returnConnection(conn2);


    final Connection conn3 = manager.borrowConnection(10);
    Thread invalidateThread = new Thread() {
      @Override
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
    startNonos = nowNanos();
    conn2 = manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
    elapsedMillis = elapsedMillis(startNonos);
    Assert.assertTrue(
        "Should have blocked for less than " + BORROW_TIMEOUT_MILLIS + " milliseconds",
        elapsedMillis < BORROW_TIMEOUT_MILLIS + ALLOWABLE_ERROR_IN_MILLIS);
    manager.returnConnection(conn2);

    final Connection conn4 = manager.borrowConnection(10);
    Thread invalidateThread2 = new Thread() {
      @Override
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
    startNonos = nowNanos();
    conn2 = manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
    elapsedMillis = elapsedMillis(startNonos);
    Assert.assertTrue(
        "Should have blocked for less than " + BORROW_TIMEOUT_MILLIS + " milliseconds",
        elapsedMillis < BORROW_TIMEOUT_MILLIS + ALLOWABLE_ERROR_IN_MILLIS);
    manager.returnConnection(conn2);
  }

  @Test
  public void testExplicitServer() throws Exception {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection conn1 = manager.borrowConnection(0);

    try {
      manager.borrowConnection(10);
      fail("Should have received an error");
    } catch (AllConnectionsInUseException expected) {
      // do nothing
    }

    Connection conn3 = manager.borrowConnection(new ServerLocation("localhost", -2), 10, false);
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

  private class UpdaterThread extends Thread {

    private AtomicReference exception;

    private final AtomicBoolean haveConnection;

    private int id;
    private final int iterations;

    public UpdaterThread(AtomicBoolean haveConnection, AtomicReference exception, int id) {
      this(haveConnection, exception, id, 10);
    }

    public UpdaterThread(AtomicBoolean haveConnection, AtomicReference exception, int id,
        int iterations) {
      this.haveConnection = haveConnection;
      this.exception = exception;
      this.id = id;
      this.iterations = iterations;
    }

    private Connection borrow(int i) {
      long startNanos = nowNanos();
      long BORROW_TIMEOUT_MILLIS = 2000;
      Connection conn = manager.borrowConnection(BORROW_TIMEOUT_MILLIS);
      if (haveConnection != null) {
        Assert.assertTrue("Updater[" + id + "] loop[" + i + "] Someone else has the connection!",
            haveConnection.compareAndSet(false, true));
      }
      long elapsedMillis = elapsedMillis(startNanos);
      Assert.assertTrue("Elapsed time (" + elapsedMillis + ") >= " + BORROW_TIMEOUT_MILLIS,
          elapsedMillis < BORROW_TIMEOUT_MILLIS + ALLOWABLE_ERROR_IN_MILLIS);
      return conn;
    }

    @Override
    public void run() {
      int i = 0;
      Connection conn = null;
      try {
        for (i = 0; i < iterations; i++) {
          conn = borrow(i);
          try {
            Thread.sleep(10);
            if (haveConnection != null) {
              Assert.assertTrue(
                  "Updater[" + id + "] loop[" + i + "] Someone else changed the connection flag",
                  haveConnection.compareAndSet(true, false));
            }
          } finally {
            manager.returnConnection(conn);
          }
        }
      } catch (Throwable t) {
        this.exception.compareAndSet(null,
            new Exception("ERROR Updater[" + id + "] loop[" + i + "]", t));
      }

    }
  }

  public class DummyFactory implements ConnectionFactory {
    public ServerLocation nextServer = new ServerLocation("localhost", -1);
    protected volatile int creates;
    protected volatile int destroys;
    protected volatile int closes;
    protected volatile int finds;

    /**
     * Wait as long as "whileCondition" is true.
     * The wait will timeout after TIMEOUT_MILLIS has elapsed.
     *
     * @return the elapsed time in milliseconds.
     */
    public synchronized long waitWhile(BooleanSupplier whileCondition) throws InterruptedException {
      final long startNanos = nowNanos();
      long remainingMillis = TIMEOUT_MILLIS;
      while (whileCondition.getAsBoolean() && remainingMillis > 0) {
        wait(remainingMillis);
        remainingMillis = TIMEOUT_MILLIS - elapsedMillis(startNanos);
      }
      return elapsedMillis(startNanos);
    }

    @Override
    public ServerDenyList getDenyList() {
      return new ServerDenyList(1);
    }


    @Override
    public ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers) {
      synchronized (this) {
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

    @Override
    public Connection createClientToServerConnection(Set excluded) {
      return createClientToServerConnection(nextServer, true);
    }


    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.cache.client.internal.ConnectionFactory#createClientToServerConnection(org.
     * apache.geode.distributed.internal.ServerLocation)
     */
    @Override
    public Connection createClientToServerConnection(final ServerLocation location,
        boolean forQueue) {
      synchronized (this) {
        creates++;
        this.notifyAll();
      }
      DistributedMember fakeMember = null;
      fakeMember = new InternalDistributedMember("localhost", 555);
      final DistributedMember member = fakeMember;

      return new Connection() {

        private Endpoint endpoint = endpointManager.referenceEndpoint(location, member);

        @Override
        public void destroy() {
          synchronized (DummyFactory.this) {
            destroys++;
            DummyFactory.this.notifyAll();
          }
        }

        @Override
        public ServerLocation getServer() {
          return location;
        }

        @Override
        public ByteBuffer getCommBuffer() {
          return null;
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
        public void close(boolean keepAlive) throws Exception {
          synchronized (DummyFactory.this) {
            closes++;
            DummyFactory.this.notifyAll();
          }
        }

        @Override
        public Endpoint getEndpoint() {
          return endpoint;
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
        public boolean isDestroyed() {
          return false;
        }

        @Override
        public void emergencyClose() {}

        @Override
        public short getWanSiteVersion() {
          return -1;
        }

        @Override
        public int getDistributedSystemId() {
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
      };
    }

    @Override
    public ClientUpdater createServerToClientConnection(Endpoint endpoint, QueueManager manager,
        boolean isPrimary, ClientUpdater failedUpdater) {
      return null;
    }
  }
}
