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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.logging.internal.spi.LogWriterLevel.FINEST;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ErrorCollector;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.client.AllConnectionsInUseException;
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
import org.apache.geode.cache.client.internal.pooling.ConnectionManagerImpl.ConnectionMap;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.LocalLogWriter;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule.ThrowingRunnable;

@Category(ClientServerTest.class)
public class ConnectionManagerJUnitTest {

  // Some machines do not have a monotonic clock.
  private static final long ALLOWABLE_ERROR_IN_MILLIS = 100;

  private final AtomicBoolean haveConnection = new AtomicBoolean();

  private ConnectionManager manager;
  private InternalLogWriter logger;
  private DummyFactory factory;
  private DistributedSystem ds;
  private ScheduledExecutorService background;
  private EndpointManager endpointManager;
  private CancelCriterion cancelCriterion;
  private PoolStats poolStats;

  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();
  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Before
  public void setUp() {
    logger = new LocalLogWriter(FINEST.intLevel(), System.out);
    factory = new DummyFactory();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");

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
  public void tearDown() {
    ds.disconnect();
    if (manager != null) {
      manager.close(false);
    }
    background.shutdownNow();
  }

  @Test
  public void testAddVarianceToInterval() {
    assertThat(ConnectionManagerImpl.addVarianceToInterval(0))
        .as("Zero gets zero variance")
        .isEqualTo(0);

    assertThat(ConnectionManagerImpl.addVarianceToInterval(300000))
        .as("Large value gets +/-10% variance")
        .isNotEqualTo(300000)
        .isGreaterThanOrEqualTo(270000)
        .isLessThanOrEqualTo(330000);

    assertThat(ConnectionManagerImpl.addVarianceToInterval(9))
        .as("Small value gets +/-1 variance")
        .isNotEqualTo(9)
        .isGreaterThanOrEqualTo(8)
        .isLessThanOrEqualTo(10);
  }

  @Test
  public void testGet() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 3, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection connection1 = manager.borrowConnection(0);
    assertThat(factory.creates.get()).isEqualTo(1);

    manager.returnConnection(connection1);
    connection1 = manager.borrowConnection(0);

    assertThat(factory.creates.get()).isEqualTo(1);

    Connection connection2 = manager.borrowConnection(0);
    manager.returnConnection(connection1);
    manager.returnConnection(connection2);

    assertThat(factory.creates.get()).isEqualTo(2);

    manager.borrowConnection(0);
    manager.borrowConnection(0);
    manager.borrowConnection(0);

    assertThat(factory.creates.get()).isEqualTo(3);

    Throwable thrown = catchThrowable(() -> {
      manager.borrowConnection(10);
    });
    assertThat(thrown).isInstanceOf(AllConnectionsInUseException.class);
  }

  @Test
  public void testPrefill() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 2, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    await("waiting for manager " + manager).untilAsserted(() -> {
      assertThat(factory.creates.get()).isEqualTo(2);
      assertThat(factory.destroys.get()).isEqualTo(0);
    });
  }

  @Test
  public void testInvalidateConnection() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, 0L, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection connection = manager.borrowConnection(0);

    assertThat(factory.creates.get()).isEqualTo(1);
    assertThat(factory.destroys.get()).isEqualTo(0);

    connection.destroy();
    manager.returnConnection(connection);

    assertThat(factory.creates.get()).isEqualTo(1);
    assertThat(factory.destroys.get()).isEqualTo(1);

    manager.borrowConnection(0);

    assertThat(factory.creates.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(1);
  }

  @Test
  public void testInvalidateServer() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    ServerLocation server1 = new ServerLocation("localhost", 1);
    ServerLocation server2 = new ServerLocation("localhost", 2);
    factory.nextServer.set(server1);
    Connection connection1 = manager.borrowConnection(0);
    Connection connection2 = manager.borrowConnection(0);
    Connection connection3 = manager.borrowConnection(0);
    factory.nextServer.set(server2);
    Connection connection4 = manager.borrowConnection(0);

    assertThat(factory.creates.get()).isEqualTo(4);
    assertThat(factory.destroys.get()).isEqualTo(0);

    manager.returnConnection(connection2);
    endpointManager.serverCrashed(connection2.getEndpoint());

    assertThat(factory.destroys.get()).isEqualTo(3);

    connection1.destroy();
    manager.returnConnection(connection1);

    assertThat(factory.destroys.get()).isEqualTo(3);

    manager.returnConnection(connection3);
    manager.returnConnection(connection4);

    assertThat(factory.destroys.get()).isEqualTo(3);

    manager.borrowConnection(0);
    assertThat(factory.creates.get()).isEqualTo(4);
    assertThat(factory.destroys.get()).isEqualTo(3);
  }

  @Test
  public void testIdleExpiration() throws Exception {
    long idleTimeoutMillis = 300;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 5, 2, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    await().untilAsserted(() -> {
      assertThat(factory.creates.get()).isEqualTo(2);
      assertThat(factory.destroys.get()).isEqualTo(0);
      assertThat(factory.closes.get()).isEqualTo(0);
      assertThat(poolStats.getIdleExpire()).isEqualTo(0);
    });

    // no need to wait; dangerous because it gives connections a chance to expire

    Connection connection1 = manager.borrowConnection(500);
    Connection connection2 = manager.borrowConnection(500);
    Connection connection3 = manager.borrowConnection(500);
    Connection connection4 = manager.borrowConnection(500);
    Connection connection5 = manager.borrowConnection(500);

    // wait to make sure checked out connections aren't timed out
    Thread.sleep(idleTimeoutMillis * 2);
    assertThat(factory.creates.get()).isEqualTo(5);
    assertThat(factory.destroys.get()).isEqualTo(0);
    assertThat(factory.closes.get()).isEqualTo(0);
    assertThat(poolStats.getIdleExpire()).isEqualTo(0);

    // make sure a connection that has been passivated can idle-expire
    connection1.passivate(true);

    long elapsedMillis = Timer.measure(() -> {
      await().untilAsserted(() -> {
        assertThat(factory.creates.get()).isEqualTo(5);
        assertThat(factory.destroys.get()).isEqualTo(1);
        assertThat(factory.closes.get()).isEqualTo(1);
        assertThat(poolStats.getIdleExpire()).isEqualTo(1);
      });
    });
    checkIdleTimeout(idleTimeoutMillis, elapsedMillis);

    // now return all other connections to pool and verify that just 2 expire
    manager.returnConnection(connection2);
    manager.returnConnection(connection3);
    manager.returnConnection(connection4);
    manager.returnConnection(connection5);

    elapsedMillis = Timer.measure(() -> {
      await().untilAsserted(() -> {
        assertThat(factory.creates.get()).isEqualTo(5);
        assertThat(factory.destroys.get()).isEqualTo(3);
        assertThat(factory.closes.get()).isEqualTo(3);
        assertThat(poolStats.getIdleExpire()).isEqualTo(3);
      });
    });
    checkIdleTimeout(idleTimeoutMillis, elapsedMillis);

    // wait to make sure min-connections don't time out
    Thread.sleep(idleTimeoutMillis * 2);
    assertThat(factory.creates.get()).isEqualTo(5);
    assertThat(factory.destroys.get()).isEqualTo(3);
    assertThat(factory.closes.get()).isEqualTo(3);
    assertThat(poolStats.getIdleExpire()).isEqualTo(3);
  }

  @Test
  public void testBug41516() throws Exception {
    long idleTimeoutMillis = 300;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 2, 1, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    long borrowTimeoutMillis = 500;

    Connection connection1 = manager.borrowConnection(borrowTimeoutMillis);
    Connection connection2 = manager.borrowConnection(borrowTimeoutMillis);

    // Return some connections, let them idle expire
    manager.returnConnection(connection1);
    manager.returnConnection(connection2);

    long elapsedMillis = Timer.measure(() -> {
      await().untilAsserted(() -> {
        assertThat(factory.destroys.get()).isEqualTo(1);
        assertThat(factory.closes.get()).isEqualTo(1);
        assertThat(poolStats.getIdleExpire()).isEqualTo(1);
      });
    });
    assertThat(elapsedMillis)
        .as("elapsedMillis " + elapsedMillis + " + ALLOWABLE_ERROR_IN_MILLIS "
            + ALLOWABLE_ERROR_IN_MILLIS)
        .isGreaterThanOrEqualTo(idleTimeoutMillis - ALLOWABLE_ERROR_IN_MILLIS);

    // Ok, now get some connections that fill our queue
    Connection ping1 =
        manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    Connection ping2 =
        manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    manager.returnConnection(ping1);
    manager.returnConnection(ping2);

    manager.borrowConnection(borrowTimeoutMillis);
    manager.borrowConnection(borrowTimeoutMillis);

    elapsedMillis = Timer.measure(() -> {
      Throwable thrown = catchThrowable(() -> {
        manager.borrowConnection(borrowTimeoutMillis);
      });
      assertThat(thrown).isInstanceOf(AllConnectionsInUseException.class);
    });
    assertThat(elapsedMillis)
        .isGreaterThanOrEqualTo(borrowTimeoutMillis - ALLOWABLE_ERROR_IN_MILLIS);
  }

  /**
   * Test borrow connection toward specific server. Max connection is 5, and there are free
   * connections in pool.
   */
  @Test
  public void test_borrow_connection_toward_specific_server_freeConnections() {
    long idleTimeoutMillis = 300;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 5, 1, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    await().untilAsserted(() -> assertThat(manager.getConnectionCount()).isOne());

    long borrowTimeoutMillis = 500;

    // seize connection toward any server
    manager.borrowConnection(borrowTimeoutMillis);
    manager.borrowConnection(borrowTimeoutMillis);

    assertThatCode(() -> {
      manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    }).doesNotThrowAnyException();
  }

  /**
   * Test borrow connection toward specific server. Max connection is 5, and there is no free
   * connections in pool.
   * After connection is returned to pool, since is not toward this specific server, wait for
   * idleTimeoutMillis,
   * so after expire (and pool size is reduced to 4) it can be seized.
   */
  @Test
  public void test_borrow_connection_toward_specific_server_no_freeConnection_wait_for_timeout()
      throws Exception {
    long idleTimeoutMillis = 300;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 5, 1, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    await().untilAsserted(() -> assertThat(manager.getConnectionCount()).isOne());

    long borrowTimeoutMillis = 500;

    // seize connection toward any server
    Connection connection1 = manager.borrowConnection(borrowTimeoutMillis);
    Connection connection2 = manager.borrowConnection(borrowTimeoutMillis);

    // Seize connection toward this specific server
    manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);

    // Return some connections, let them idle expire
    manager.returnConnection(connection1);
    manager.returnConnection(connection2);

    long elapsedMillis = Timer.measure(() -> {
      Throwable thrown = catchThrowable(() -> {
        manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, true);
      });
      assertThat(thrown).isInstanceOf(AllConnectionsInUseException.class);
    });
    assertThat(elapsedMillis)
        .isGreaterThanOrEqualTo(borrowTimeoutMillis - ALLOWABLE_ERROR_IN_MILLIS);
  }

  /**
   * Test borrow connection toward specific server (use). Max connection is 5, and there is no free
   * connections in pool.
   * We are waiting for returnConnection for connection toward this specific server.
   * After it is returned, we will reuse it.
   */
  @Test
  public void test_borrow_connection_toward_specific_server_no_freeConnection_wait_returnConnection_toward_this_server() {
    long idleTimeoutMillis = 800;
    manager =
        new ConnectionManagerImpl("pool", factory, endpointManager, 5, 1, idleTimeoutMillis, -1,
            logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    await().untilAsserted(() -> assertThat(manager.getConnectionCount()).isOne());

    long borrowTimeoutMillis = 500;

    // seize connection toward any server
    manager.borrowConnection(borrowTimeoutMillis);
    manager.borrowConnection(borrowTimeoutMillis);

    // Seize connection toward this specific server
    Connection ping1 =
        manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    Connection ping2 =
        manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);
    manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, false);

    // Return some connections, let them idle expire
    manager.returnConnection(ping1);
    manager.returnConnection(ping2);

    assertThatCode(() -> {
      manager.borrowConnection(new ServerLocation("localhost", 5), borrowTimeoutMillis, true);
    }).doesNotThrowAnyException();
  }

  @Test
  public void testLifetimeExpiration() throws Exception {
    int lifetimeTimeout = 500;
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 2, 2, -1, lifetimeTimeout,
        logger, 60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    await().untilAsserted(() -> {
      assertThat(factory.creates.get()).isEqualTo(2);
      assertThat(factory.destroys.get()).isEqualTo(0);
      assertThat(factory.finds.get()).isEqualTo(0);
    });

    // need to start a thread that keeps the connections busy
    // so that their last access time keeps changing

    Collection<Future<Void>> updaters = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      updaters
          .add(executorServiceRule.submit(new UpdaterThread(i, lifetimeTimeout / 10 * 2, false)));
    }

    long elapsedMillis = Timer.measure(() -> {
      await().untilAsserted(() -> {
        assertThat(factory.finds.get()).isEqualTo(2);
        // server shouldn't have changed so no increase in creates or destroys
        assertThat(factory.creates.get()).isEqualTo(2);
        assertThat(factory.destroys.get()).isEqualTo(0);
        assertThat(factory.closes.get()).isEqualTo(0);
      });
    });
    assertThat(elapsedMillis)
        .withFailMessage("took too long to expire lifetime; expected=" + lifetimeTimeout
            + " but took=" + elapsedMillis)
        .isLessThan(lifetimeTimeout * 5);

    for (Future<Void> updater : updaters) {
      updater.get(getTimeout().toMillis(), MILLISECONDS);
    }
  }

  @Test
  public void testExclusiveConnectionAccess() throws Exception {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Collection<Future<Void>> updaters = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      updaters.add(executorServiceRule.submit(new UpdaterThread(i)));
    }

    for (Future<Void> updater : updaters) {
      updater.get(getTimeout().toMillis(), MILLISECONDS);
    }
  }

  @Test
  public void testClose() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 10, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection connection = manager.borrowConnection(0);

    manager.borrowConnection(0);
    manager.returnConnection(connection);

    assertThat(factory.creates.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(0);

    manager.close(false);

    assertThat(factory.closes.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(2);
  }

  @Test
  public void testExchangeConnection() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 2, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection connection1 = manager.borrowConnection(10);
    Connection connection2 = manager.borrowConnection(10);

    Throwable thrown = catchThrowable(() -> {
      manager.borrowConnection(10);
    });
    assertThat(thrown).isInstanceOf(AllConnectionsInUseException.class);

    assertThat(factory.creates.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(0);
    assertThat(manager.getConnectionCount()).isEqualTo(2);

    Connection connection3 = manager.exchangeConnection(connection1, emptySet());

    assertThat(factory.creates.get()).isEqualTo(3);
    assertThat(factory.destroys.get()).isEqualTo(1);
    assertThat(manager.getConnectionCount()).isEqualTo(2);

    manager.returnConnection(connection2);

    assertThat(factory.creates.get()).isEqualTo(3);
    assertThat(factory.destroys.get()).isEqualTo(1);
    assertThat(manager.getConnectionCount()).isEqualTo(2);

    Connection connection4 =
        manager.exchangeConnection(connection3, singleton(connection3.getServer()));

    assertThat(factory.creates.get()).isEqualTo(4);
    assertThat(factory.destroys.get()).isEqualTo(2);
    assertThat(manager.getConnectionCount()).isEqualTo(2);

    manager.returnConnection(connection4);
  }

  /**
   * This tests that a deadlock between connection formation and connection pool closing has been
   * fixed. See GEODE-4615
   */
  @Test
  public void testThatMapCloseCausesCacheClosedException() throws Exception {
    ConnectionManagerImpl connectionManagerImpl = new ConnectionManagerImpl("pool", factory,
        endpointManager, 2, 0, -1, -1, logger, 60 * 1000, cancelCriterion, poolStats);
    manager = connectionManagerImpl;
    manager.start(background);
    ConnectionMap connectionMap = connectionManagerImpl.allConnectionsMap;

    CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

    Future<Void> future = executorServiceRule.submit(() -> {
      cyclicBarrier.await(getTimeout().toMillis(), MILLISECONDS);
      connectionMap.close(false);
    });

    Connection connection = manager.borrowConnection(0);
    synchronized (connection) {
      cyclicBarrier.await(getTimeout().toMillis(), MILLISECONDS);

      // the other thread will now try to close the connection map but it will block
      // because this thread has locked one of the connections
      await().until(() -> connectionMap.closing);

      Throwable thrown = catchThrowable(() -> {
        manager.borrowConnection(0);
      });
      assertThat(thrown).isInstanceOf(CacheClosedException.class);
    }

    future.get(getTimeout().toMillis(), MILLISECONDS);
  }

  @Test
  public void testBlocking() throws Exception {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection connection1 = manager.borrowConnection(10);

    long borrowTimeoutMillis1 = 300;

    long elapsedMillis = Timer.measure(() -> {
      Throwable thrown = catchThrowable(() -> {
        manager.borrowConnection(borrowTimeoutMillis1);
      });
      assertThat(thrown).isInstanceOf(AllConnectionsInUseException.class);
    });
    assertThat(elapsedMillis)
        .withFailMessage(
            "Should have blocked for " + borrowTimeoutMillis1 + " millis for a connection")
        .isGreaterThanOrEqualTo(borrowTimeoutMillis1 - ALLOWABLE_ERROR_IN_MILLIS);

    Future<Void> returnConnectionFuture = executorServiceRule.submit(() -> {
      Thread.sleep(50);

      manager.returnConnection(connection1);
    });

    long borrowTimeoutMillis2 = 5000;

    AtomicReference<Connection> connection2 = new AtomicReference<>();
    elapsedMillis = Timer.measure(() -> {
      connection2.set(manager.borrowConnection(borrowTimeoutMillis2));
    });
    assertThat(elapsedMillis)
        .withFailMessage(
            "Should have blocked for less than " + borrowTimeoutMillis2 + " milliseconds")
        .isLessThan(borrowTimeoutMillis2 + ALLOWABLE_ERROR_IN_MILLIS);

    manager.returnConnection(connection2.get());

    Connection connection3 = manager.borrowConnection(10);

    Future<Void> invalidateFuture1 = executorServiceRule.submit(() -> {
      Thread.sleep(50);

      connection3.destroy();
      manager.returnConnection(connection3);
    });

    elapsedMillis = Timer.measure(() -> {
      connection2.set(manager.borrowConnection(borrowTimeoutMillis2));
    });
    assertThat(elapsedMillis)
        .withFailMessage(
            "Should have blocked for less than " + borrowTimeoutMillis2 + " milliseconds")
        .isLessThan(borrowTimeoutMillis2 + ALLOWABLE_ERROR_IN_MILLIS);

    manager.returnConnection(connection2.get());

    Connection connection4 = manager.borrowConnection(10);

    Future<Void> invalidateFuture2 = executorServiceRule.submit(() -> {
      Thread.sleep(50);

      endpointManager.serverCrashed(connection4.getEndpoint());
      manager.returnConnection(connection4);
    });

    elapsedMillis = Timer.measure(() -> {
      connection2.set(manager.borrowConnection(borrowTimeoutMillis2));
    });
    assertThat(elapsedMillis)
        .withFailMessage(
            "Should have blocked for less than " + borrowTimeoutMillis2 + " milliseconds")
        .isLessThan(borrowTimeoutMillis2 + ALLOWABLE_ERROR_IN_MILLIS);

    manager.returnConnection(connection2.get());

    returnConnectionFuture.get(getTimeout().toMillis(), MILLISECONDS);
    invalidateFuture1.get(getTimeout().toMillis(), MILLISECONDS);
    invalidateFuture2.get(getTimeout().toMillis(), MILLISECONDS);
  }

  @Test
  public void testExplicitServer() {
    manager = new ConnectionManagerImpl("pool", factory, endpointManager, 1, 0, -1, -1, logger,
        60 * 1000, cancelCriterion, poolStats);
    manager.start(background);

    Connection connection1 = manager.borrowConnection(0);

    Throwable thrown = catchThrowable(() -> {
      manager.borrowConnection(10);
    });
    assertThat(thrown).isInstanceOf(AllConnectionsInUseException.class);

    Connection connection2 =
        manager.borrowConnection(new ServerLocation("localhost", -2), 10, false);

    assertThat(factory.creates.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(0);
    assertThat(factory.closes.get()).isEqualTo(0);

    manager.returnConnection(connection2);

    assertThat(factory.creates.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(1);
    assertThat(factory.closes.get()).isEqualTo(1);

    manager.returnConnection(connection1);

    assertThat(factory.creates.get()).isEqualTo(2);
    assertThat(factory.destroys.get()).isEqualTo(1);
    assertThat(factory.closes.get()).isEqualTo(1);
  }

  private void checkIdleTimeout(long idleTimeoutMillis, long elapsedMillis) {
    assertThat(elapsedMillis)
        .as("Elapsed " + elapsedMillis + " is less than idle timeout " + idleTimeoutMillis)
        .isGreaterThanOrEqualTo(idleTimeoutMillis - ALLOWABLE_ERROR_IN_MILLIS);

    assertThat(elapsedMillis)
        .as("Elapsed " + elapsedMillis + " is greater than idle timeout " + idleTimeoutMillis)
        .isLessThanOrEqualTo(idleTimeoutMillis + ALLOWABLE_ERROR_IN_MILLIS);
  }

  private static class Timer {

    static long measure(ThrowingRunnable task) throws Exception {
      long startNanos = System.nanoTime();
      task.run();
      return NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
  }

  private class UpdaterThread implements ThrowingRunnable {

    private static final long BORROW_TIMEOUT_MILLIS = 2000;

    private final int id;
    private final int iterations;
    private final boolean doAssertions;

    private UpdaterThread(int id) {
      this(id, 10, true);
    }

    private UpdaterThread(int id, int iterations, boolean doAssertions) {
      this.id = id;
      this.iterations = iterations;
      this.doAssertions = doAssertions;
    }

    @Override
    public void run() {
      for (int i = 0; i < iterations; i++) {
        Connection connection = borrow(i);
        try {
          Thread.sleep(10);
          doTask("Updater[" + id + "] loop[" + i + "] Someone else changed the connection flag",
              () -> haveConnection.compareAndSet(true, false));
        } catch (Throwable throwable) {
          errorCollector.addError(throwable);
          break;
        } finally {
          manager.returnConnection(connection);
        }
      }
    }

    private Connection borrow(int i) {
      try {
        return doBorrow(i);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private Connection doBorrow(int i) throws Exception {
      AtomicReference<Connection> connection = new AtomicReference<>();

      long elapsedMillis = Timer.measure(() -> {
        connection.set(manager.borrowConnection(BORROW_TIMEOUT_MILLIS));

        doTask("Updater[" + id + "] loop[" + i + "] Someone else has the connection!",
            () -> haveConnection.compareAndSet(false, true));
      });

      doTask("Elapsed time (" + elapsedMillis + ") >= " + BORROW_TIMEOUT_MILLIS,
          () -> elapsedMillis < BORROW_TIMEOUT_MILLIS + ALLOWABLE_ERROR_IN_MILLIS);
      return connection.get();
    }

    private void doTask(String message, BooleanSupplier task) {
      boolean result = task.getAsBoolean();
      if (doAssertions) {
        assertThat(result)
            .as(message)
            .isTrue();
      }
    }
  }

  private class DummyFactory implements ConnectionFactory {

    private final AtomicReference<ServerLocation> nextServer =
        new AtomicReference<>(new ServerLocation("localhost", -1));
    private final AtomicInteger creates = new AtomicInteger();
    private final AtomicInteger destroys = new AtomicInteger();
    private final AtomicInteger closes = new AtomicInteger();
    private final AtomicInteger finds = new AtomicInteger();

    @Override
    public ServerDenyList getDenyList() {
      return new ServerDenyList(1);
    }

    @Override
    public ServerLocation findBestServer(ServerLocation currentServer, Set excludedServers) {
      finds.incrementAndGet();
      if (excludedServers != null) {
        if (excludedServers.contains(nextServer.get())) {
          return null;
        }
      }
      return nextServer.get();
    }

    @Override
    public Connection createClientToServerConnection(Set excluded) {
      return createClientToServerConnection(nextServer.get(), true);
    }

    @Override
    public Connection createClientToServerConnection(final ServerLocation location,
        boolean forQueue) {
      creates.incrementAndGet();
      DistributedMember member = new InternalDistributedMember("localhost", 555);
      return new Connection() {

        private final Endpoint endpoint = endpointManager.referenceEndpoint(location, member);

        @Override
        public void destroy() {
          destroys.incrementAndGet();
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
        public void close(boolean keepAlive) {
          closes.incrementAndGet();
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
        public void emergencyClose() {
          // nothing
        }

        @Override
        public short getWanSiteVersion() {
          return -1;
        }

        @Override
        public int getDistributedSystemId() {
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
      };
    }

    @Override
    public ClientUpdater createServerToClientConnection(Endpoint endpoint, QueueManager manager,
        boolean isPrimary, ClientUpdater failedUpdater) {
      return null;
    }
  }
}
