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
package org.apache.geode.distributed.internal.tcpserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.tier.sockets.TcpServerFactory;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class TcpServerJUnitTest {

  private/* GemStoneAddition */ InetAddress localhost;
  private/* GemStoneAddition */ int port;
  private SimpleStats stats;
  private TcpServer server;

  @Before
  public void setup() {
    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
  }

  @After
  public void teardown() {
    SocketCreatorFactory.close();
  }

  private void start(TcpHandler handler) throws IOException {
    localhost = InetAddress.getLocalHost();
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    stats = new SimpleStats();
    server = new TcpServerFactory().makeTcpServer(port, localhost, new Properties(), null, handler,
        stats, "server thread", null);
    server.start();
  }

  @Test
  public void test() throws Exception {
    EchoHandler handler = new EchoHandler();
    start(handler);

    TcpClient tcpClient = new TcpClient();

    TestObject test = new TestObject();
    test.id = 5;
    TestObject result =
        (TestObject) tcpClient.requestToServer(localhost, port, test, 60 * 1000);
    assertEquals(test.id, result.id);

    String[] info = tcpClient.getInfo(localhost, port);
    assertNotNull(info);
    assertTrue(info.length > 1);

    try {
      tcpClient.stop(localhost, port);
    } catch (ConnectException ignore) {
      // must not be running
    }
    server.join(60 * 1000);
    assertFalse(server.isAlive());
    assertTrue(handler.shutdown);

    assertEquals(4, stats.started.get());
    assertEquals(4, stats.ended.get());

  }

  @Test
  public void testConcurrency() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    DelayHandler handler = new DelayHandler(latch);
    start(handler);

    TcpClient tcpClient = new TcpClient();

    final AtomicBoolean done = new AtomicBoolean();
    Thread delayedThread = new Thread() {
      public void run() {
        Boolean delay = Boolean.valueOf(true);
        try {
          tcpClient.requestToServer(localhost, port, delay, 60 * 1000);
        } catch (IOException e) {
          e.printStackTrace();
        } catch (ClassNotFoundException e) {
          e.printStackTrace();
        }
        done.set(true);
      }
    };
    delayedThread.start();
    try {
      Thread.sleep(500);
      assertFalse(done.get());
      tcpClient.requestToServer(localhost, port, Boolean.valueOf(false), 60 * 1000);
      assertFalse(done.get());

      latch.countDown();
      Thread.sleep(500);
      assertTrue(done.get());
    } finally {
      latch.countDown();
      delayedThread.join(60 * 1000);
      assertTrue(!delayedThread.isAlive()); // GemStoneAddition
      try {
        tcpClient.stop(localhost, port);
      } catch (ConnectException ignore) {
        // must not be running
      }
      server.join(60 * 1000);
    }
  }

  @Test
  public void testNewConnectionsAcceptedAfterSocketException() throws IOException,
      ClassNotFoundException, InterruptedException {
    // Initially mock the handler to throw a SocketException. We want to verify that the server
    // can recover and serve new client requests after a SocketException is thrown.
    TcpHandler mockTcpHandler = mock(TcpHandler.class);
    doThrow(SocketException.class).when(mockTcpHandler).processRequest(any(Object.class));
    start(mockTcpHandler);

    TcpClient tcpClient = new TcpClient();

    // Due to the mocked handler, an EOFException will be thrown on the client. This is expected,
    // so we just catch it.
    try {
      tcpClient.requestToServer(localhost, port, new TestObject(), 60 * 1000);
    } catch (EOFException eofEx) {
    }

    // Change the mock handler behavior to echo the request back
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return invocation.getArgument(0);
      }
    }).when(mockTcpHandler).processRequest(any(Object.class));

    // Perform another request and validate that it was served successfully
    TestObject test = new TestObject();
    test.id = 5;
    TestObject result =
        (TestObject) tcpClient.requestToServer(localhost, port, test, 60 * 1000);

    assertEquals(test.id, result.id);
    String[] info = tcpClient.getInfo(localhost, port);
    assertNotNull(info);
    assertTrue(info.length > 1);

    try {
      tcpClient.stop(localhost, port);
    } catch (ConnectException ignore) {
      // must not be running
    }
    server.join(60 * 1000);
    assertFalse(server.isAlive());

    assertEquals(5, stats.started.get());
    assertEquals(5, stats.ended.get());
  }

  private static class TestObject implements DataSerializable {
    int id;

    public TestObject() {

    }

    public void fromData(DataInput in) throws IOException {
      id = in.readInt();
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(id);
    }

  }

  private/* GemStoneAddition */ static class EchoHandler implements TcpHandler {

    protected/* GemStoneAddition */ boolean shutdown;


    public void init(TcpServer tcpServer) {
      // TODO Auto-generated method stub

    }

    public Object processRequest(Object request) throws IOException {
      return request;
    }

    public void shutDown() {
      shutdown = true;
    }

    public void restarting(DistributedSystem ds, GemFireCache cache,
        InternalConfigurationPersistenceService sharedConfig) {}

    public void endRequest(Object request, long startTime) {}

    public void endResponse(Object request, long startTime) {}

  }

  private static class DelayHandler implements TcpHandler {

    private CountDownLatch latch;

    public DelayHandler(CountDownLatch latch) {
      this.latch = latch;
    }

    public void init(TcpServer tcpServer) {}

    public Object processRequest(Object request) throws IOException {
      Boolean delay = (Boolean) request;
      if (delay.booleanValue()) {
        try {
          latch.await(120 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return delay;
    }

    public void shutDown() {}

    public void restarting(DistributedSystem ds, GemFireCache cache,
        InternalConfigurationPersistenceService sharedConfig) {}

    public void endRequest(Object request, long startTime) {}

    public void endResponse(Object request, long startTime) {}
  }

  private/* GemStoneAddition */ static class SimpleStats implements PoolStatHelper {
    AtomicInteger started = new AtomicInteger();
    AtomicInteger ended = new AtomicInteger();


    public void endJob() {
      started.incrementAndGet();
    }

    public void startJob() {
      ended.incrementAndGet();
    }
  }
}
