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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.serialization.BasicSerializable;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializerFactory;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class TcpServerJUnitTest {

  private static final int TIMEOUT = 60 * 1000;
  private InetAddress localhost;
  private int port;
  private TcpServer server;

  private void startTcpServerWithHandler(TcpHandler handler) throws IOException {
    localhost = InetAddress.getLocalHost();

    DSFIDSerializer serializer = new DSFIDSerializerFactory().create();
    server = new TcpServer(port, localhost, handler,
        "server thread",
        (x, y, z) -> false, // protobuf hook - not needed here
        () -> 0, // stats time
        () -> LoggingExecutors.newCachedThreadPool("test thread", true),
        null,
        serializer.getObjectSerializer(),
        serializer.getObjectDeserializer(),
        "blahblahblah",
        "blahblahblah");
    server.start();
    port = server.getPort();
    assertThat(port).isGreaterThan(0);
    assertThat(server.isShuttingDown()).isFalse();
    assertThat(server.getSocketAddress()).isNotNull();
    assertThat(server.isAlive()).isTrue();
  }

  @Test
  public void testConnectToUnknownHost() {
    final TcpClient tcpClient = createTcpClient();
    @SuppressWarnings("deprecation")
    InfoRequest testInfoRequest = new InfoRequest();
    assertThatThrownBy(() -> tcpClient.requestToServer(new HostAndPort("unknown host name", port),
        testInfoRequest, TIMEOUT))
            .as("Hostname resolved unexpectedly. Check for DNS hijacking in addition to code errors.")
            .isInstanceOf(UnknownHostException.class);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testClientGetInfo() throws Exception {
    TcpHandler handler = new InfoRequestHandler();
    startTcpServerWithHandler(handler);

    final TcpClient tcpClient = createTcpClient();

    InfoRequest testInfoRequest = new InfoRequest();
    InfoResponse testInfoResponse =
        (InfoResponse) tcpClient.requestToServer(new HostAndPort(localhost.getHostAddress(), port),
            testInfoRequest, TIMEOUT);
    assertThat(testInfoResponse.getInfo()[0]).contains("geode-tcp-server");

    String[] requestedInfo = tcpClient.getInfo(new HostAndPort(localhost.getHostAddress(), port));
    assertNotNull(requestedInfo);
    assertTrue(requestedInfo.length > 1);
    assertThat(requestedInfo[0]).contains("geode-tcp-server");

    stopServer(tcpClient);
  }

  private TcpClient createTcpClient() {
    DSFIDSerializer serializer = new DSFIDSerializerFactory().create();
    TcpSocketCreator socketCreator = new TcpSocketCreatorImpl();
    return new TcpClient(socketCreator,
        serializer.getObjectSerializer(),
        serializer.getObjectDeserializer(), TcpSocketFactory.DEFAULT);
  }

  @Test
  public void testConcurrency() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    DelayHandler handler = new DelayHandler(latch);
    startTcpServerWithHandler(handler);
    final TcpClient tcpClient = createTcpClient();

    final AtomicBoolean done = new AtomicBoolean();
    Thread delayedThread = new Thread(() -> {
      try {
        tcpClient.requestToServer(new HostAndPort(localhost.getHostAddress(), port),
            new TestObject(1),
            TIMEOUT);
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
      }
      done.set(true);
    });
    delayedThread.start();
    try {
      Thread.sleep(500);
      assertFalse(done.get());
      tcpClient.requestToServer(new HostAndPort(localhost.getHostAddress(), port),
          new TestObject(0), TIMEOUT);
      assertFalse(done.get());

      latch.countDown();
      Thread.sleep(500);
      assertTrue(done.get());
    } finally {
      latch.countDown();
      delayedThread.join(TIMEOUT);
      assertFalse(delayedThread.isAlive()); // GemStoneAddition
      stopServer(tcpClient);
    }
  }

  @Test
  public void testNewConnectionsAcceptedAfterSocketException() throws IOException,
      ClassNotFoundException, InterruptedException {
    // Initially mock the handler to throw a SocketException. We want to verify that the server
    // can recover and serve new client requests after a SocketException is thrown.
    TcpHandler mockTcpHandler = mock(TcpHandler.class);
    doThrow(SocketException.class).when(mockTcpHandler).processRequest(any(Object.class));
    startTcpServerWithHandler(mockTcpHandler);
    final TcpClient tcpClient = createTcpClient();

    // Due to the mocked handler, an EOFException will be thrown on the client. This is expected.
    assertThatThrownBy(
        () -> tcpClient.requestToServer(new HostAndPort(localhost.getHostAddress(), port),
            new TestObject(), TIMEOUT))
                .isInstanceOf(EOFException.class);

    // Change the mock handler behavior to echo the request back
    doAnswer(invocation -> invocation.getArgument(0)).when(mockTcpHandler)
        .processRequest(any(Object.class));

    // Perform another request and validate that it was served successfully
    TestObject test = new TestObject();
    test.id = 5;
    TestObject result =
        (TestObject) tcpClient.requestToServer(new HostAndPort(localhost.getHostAddress(), port),
            test, TIMEOUT);

    assertEquals(test.id, result.id);

    stopServer(tcpClient);
  }

  private void stopServer(final TcpClient tcpClient) throws InterruptedException {
    try {
      tcpClient.stop(new HostAndPort(localhost.getHostAddress(), port));
    } catch (ConnectException ignore) {
      // must not be running
    }
    server.join(TIMEOUT);
    assertFalse(server.isAlive());
  }

  private static class TestObject implements BasicSerializable {
    int id;

    public TestObject() {}

    public TestObject(int id) {
      this.id = id;
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context) throws IOException {
      id = in.readInt();
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      out.writeInt(id);
    }

  }

  private static class DelayHandler implements TcpHandler {

    private CountDownLatch latch;

    public DelayHandler(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void init(TcpServer tcpServer) {}

    @Override
    public Object processRequest(Object request) {
      TestObject delay = (TestObject) request;
      if (delay.id > 0) {
        try {
          latch.await(120 * 1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return delay;
    }

    @Override
    public void shutDown() {}

    @Override
    public void endRequest(Object request, long startTime) {}

    @Override
    public void endResponse(Object request, long startTime) {}
  }


  public static class InfoRequestHandler implements TcpHandler {
    public InfoRequestHandler() {}

    @SuppressWarnings("deprecation")
    @Override
    public Object processRequest(final Object request) {
      String[] info = new String[2];
      info[0] = System.getProperty("user.dir");
      info[1] = System.getProperty("java.version");
      return new InfoResponse(info);
    }

    @Override
    public void endRequest(final Object request, final long startTime) {}

    @Override
    public void endResponse(final Object request, final long startTime) {}

    @Override
    public void shutDown() {}

    @Override
    public void init(final TcpServer tcpServer) {}
  }

}
