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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.LocatorRequestDeserializer;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.DSFIDSerializer;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Exercises the request-reading path of a locator's {@link TcpServer}, confirming that the type
 * read
 * off the request stream is constrained to the locator's request types rather than any type the
 * stream names, while the real request types are still read.
 */
@Category({MembershipTest.class})
public class TcpServerRequestFormatIntegrationTest {

  private static final int TIMEOUT = 60 * 1000;

  private InetAddress localhost;
  private TcpServer server;
  private int port;
  private DSFIDSerializer serializer;
  private final AtomicReference<Object> handledRequest = new AtomicReference<>();

  @Before
  public void setUp() throws IOException {
    ProbeObject.reconstructed = false;
    localhost = InetAddress.getLoopbackAddress();

    // Clients encode requests with the general serializer, exactly as a real client does.
    serializer = InternalDataSerializer.getDSFIDSerializer();

    TcpHandler handler = new TcpHandler() {
      @Override
      public Object processRequest(Object request) {
        handledRequest.set(request);
        return null;
      }

      @Override
      public void endRequest(Object request, long startTime) {}

      @Override
      public void endResponse(Object request, long startTime) {}

      @Override
      public void shutDown() {}

      @Override
      public void init(TcpServer tcpServer) {}
    };

    // The server reads requests the way a locator does: through the locator request deserializer.
    server = new TcpServer(0, localhost, handler,
        "test locator thread",
        (socket, input, firstByte) -> false,
        () -> 0,
        () -> LoggingExecutors.newCachedThreadPool("test thread", true),
        null,
        serializer.getObjectSerializer(),
        new LocatorRequestDeserializer(),
        "blahblahblah",
        "blahblahblah");
    server.start();
    port = server.getPort();
    assertThat(port).isGreaterThan(0);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      TcpClient tcpClient = new TcpClient(new TcpSocketCreatorImpl(),
          serializer.getObjectSerializer(),
          serializer.getObjectDeserializer(), TcpSocketFactory.DEFAULT);
      try {
        tcpClient.stop(new HostAndPort(localhost.getHostAddress(), port));
      } catch (Exception ignore) {
        // best-effort stop
      }
      server.join(TIMEOUT);
    }
  }

  /**
   * A request naming a type the locator does not expect is rejected before that type is
   * reconstructed. {@link ProbeObject} is not a locator request type, so its {@code readObject} is
   * never run.
   */
  @Test
  public void doesNotReconstructUnexpectedRequestType() throws Exception {
    sendRequest(new ProbeObject());

    assertThat(ProbeObject.reconstructed).isFalse();
    assertThat(handledRequest.get()).isNull();
  }

  /**
   * A request carried with an explicit class name that the locator does expect is still read. This
   * covers the data-serialized form.
   */
  @Test
  public void readsExpectedDataSerializableRequest() throws Exception {
    @SuppressWarnings("deprecation")
    InfoRequest request = new InfoRequest();
    sendRequest(request);

    await().untilAsserted(() -> assertThat(handledRequest.get()).isInstanceOf(InfoRequest.class));
  }

  /**
   * A request carried in Java-serialized form whose class the locator expects is still read.
   */
  @Test
  public void readsExpectedSerializableRequest() throws Exception {
    sendRequest(new ClusterManagementServiceInfoRequest());

    await().untilAsserted(
        () -> assertThat(handledRequest.get())
            .isInstanceOf(ClusterManagementServiceInfoRequest.class));
  }

  private void sendRequest(Object request) throws Exception {
    byte[] encoded = encodeRequest(request);
    try (Socket socket = new Socket(localhost, port)) {
      socket.setSoTimeout(TIMEOUT);
      DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      out.write(encoded);
      out.flush();
      // Drain whatever the server sends (or until it closes) so the request is fully processed.
      DataInputStream in = new DataInputStream(socket.getInputStream());
      try {
        while (in.read() != -1) {
          // consume
        }
      } catch (IOException ignore) {
        // server closed the socket
      }
    }
  }

  /**
   * Frames a request the way {@code TcpClient} does: the gossip version int (whose leading byte is
   * the gossip marker), the serialization version ordinal, then the serialized request object.
   */
  private byte[] encodeRequest(Object request) throws IOException {
    HeapDataOutputStream body = new HeapDataOutputStream(KnownVersion.CURRENT);
    serializer.getObjectSerializer().writeObject(request, body);

    HeapDataOutputStream out = new HeapDataOutputStream(KnownVersion.CURRENT);
    out.writeInt(TcpServer.GOSSIPVERSION);
    out.writeShort(KnownVersion.CURRENT_ORDINAL);
    out.write(body.toByteArray());
    return out.toByteArray();
  }

  /**
   * Test-only type that records whether it was reconstructed. Not a locator request type.
   */
  public static class ProbeObject implements Serializable {
    static boolean reconstructed;

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();
      reconstructed = true;
    }
  }
}
