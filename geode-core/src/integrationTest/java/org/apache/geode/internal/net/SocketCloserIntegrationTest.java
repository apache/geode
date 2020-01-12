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
package org.apache.geode.internal.net;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * Tests the default SocketCloser.
 */
@Category(MembershipTest.class)
public class SocketCloserIntegrationTest {

  private SocketCloser socketCloser;

  @Before
  public void setUp() throws Exception {
    this.socketCloser = createSocketCloser();
  }

  @After
  public void tearDown() throws Exception {
    this.socketCloser.close();
  }

  private Socket createClosableSocket() {
    return new Socket();
  }

  protected SocketCloser createSocketCloser() {
    return new SocketCloser();
  }

  /**
   * Test that close requests are async.
   */
  @Test
  public void testAsync() {
    final CountDownLatch countDownLatch = new CountDownLatch(1);
    final AtomicInteger waitingToClose = new AtomicInteger(0);

    final int SOCKET_COUNT = 100;
    final int REMOTE_CLIENT_COUNT = 200;

    List<Socket> trackedSockets = new ArrayList<>();
    // Schedule a 100 sockets for async close.
    // They should all be stuck on countDownLatch.
    for (int i = 0; i < REMOTE_CLIENT_COUNT; i++) {
      Socket[] aSockets = new Socket[SOCKET_COUNT];
      String address = i + "";
      for (int j = 0; j < SOCKET_COUNT; j++) {
        aSockets[j] = createClosableSocket();
        trackedSockets.add(aSockets[j]);
        this.socketCloser.asyncClose(aSockets[j], address, () -> {
          try {
            waitingToClose.incrementAndGet();
            countDownLatch.await(5, TimeUnit.MINUTES);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        });
      }
    }

    // close the socketCloser first to verify that the sockets
    // that have already been scheduled will be still be closed.
    this.socketCloser.close();
    countDownLatch.countDown();
    // now all the sockets should get closed; use a wait criteria
    // since a thread pool is doing to closes
    await().until(() -> {
      boolean areAllClosed = true;
      for (Iterator<Socket> iterator = trackedSockets.iterator(); iterator.hasNext();) {
        Socket socket = iterator.next();
        if (socket.isClosed()) {
          iterator.remove();
          continue;
        }
        areAllClosed = false;
      }
      return areAllClosed;
    });
  }

  /**
   * Verify that requesting an asyncClose on an already closed socket is a noop.
   */
  @Test
  public void testClosedSocket() throws Exception {
    final AtomicBoolean runnableCalled = new AtomicBoolean();

    Socket s = createClosableSocket();
    s.close();
    this.socketCloser.asyncClose(s, "A", () -> runnableCalled.set(true));
    await().until(() -> !runnableCalled.get());
  }

  /**
   * Verify that a closed SocketCloser will still close an open socket
   */
  @Test
  public void testClosedSocketCloser() {
    final AtomicBoolean runnableCalled = new AtomicBoolean();

    final Socket closableSocket = createClosableSocket();
    this.socketCloser.close();
    this.socketCloser.asyncClose(closableSocket, "A", () -> runnableCalled.set(true));
    await()
        .until(() -> runnableCalled.get() && closableSocket.isClosed());
  }
}
