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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

public class ByteBufferSharingImplTest {

  @FunctionalInterface
  private static interface Foo {
    void run() throws IOException;
  }

  private ByteBufferSharingImpl sharing;
  private BufferPool poolMock;
  private CountDownLatch clientHasOpenedResource;
  private CountDownLatch clientMayComplete;

  @Before
  public void before() {
    poolMock = mock(BufferPool.class);
    sharing =
        new ByteBufferSharingImpl(mock(ByteBuffer.class), BufferPool.BufferType.TRACKED_SENDER,
            poolMock);
    clientHasOpenedResource = new CountDownLatch(1);
    clientMayComplete = new CountDownLatch(1);
  }

  @Test
  public void balancedCloseOwnerIsLastReferenceHolder() throws InterruptedException {
    resourceOwnerIsLastReferenceHolder("client with balanced close calls", () -> {
      try (final ByteBufferSharing _unused = sharing.open()) {
      }
    });
  }

  @Test
  public void extraCloseOwnerIsLastReferenceHolder() throws InterruptedException {
    resourceOwnerIsLastReferenceHolder("client with extra close calls", () -> {
      final ByteBufferSharing sharing2 = sharing.open();
      sharing2.close();
      verify(poolMock, times(0)).releaseBuffer(any(), any());
      assertThatThrownBy(() -> sharing2.close()).isInstanceOf(IllegalMonitorStateException.class);
      verify(poolMock, times(0)).releaseBuffer(any(), any());
    });
  }

  @Test
  public void balancedCloseClientIsLastReferenceHolder() throws InterruptedException {
    clientIsLastReferenceHolder("client with balanced close calls", () -> {
      try (final ByteBufferSharing _unused = sharing.open()) {
        clientHasOpenedResource.countDown();
        blockClient();
      }
    });
  }

  @Test
  public void extraCloseClientIsLastReferenceHolder() throws InterruptedException {
    clientIsLastReferenceHolder("client with extra close calls", () -> {
      final ByteBufferSharing sharing2 = sharing.open();
      clientHasOpenedResource.countDown();
      blockClient();
      sharing2.close();
      verify(poolMock, times(1)).releaseBuffer(any(), any());
      assertThatThrownBy(() -> sharing2.close()).isInstanceOf(IllegalMonitorStateException.class);
      System.out.println("here");
    });
  }

  @Test
  public void extraCloseDoesNotPrematurelyReturnBufferToPool() throws IOException {
    final ByteBufferSharing sharing2 = sharing.open();
    sharing2.close();
    assertThatThrownBy(() -> sharing2.close()).isInstanceOf(IllegalMonitorStateException.class);
    verify(poolMock, times(0)).releaseBuffer(any(), any());
    sharing.destruct();
    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  @Test
  public void extraCloseDoesNotDecrementRefCount() throws IOException {
    final ByteBufferSharing sharing2 = sharing.open();
    sharing2.close();
    assertThatThrownBy(() -> sharing2.close()).isInstanceOf(IllegalMonitorStateException.class);
    final ByteBufferSharing sharing3 = this.sharing.open();
    sharing.destruct();
    verify(poolMock, times(0)).releaseBuffer(any(), any());
  }

  private void resourceOwnerIsLastReferenceHolder(final String name, final Foo client)
      throws InterruptedException {
    /*
     * Thread.currentThread() is thread is playing the role of the (ByteBuffer) resource owner
     */

    /*
     * clientThread thread is playing the role of the client (of the resource owner)
     */
    final Thread clientThread = new Thread(asRunnable(client), name);
    clientThread.start();
    clientThread.join();

    verify(poolMock, times(0)).releaseBuffer(any(), any());

    sharing.destruct();

    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  private void clientIsLastReferenceHolder(final String name, final Foo client)
      throws InterruptedException {
    /*
     * Thread.currentThread() is thread is playing the role of the (ByteBuffer) resource owner
     */

    /*
     * clientThread thread is playing the role of the client (of the resource owner)
     */
    final Thread clientThread = new Thread(asRunnable(client), name);
    clientThread.start();

    clientHasOpenedResource.await();

    sharing.destruct();

    verify(poolMock, times(0)).releaseBuffer(any(), any());

    clientMayComplete.countDown(); // let client finish

    clientThread.join();

    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  private void blockClient() {
    try {
      clientMayComplete.await();
    } catch (InterruptedException e) {
      fail("test client thread interrupted: " + e);
    }
  }

  private Runnable asRunnable(final Foo client) {
    return () -> {
      try {
        client.run();
      } catch (IOException e) {
        fail("client thread threw: ", e);
      }
    };
  }

}
