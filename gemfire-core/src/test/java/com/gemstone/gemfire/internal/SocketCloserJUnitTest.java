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
package com.gemstone.gemfire.internal;


import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests the default SocketCloser.
 */
@Category(UnitTest.class)
public class SocketCloserJUnitTest {

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
    final CountDownLatch cdl = new CountDownLatch(1);
    final AtomicInteger waitingToClose = new AtomicInteger(0);
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          waitingToClose.incrementAndGet();
          cdl.await();
        } catch (InterruptedException e) {
        }
      }
    };
    
    final int SOCKET_COUNT = 100;
    final Socket[] aSockets = new Socket[SOCKET_COUNT];
    for (int i=0; i < SOCKET_COUNT; i++) {
      aSockets[i] = createClosableSocket();
    }
    // Schedule a 100 sockets for async close.
    // They should all be stuck on cdl.
    for (int i=0; i < SOCKET_COUNT; i++) {
      this.socketCloser.asyncClose(aSockets[i], "A", r);
    }
    // Make sure the sockets have not been closed
    for (int i=0; i < SOCKET_COUNT; i++) {
      assertEquals(false, aSockets[i].isClosed());
    }
    final Socket[] bSockets = new Socket[SOCKET_COUNT];
    for (int i=0; i < SOCKET_COUNT; i++) {
      bSockets[i] = createClosableSocket();
    }
    // Schedule a 100 sockets for async close.
    // They should all be stuck on cdl.
    for (int i=0; i < SOCKET_COUNT; i++) {
      this.socketCloser.asyncClose(bSockets[i], "B", r);
    }
    // Make sure the sockets have not been closed
    for (int i=0; i < SOCKET_COUNT; i++) {
      assertEquals(false, bSockets[i].isClosed());
    }
    // close the socketCloser first to verify that the sockets
    // that have already been scheduled will be still be closed.
    this.socketCloser.releaseResourcesForAddress("A");
    this.socketCloser.close();
    // Each thread pool (one for A and one for B) has a max of 8 threads.
    // So verify that this many are currently waiting on cdl.
    {
      final int maxThreads = this.socketCloser.getMaxThreads();
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          return waitingToClose.get() == 2*maxThreads;
        }
        public String description() {
          return "expected " + 2*maxThreads + " waiters but found only " + waitingToClose.get();
        }
      };
      DistributedTestCase.waitForCriterion(wc, 5000, 10, true);
    }
    // now count down the latch that allows the sockets to close
    cdl.countDown();
    // now all the sockets should get closed; use a wait criteria
    // since a thread pool is doing to closes
    {
      WaitCriterion wc = new WaitCriterion() {
        public boolean done() {
          for (int i=0; i < SOCKET_COUNT; i++) {
            if (!aSockets[i].isClosed() || !bSockets[i].isClosed()) {
              return false;
            }
          }
          return true;
        }
        public String description() {
          return "one or more sockets did not close";
        }
      };
      DistributedTestCase.waitForCriterion(wc, 5000, 10, true);
    }
  }
  
  /**
   * Verify that requesting an asyncClose on an already
   * closed socket is a noop.
   */
  @Test
  public void testClosedSocket() throws IOException {
    final AtomicBoolean runnableCalled = new AtomicBoolean();
    Runnable r = new Runnable() {
      @Override
      public void run() {
        runnableCalled.set(true);
      }
    };
    
    Socket s = createClosableSocket();
    s.close();
    this.socketCloser.asyncClose(s, "A", r);
    DistributedTestCase.pause(10);
    assertEquals(false, runnableCalled.get());
  }
  
  /**
   * Verify that a closed SocketCloser will still close an open socket
   */
  @Test
  public void testClosedSocketCloser() {
    final AtomicBoolean runnableCalled = new AtomicBoolean();
    Runnable r = new Runnable() {
      @Override
      public void run() {
        runnableCalled.set(true);
      }
    };
    
    final Socket s = createClosableSocket();
    this.socketCloser.close();
    this.socketCloser.asyncClose(s, "A", r);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return runnableCalled.get() && s.isClosed(); 
      }
      public String description() {
        return "runnable was not called or socket was not closed";
      }
    };
    DistributedTestCase.waitForCriterion(wc, 5000, 10, true);
  }
}
