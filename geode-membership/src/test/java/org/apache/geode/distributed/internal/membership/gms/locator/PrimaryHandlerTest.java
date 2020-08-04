/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal.membership.gms.locator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.distributed.internal.tcpserver.TcpHandler;

public class PrimaryHandlerTest {

  @Test
  public void processRequest() throws IOException {
    final AtomicInteger sleepCount = new AtomicInteger();
    final TcpHandler fallbackHandler = null;
    final int locatorWaitTime = 5;
    final long startTime = System.currentTimeMillis();
    PrimaryHandler primaryHandler = new PrimaryHandler(fallbackHandler, locatorWaitTime,
        x -> {
          sleepCount.incrementAndGet();
          Thread.sleep(x);
        });
    // process a request that has no handler - this should loop calling the sleep function
    // until locatorWaitTime elapses
    final Object result = primaryHandler.processRequest(new Object());
    // under normal circumstances we'd have locatorWaitTime sleeps, but delays might make it fewer
    // so let's just make sure there were at least two
    assertThat(sleepCount.get()).isGreaterThan(1);
    assertThat(result).isNull();
    assertThat(System.currentTimeMillis())
        .isGreaterThanOrEqualTo(startTime + (1000 * locatorWaitTime));
  }

  @Test
  public void fallbackHandler() throws IOException {
    final AtomicInteger handlerInvoked = new AtomicInteger();
    final TcpHandler fallbackHandler = mock(TcpHandler.class);
    when(fallbackHandler.processRequest(isA(Object.class))).thenAnswer(context -> {
      handlerInvoked.incrementAndGet();
      return context.getArgument(0);
    });
    final int locatorWaitTime = 5;
    PrimaryHandler primaryHandler = new PrimaryHandler(fallbackHandler, locatorWaitTime,
        x -> Thread.sleep(x));
    // process a request that has no handler - this should invoke fallbackHandler
    final Object request = new Object();
    final Object result = primaryHandler.processRequest(request);
    assertThat(result).isEqualTo(request);
    assertThat(handlerInvoked.get()).isEqualTo(1);
  }


  @Test
  public void registeredHandler() throws IOException {
    final AtomicInteger handlerInvoked = new AtomicInteger();
    final TcpHandler registeredHandler = mock(TcpHandler.class);
    when(registeredHandler.processRequest(isA(Object.class))).thenAnswer(context -> {
      handlerInvoked.incrementAndGet();
      return context.getArgument(0);
    });
    final int locatorWaitTime = 5;
    PrimaryHandler primaryHandler = new PrimaryHandler(registeredHandler, locatorWaitTime,
        x -> Thread.sleep(x));
    primaryHandler.addHandler(FindCoordinatorRequest.class, registeredHandler);
    // process a request that has a registered handler - this should invoke registeredHandler
    final Object request = new FindCoordinatorRequest<>();
    final Object result = primaryHandler.processRequest(request);
    assertThat(result).isEqualTo(request);
    assertThat(handlerInvoked.get()).isEqualTo(1);
    assertThat(primaryHandler.isHandled(FindCoordinatorRequest.class)).isTrue();
  }
}
