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
package org.apache.geode.management.internal.resource;

import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.List;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;

public class QueuedResourceEventNotifierTest {

  private static final long TIMEOUT_MILLIS = getTimeout().getValueInMS();

  private QueuedResourceEventNotifier notifier;

  @Before
  public void setup() {
    notifier = new QueuedResourceEventNotifier();
  }

  @Test
  public void isAFunctionalCollection() {
    assertThat(notifier.getResourceEventListeners()).isEmpty();

    ResourceEventListener mockListener = mock(ResourceEventListener.class);
    notifier.addResourceEventListener(mockListener);

    assertThat(notifier.getResourceEventListeners())
        .containsOnly(mockListener);
  }

  @Test
  public void closeClears() {
    notifier.close();

    assertThat(notifier.getResourceEventListeners()).isEmpty();
  }

  @Test
  public void closeStopsFunctioning() {
    notifier.close();

    ResourceEventListener mock = mock(ResourceEventListener.class);
    notifier.addResourceEventListener(mock);

    notifier.close();
    assertThat(notifier.getResourceEventListeners()).isEmpty();

    notifier.handleResourceEvent(ResourceEvent.REGION_CREATE, new Object());
    verifyZeroInteractions(mock);
  }

  @Test
  public void furtherEventsWillNotTriggerAfterClose() {
    notifier.addResourceEventListener(getSlowResourceEventListener());

    Future<Void> future1 = notifier.handleResourceEventAsync(ResourceEvent.REGION_CREATE, null);
    Future<Void> future2 = notifier.handleResourceEventAsync(ResourceEvent.REGION_CREATE, null);

    List<Runnable> shutdownEvents = notifier.shutdown();
    assertThat(shutdownEvents).hasSize(1);

    assertThat(future1.isDone()).isTrue();
    assertThat(future2.isDone()).isFalse();

  }

  @Test
  public void handleResourceEventInvokesOnAllListeners() {
    ResourceEventListener mockListener = mock(ResourceEventListener.class);
    ResourceEventListener mockListener2 = mock(ResourceEventListener.class);

    notifier.addResourceEventListener(mockListener);
    notifier.addResourceEventListener(mockListener2);

    ResourceEvent event = ResourceEvent.CACHE_CREATE;
    Object resource = new Object();

    notifier.handleResourceEvent(event, resource);

    verify(mockListener, timeout(TIMEOUT_MILLIS)).handleEvent(eq(event), eq(resource));
    verify(mockListener2, timeout(TIMEOUT_MILLIS)).handleEvent(eq(event), eq(resource));
  }

  private ResourceEventListener getSlowResourceEventListener() {
    return (event, resource) -> {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException e) {
        // ok
      }
    };
  }
}
