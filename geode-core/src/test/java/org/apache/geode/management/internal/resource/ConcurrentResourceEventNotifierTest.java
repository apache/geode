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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.internal.beans.ManagementListener;

/**
 * Unit tests for {@link ConcurrentResourceEventNotifier}.
 */
public class ConcurrentResourceEventNotifierTest {

  private ConcurrentResourceEventNotifier concurrentResourceEventNotifier;
  private ManagementListener managementListener;

  @Before
  public void setup() {
    managementListener = mock(ManagementListener.class);
    concurrentResourceEventNotifier = new ConcurrentResourceEventNotifier();
    concurrentResourceEventNotifier.addResourceListener(managementListener);
  }

  @Test
  public void isAFunctionalCollection() {
    assertThat(concurrentResourceEventNotifier.getResourceListeners())
        .containsOnly(managementListener);

    ResourceEventListener mockListener = mock(ResourceEventListener.class);
    concurrentResourceEventNotifier.addResourceListener(mockListener);

    assertThat(concurrentResourceEventNotifier.getResourceListeners())
        .containsOnly(managementListener, mockListener);
  }

  @Test
  public void closeClears() {
    concurrentResourceEventNotifier.close();

    assertThat(concurrentResourceEventNotifier.getResourceListeners()).isEmpty();
  }

  @Test
  public void handleResourceEventInvokesOnAllListeners() {
    ResourceEventListener mockListener = mock(ResourceEventListener.class);
    concurrentResourceEventNotifier.addResourceListener(mockListener);

    ResourceEvent event = ResourceEvent.CACHE_CREATE;
    Object resource = new Object();

    concurrentResourceEventNotifier.handleResourceEvent(event, resource);

    verify(managementListener).handleEvent(eq(event), eq(resource));
    verify(mockListener).handleEvent(eq(event), eq(resource));
  }
}
