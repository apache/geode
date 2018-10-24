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
package org.apache.geode.management.internal.beans;

import static org.apache.geode.distributed.internal.ResourceEvent.CACHE_CREATE;
import static org.apache.geode.distributed.internal.ResourceEvent.CACHE_REMOVE;
import static org.apache.geode.distributed.internal.ResourceEvent.SYSTEM_ALERT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.InOrder;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.ManagementTest;

/**
 * Unit tests for {@link ManagementListener}.
 */
@Category(ManagementTest.class)
public class ManagementListenerTest {

  private InternalDistributedSystem system;
  private Lock readLock;
  private Lock writeLock;
  private ReadWriteLock readWriteLock;
  private InOrder readLockInOrder;
  private InOrder writeLockInOrder;

  private ManagementListener managementListener;

  @Before
  public void setUp() {
    system = mock(InternalDistributedSystem.class);
    ManagementAdapter managementAdapter = mock(ManagementAdapter.class);
    readWriteLock = spy(ReadWriteLock.class);
    readLock = spy(Lock.class);
    writeLock = spy(Lock.class);

    when(system.getCache()).thenReturn(mock(InternalCache.class));
    when(system.isConnected()).thenReturn(true);

    when(readWriteLock.readLock()).thenReturn(readLock);
    when(readWriteLock.writeLock()).thenReturn(writeLock);

    readLockInOrder = inOrder(readLock, readLock);
    writeLockInOrder = inOrder(writeLock, writeLock);

    managementListener = new ManagementListener(system, managementAdapter, readWriteLock);
  }

  @Test
  public void shouldProceedReturnsTrueIfSystemNotConnectedForCacheRemoveEvent() {
    when(system.isConnected()).thenReturn(false);

    assertThat(managementListener.shouldProceed(CACHE_REMOVE)).isTrue();
  }

  @Test
  public void shouldProceedReturnsFalseIfSystemNotConnectedForOtherEvents() {
    when(system.isConnected()).thenReturn(false);

    for (ResourceEvent resourceEvent : ResourceEvent.values()) {
      if (resourceEvent != CACHE_REMOVE) {
        assertThat(managementListener.shouldProceed(CACHE_REMOVE)).isTrue();
      }
    }
  }

  @Test
  public void shouldProceedReturnsTrueIfSystemIsConnected() {
    for (ResourceEvent resourceEvent : ResourceEvent.values()) {
      assertThat(managementListener.shouldProceed(resourceEvent)).isTrue();
    }
  }

  @Test
  public void shouldProceedReturnsFalseIfNoCache() {
    when(system.getCache()).thenReturn(null);

    for (ResourceEvent resourceEvent : ResourceEvent.values()) {
      assertThat(managementListener.shouldProceed(resourceEvent)).isFalse();
    }
  }

  @Test
  public void handleEventUsesWriteLockForCacheCreateEvent() {
    managementListener.handleEvent(CACHE_CREATE, null);

    writeLockInOrder.verify(writeLock).lock();
    writeLockInOrder.verify(writeLock).unlock();
  }

  @Test
  public void handleEventUsesWriteLockForCacheRemoveEvent() {
    managementListener.handleEvent(CACHE_REMOVE, null);

    writeLockInOrder.verify(writeLock).lock();
    writeLockInOrder.verify(writeLock).unlock();
  }

  @Test
  public void handleEventDoesNotUseLocksForSystemAlertEvent() {
    managementListener.handleEvent(SYSTEM_ALERT, null);

    verifyZeroInteractions(readWriteLock);
    verifyZeroInteractions(readLock);
    verifyZeroInteractions(writeLock);
  }

  @Test
  public void handleEventUsesReadLockForOtherEvents() {
    for (ResourceEvent resourceEvent : ResourceEvent.values()) {
      if (resourceEvent != CACHE_CREATE && resourceEvent != CACHE_REMOVE
          && resourceEvent != SYSTEM_ALERT) {
        managementListener.handleEvent(resourceEvent, null);

        readLockInOrder.verify(readLock).lock();
        readLockInOrder.verify(readLock).unlock();
      }
    }
  }
}
