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
package org.apache.geode.internal.cache.wan.parallel;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.WaitUntilGatewaySenderFlushedCoordinatorJUnitTest;
import org.apache.geode.internal.cache.wan.parallel.WaitUntilParallelGatewaySenderFlushedCoordinator.WaitUntilBucketRegionQueueFlushedCallable;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class WaitUntilParallelGatewaySenderFlushedCoordinatorJUnitTest
    extends WaitUntilGatewaySenderFlushedCoordinatorJUnitTest {

  private PartitionedRegion region;

  protected void createGatewaySender() {
    super.createGatewaySender();
    ConcurrentParallelGatewaySenderQueue queue =
        (ConcurrentParallelGatewaySenderQueue) spy(getQueue());
    doReturn(queue).when(this.sender).getQueue();
    this.region = mock(PartitionedRegion.class);
    doReturn(this.region).when(queue).getRegion();
  }

  protected AbstractGatewaySenderEventProcessor getEventProcessor() {
    ConcurrentParallelGatewaySenderEventProcessor processor =
        spy(new ConcurrentParallelGatewaySenderEventProcessor(this.sender));
    return processor;
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedSuccessfulNotInitiator() throws Throwable {
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(this.sender, 1000l,
            TimeUnit.MILLISECONDS, false);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getSuccessfulCallables()).when(coordinatorSpy)
        .buildWaitUntilBucketRegionQueueFlushedCallables(this.region);
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertTrue(result);
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedUnsuccessfulNotInitiator() throws Throwable {
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(this.sender, 1000l,
            TimeUnit.MILLISECONDS, false);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getUnsuccessfulCallables()).when(coordinatorSpy)
        .buildWaitUntilBucketRegionQueueFlushedCallables(this.region);
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertFalse(result);
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedSuccessfulInitiator() throws Throwable {
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(this.sender, 1000l,
            TimeUnit.MILLISECONDS, true);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getSuccessfulCallables()).when(coordinatorSpy)
        .buildWaitUntilBucketRegionQueueFlushedCallables(this.region);
    doReturn(true).when(coordinatorSpy).waitUntilFlushedOnRemoteMembers(this.region);
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertTrue(result);
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedUnsuccessfulInitiator() throws Throwable {
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(this.sender, 1000l,
            TimeUnit.MILLISECONDS, true);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getSuccessfulCallables()).when(coordinatorSpy)
        .buildWaitUntilBucketRegionQueueFlushedCallables(this.region);
    doReturn(false).when(coordinatorSpy).waitUntilFlushedOnRemoteMembers(this.region);
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertFalse(result);
  }

  private List<WaitUntilBucketRegionQueueFlushedCallable> getSuccessfulCallables()
      throws Exception {
    List callables = new ArrayList();
    WaitUntilBucketRegionQueueFlushedCallable callable =
        mock(WaitUntilBucketRegionQueueFlushedCallable.class);
    when(callable.call()).thenReturn(true);
    callables.add(callable);
    return callables;
  }

  private List<WaitUntilBucketRegionQueueFlushedCallable> getUnsuccessfulCallables()
      throws Exception {
    List callables = new ArrayList();
    WaitUntilBucketRegionQueueFlushedCallable callable =
        mock(WaitUntilBucketRegionQueueFlushedCallable.class);
    when(callable.call()).thenReturn(false);
    callables.add(callable);
    return callables;
  }
}
