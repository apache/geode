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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.BucketRegionQueue;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.WaitUntilGatewaySenderFlushedCoordinatorJUnitTest;
import org.apache.geode.internal.cache.wan.parallel.WaitUntilParallelGatewaySenderFlushedCoordinator.WaitUntilBucketRegionQueueFlushedCallable;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class WaitUntilParallelGatewaySenderFlushedCoordinatorJUnitTest
    extends WaitUntilGatewaySenderFlushedCoordinatorJUnitTest {

  private PartitionedRegion region;
  private BucketRegionQueue brq;

  @Override
  protected void createGatewaySender() {
    super.createGatewaySender();
    ConcurrentParallelGatewaySenderQueue queue =
        (ConcurrentParallelGatewaySenderQueue) spy(getQueue());
    doReturn(queue).when(sender).getQueue();
    region = mock(PartitionedRegion.class);
    doReturn(region).when(queue).getRegion();
    brq = mock(BucketRegionQueue.class);
  }

  @Override
  protected AbstractGatewaySenderEventProcessor getEventProcessor() {
    ConcurrentParallelGatewaySenderEventProcessor processor =
        spy(new ConcurrentParallelGatewaySenderEventProcessor(sender, null, false));
    return processor;
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedSuccessfulNotInitiator() throws Throwable {
    long timeout = 5000;
    TimeUnit unit = TimeUnit.MILLISECONDS;
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(sender, timeout, unit, false);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getLocalBucketRegions()).when(coordinatorSpy).getLocalBucketRegions(any());
    doReturn(getCallableResult(true)).when(coordinatorSpy)
        .createWaitUntilBucketRegionQueueFlushedCallable(any(), anyLong(), any());
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertTrue(result);
  }

  @Test
  public void waitUntilFlushShouldExitEarlyIfTimeoutElapses() throws Throwable {
    long timeout = 500;
    TimeUnit unit = TimeUnit.MILLISECONDS;
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(sender, timeout, unit, false);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);

    Set<BucketRegion> bucketRegions = new HashSet<>();
    for (int i = 0; i < 5000; i++) {
      bucketRegions.add(mock(BucketRegionQueue.class));
    }
    doReturn(bucketRegions).when(coordinatorSpy).getLocalBucketRegions(any());

    WaitUntilBucketRegionQueueFlushedCallable callable =
        mock(WaitUntilBucketRegionQueueFlushedCallable.class);
    when(callable.call()).then(invocation -> {
      Thread.sleep(100);
      return true;
    });
    doReturn(callable).when(coordinatorSpy).createWaitUntilBucketRegionQueueFlushedCallable(any(),
        anyLong(), any());
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertFalse(result);

    verify(callable, atMost(50)).call();
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedUnsuccessfulNotInitiator() throws Throwable {
    long timeout = 5000;
    TimeUnit unit = TimeUnit.MILLISECONDS;
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(sender, timeout, unit, false);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getLocalBucketRegions()).when(coordinatorSpy).getLocalBucketRegions(any());
    doReturn(getCallableResult(false)).when(coordinatorSpy)
        .createWaitUntilBucketRegionQueueFlushedCallable(any(), anyLong(), any());
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertFalse(result);
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedSuccessfulInitiator() throws Throwable {
    long timeout = 5000;
    TimeUnit unit = TimeUnit.MILLISECONDS;
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(sender, timeout, unit, true);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getLocalBucketRegions()).when(coordinatorSpy).getLocalBucketRegions(any());
    doReturn(getCallableResult(true)).when(coordinatorSpy)
        .createWaitUntilBucketRegionQueueFlushedCallable(any(), anyLong(), any());
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertTrue(result);
  }

  @Test
  public void testWaitUntilParallelGatewaySenderFlushedUnsuccessfulInitiator() throws Throwable {
    long timeout = 5000;
    TimeUnit unit = TimeUnit.MILLISECONDS;
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinator =
        new WaitUntilParallelGatewaySenderFlushedCoordinator(sender, timeout, unit, true);
    WaitUntilParallelGatewaySenderFlushedCoordinator coordinatorSpy = spy(coordinator);
    doReturn(getLocalBucketRegions()).when(coordinatorSpy).getLocalBucketRegions(any());
    doReturn(getCallableResult(false)).when(coordinatorSpy)
        .createWaitUntilBucketRegionQueueFlushedCallable(any(), anyLong(), any());
    boolean result = coordinatorSpy.waitUntilFlushed();
    assertFalse(result);
  }

  private WaitUntilBucketRegionQueueFlushedCallable getCallableResult(boolean expectedResult)
      throws Exception {
    WaitUntilBucketRegionQueueFlushedCallable callable =
        mock(WaitUntilBucketRegionQueueFlushedCallable.class);
    when(callable.call()).thenReturn(expectedResult);
    return callable;
  }

  private Set<BucketRegionQueue> getLocalBucketRegions() {
    Set<BucketRegionQueue> localBucketRegions = new HashSet<BucketRegionQueue>();
    localBucketRegions.add(brq);
    return localBucketRegions;
  }
}
