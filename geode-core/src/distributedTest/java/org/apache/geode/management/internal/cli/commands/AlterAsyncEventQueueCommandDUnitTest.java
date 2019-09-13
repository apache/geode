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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl.DEFAULT_BATCH_TIME_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category({AEQTest.class})
public class AlterAsyncEventQueueCommandDUnitTest {

  private static final int ALTERED_BATCH_SIZE = 200;
  private static final int ALTERED_BATCH_TIME_INTERVAL = 300;
  private static final int ALTERED_MAXIMUM_QUEUE_MEMORY = 400;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1;

  @Before
  public void beforeClass() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testAlterAsyncEventQueue() throws Exception {
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 --listener="
        + MyAsyncEventListener.class.getName()).statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory())
          .isEqualTo(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
    });

    gfsh.executeAndAssertThat("alter async-event-queue --id=queue1 --batch-size="
        + ALTERED_BATCH_SIZE + " --batch-time-interval=" + ALTERED_BATCH_TIME_INTERVAL
        + " --max-queue-memory=" + ALTERED_MAXIMUM_QUEUE_MEMORY).statusIsSuccess();

    // verify that server1's event queue still has the default value
    // without restart
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory())
          .isEqualTo(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });

    // restart locator and server without clearing the file system
    server1.stop(false);
    locator.stop(false);

    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    // verify that server1's queue is updated
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(ALTERED_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(ALTERED_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory()).isEqualTo(ALTERED_MAXIMUM_QUEUE_MEMORY);
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });
  }

  @Test
  public void whenAlterCommandUsedToChangeFromPauseToResumeThenAEQBehaviorMustChange()
      throws Exception {
    gfsh.executeAndAssertThat(
        "create async-event-queue --pause-event-processing=true --id=queue1 --group=group1 --listener="
            + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(queue.isDispatchingPaused()).isTrue();
      assertThat(queue.getMaximumQueueMemory())
          .isEqualTo(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
    });

    gfsh.executeAndAssertThat(
        "alter async-event-queue --id=queue1 --pause-event-processing=false --batch-size="
            + ALTERED_BATCH_SIZE + " --batch-time-interval=" + ALTERED_BATCH_TIME_INTERVAL
            + " --max-queue-memory=" + ALTERED_MAXIMUM_QUEUE_MEMORY)
        .statusIsSuccess();

    // verify that server1's event queue still has the default value
    // without restart
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory())
          .isEqualTo(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
      assertThat(queue.isDispatchingPaused()).isTrue();
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });

    // restart locator and server without clearing the file system
    server1.stop(false);
    locator.stop(false);

    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    // verify that server1's queue is updated
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(ALTERED_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(ALTERED_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory()).isEqualTo(ALTERED_MAXIMUM_QUEUE_MEMORY);
      assertThat(queue.isDispatchingPaused()).isFalse();
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });
  }

  @Test
  public void whenAlterCommandUsedToChangeFromResumeStateToPausedThenAEQBehaviorMustChange()
      throws Exception {
    gfsh.executeAndAssertThat(
        "create async-event-queue --pause-event-processing=false --id=queue1 --group=group1 --listener="
            + MyAsyncEventListener.class.getName())
        .statusIsSuccess();

    locator.waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers("queue1", 1);

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(queue.isDispatchingPaused()).isFalse();
      assertThat(queue.getMaximumQueueMemory())
          .isEqualTo(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
    });

    gfsh.executeAndAssertThat(
        "alter async-event-queue --id=queue1 --pause-event-processing=true --batch-size="
            + ALTERED_BATCH_SIZE + " --batch-time-interval=" + ALTERED_BATCH_TIME_INTERVAL
            + " --max-queue-memory=" + ALTERED_MAXIMUM_QUEUE_MEMORY)
        .statusIsSuccess();

    // verify that server1's event queue still has the default value
    // without restart
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory())
          .isEqualTo(GatewaySender.DEFAULT_MAXIMUM_QUEUE_MEMORY);
      assertThat(queue.isDispatchingPaused()).isFalse();
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });

    // restart locator and server without clearing the file system
    server1.stop(false);
    locator.stop(false);

    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    // verify that server1's queue is updated
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(ALTERED_BATCH_SIZE);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(ALTERED_BATCH_TIME_INTERVAL);
      assertThat(queue.getMaximumQueueMemory()).isEqualTo(ALTERED_MAXIMUM_QUEUE_MEMORY);
      assertThat(queue.isDispatchingPaused()).isTrue();
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });
  }
}
