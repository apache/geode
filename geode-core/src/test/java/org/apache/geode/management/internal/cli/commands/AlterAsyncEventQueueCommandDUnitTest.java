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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.MyAsyncEventListener;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;


@Category({DistributedTest.class, AEQTest.class})
public class AlterAsyncEventQueueCommandDUnitTest {

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

    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue1", 1);

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(100);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(5);
      assertThat(queue.getMaximumQueueMemory()).isEqualTo(100);
    });

    gfsh.executeAndAssertThat("alter async-event-queue --id=queue1 " + "--batch-size=200 "
        + "--batch-time-interval=300 " + "--max-queue-memory=400").statusIsSuccess();

    // verify that server1's event queue still has the default value
    // without restart
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(100);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(5);
      assertThat(queue.getMaximumQueueMemory()).isEqualTo(100);
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });

    // restart locator and server without clearing the file system
    lsRule.stopMember(1, false);
    lsRule.stopMember(0, false);

    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    // verify that server1's queue is updated
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      AsyncEventQueue queue = cache.getAsyncEventQueue("queue1");
      assertThat(queue.getBatchSize()).isEqualTo(200);
      assertThat(queue.getBatchTimeInterval()).isEqualTo(300);
      assertThat(queue.getMaximumQueueMemory()).isEqualTo(400);
      assertThat(cache.getAsyncEventQueue("queue2")).isNull();
    });
  }
}
