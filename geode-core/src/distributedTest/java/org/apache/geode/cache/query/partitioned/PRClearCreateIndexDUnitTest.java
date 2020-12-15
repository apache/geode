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

package org.apache.geode.cache.query.partitioned;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionMessageObserver;
import org.apache.geode.internal.cache.DistributedClearOperation;
import org.apache.geode.internal.cache.DistributedClearOperation.ClearRegionMessage;
import org.apache.geode.internal.cache.PartitionedRegionClearMessage;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PRClearCreateIndexDUnitTest implements Serializable {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(4, true);

  private MemberVM primary, secondary;
  private ClientVM client;

  @Before
  public void before() throws Exception {
    int locatorPort = ClusterStartupRule.getDUnitLocatorPort();
    primary = cluster.startServerVM(0, locatorPort);
    secondary = cluster.startServerVM(1, locatorPort);

    // create region on server1 first, making sure server1 has the primary bucket
    primary.invoke(() -> {
      DistributionMessageObserver.setInstance(new MessageObserver());
      Region<Object, Object> region =
          ClusterStartupRule.memberStarter.createPartitionRegion("regionA",
              f -> f.setTotalNumBuckets(1).setRedundantCopies(2));
      IntStream.range(0, 100).forEach(i -> region.put(i, "value" + i));
    });

    // server2 has the secondary bucket
    secondary.invoke(() -> {
      DistributionMessageObserver.setInstance(new MessageObserver());
      ClusterStartupRule.memberStarter.createPartitionRegion("regionA",
          f -> f.setTotalNumBuckets(1).setRedundantCopies(2));
    });
  }

  @After
  public void after() throws Exception {
    primary.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
    });
    secondary.invoke(() -> {
      DistributionMessageObserver.setInstance(null);
    });
  }

  // All tests create index on secondary members. These tests are making sure we are requesting
  // locks for clear on secondary members as well. If we create index on the primary, the clear
  // and createIndex will run sequentially so there would be no error. But if we create index on
  // the secondary member and if the secondary member will not
  // request a lock for clear operation, it will result in an EntryDestroyedException when create
  // index is happening.

  // Note: OP_LOCK_FOR_CLEAR, OP_CLEAR, OP_UNLOCK_FOR_CLEAR are messages for secondary members
  // OP_LOCK_FOR_PR_CLEAR, OP_UNLOCK_FOR_PR_CLEAR, OP_PR_CLEAR can be for anybody

  @Test
  // all local buckets are primary, so only OP_LOCK_FOR_CLEAR and OP_CLEAR messages are sent to the
  // secondary member
  // in the end an OP_PR_CLEAR is sent to the secondary for no effect
  public void clearFromPrimaryMember() throws Exception {
    AsyncInvocation createIndex = secondary.invokeAsync(PRClearCreateIndexDUnitTest::createIndex);
    AsyncInvocation clear = primary.invokeAsync(PRClearCreateIndexDUnitTest::clear);

    createIndex.get();
    clear.get();

    // assert that secondary member received these messages
    primary.invoke(() -> verifyEvents(false, false, false, false));
    secondary.invoke(() -> verifyEvents(false, true, true, true));
  }

  @Test
  // all local buckets are secondary, so an OP_PR_CLEAR is sent to the primary member, from there
  // a OP_LOCK_FOR_CLEAR and OP_CLEAR messages are sent back to the secondary
  public void clearFromSecondaryMember() throws Exception {
    AsyncInvocation createIndex = secondary.invokeAsync(PRClearCreateIndexDUnitTest::createIndex);
    AsyncInvocation clear = secondary.invokeAsync(PRClearCreateIndexDUnitTest::clear);

    createIndex.get();
    clear.get();

    // assert that secondary member received these messages
    primary.invoke(() -> verifyEvents(false, true, false, false));
    secondary.invoke(() -> verifyEvents(false, false, true, true));
  }

  /**
   * For interested client connecting to secondary member
   * 1. locks all local region
   * 2. send OP_LOCK_FOR_PR_CLEAR to lock all other members
   * 3. send OP_PR_CLEAR to primary to clear
   * 4. primary will send a OP_CLEAR message back to the secondary to clear
   */
  @Test
  public void clearFromInterestedClientConnectingToSecondaryMember() throws Exception {
    int port = secondary.getPort();
    client = cluster.startClientVM(2, c -> c.withServerConnection(port).withPoolSubscription(true));
    AsyncInvocation createIndex = secondary.invokeAsync(PRClearCreateIndexDUnitTest::createIndex);

    AsyncInvocation clear = client.invokeAsync(() -> {
      Thread.sleep(200);
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> regionA =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("regionA");
      regionA.registerInterestForAllKeys();
      regionA.clear();
    });

    createIndex.get();
    clear.get();
    primary.invoke(() -> verifyEvents(true, true, false, false));
    secondary.invoke(() -> verifyEvents(false, false, false, true));
  }

  @Test
  /**
   * For interested client connecting to primary member, behaves like starting from primary member
   * except it locks first
   * 1. locks local region
   * 2. send OP_LOCK_FOR_PR_CLEAR to lock all other members
   * 3. then since it already locked the current member, won't send a OP_LOCK_FOR_CLEAR message
   * to secondaries, only OP_CLEAR will be sent
   */
  public void clearFromInterestedClientConnectingToPrimaryMember() throws Exception {
    int port = primary.getPort();
    client = cluster.startClientVM(2, c -> c.withServerConnection(port).withPoolSubscription(true));
    AsyncInvocation createIndex = secondary.invokeAsync(PRClearCreateIndexDUnitTest::createIndex);

    AsyncInvocation clear = client.invokeAsync(() -> {
      Thread.sleep(200);
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Object, Object> regionA =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create("regionA");
      regionA.registerInterestForAllKeys();
      regionA.clear();
    });

    createIndex.get();
    clear.get();
    primary.invoke(() -> verifyEvents(false, false, false, false));
    secondary.invoke(() -> verifyEvents(true, true, false, true));
  }

  private static void clear() throws InterruptedException {
    // start the clear a bit later that the createIndex operation
    Thread.sleep(200);
    Region region = ClusterStartupRule.getCache().getRegion("/regionA");
    region.clear();
  }

  private static void createIndex() {
    QueryService queryService = ClusterStartupRule.getCache().getQueryService();
    // run create index multiple times to make sure the clear operation fall inside a
    // createIndex Operation
    IntStream.range(0, 10).forEach(i -> {
      try {
        queryService.createIndex("index" + i, "name" + i, "/regionA");
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    });
  }

  private static void verifyEvents(boolean lockOthers, boolean clearOthers, boolean lockSecondary,
      boolean clearSecondary) {
    MessageObserver observer = (MessageObserver) DistributionMessageObserver.getInstance();
    assertThat(observer.isLock_others())
        .describedAs("OP_LOCK_FOR_PR_CLEAR received: %s", observer.isLock_others())
        .isEqualTo(lockOthers);
    assertThat(observer.isClear_others())
        .describedAs("OP_PR_CLEAR received: %s", observer.isClear_others()).isEqualTo(clearOthers);
    assertThat(observer.isLock_secondary())
        .describedAs("OP_LOCK_FOR_CLEAR received: %s", observer.isLock_secondary())
        .isEqualTo(lockSecondary);
    assertThat(observer.isClear_secondary())
        .describedAs("OP_CLEAR received: %s", observer.isClear_secondary())
        .isEqualTo(clearSecondary);
  }

  private static class MessageObserver extends DistributionMessageObserver {
    private boolean lock_secondary = false;
    private boolean clear_secondary = false;
    private boolean clear_others = false;
    private boolean lock_others = false;

    @Override
    public void beforeProcessMessage(ClusterDistributionManager dm, DistributionMessage message) {
      if (message instanceof ClearRegionMessage) {
        ClearRegionMessage clearMessage = (ClearRegionMessage) message;
        if (clearMessage
            .getOperationType() == DistributedClearOperation.OperationType.OP_LOCK_FOR_CLEAR) {
          lock_secondary = true;
        }
        if (clearMessage.getOperationType() == DistributedClearOperation.OperationType.OP_CLEAR) {
          clear_secondary = true;
        }
      }
      if (message instanceof PartitionedRegionClearMessage) {
        PartitionedRegionClearMessage clearMessage = (PartitionedRegionClearMessage) message;
        if (clearMessage
            .getOp() == PartitionedRegionClearMessage.OperationType.OP_LOCK_FOR_PR_CLEAR) {
          lock_others = true;
        }
        if (clearMessage.getOp() == PartitionedRegionClearMessage.OperationType.OP_PR_CLEAR) {
          clear_others = true;
        }
      }
    }

    public boolean isLock_secondary() {
      return lock_secondary;
    }

    public boolean isClear_secondary() {
      return clear_secondary;
    }

    public boolean isClear_others() {
      return clear_others;
    }

    public boolean isLock_others() {
      return lock_others;
    }
  }

}
