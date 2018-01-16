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

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.deadlock.GemFireDeadlockDetectorDUnitTest;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.concurrent.FileBasedCountDownLatch;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

/**
 * Distributed tests for show deadlock command in {@link ShowDeadlockCommand}.
 *
 * @see GemFireDeadlockDetectorDUnitTest
 */
@Category(DistributedTest.class)
public class ShowDeadlockDUnitTest {
  private static Thread stuckThread = null;
  private static final Lock LOCK = new ReentrantLock();

  private MemberVM server1;
  private MemberVM server2;

  private File outputFile;
  private String showDeadlockCommand;

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void setup() throws Exception {
    outputFile = new File(temporaryFolder.getRoot(), "dependency.txt").getAbsoluteFile();
    showDeadlockCommand = "show dead-locks --file=" + outputFile.getAbsolutePath();
    outputFile.delete();

    MemberVM locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, locator.getPort());
    server2 = lsRule.startServerVM(2, locator.getPort());

    gfsh.connect(locator);
  }

  @After
  public final void after() throws Exception {
    server1.invoke(() -> stuckThread.interrupt());
    server2.invoke(() -> stuckThread.interrupt());
  }

  @Test
  public void testNoDeadlock() throws Exception {
    gfsh.executeAndAssertThat(showDeadlockCommand).statusIsSuccess();
    String commandOutput = gfsh.getGfshOutput();

    assertThat(commandOutput).startsWith(CliStrings.SHOW_DEADLOCK__NO__DEADLOCK);
    assertThat(outputFile).exists();
  }

  @Test
  public void testDistributedDeadlockWithFunction() throws Exception {
    FileBasedCountDownLatch countDownLatch = new FileBasedCountDownLatch(2);

    // This thread locks the lock in server1 first, then server2.
    lockTheLocks(server1, server2, countDownLatch);
    // This thread locks the lock server2 first, then server1.
    lockTheLocks(server2, server1, countDownLatch);

    Awaitility.await().atMost(5, TimeUnit.MINUTES).pollDelay(5, TimeUnit.SECONDS).until(() -> {
      gfsh.executeAndAssertThat(showDeadlockCommand).statusIsSuccess();
      String commandOutput = gfsh.getGfshOutput();
      assertThat(commandOutput).startsWith(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED);
      assertThat(outputFile).exists();
    });
  }

  private void lockTheLocks(MemberVM thisVM, final MemberVM thatVM,
      FileBasedCountDownLatch countDownLatch) {
    thisVM.invokeAsync(() -> {
      LOCK.lock();
      countDownLatch.countDown();
      countDownLatch.await();
      // At this point each VM will hold its own lock.
      lockRemoteVM(thatVM);
      LOCK.unlock();
    });
  }

  private static void lockRemoteVM(MemberVM vmToLock) {
    InternalDistributedMember thatInternalMember = getInternalDistributedMember(vmToLock);

    ResultCollector collector =
        FunctionService.onMember(thatInternalMember).execute(new LockFunction());
    collector.getResult();
  }

  private static InternalDistributedMember getInternalDistributedMember(MemberVM memberVM) {
    return memberVM.getVM().invoke(
        () -> ClusterStartupRule.getCache().getInternalDistributedSystem().getDistributedMember());
  }

  private static class LockFunction implements Function<Object> {
    @Override
    public void execute(FunctionContext<Object> context) {
      stuckThread = Thread.currentThread();
      try {
        LOCK.tryLock(5, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        context.getResultSender().lastResult(null);
      }
    }
  }
}
