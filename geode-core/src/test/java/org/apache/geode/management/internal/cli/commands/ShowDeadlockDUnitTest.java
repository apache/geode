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

import static org.apache.geode.distributed.ConfigurationProperties.*;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.deadlock.GemFireDeadlockDetector;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandProcessor;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

/**
 * This DUnit tests uses same code as GemFireDeadlockDetectorDUnitTest and uses the command
 * processor for executing the "show deadlock" command
 */
@Category(DistributedTest.class)
public class ShowDeadlockDUnitTest extends JUnit4CacheTestCase {

  private static final long serialVersionUID = 1L;
  private static final Set<Thread> stuckThreads =
      Collections.synchronizedSet(new HashSet<Thread>());
  private static final Map<String, String> EMPTY_ENV = Collections.emptyMap();

  @Override
  public final void postSetUp() throws Exception {
    // This test does not require an actual Gfsh connection to work, however when run as part of a
    // suite, prior tests
    // may mess up the environment causing this test to fail. Setting this prevents false failures.
    CliUtil.isGfshVM = false;
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    invokeInEveryVM(new SerializableRunnable() {
      private static final long serialVersionUID = 1L;

      public void run() {
        for (Thread thread : stuckThreads) {
          thread.interrupt();
        }
      }
    });
    CliUtil.isGfshVM = true;
  }

  @Test
  public void testNoDeadlock() throws ClassNotFoundException, IOException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Make sure a deadlock from a previous test is cleared.
    disconnectAllFromDS();

    createCache(vm0);
    createCache(vm1);
    createCache(new Properties());

    String fileName = "dependency.txt";
    GemFireDeadlockDetector detect = new GemFireDeadlockDetector();
    assertEquals(null, detect.find().findCycle());

    CommandProcessor commandProcessor = new CommandProcessor();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    csb.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, fileName);
    Result result = commandProcessor.createCommandStatement(csb.toString(), EMPTY_ENV).process();

    String deadLockOutputFromCommand = getResultAsString(result);

    getLogWriter().info("output = " + deadLockOutputFromCommand);
    assertEquals(true, result.hasIncomingFiles());
    assertEquals(true, result.getStatus().equals(Status.OK));
    assertEquals(true,
        deadLockOutputFromCommand.startsWith(CliStrings.SHOW_DEADLOCK__NO__DEADLOCK));
    result.saveIncomingFiles(null);
    File file = new File(fileName);
    assertTrue(file.exists());
    file.delete();

    disconnectAllFromDS();
  }

  private static final Lock lock = new ReentrantLock();

  @Test
  public void testDistributedDeadlockWithFunction()
      throws InterruptedException, ClassNotFoundException, IOException {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    String filename = "gfeDependency.txt";
    InternalDistributedMember member1 = createCache(vm0);
    final InternalDistributedMember member2 = createCache(vm1);
    createCache(new Properties());
    // Have two threads lock locks on different members in different orders.
    // This thread locks the lock member1 first, then member2.
    lockTheLocks(vm0, member2);
    // This thread locks the lock member2 first, then member1.
    lockTheLocks(vm1, member1);

    Thread.sleep(5000);
    CommandProcessor commandProcessor = new CommandProcessor();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    csb.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, filename);
    Result result = commandProcessor.createCommandStatement(csb.toString(), EMPTY_ENV).process();

    String deadLockOutputFromCommand = getResultAsString(result);
    getLogWriter().info("Deadlock = " + deadLockOutputFromCommand);
    result.saveIncomingFiles(null);
    assertEquals(true,
        deadLockOutputFromCommand.startsWith(CliStrings.SHOW_DEADLOCK__DEADLOCK__DETECTED));
    assertEquals(true, result.getStatus().equals(Status.OK));
    File file = new File(filename);
    assertTrue(file.exists());
    file.delete();

  }


  private void createCache(Properties props) {
    getSystem(props);
    final Cache cache = getCache();
  }

  private Properties createProperties(Host host, int locatorPort) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    // props.setProperty(DistributionConfig.LOCATORS_NAME, getServerHostName(host) + "[" +
    // locatorPort + "]");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.put(ENABLE_NETWORK_PARTITION_DETECTION, "true");
    return props;
  }

  private void lockTheLocks(VM vm0, final InternalDistributedMember member) {
    vm0.invokeAsync(new SerializableRunnable() {

      private static final long serialVersionUID = 1L;

      public void run() {
        lock.lock();
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("interrupted", e);
        }
        ResultCollector collector =
            FunctionService.onMember(basicGetSystem(), member).execute(new TestFunction());
        // wait the function to lock the lock on member.
        collector.getResult();
        lock.unlock();
      }
    });
  }

  private void lockTheDLocks(VM vm, final String first, final String second) {
    vm.invokeAsync(new SerializableRunnable() {

      private static final long serialVersionUID = 1L;

      public void run() {
        getCache();
        DistributedLockService dls = DistributedLockService.create("deadlock_test", getSystem());
        dls.lock(first, 10 * 1000, -1);

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        dls.lock(second, 10 * 1000, -1);
      }
    });
  }

  private InternalDistributedMember createCache(VM vm) {
    return (InternalDistributedMember) vm.invoke(new SerializableCallable() {
      /**
       *
       */
      private static final long serialVersionUID = 1L;

      public Object call() {
        getCache();
        return getSystem().getDistributedMember();
      }
    });
  }

  private String getResultAsString(Result result) {
    StringBuilder sb = new StringBuilder();
    while (result.hasNextLine()) {
      sb.append(result.nextLine());
    }

    return sb.toString();
  }

  private static class TestFunction implements Function {

    private static final long serialVersionUID = 1L;
    private static final int LOCK_WAIT_TIME = 1000;

    public boolean hasResult() {
      return true;
    }

    public void execute(FunctionContext context) {
      try {
        stuckThreads.add(Thread.currentThread());
        lock.tryLock(LOCK_WAIT_TIME, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // ingore
      }
      context.getResultSender().lastResult(null);
    }

    public String getId() {
      return getClass().getCanonicalName();
    }

    public boolean optimizeForWrite() {
      return false;
    }

    public boolean isHA() {
      return false;
    }
  }
}

