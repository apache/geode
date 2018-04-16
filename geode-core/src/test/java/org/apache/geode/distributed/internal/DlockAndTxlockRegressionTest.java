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
package org.apache.geode.distributed.internal;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.CancelException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.categories.DLockTest;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category({DLockTest.class, DistributedTest.class})
public class DlockAndTxlockRegressionTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  public static final String TRANSACTION_COUNT = "transactionCount";

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.setProperty(ConfigurationProperties.DISABLE_AUTO_RECONNECT, "true");
    properties.setProperty(ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION, "false");
    properties.setProperty(ConfigurationProperties.MEMBER_TIMEOUT, "1000");
    properties.setProperty(ConfigurationProperties.NAME,
        "vm" + Integer.getInteger(DUnitLauncher.VM_NUM_PARAM));
    System.getProperties().remove("gemfire.member-timeout");
    System.getProperties().remove("gemfire.log-level");
    return properties;
  }

  /**
   * Distributed locks are released quickly when a server crashes but transaction locks are
   * released in a background "pooled waiting" thread because the release involves communicating
   * with participants of the transaction. This makes the pattern of<br>
   * 1. get dlock,<br>
   * 2. perform transaction<br>
   * sometimes fail if the background cleanup takes too long. You may get the dlock but then get a
   * CommitConflictException when committing the transaction due to lingering tx locks from the
   * crashed server. The fix makes tx lock acquisition wait for the cleanup to finish.
   */
  @Test
  public void testDLockProtectsAgainstTransactionConflict() throws Exception {
    IgnoredException.addIgnoredException(
        "DistributedSystemDisconnectedException|ForcedDisconnectException|Possible loss of quorum|InterruptedException");
    // create four nodes to perform dlock & transactions and then
    // kill & restart each one using a forced disconnect.
    Host host = Host.getHost(0);
    VM[] servers = new VM[] {host.getVM(0), host.getVM(1), host.getVM(2)};
    for (VM vm : servers) {
      vm.invoke(() -> createCacheAndRegion());
    }

    servers[0].invoke(new SerializableRunnable() {
      public void run() {
        becomeLockGrantor();
      }
    });

    AsyncInvocation[] asyncInvocations = new AsyncInvocation[servers.length];
    for (int i = 0; i < servers.length; i++) {
      asyncInvocations[i] = servers[i].invokeAsync(() -> performOps());
    }

    // this test uses the DUnit blackboard to coordinate actions between JVMs
    getBlackboard().initBlackboard();
    getBlackboard().setMailbox(TRANSACTION_COUNT, 0);

    try {
      for (int i = 0; i < servers.length; i++) {
        checkAsyncInvocations(asyncInvocations);

        // clobber the current lock grantor
        VM vm = servers[i];
        System.out.println("TEST: killing vm " + i);
        vm.invoke("force disconnect", () -> forceDisconnect());
        asyncInvocations[i].join();
        System.out.println("TEST: recreating vm " + i);
        vm.invoke("create cache", () -> createCacheAndRegion());
        asyncInvocations[i] = vm.invokeAsync(() -> performOps());

        // move the grantor into the next VM to be clobbered
        int nextServer = (i + 1) % (servers.length - 1);
        System.out.println("TEST: moving the lock grantor to vm " + nextServer);
        servers[nextServer].invoke("become lock grantor", () -> becomeLockGrantor());

        int txCount = getBlackboard().getMailbox(TRANSACTION_COUNT);
        int newTxCount = txCount + 5;
        try {
          Awaitility.await("check for new transactions").atMost(30, TimeUnit.SECONDS).until(() -> {
            checkAsyncInvocations(asyncInvocations);
            int newCount = getBlackboard().getMailbox(TRANSACTION_COUNT);
            return newCount >= newTxCount;
          });
        } catch (ConditionTimeoutException e) {
          for (VM server : servers) {
            server.invoke(() -> OSProcess.printStacks(0));
          }
          throw e;
        }
        System.out.println("TEST: after rolling server " + i + " the transaction count is "
            + getBlackboard().getMailbox(TRANSACTION_COUNT));
      }

    } finally {

      for (VM vm : servers) {
        vm.invoke(() -> forceDisconnect());
      }

      Throwable failure = null;
      int asyncIndex = 0;
      for (AsyncInvocation asyncInvocation : asyncInvocations) {
        asyncInvocation.join(30000);
        if (asyncInvocation.exceptionOccurred()) {
          failure = asyncInvocation.getException();
        }
        asyncIndex++;
      }
      if (failure != null) {
        throw new RuntimeException("test failed", failure);
      }
    }
  }

  private void checkAsyncInvocations(AsyncInvocation[] asyncInvocations) {
    for (AsyncInvocation asyncInvocation : asyncInvocations) {
      if (!asyncInvocation.isAlive() && asyncInvocation.exceptionOccurred()) {
        throw new RuntimeException("", asyncInvocation.getException());
      }
    }
  }

  public void forceDisconnect() {
    Cache existingCache = basicGetCache();
    if (existingCache != null && !existingCache.isClosed()) {
      DistributedTestUtils.crashDistributedSystem(getCache().getDistributedSystem());
    }
  }

  public void createCacheAndRegion() {
    Cache cache = getCache();
    cache.createRegionFactory(RegionShortcut.REPLICATE).setConcurrencyChecksEnabled(false)
        .create("TestRegion");
    DistributedLockService dlockService =
        DistributedLockService.create("Bulldog", cache.getDistributedSystem());
  }

  public void becomeLockGrantor() {
    DistributedLockService dlockService = DistributedLockService.getServiceNamed("Bulldog");
    dlockService.becomeLockGrantor();
  }

  public void performOps() {
    Cache cache = getCache();
    Region region = cache.getRegion("TestRegion");
    DistributedLockService dlockService = DistributedLockService.getServiceNamed("Bulldog");
    Random random = new Random();

    while (!cache.isClosed()) {
      boolean locked = false;
      try {
        locked = dlockService.lock("testDLock", 500, 60_000);
        if (!locked) {
          // this could happen if we're starved out for 30sec by other VMs
          continue;
        }

        cache.getCacheTransactionManager().begin();

        region.put("TestKey", "TestValue" + random.nextInt(100000));

        try {
          cache.getCacheTransactionManager().commit();
        } catch (CommitConflictException e) {
          throw new RuntimeException("dlock failed to prevent a transaction conflict", e);
        }

        int txCount = getBlackboard().getMailbox(TRANSACTION_COUNT);
        getBlackboard().setMailbox(TRANSACTION_COUNT, txCount + 1);

      } catch (CancelException | IllegalStateException e) {
        // okay to ignore
      } finally {
        if (locked) {
          try {
            dlockService.unlock("testDLock");
          } catch (CancelException | IllegalStateException e) {
            // shutting down
          }
        }
      }
    }
  }
}
