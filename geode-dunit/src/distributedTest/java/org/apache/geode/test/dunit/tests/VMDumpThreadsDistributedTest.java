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
package org.apache.geode.test.dunit.tests;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class VMDumpThreadsDistributedTest implements Serializable {

  private static final long TIMEOUT_MILLIS = getTimeout().getValueInMS();

  private static final AtomicReference<ExecutorService> executor = new AtomicReference<>();
  private static final AtomicReference<CountDownLatch> latch = new AtomicReference<>();
  private static final AtomicReference<ThreadInfo> threadInfo1 = new AtomicReference<>();
  private static final AtomicReference<ThreadInfo> threadInfo2 = new AtomicReference<>();

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Before
  public void setUp() {
    getVM(0).invoke(() -> {
      executor.set(Executors.newFixedThreadPool(2));
      latch.set(new CountDownLatch(1));
    });
  }

  @After
  public void tearDown() {
    getVM(0).invoke(() -> {
      latch.get().countDown();
      executor.get().shutdown();
    });
  }

  @Test
  public void threadDumpOfVmContainsRemoteThreads() {
    ThreadInfo remoteThreadInfo1 = getVM(0).invoke(() -> {
      executor.get().submit(() -> {
        threadInfo1.set(new ThreadInfo(Thread.currentThread()));
        latch.get();
      });

      await().untilAsserted(() -> assertThat(threadInfo1.get()).isNotNull());
      return threadInfo1.get();
    });

    ThreadInfo remoteThreadInfo2 = getVM(0).invoke(() -> {
      executor.get().submit(() -> {
        threadInfo2.set(new ThreadInfo(Thread.currentThread()));
        syncMethod();
      });

      await().untilAsserted(() -> assertThat(threadInfo2.get()).isNotNull());
      return threadInfo2.get();
    });

    assertThat(remoteThreadInfo1.getThreadId()).isNotEqualTo(remoteThreadInfo2.getThreadId());
    assertThat(remoteThreadInfo1.getThreadName()).isNotEqualTo(remoteThreadInfo2.getThreadName());

    String threadDump = getVM(0).invoke(VM::dumpThreads);

    assertThat(threadDump)
        .contains("\"" + remoteThreadInfo1.getThreadName() + "\" Id=" + remoteThreadInfo1.threadId +
            " WAITING on java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject@")
        .contains("\"" + remoteThreadInfo2.getThreadName() + "\" Id=" + remoteThreadInfo2.threadId +
            " TIMED_WAITING on java.util.concurrent.CountDownLatch$Sync@")
        .contains("\"main\" Id=1 TIMED_WAITING");
  }

  private synchronized void syncMethod() {
    try {
      latch.get().await(TIMEOUT_MILLIS, MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static class ThreadInfo implements Serializable {

    private final String threadName;
    private final long threadId;

    ThreadInfo(Thread thread) {
      this(thread.getName(), thread.getId());
    }

    private ThreadInfo(String threadName, long threadId) {
      this.threadName = threadName;
      this.threadId = threadId;
    }

    String getThreadName() {
      return threadName;
    }

    long getThreadId() {
      return threadId;
    }

    @Override
    public String toString() {
      return "ThreadInfo(name=" + threadName + ", id=" + threadId + ")";
    }
  }
}
