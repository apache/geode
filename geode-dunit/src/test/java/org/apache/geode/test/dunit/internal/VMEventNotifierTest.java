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
package org.apache.geode.test.dunit.internal;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

/**
 * Unit tests for {@link VMEventNotifier};
 */
@RunWith(JUnitParamsRunner.class)
public class VMEventNotifierTest {

  private static final long TIMEOUT_MILLIS = GeodeAwaitility.getTimeout().toMillis();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private final CountDownLatch startLatch = new CountDownLatch(1);
  private final CountDownLatch stopLatch = new CountDownLatch(1);

  private VMEventListener vmEventListener1;
  private VMEventListener vmEventListener2;
  private VM vm;

  private VMEventNotifier vmEventNotifier;

  @Before
  public void setUp() {
    vmEventListener1 = mock(VMEventListener.class);
    vmEventListener2 = mock(VMEventListener.class);
    vm = mock(VM.class);

    vmEventNotifier = new VMEventNotifier();
  }

  @Test
  @Parameters({"AFTER_CREATE_VM", "BEFORE_BOUNCE_VM", "AFTER_BOUNCE_VM"})
  @TestCaseName("{method}({params})")
  public void addsListenerConcurrentlyWithNotification(Notification notification) throws Exception {
    doAnswer(invocation -> {
      startLatch.countDown();
      stopLatch.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      return null;
    }).when(vmEventListener1).afterCreateVM(vm);

    vmEventNotifier.addVMEventListener(vmEventListener1);

    Future<Void> notifiedFuture = executorServiceRule.submit(() -> {
      notification.notify(vmEventNotifier, vm);
      return null;
    });

    startLatch.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    vmEventNotifier.addVMEventListener(vmEventListener2);

    stopLatch.countDown();

    notifiedFuture.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    verify(vmEventListener1).afterCreateVM(eq(vm));
    verifyZeroInteractions(vmEventListener2);
  }

  @Test
  @Parameters({"AFTER_CREATE_VM", "BEFORE_BOUNCE_VM", "AFTER_BOUNCE_VM"})
  @TestCaseName("{method}({params})")
  public void removesListenerConcurrentlyWithNotification(Notification notification)
      throws Exception {
    doAnswer(invocation -> {
      startLatch.countDown();
      stopLatch.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
      return null;
    }).when(vmEventListener1).afterCreateVM(vm);

    vmEventNotifier.addVMEventListener(vmEventListener1);
    vmEventNotifier.addVMEventListener(vmEventListener2);

    Future<Void> notifiedFuture = executorServiceRule.submit(() -> {
      notification.notify(vmEventNotifier, vm);
      return null;
    });

    startLatch.await(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    vmEventNotifier.removeVMEventListener(vmEventListener2);

    stopLatch.countDown();

    notifiedFuture.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    verify(vmEventListener1).afterCreateVM(eq(vm));
    verify(vmEventListener2).afterCreateVM(eq(vm));
  }

  @SuppressWarnings("unused")
  private enum Notification {
    AFTER_CREATE_VM(params -> params.vmEventNotifier().notifyAfterCreateVM(params.vm())),
    BEFORE_BOUNCE_VM(params -> params.vmEventNotifier().notifyAfterCreateVM(params.vm())),
    AFTER_BOUNCE_VM(params -> params.vmEventNotifier().notifyAfterCreateVM(params.vm()));

    private final Consumer<NotificationParams> strategy;

    Notification(Consumer<NotificationParams> strategy) {
      this.strategy = strategy;
    }

    void notify(VMEventNotifier vmEventNotifier, VM vm) {
      strategy.accept(new NotificationParams(vmEventNotifier, vm));
    }
  }

  private static class NotificationParams {
    private final VMEventNotifier vmEventNotifier;
    private final VM vm;

    NotificationParams(VMEventNotifier vmEventNotifier, VM vm) {
      this.vmEventNotifier = vmEventNotifier;
      this.vm = vm;
    }

    VMEventNotifier vmEventNotifier() {
      return vmEventNotifier;
    }

    VM vm() {
      return vm;
    }
  }
}
