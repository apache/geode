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
package org.apache.geode.test.dunit.rules.tests;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Distributed tests for {@link VMEventListener} callbacks.
 */
public class VmEventListenerDistributedTest {

  @Rule
  public AccessibleDistributedRule distributedRule = spy(new AccessibleDistributedRule());

  private ArgumentCaptor<VM> vmCaptor;
  private int beforeVmCount;
  private VM vm;

  @Before
  public void setUp() {
    vmCaptor = ArgumentCaptor.forClass(VM.class);
    beforeVmCount = getVMCount();
    vm = getVM(0);
  }

  @Test
  public void afterCreateVmIsInvokedForNewlyCreatedVm() {
    getVM(beforeVmCount);

    verify(distributedRule).afterCreateVM(eq(getVM(beforeVmCount)));
  }

  @Test
  public void afterCreateVmIsInvokedForMultipleNewlyCreatedVms() {
    // getVM implicitly creates intervening VMs between beforeVmCount and beforeVmCount+2
    getVM(beforeVmCount + 2);

    verify(distributedRule, times(3)).afterCreateVM(vmCaptor.capture());
    assertThat(vmCaptor.getAllValues()).containsExactly(getVM(beforeVmCount),
        getVM(beforeVmCount + 1), getVM(beforeVmCount + 2));
  }

  @Test
  public void beforeBounceVmIsInvokedWhenInvokingBounce() {
    vm.bounce();

    verify(distributedRule).beforeBounceVM(eq(vm));
  }

  @Test
  public void afterBounceVmIsInvokedWhenInvokingBounce() {
    vm.bounce();

    verify(distributedRule).afterBounceVM(eq(vm));
  }

  @Test
  public void beforeAndAfterBounceVmAreInvokedInOrderWhenInvokingBounce() {
    vm.bounce();

    InOrder inOrder = inOrder(distributedRule);
    inOrder.verify(distributedRule).beforeBounceVM(eq(vm));
    inOrder.verify(distributedRule).afterBounceVM(eq(vm));
  }

  @Test
  public void beforeBounceVmIsInvokedWhenInvokingBounceForcibly() {
    vm.bounceForcibly();

    verify(distributedRule).beforeBounceVM(eq(vm));
  }

  @Test
  public void afterBounceVmIsInvokedWhenInvokingBounceForcibly() {
    vm.bounceForcibly();

    verify(distributedRule).afterBounceVM(eq(vm));
  }

  @Test
  public void beforeAndAfterBounceVmAreInvokedInOrderWhenInvokingBounceForcibly() {
    vm.bounceForcibly();

    InOrder inOrder = inOrder(distributedRule);
    inOrder.verify(distributedRule).beforeBounceVM(eq(vm));
    inOrder.verify(distributedRule).afterBounceVM(eq(vm));
  }

  /**
   * Increase visibility of {@link VMEventListener} callbacks in {@link DistributedRule}.
   */
  private static class AccessibleDistributedRule extends DistributedRule {

    @Override
    public void afterCreateVM(VM vm) {
      // exposed for spy
    }

    @Override
    public void beforeBounceVM(VM vm) {
      // exposed for spy
    }

    @Override
    public void afterBounceVM(VM vm) {
      // exposed for spy
    }
  }
}
