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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;

import java.io.Serializable;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.junit.rules.UseJacksonForJsonPathRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

/**
 * Distributed version of UseJacksonForJsonPathRule JUnit Rule that configures json-path to use
 * {@code JacksonJsonProvider} in all DUnit VMs (except for the hidden Locator VM) in addition to
 * the JVM running JUnit (known as the Controller VM).
 *
 * <p>
 * DistributedUseJacksonForJsonPathRule can be used in tests that need to use json-path-assert:
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedUseJacksonForJsonPathRule useJacksonForJsonPathRule = new DistributedUseJacksonForJsonPathRule();
 *
 * {@literal @}Test
 * public void hasAssertionsUsingJsonPathMatchers() {
 *   ...
 *   assertThat(json, isJson());
 *   assertThat(json, hasJsonPath("$.result"));
 * }
 * </pre>
 */
@SuppressWarnings("serial,unused")
public class DistributedUseJacksonForJsonPathRule extends UseJacksonForJsonPathRule implements
    SerializableTestRule {

  private static volatile UseJacksonForJsonPathRule instance = new UseJacksonForJsonPathRule();

  private final int vmCount;
  private final RemoteInvoker invoker;
  private final VMEventListener vmEventListener;

  public DistributedUseJacksonForJsonPathRule() {
    this(DEFAULT_VM_COUNT, new RemoteInvoker());
  }

  public DistributedUseJacksonForJsonPathRule(final int vmCount) {
    this(vmCount, new RemoteInvoker());
  }

  private DistributedUseJacksonForJsonPathRule(final int vmCount, final RemoteInvoker invoker) {
    this.vmCount = vmCount;
    this.invoker = invoker;
    vmEventListener = new InternalVMEventListener();
  }

  @Override
  public void before() {
    DUnitLauncher.launchIfNeeded(vmCount);
    VM.addVMEventListener(vmEventListener);

    invoker.invokeInEveryVMAndController(this::doBefore);
  }

  @Override
  public void after() {
    VM.removeVMEventListener(vmEventListener);

    invoker.invokeInEveryVMAndController(this::doAfter);
  }

  private void afterCreateVM(VM vm) {
    vm.invoke(this::doBefore);
  }

  private void afterBounceVM(VM vm) {
    vm.invoke(this::doBefore);
  }

  private void doBefore() {
    instance = new UseJacksonForJsonPathRule();
    instance.before();
  }

  private void doAfter() {
    instance.after();
    instance = null;
  }

  /**
   * VMEventListener for DistributedUseJacksonForJsonPathRule.
   */
  private class InternalVMEventListener implements VMEventListener, Serializable {

    @Override
    public void afterCreateVM(VM vm) {
      DistributedUseJacksonForJsonPathRule.this.afterCreateVM(vm);
    }

    @Override
    public void afterBounceVM(VM vm) {
      DistributedUseJacksonForJsonPathRule.this.afterBounceVM(vm);
    }
  }
}
