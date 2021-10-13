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

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.VMEventListener;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.internal.TestHistoryLogger;
import org.apache.geode.test.junit.rules.serializable.SerializableStatement;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

class AbstractDistributedRule implements SerializableTestRule {

  private final int vmCount;
  private final RemoteInvoker invoker;
  private final boolean classloaderIsolated;

  // if you alter vmEventListener at all, make sure VmEventListenerDistributedTest still passes
  private volatile VMEventListener vmEventListener;

  private static final boolean RUN_VM_CLASSLOADER_ISOLATED =
      System.getenv("CLASSLOADER_ISOLATED") != null
          && Boolean.parseBoolean(System.getenv("CLASSLOADER_ISOLATED"));

  AbstractDistributedRule() {
    this(DEFAULT_VM_COUNT);
  }

  AbstractDistributedRule(final int vmCount) {
    this(vmCount, RUN_VM_CLASSLOADER_ISOLATED);
  }

  AbstractDistributedRule(final int vmCount, boolean classloaderIsolated) {
    this(vmCount, classloaderIsolated, new RemoteInvoker());
  }

  private AbstractDistributedRule(final int vmCount, boolean classloaderIsolated,
      final RemoteInvoker invoker) {
    this.vmCount = vmCount;
    this.classloaderIsolated = classloaderIsolated;
    this.invoker = invoker;
  }

  @Override
  public Statement apply(final Statement base, final Description description) {
    return new SerializableStatement() {
      @Override
      public void evaluate() throws Throwable {
        beforeDistributedTest(description);
        try {
          base.evaluate();
        } finally {
          afterDistributedTest(description);
        }
      }
    };
  }

  void beforeDistributedTest(final Description description) throws Throwable {
    TestHistoryLogger.logTestHistory(description.getTestClass().getSimpleName(),
        description.getMethodName());
    DUnitLauncher.launchIfNeeded(vmCount, true, classloaderIsolated);
    System.out.println("\n\n[setup] START TEST " + description.getClassName() + "."
        + description.getMethodName());

    vmEventListener = new InternalVMEventListener();
    VM.addVMEventListener(vmEventListener);
    before();
  }

  void afterDistributedTest(final Description description) throws Throwable {
    VM.removeVMEventListener(vmEventListener);
    after();

    System.out.println("\n\n[setup] END TEST " + description.getTestClass().getSimpleName()
        + "." + description.getMethodName());
  }

  protected void before() throws Throwable {
    // override if needed
  }

  protected void after() throws Throwable {
    // override if needed
  }

  protected RemoteInvoker invoker() {
    return invoker;
  }

  protected int vmCount() {
    return vmCount;
  }

  protected void afterCreateVM(VM vm) {
    // override if needed
  }

  protected void beforeBounceVM(VM vm) {
    // override if needed
  }

  protected void afterBounceVM(VM vm) {
    // override if needed
  }

  private class InternalVMEventListener implements VMEventListener, Serializable {

    @Override
    public void afterCreateVM(VM vm) {
      AbstractDistributedRule.this.afterCreateVM(vm);
    }

    @Override
    public void beforeBounceVM(VM vm) {
      AbstractDistributedRule.this.beforeBounceVM(vm);
    }

    @Override
    public void afterBounceVM(VM vm) {
      AbstractDistributedRule.this.afterBounceVM(vm);
    }
  }
}
