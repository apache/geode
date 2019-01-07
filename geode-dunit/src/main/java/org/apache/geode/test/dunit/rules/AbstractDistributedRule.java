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
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.internal.TestHistoryLogger;
import org.apache.geode.test.junit.rules.serializable.SerializableStatement;
import org.apache.geode.test.junit.rules.serializable.SerializableTestRule;

class AbstractDistributedRule implements SerializableTestRule {

  private final int vmCount;
  private final RemoteInvoker invoker;

  private volatile int beforeVmCount;

  protected AbstractDistributedRule() {
    this(DEFAULT_VM_COUNT);
  }

  protected AbstractDistributedRule(final int vmCount) {
    this(vmCount, new RemoteInvoker());
  }

  protected AbstractDistributedRule(final int vmCount, final RemoteInvoker invoker) {
    this.vmCount = vmCount;
    this.invoker = invoker;
  }


  @Override
  public Statement apply(final Statement base, final Description description) {
    return statement(base, description);
  }

  private Statement statement(final Statement base, Description testDescription) {
    return new SerializableStatement() {
      @Override
      public void evaluate() throws Throwable {
        beforeDistributedTest(testDescription);
        before();
        try {
          base.evaluate();
        } finally {
          after();
          afterDistributedTest(testDescription);
        }
      }
    };
  }

  private void beforeDistributedTest(Description testDescription) throws Throwable {
    TestHistoryLogger.logTestHistory(testDescription.getTestClass().getSimpleName(),
        testDescription.getMethodName());
    DUnitLauncher.launchIfNeeded(vmCount);
    beforeVmCount = getVMCount();
    System.out.println("\n\n[setup] START TEST " + testDescription.getClassName() + "."
        + testDescription.getMethodName());
  }

  private void afterDistributedTest(Description testDescription) throws Throwable {
    System.out.println("\n\n[setup] END TEST " + testDescription.getTestClass().getSimpleName()
        + "." + testDescription.getMethodName());
    int afterVmCount = getVMCount();
    assertThat(afterVmCount).isEqualTo(beforeVmCount);
  }

  protected void before() throws Throwable {
    // override
  }

  protected void after() throws Throwable {
    // override
  }

  protected RemoteInvoker invoker() {
    return invoker;
  }

  protected int vmCount() {
    return vmCount;
  }
}
