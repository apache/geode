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

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.rules.accessible.AccessibleRestoreSystemProperties;

/**
 * Distributed version of RestoreSystemProperties JUnit Rule that restores system properties in all
 * DUnit VMs (except for the hidden Locator VM) in addition to the JVM running JUnit (known as the
 * Controller VM).
 */
public class DistributedRestoreSystemProperties extends AbstractDistributedRule {

  private static volatile AccessibleRestoreSystemProperties restoreSystemProperties;

  public DistributedRestoreSystemProperties() {
    // default vmCount
  }

  public DistributedRestoreSystemProperties(int vmCount) {
    super(vmCount);
  }

  @Override
  public void before() throws Exception {
    invoker().invokeInEveryVMAndController(this::invokeBefore);
  }

  @Override
  public void after() {
    invoker().invokeInEveryVMAndController(this::invokeAfter);
  }

  @Override
  protected void afterCreateVM(VM vm) {
    vm.invoke(this::invokeBefore);
  }

  @Override
  protected void afterBounceVM(VM vm) {
    vm.invoke(this::invokeBefore);
  }

  private void invokeBefore() throws Exception {
    try {
      restoreSystemProperties = new AccessibleRestoreSystemProperties();
      restoreSystemProperties.before();
    } catch (Throwable throwable) {
      if (throwable instanceof Exception) {
        throw (Exception) throwable;
      }
      throw new RuntimeException(throwable);
    }
  }

  private void invokeAfter() {
    restoreSystemProperties.after();
  }
}
