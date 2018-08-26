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

import org.apache.geode.test.junit.rules.accessible.AccessibleRestoreSystemProperties;

/**
 * Distributed version of RestoreSystemProperties which affects all DUnit JVMs including the Locator
 * JVM.
 */
public class DistributedRestoreSystemProperties extends AbstractDistributedRule {

  private static final AccessibleRestoreSystemProperties restoreSystemProperties =
      new AccessibleRestoreSystemProperties();

  public DistributedRestoreSystemProperties() {
    // nothing
  }

  @Override
  public void before() throws Exception {
    invoker().invokeInEveryVMAndController(() -> invokeBefore());
  }

  @Override
  public void after() {
    invoker().invokeInEveryVMAndController(() -> invokeAfter());
  }

  private void invokeBefore() throws Exception {
    try {
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
