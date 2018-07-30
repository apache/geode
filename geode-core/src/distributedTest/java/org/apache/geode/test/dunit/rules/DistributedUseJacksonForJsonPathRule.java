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

import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.junit.rules.UseJacksonForJsonPathRule;

public class DistributedUseJacksonForJsonPathRule extends UseJacksonForJsonPathRule {

  private static volatile UseJacksonForJsonPathRule instance = new UseJacksonForJsonPathRule();

  private final RemoteInvoker invoker;

  private volatile int beforeVmCount;

  public DistributedUseJacksonForJsonPathRule() {
    this(new RemoteInvoker());
  }

  public DistributedUseJacksonForJsonPathRule(final RemoteInvoker invoker) {
    this.invoker = invoker;
  }

  @Override
  public void before() {
    DUnitLauncher.launchIfNeeded();
    beforeVmCount = getVMCount();

    invoker.invokeInEveryVMAndController(() -> invokeBefore());
  }

  @Override
  public void after() {
    int afterVmCount = getVMCount();
    assertThat(afterVmCount).isEqualTo(beforeVmCount);

    invoker.invokeInEveryVMAndController(() -> invokeAfter());
  }

  private static void invokeBefore() {
    instance = new UseJacksonForJsonPathRule();
    instance.before();
  }

  private static void invokeAfter() {
    instance.after();
    instance = null;
  }
}
