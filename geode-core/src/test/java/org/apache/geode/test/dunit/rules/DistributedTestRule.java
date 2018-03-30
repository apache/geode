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
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.dunit.standalone.DUnitLauncher;

/**
 * JUnit Rule that launches DistributedTest VMs without {@code DistributedTestCase}. Class may need
 * to implement {@code Serializable}.
 *
 * <p>
 * {@code DistributedTestRule} follows the standard convention of using a {@code Builder} for
 * configuration as introduced in the JUnit {@code Timeout} rule.
 *
 * <p>
 * {@code DistributedTestRule} can be used in DistributedTests as a {@code ClassRule}:
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedTestRule distributedTestRule = new DistributedTestRule();
 *
 * {@literal @}Test
 * public void shouldHaveFourDUnitVMsByDefault() {
 *   assertThat(Host.getHost(0).getVMCount()).isEqualTo(4);
 * }
 * </pre>
 */
@SuppressWarnings("unused")
public class DistributedTestRule extends DistributedExternalResource {

  private final int vmCount;

  public static Builder builder() {
    return new Builder();
  }

  public DistributedTestRule() {
    this(new Builder());
  }

  public DistributedTestRule(final int vmCount) {
    this(new Builder().withVMCount(vmCount));
  }

  DistributedTestRule(final Builder builder) {
    vmCount = builder.vmCount;
  }

  @Override
  protected void before() throws Throwable {
    DUnitLauncher.launchIfNeeded();
    for (int i = 0; i < vmCount; i++) {
      assertThat(getVM(i)).isNotNull();
    }
  }

  public static class Builder {

    private int vmCount = DEFAULT_VM_COUNT;

    public Builder withVMCount(final int vmCount) {
      if (vmCount < 0) {
        throw new IllegalArgumentException("VM count must be positive integer");
      }
      this.vmCount = vmCount;
      return this;
    }

    public DistributedTestRule build() {
      return new DistributedTestRule(this);
    }
  }
}
