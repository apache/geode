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
package org.apache.geode.test.dunit.examples;

import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * {@code DistributedRule} can also be used in DistributedTests as a {@code ClassRule}. This ensures
 * that DUnit VMs will be available to non-Class {@code Rule}s. However, you may want to declare
 * {@code DistributedRule.TearDown} as a non-Class {@code Rule} so that check for suspect strings is
 * performed after each test method.
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public DistributedRule.TearDown distributedRuleTearDown = new DistributedRule.TearDown();
 *
 * {@literal @}Test
 * public void shouldHaveFourDUnitVMsByDefault() {
 *   assertThat(getVMCount()).isEqualTo(4);
 * }
 * </pre>
 */
public class DistributedRuleAsClassRuleExampleTest {

  @ClassRule
  public static DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedRule.TearDown distributedRuleTearDown = new DistributedRule.TearDown();

  @Test
  public void hasFourVMsByDefault() {
    assertThat(getVMCount()).isEqualTo(4);
  }
}
