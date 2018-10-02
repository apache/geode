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
package org.apache.geode.test.dunit.tests;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.test.dunit.DistributedTestCase;


@SuppressWarnings("serial")
public class GetTestMethodNameDistributedTest extends DistributedTestCase {

  @Test
  public void testGetTestMethodName() {
    assertGetTestMethodName("testGetTestMethodName");
  }

  @Test
  public void testGetTestMethodNameChanges() {
    assertGetTestMethodName("testGetTestMethodNameChanges");
  }

  @Test
  public void testGetTestMethodNameInAllVMs() {
    assertGetTestMethodName("testGetTestMethodNameInAllVMs");

    for (int vmIndex = 0; vmIndex < getVMCount(); vmIndex++) {
      getVM(vmIndex).invoke(() -> assertGetTestMethodName("testGetTestMethodNameInAllVMs"));
    }
  }

  private void assertGetTestMethodName(final String expected) {
    assertThat(getTestMethodName()).isEqualTo(expected);
  }
}
