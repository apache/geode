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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class GetPidAndIdAfterBounceDistributedTest {

  private int[] idsBefore;
  private int[] pidsBefore;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Before
  public void setUp() throws Exception {
    idsBefore = new int[Host.getHost(0).getVMCount()];
    pidsBefore = new int[Host.getHost(0).getVMCount()];

    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
      idsBefore[i] = Host.getHost(0).getVM(i).getId();
      pidsBefore[i] = Host.getHost(0).getVM(i).getPid();
      Host.getHost(0).getVM(i).bounce();
    }
  }

  @Test
  public void getIdShouldReturnSameValueAfterBounce() throws Exception {
    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
      assertThat(Host.getHost(0).getVM(i).getId()).isEqualTo(idsBefore[i]);
    }
  }

  @Test
  public void getPidShouldReturnDifferentValueAfterBounce() throws Exception {
    for (int i = 0; i < Host.getHost(0).getVMCount(); i++) {
      assertThat(Host.getHost(0).getVM(i).getPid()).isNotEqualTo(pidsBefore[i]);
    }
  }
}
