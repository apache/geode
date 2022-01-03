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
package org.apache.geode.test.dunit.rules.tests;

import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.test.dunit.VM.getAllVMs;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.apache.geode.test.dunit.VM.toArray;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.lang.SystemProperty;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

/**
 * Distributed tests for {@link DistributedDiskDirRule}.
 */
@SuppressWarnings("serial")
public class DistributedDiskDirRuleDistributedTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedDiskDirRule distributedDiskDirRule = new DistributedDiskDirRule();

  @Test
  public void setsDefaultDiskDirsPropertyInEveryVm() {
    for (VM vm : toArray(getAllVMs(), getController())) {
      vm.invoke(() -> {
        String propertyValue =
            System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY);

        assertThat(propertyValue)
            .isEqualTo(distributedDiskDirRule.getDiskDirFor(vm).getAbsolutePath());
      });
    }
  }

  @Test
  public void everyVmHasUniqueDefaultDiskDirsValue() {
    List<String> propertyValues = new ArrayList<>();

    for (VM vm : toArray(getAllVMs(), getController())) {
      String propertyValue =
          vm.invoke(
              () -> System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY));
      assertThat(propertyValues).doesNotContain(propertyValue);
      propertyValues.add(propertyValue);
    }

    assertThat(propertyValues).hasSize(getVMCount() + 1);
  }

  @Test
  public void defaultDiskDirsPropertyIsSetInNewVm() {
    VM newVM = getVM(getVMCount());

    String propertyValue =
        newVM.invoke(
            () -> System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY));

    assertThat(propertyValue).isNotNull();
  }

  @Test
  public void defaultDiskDirsPropertyIsKeptInBouncedVm() {
    String propertyValueBeforeBounce =
        getVM(0).invoke(
            () -> System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY));

    getVM(0).bounce();

    String propertyValueAfterBounce =
        getVM(0).invoke(
            () -> System.getProperty(SystemProperty.DEFAULT_PREFIX + DEFAULT_DISK_DIRS_PROPERTY));
    assertThat(propertyValueAfterBounce).isEqualTo(propertyValueBeforeBounce);
  }
}
