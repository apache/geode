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

import static org.apache.geode.test.junit.runners.TestRunner.runTestWithExpectedFailure;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.notification.Failure;

import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.UnitTest;

@Ignore("GEODE-4885")
@Category(UnitTest.class)
public class DistributedRestoreSystemPropertiesTest {

  @Test
  public void withoutDUnitThrowsIllegalStateException() {
    Failure failure = runTestWithExpectedFailure(WithoutDUnit.class);
    assertThat(failure.getException()).isInstanceOf(IllegalStateException.class);
    assertThat(failure.getMessage()).isEqualTo("DUnit VMs have not been launched");
  }

  @Test
  public void isaRestoreSystemProperties() {
    assertThat(new DistributedRestoreSystemProperties())
        .isInstanceOf(RestoreSystemProperties.class);
  }

  public static class WithoutDUnit {

    @Rule
    public DistributedRestoreSystemProperties restoreSystemProperties =
        new DistributedRestoreSystemProperties();

    @Test
    public void doTest() {
      // nothing
    }
  }
}
