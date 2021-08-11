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
package org.apache.geode.test.version;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;

@Category(BackwardCompatibilityTest.class)
public class VersionManagerUpgradeTest {

  @Test
  public void exceptionIsNotThrownInInitialization() {
    VersionManager instance =
        VersionManager.getInstance("--nonexistent-file?--");
    assertThat(instance.getLoadFailure()).isNotEmpty();
  }

  @Test
  public void exceptionIsThrownOnUse() {
    VersionManager instance =
        VersionManager.getInstance("--nonexistent-file?--");
    assertThat(instance.getLoadFailure()).isNotEmpty();

    Throwable thrown = catchThrowable(() -> instance.getVersionsWithoutCurrent());
    assertThat(thrown).hasMessage(instance.getLoadFailure());

    thrown = catchThrowable(() -> instance.getVersions());
    assertThat(thrown).hasMessage(instance.getLoadFailure());
  }

  @Test
  public void managerIsAbleToFindVersions() {
    VersionManager instance = VersionManager.getInstance();

    assertThat(instance.getVersionsWithoutCurrent()).isNotEmpty();
  }
}
