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
package org.apache.geode.test.dunit.standalone;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.UnitTest;

@Category({UnitTest.class, BackwardCompatibilityTest.class})
public class VersionManagerJUnitTest {

  @Test
  public void exceptionIsNotThrownInInitialization() throws Exception {
    VersionManager instance =
        VersionManager.getInstance("--nonexistent-file?--", "--nonexistent-install-file--");
    Assert.assertNotEquals("", instance.loadFailure);
  }

  @Test
  public void exceptionIsThrownOnUse() throws Exception {
    VersionManager instance =
        VersionManager.getInstance("--nonexistent-file?--", "--nonexistent-install-file--");
    Assert.assertNotEquals("", instance.loadFailure);
    assertThatThrownBy(() -> instance.getVersionsWithoutCurrent()).hasMessage(instance.loadFailure);
    assertThatThrownBy(() -> instance.getVersions()).hasMessage(instance.loadFailure);
  }

  @Test
  public void managerIsAbleToFindVersions() throws Exception {
    VersionManager instance = VersionManager.getInstance();
    Assert.assertTrue(instance.getVersionsWithoutCurrent().size() > 0);
  }
}
