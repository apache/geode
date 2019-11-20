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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

public class ChangeProductVersionDistributedTest implements Serializable {

  private static final TestVersion NEW_PRODUCT_VERSION =
      TestVersion.valueOf(VersionManager.CURRENT_VERSION);

  @Rule
  public transient DistributedRule distributedRule =
      DistributedRule.builder().withVMCount(0).build();

  private static TestVersion getOldProductVersion() {

    final Map<Boolean, List<TestVersion>> groups =
        VersionManager.getInstance().getVersionsWithoutCurrent().stream()
            .map(TestVersion::valueOf)
            .collect(Collectors.partitioningBy(v -> v.lessThan(NEW_PRODUCT_VERSION)));

    final List<TestVersion> olderVersions = groups.get(true);

    if (olderVersions.size() < 1) {
      throw new AssertionError(
          "Failed to find old version to test with. Current version is: " + NEW_PRODUCT_VERSION);
    }

    return olderVersions.get(olderVersions.size() - 1);
  }

  @Test
  public void testChangeProductVersions() {
    final TestVersion oldProductVersion = getOldProductVersion();

    VM jvm = Host.getHost(0).getVM(oldProductVersion.toString(), 0);

    jvm.invoke(() -> {
      assertProductVersionEquals(oldProductVersion.toString());
    });

    jvm = Host.getHost(0).getVM(NEW_PRODUCT_VERSION.toString(), 0);

    jvm.invoke(() -> {
      assertProductVersionEquals(NEW_PRODUCT_VERSION.toString());
    });

  }

  public static void assertProductVersionEquals(final String expectedVersion) {

    final String actualVersion = System.getProperty(DUnitLauncher.VM_VERSION_PARAM);

    System.out.println(String.format("expected: %s, actual: %s", expectedVersion, actualVersion));
    assertThat(actualVersion).isEqualTo(expectedVersion);
  }

}
