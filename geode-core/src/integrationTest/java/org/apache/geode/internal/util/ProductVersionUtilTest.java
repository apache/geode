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

package org.apache.geode.internal.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import org.apache.geode.internal.GeodeVersion;
import org.apache.geode.internal.version.ComponentVersion;
import org.apache.geode.internal.version.DistributionVersion;

public class ProductVersionUtilTest {
  @Test
  public void getDistributionVersionReturnFakeVersion() {
    assertThat(ProductVersionUtil.getDistributionVersion()).isInstanceOf(FakeVersion.class);
  }

  @Test
  public void getComponentVersionsReturnsGeodeVersionAndFakeVersion() {
    assertThat(ProductVersionUtil.getComponentVersions())
        .hasSize(2)
        .hasAtLeastOneElementOfType(GeodeVersion.class)
        .hasAtLeastOneElementOfType(FakeVersion.class);
  }

  @Test
  public void appendFullVersionAppendsGeodeVersionAndFakeVersion() throws IOException {
    assertThat(ProductVersionUtil.appendFullVersion(new StringBuilder()))
        .contains("Apache Geode")
        .contains("Source-Revision")
        .contains("Build-Id")
        .contains("Fake Distribution")
        .contains("Fake-Attribute");
  }

  @Test
  public void getFullVersionContainsGeodeVersionAndFakeVersion() {
    assertThat(ProductVersionUtil.getFullVersion())
        .contains("Apache Geode")
        .contains("Source-Revision")
        .contains("Build-Id")
        .contains("Fake Distribution")
        .contains("Fake-Attribute");
  }

  public static class FakeVersion implements DistributionVersion, ComponentVersion {

    @Override
    public @NotNull String getName() {
      return "Fake Distribution";
    }

    @Override
    public @NotNull String getVersion() {
      return "1.2.3";
    }

    @Override
    public @NotNull Map<@NotNull String, @NotNull String> getDetails() {
      return new HashMap<String, String>() {
        {
          put("Version", getVersion());
          put("Fake-Attribute", "42");
        }
      };
    }
  }
}
