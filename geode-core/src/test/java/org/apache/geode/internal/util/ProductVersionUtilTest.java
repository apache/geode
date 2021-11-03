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

import org.junit.Test;

import org.apache.geode.internal.GeodeVersion;

public class ProductVersionUtilTest {

  @Test
  public void getDistributionVersionReturnGeodeVersion() {
    assertThat(ProductVersionUtil.getDistributionVersion()).isInstanceOf(GeodeVersion.class);
  }

  @Test
  public void getComponentVersionsReturnsGeodeVersionOnly() {
    assertThat(ProductVersionUtil.getComponentVersions()).singleElement()
        .isInstanceOf(GeodeVersion.class);
  }

  @Test
  public void appendFullVersionAppendsGeodeVersion() throws IOException {
    assertThat(ProductVersionUtil.appendFullVersion(new StringBuilder())).contains("Apache Geode");
  }

  @Test
  public void getFullVersionContainsGeodeVersion() {
    assertThat(ProductVersionUtil.getFullVersion())
        .contains("Apache Geode")
        .contains("Source-Revision")
        .contains("Build-Id");
  }

}
