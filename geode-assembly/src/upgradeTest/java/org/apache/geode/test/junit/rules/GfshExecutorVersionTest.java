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
package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.util.ProductVersionUtil;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecutor;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

@Category(GfshTest.class)
public class GfshExecutorVersionTest {

  @Rule(order = 0)
  public FolderRule folderRule = new FolderRule();
  @Rule(order = 1)
  public GfshRule gfshRule = new GfshRule();

  @Test
  public void contextUsesCurrentGeodeVersionByDefault() {
    String currentVersion = ProductVersionUtil.getDistributionVersion().getVersion();

    GfshExecutor executor = gfshRule.executor().build(folderRule.getFolder().toPath());

    assertThat(executor.execute("version").getOutputText()).contains(currentVersion);
  }

  @Test
  public void contextUsesSpecifiedGeodeVersion() {
    String specifiedVersion = "1.3.0";

    GfshExecutor executor = gfshRule.executor()
        .withGeodeVersion(specifiedVersion)
        .build(folderRule.getFolder().toPath());

    assertThat(executor.execute("version").getOutputText()).contains(specifiedVersion);
  }
}
