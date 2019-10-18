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
package org.apache.geode.cache.query.dunit;

import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DefaultQueryServiceDeprecationTest implements Serializable {

  private MemberVM locator;

  @ClassRule
  public static TemporaryFolder folderRule = new TemporaryFolder();

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void startUp() {
    locator = cluster.startLocatorVM(0);
  }

  @Test
  public void warningMessageIsOnlyLoggedOnceWhenDeprecatedPropertyUsed() throws IOException {
    File logFile = folderRule.newFile("customLog1.log");

    MemberVM server = cluster.startServerVM(1,
        x -> x.withConnectionToLocator(locator.getPort()).withSystemProperty(
            GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation", "true")
            .withProperty("log-file", logFile.getAbsolutePath()));

    server.invoke(() -> {
      ClusterStartupRule.getCache().getQueryService();
      ClusterStartupRule.getCache().getQueryService();
      LogFileAssert.assertThat(logFile).containsOnlyOnce(DefaultQueryService.DEPRECATION_WARNING);
    });
    server.getVM().bounce();
  }

  @Test
  public void warningMessageIsNotLoggedWhenDeprecatedPropertyIsNotUsed() throws IOException {
    File logFile = folderRule.newFile("customLog2.log");

    MemberVM server =
        cluster.startServerVM(1, x -> x.withConnectionToLocator(locator.getPort())
            .withProperty("log-file", logFile.getAbsolutePath()));
    server.invoke(() -> {
      ClusterStartupRule.getCache().getQueryService();
      LogFileAssert.assertThat(logFile).doesNotContain(DefaultQueryService.DEPRECATION_WARNING);
    });
    server.getVM().bounce();
  }
}
