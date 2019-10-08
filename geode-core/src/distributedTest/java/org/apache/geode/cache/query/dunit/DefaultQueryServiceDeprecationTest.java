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
import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class DefaultQueryServiceDeprecationTest implements Serializable {
  MemberVM locator;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void startUp() {
    locator = cluster.startLocatorVM(0);
  }

  @After
  public void shutDown() {
    cluster.stop(0);
  }

  @Test
  public void warningMessageIsOnlyLoggedOnceWhenDeprecatedPropertyUsed() {
    MemberVM server = cluster.startServerVM(1,
        x -> x.withConnectionToLocator(locator.getPort()).withLogFile().withSystemProperty(
            GEMFIRE_PREFIX + "QueryService.allowUntrustedMethodInvocation", "true"));

    server.invoke(() -> {
      File logFile = new File(server.getName() + ".log");
      ClusterStartupRule.getCache().getQueryService();
      ClusterStartupRule.getCache().getQueryService();
      LogFileAssert.assertThat(logFile).containsOnlyOnce(DefaultQueryService.DEPRECIATION_WARNING);
    });
    server.getVM().bounce();
  }

  @Test
  public void warningMessageIsNotLoggedWhenDeprecatedPropertyIsNotUsed() {
    MemberVM server =
        cluster.startServerVM(1, x -> x.withConnectionToLocator(locator.getPort()).withLogFile());
    server.invoke(() -> {
      File logFile = new File(server.getName() + ".log");
      ClusterStartupRule.getCache().getQueryService();
      LogFileAssert.assertThat(logFile).doesNotContain(DefaultQueryService.DEPRECIATION_WARNING);
    });
    server.getVM().bounce();
  }
}
