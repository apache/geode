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
package org.apache.geode.management.internal.cli.commands;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.EvictionTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({EvictionTest.class})
@RunWith(JUnitParamsRunner.class)
public class AlterTimeToLiveExpirationOnProxyRegionDUnitTest {
  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();
  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  public Object[] getRegionTypePairs() {
    return new Object[] {
        new Object[] {"REPLICATE", "REPLICATE_PROXY"},
        new Object[] {"PARTITION", "PARTITION_PROXY"},
        new Object[] {"PARTITION_REDUNDANT", "PARTITION_PROXY_REDUNDANT"}
    };
  }

  @Test
  @Parameters(method = "getRegionTypePairs")
  @TestCaseName("[{index}] {method} Non Proxy Region Type:{0}; Proxy Region Type:{1}")
  public void whenExpirationIsSetUsingAlterOnProxyRegionThenItShouldNotThrowException(
      String nonProxyRegionType, String proxyRegionType) throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    MemberVM server1 = clusterStartupRule.startServerVM(1, "non-proxy", locator.getPort());
    MemberVM server2 = clusterStartupRule.startServerVM(2, "proxy", locator.getPort());
    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat(
        "create region --name=region --type=" + nonProxyRegionType
            + " --enable-statistics=true --group=non-proxy")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=region --type=" + proxyRegionType
            + " --enable-statistics=true --group=proxy")
        .statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter region --name=region --entry-time-to-live-expiration=1000 --entry-time-to-live-expiration-action=destroy --group=non-proxy")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "alter region --name=region --entry-time-to-live-expiration=1000 --entry-time-to-live-expiration-action=destroy --group=proxy")
        .statusIsSuccess();

  }

}
