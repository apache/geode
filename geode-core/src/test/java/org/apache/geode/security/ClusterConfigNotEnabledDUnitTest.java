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

package org.apache.geode.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class ClusterConfigNotEnabledDUnitTest {
  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Test
  public void serverShouldNotRequestClusterConfig() throws Exception {
    Properties properties = new Properties();
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    MemberVM locator = lsRule.startLocatorVM(0, properties);
    MemberVM server = lsRule.startServerVM(1, locator.getPort());

    server.invoke(() -> {
      DM dm = LocatorServerStartupRule.getCache().getDistributionManager();
      Map<InternalDistributedMember, Collection<String>> locatorsWithClusterConfig =
          dm.getAllHostedLocatorsWithSharedConfiguration();
      assertThat(locatorsWithClusterConfig).isEmpty();
    });
  }
}
