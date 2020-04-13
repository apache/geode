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

package org.apache.geode.tools.pulse;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.ManagementService;
import org.apache.geode.test.junit.categories.PulseTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.EmbeddedPulseRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.tools.pulse.internal.data.Cluster;

@Category({SecurityTest.class, PulseTest.class})
public class PulseSecurityIntegrationTest {

  @Rule
  public LocatorStarterRule locator =
      new LocatorStarterRule().withSecurityManager(SimpleSecurityManager.class).withAutoStart();

  @Rule
  public EmbeddedPulseRule pulse = new EmbeddedPulseRule();

  @Test
  public void getAttributesWithSecurityManager() {
    pulse.useJmxPort(locator.getJmxPort());

    ManagementService service =
        ManagementService.getExistingManagementService(locator.getLocator().getCache());

    await()
        .untilAsserted(() -> assertThat(service.getMemberMXBean()).isNotNull());

    Cluster cluster = pulse.getRepository().getClusterWithUserNameAndPassword("cluster", "cluster");
    Cluster.Member[] members = cluster.getMembers();
    assertThat(members.length).isEqualTo(1);
    assertThat(members[0].getName()).isEqualTo("locator");
  }

  @Test
  public void getAttributesWithSecurityManagerAndTokenLogin() {
    String tokenValue = "atleast20charactersoftokenimsure";
    String userName = "cluster";

    pulse.useJmxPort(locator.getJmxPort());

    ManagementService service =
        ManagementService.getExistingManagementService(locator.getLocator().getCache());

    await()
        .untilAsserted(() -> assertThat(service.getMemberMXBean()).isNotNull());

    Cluster cluster = pulse.getRepository().getClusterWithCredentials(userName, tokenValue);
    Cluster.Member[] members = cluster.getMembers();
    assertThat(members.length).isEqualTo(1);
    assertThat(members[0].getName()).isEqualTo("locator");
  }

}
