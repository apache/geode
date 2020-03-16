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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.RegionsTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({RegionsTest.class})
public class AlterRegionCommandIntegrationTest {
  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "REPLICATED")
          .withRegion(RegionShortcut.PARTITION, "PARTITION");

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void before() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void validateGroup() {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --group=unknown").statusIsError()
        .containsOutput("No Members Found");
  }

  @Test
  public void invalidCacheListener() {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --cache-listener=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName[] for option 'cache-listener'");
  }

  @Test
  public void invalidCacheLoader() {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --cache-loader=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName for option 'cache-loader'");
  }

  @Test
  public void invalidCacheWriter() {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --cache-writer=abc-def")
        .statusIsError().containsOutput(
            "java.lang.IllegalArgumentException: Failed to convert 'abc-def' to type ClassName for option 'cache-writer'");
  }

  @Test
  public void invalidEvictionMax() {
    gfsh.executeAndAssertThat("alter region --name=/REPLICATED --eviction-max=-1").statusIsError()
        .containsOutput("Specify 0 or a positive integer value for eviction-max");
  }


  @Test
  public void alterRegionWithGatewaySender() {
    Region<?, ?> region = server.getCache().getRegion("/REPLICATED");
    region.getAttributesMutator().addGatewaySenderId("1");
    gfsh.executeAndAssertThat("alter region --name=REPLICATED --gateway-sender-id='1,2'")
        .statusIsSuccess();
    assertThat(region.getAttributes().getGatewaySenderIds()).containsExactly("1", "2");

    gfsh.executeAndAssertThat("alter region --name=REPLICATED --gateway-sender-id=''")
        .statusIsSuccess();
    assertThat(region.getAttributes().getGatewaySenderIds()).isNotNull().isEmpty();
  }

  @Test
  public void alterRegionWithAsyncEventQueue() {
    Region<?, ?> region = server.getCache().getRegion("/REPLICATED");
    region.getAttributesMutator().addAsyncEventQueueId("1");
    gfsh.executeAndAssertThat("alter region --name=REPLICATED --async-event-queue-id='1,2'")
        .statusIsSuccess();
    assertThat(region.getAttributes().getAsyncEventQueueIds()).containsExactly("1", "2");

    gfsh.executeAndAssertThat("alter region --name=REPLICATED --async-event-queue-id=''")
        .statusIsSuccess();
    assertThat(region.getAttributes().getAsyncEventQueueIds()).isNotNull().isEmpty();
  }
}
