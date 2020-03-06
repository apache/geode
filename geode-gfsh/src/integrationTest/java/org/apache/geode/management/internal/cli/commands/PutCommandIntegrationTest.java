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

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.dto.Car;
import org.apache.geode.management.internal.cli.dto.Key;
import org.apache.geode.management.internal.cli.dto.Key1;
import org.apache.geode.management.internal.cli.dto.ObjectWithCharAttr;
import org.apache.geode.management.internal.cli.dto.Value;
import org.apache.geode.management.internal.cli.dto.Value2;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


@Category({GfshTest.class})
public class PutCommandIntegrationTest {

  private static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "testRegion");

  private static GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @ClassRule
  public static RuleChain chain = RuleChain.outerRule(server).around(gfsh);


  @After
  public void after() {
    // clear the region after each test
    server.getCache().getRegion("testRegion").clear();
  }

  @Test
  public void putWithoutSlash() {
    gfsh.executeAndAssertThat("put --region=testRegion --key=key1 --value=value1")
        .statusIsSuccess();
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value1");
  }


  @Test
  @SuppressWarnings("deprecation")
  public void putWithSlash() {
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value1")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void putIfNotExists() {
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value1")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value1");

    // if unspecified if-not-exits=false, so override
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value2")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value2");

    // if specified then true, so skip it
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value3 --if-not-exists")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value2");

    // if specified and set to false then honor it
    gfsh.executeAndAssertThat(
        "put --region=/testRegion --key=key1 --value=value3 --if-not-exists=false")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value3");
  }

  @Test
  @SuppressWarnings("deprecation")
  // Bug : 51587 : GFSH command failing when ; is present in either key or value in put operation
  public void putWithSemicolon() {
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1;key1 --value=value1;value1")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1;key1"))
        .isEqualTo("value1;value1");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void putIfAbsent() {
    // skip-if-exists is deprecated.
    gfsh.executeAndAssertThat("help put").statusIsSuccess()
        .containsOutput("(Deprecated: Use --if-not-exists).");

    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value1")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true");

    // if unspecified then override
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value2")
        .statusIsSuccess();
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value2");

    // if specified then don't override
    gfsh.executeAndAssertThat("put --region=/testRegion --key=key1 --value=value3 --skip-if-exists")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value2");

    // if specified and set to false then honor it
    gfsh.executeAndAssertThat(
        "put --region=/testRegion --key=key1 --value=value3 --skip-if-exists=false")
        .statusIsSuccess().containsKeyValuePair("Result", "true");
    assertThat(server.getCache().getRegion("testRegion").get("key1")).isEqualTo("value3");
  }

  @Test
  public void putWithSimpleJson() {
    gfsh.executeAndAssertThat(
        "put --region=testRegion --key=('key':'1') --value=('value':'1') " + "--key-class="
            + Key.class.getCanonicalName() + " --value-class=" + Value.class.getCanonicalName())
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true");
  }

  @Test
  public void putWithCorrectJsonSyntax() {
    gfsh.executeAndAssertThat(
        "put --region=testRegion --key={\"key\":\"1\"} --value={\"value\":\"1\"} " + "--key-class="
            + Key.class.getCanonicalName() + " --value-class=" + Value.class.getCanonicalName())
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true");
  }

  @Test
  public void putWithInvalidJson() {
    gfsh.executeAndAssertThat(
        "put --region=testRegion --key=('key':'1') --value=(value:2) " + "--key-class="
            + Key.class.getCanonicalName() + " --value-class=" + Value.class.getCanonicalName())
        .statusIsError().containsOutput("Failed to convert input key");
  }

  @Test
  public void putWithComplicatedJson() {
    String keyJson = "('id':'1','name':'name1')";
    String stateJson =
        "('stateName':'State1','population':10,'capitalCity':'capital1','areaInSqKm':100)";
    String carJson =
        "\"('attributes':?map,'make':'make1','model':'modle1','colors':?list,'attributeSet':?set)\"";

    // put the state json
    String command =
        "put --region=testRegion --key=" + keyJson + " --value=" + stateJson + " --key-class="
            + Key1.class.getCanonicalName() + " --value-class=" + Value2.class.getCanonicalName();
    gfsh.executeAndAssertThat(command)
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true");

    // put the car json
    String list = "['red','white','blue']";
    String set = "['red','white','blue']";
    String map = "{'power':'90hp'}";
    String valueJson = carJson.replaceAll("\\?list", list);
    valueJson = valueJson.replaceAll("\\?set", set);
    valueJson = valueJson.replaceAll("\\?map", map);
    command = "put --region=testRegion --key=" + keyJson + " --value=" + valueJson + " --key-class="
        + Key1.class.getCanonicalName() + " --value-class=" + Car.class.getCanonicalName();
    gfsh.executeAndAssertThat(command)
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true");

    // put with json with single character field
    command = "put --region=testRegion --key-class=" + ObjectWithCharAttr.class.getCanonicalName()
        + " --value=456 --key=('name':'hesdfdsfy2','t':456,'c':'d')";
    gfsh.executeAndAssertThat(command)
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true");
  }
}
