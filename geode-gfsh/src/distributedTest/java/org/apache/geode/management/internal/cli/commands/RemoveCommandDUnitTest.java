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

import static org.apache.geode.management.internal.cli.commands.RemoveCommand.REGION_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;


public class RemoveCommandDUnitTest {
  private static final String REPLICATE_REGION_NAME = "replicateRegion";
  private static final String PARTITIONED_REGION_NAME = "partitionedRegion";
  private static final String EMPTY_STRING = "";

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM server1;
  private MemberVM server2;

  @Before
  public void setup() throws Exception {
    MemberVM locator = clusterStartupRule.startLocatorVM(0);
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create region --name=" + REPLICATE_REGION_NAME + " --type=REPLICATE")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create region --name=" + PARTITIONED_REGION_NAME + " --type=PARTITION").statusIsSuccess();

    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + REPLICATE_REGION_NAME, 2);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/" + PARTITIONED_REGION_NAME, 2);

    VMProvider.invokeInEveryMember(RemoveCommandDUnitTest::populateTestRegions, server1, server2);
  }

  private static void populateTestRegions() {
    Cache cache = CacheFactory.getAnyInstance();

    Region<String, String> replicateRegion = cache.getRegion(REPLICATE_REGION_NAME);
    replicateRegion.put(EMPTY_STRING, "valueForEmptyKey");
    replicateRegion.put("key1", "value1");
    replicateRegion.put("key2", "value2");

    Region<String, String> partitionedRegion = cache.getRegion(PARTITIONED_REGION_NAME);
    partitionedRegion.put("key1", "value1");
    partitionedRegion.put("key2", "value2");
  }

  @Test
  public void removeFromInvalidRegion() {
    String command = "remove --all --region=NotAValidRegion";

    gfsh.executeAndAssertThat(command).statusIsError()
        .containsOutput(String.format(REGION_NOT_FOUND, "/NotAValidRegion"));
  }

  @Test
  public void removeWithNoKeyOrAllSpecified() {
    String command = "remove --region=" + REPLICATE_REGION_NAME;

    gfsh.executeAndAssertThat(command).statusIsError().containsOutput("Key is Null");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void removeKeyFromReplicateRegion() {
    String command = "remove --key=key1 --region=" + REPLICATE_REGION_NAME;

    gfsh.executeAndAssertThat(command).statusIsSuccess().containsKeyValuePair("Result", "true")
        .containsKeyValuePair("Key Class", "java.lang.String").containsKeyValuePair("Key", "key1");

    server1.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, "key1"));
    server2.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, "key1"));

    server1.invoke(() -> verifyKeyIsPresent(REPLICATE_REGION_NAME, "key2"));
    server2.invoke(() -> verifyKeyIsPresent(REPLICATE_REGION_NAME, "key2"));
  }

  @Test
  @SuppressWarnings("deprecation")
  public void removeKeyFromPartitionedRegion() {
    String command = "remove --key=key1 --region=" + PARTITIONED_REGION_NAME;

    gfsh.executeAndAssertThat(command).statusIsSuccess().containsKeyValuePair("Result", "true")
        .containsKeyValuePair("Key Class", "java.lang.String").containsKeyValuePair("Key", "key1");

    server1.invoke(() -> verifyKeyIsRemoved(PARTITIONED_REGION_NAME, "key1"));
    server2.invoke(() -> verifyKeyIsRemoved(PARTITIONED_REGION_NAME, "key1"));

    server1.invoke(() -> verifyKeyIsPresent(PARTITIONED_REGION_NAME, "key2"));
    server2.invoke(() -> verifyKeyIsPresent(PARTITIONED_REGION_NAME, "key2"));
  }

  @Test
  public void removeAllFromReplicateRegion() {
    String command = "remove --all --region=" + REPLICATE_REGION_NAME;

    gfsh.executeAndAssertThat("list regions").statusIsSuccess();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertThat(gfsh.getGfshOutput()).contains("Cleared all keys in the region");

    server1.invoke(() -> verifyAllKeysAreRemoved(REPLICATE_REGION_NAME));
    server2.invoke(() -> verifyAllKeysAreRemoved(REPLICATE_REGION_NAME));
  }


  @Test
  public void removeAllFromPartitionedRegion() {
    String command = "remove --all --region=" + PARTITIONED_REGION_NAME;

    // Maybe this should return an "error" status, but the current behavior is status "OK"
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    assertThat(gfsh.getGfshOutput())
        .contains("Option --all is not supported on partitioned region");
  }

  /**
   * Test remove from a region with a zero-length string key (GEODE-2269)
   */
  @Test
  public void removeEmptyKey() {
    server1.invoke(() -> verifyKeyIsPresent(REPLICATE_REGION_NAME, EMPTY_STRING));
    server2.invoke(() -> verifyKeyIsPresent(REPLICATE_REGION_NAME, EMPTY_STRING));

    String command = "remove --key=\"\" --region=" + REPLICATE_REGION_NAME;
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, EMPTY_STRING));
    server2.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, EMPTY_STRING));
  }

  private static void verifyAllKeysAreRemoved(String regionName) {
    Region<?, ?> region = getRegion(regionName);
    assertThat(region.size()).isEqualTo(0);
  }

  private static void verifyKeyIsRemoved(String regionName, String key) {
    Region<?, ?> region = getRegion(regionName);
    assertThat(region.get(key)).isNull();
  }

  private static void verifyKeyIsPresent(String regionName, String key) {
    Region<?, ?> region = getRegion(regionName);
    assertThat(region.get(key)).isNotNull();
  }

  private static Region<?, ?> getRegion(String regionName) {
    return CacheFactory.getAnyInstance().getRegion(regionName);
  }
}
