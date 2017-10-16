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

import static org.apache.geode.management.internal.cli.commands.DataCommandsUtils.getRegionAssociatedMembers;
import static org.apache.geode.management.internal.cli.commands.RemoveCommand.REGION_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;

@Category(DistributedTest.class)
public class RemoveCommandDUnitTest implements Serializable {
  private static final String REPLICATE_REGION_NAME = "replicateRegion";
  private static final String PARTITIONED_REGION_NAME = "partitionedRegion";
  private static final String EMPTY_STRING = "";

  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public transient GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  private transient MemberVM locator;
  private transient MemberVM server1;
  private transient MemberVM server2;

  @Before
  public void setup() throws Exception {
    locator = locatorServerStartupRule.startLocatorVM(0);

    server1 = locatorServerStartupRule.startServerVM(1, locator.getPort());
    server2 = locatorServerStartupRule.startServerVM(2, locator.getPort());

    server1.invoke(this::populateTestRegions);
    server2.invoke(this::populateTestRegions);

    gfsh.connectAndVerify(locator);

    Awaitility.await().atMost(2, TimeUnit.MINUTES).until(() -> locator.getVM()
        .invoke((SerializableCallableIF<Boolean>) this::regionMBeansAreInitialized));
  }

  private void populateTestRegions() {
    Cache cache = CacheFactory.getAnyInstance();
    Region<String, String> replicateRegion =
        cache.<String, String>createRegionFactory(RegionShortcut.REPLICATE)
            .create(REPLICATE_REGION_NAME);
    Region<String, String> partitionedRegion =
        cache.<String, String>createRegionFactory(RegionShortcut.PARTITION)
            .create(PARTITIONED_REGION_NAME);

    replicateRegion.put(EMPTY_STRING, "valueForEmptyKey");
    replicateRegion.put("key1", "value1");
    replicateRegion.put("key2", "value2");

    partitionedRegion.put("key1", "value1");
    partitionedRegion.put("key2", "value2");
  }

  @Test
  public void removeFromInvalidRegion() throws Exception {
    String command = "remove --all --region=NotAValidRegion";

    gfsh.executeAndVerifyCommandError(command, String.format(REGION_NOT_FOUND, "/NotAValidRegion"));
  }

  @Test
  public void removeWithNoKeyOrAllSpecified() throws Exception {
    String command = "remove --region=" + REPLICATE_REGION_NAME;

    gfsh.executeAndVerifyCommandError(command);
    assertThat(gfsh.getGfshOutput()).contains("Key is Null");
  }

  @Test
  public void removeKeyFromReplicateRegion() {
    String command = "remove --key=key1 --region=" + REPLICATE_REGION_NAME;

    gfsh.executeAndVerifyCommand(command);

    String output = gfsh.getGfshOutput();
    assertThat(output).containsPattern("Result\\s+:\\s+true");
    assertThat(output).containsPattern("Key Class\\s+:\\s+java.lang.String");
    assertThat(output).containsPattern("Key\\s+:\\s+key1");

    server1.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, "key1"));
    server2.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, "key1"));

    server1.invoke(() -> verifyKeyIsPresent(REPLICATE_REGION_NAME, "key2"));
    server2.invoke(() -> verifyKeyIsPresent(REPLICATE_REGION_NAME, "key2"));
  }

  @Test
  public void removeKeyFromPartitionedRegion() {
    String command = "remove --key=key1 --region=" + PARTITIONED_REGION_NAME;

    gfsh.executeAndVerifyCommand(command);

    String output = gfsh.getGfshOutput();
    assertThat(output).containsPattern("Result\\s+:\\s+true");
    assertThat(output).containsPattern("Key Class\\s+:\\s+java.lang.String");
    assertThat(output).containsPattern("Key\\s+:\\s+key1");

    server1.invoke(() -> verifyKeyIsRemoved(PARTITIONED_REGION_NAME, "key1"));
    server2.invoke(() -> verifyKeyIsRemoved(PARTITIONED_REGION_NAME, "key1"));

    server1.invoke(() -> verifyKeyIsPresent(PARTITIONED_REGION_NAME, "key2"));
    server2.invoke(() -> verifyKeyIsPresent(PARTITIONED_REGION_NAME, "key2"));
  }

  @Test
  public void removeAllFromReplicateRegion() {
    String command = "remove --all --region=" + REPLICATE_REGION_NAME;

    gfsh.executeAndVerifyCommand("list regions");
    gfsh.executeAndVerifyCommand(command);

    assertThat(gfsh.getGfshOutput()).contains("Cleared all keys in the region");

    server1.invoke(() -> verifyAllKeysAreRemoved(REPLICATE_REGION_NAME));
    server2.invoke(() -> verifyAllKeysAreRemoved(REPLICATE_REGION_NAME));
  }


  @Test
  public void removeAllFromPartitionedRegion() {
    String command = "remove --all --region=" + PARTITIONED_REGION_NAME;

    // Maybe this should return an "error" status, but the current behavior is status "OK"
    gfsh.executeAndVerifyCommand(command);

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
    gfsh.executeAndVerifyCommand(command);

    server1.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, EMPTY_STRING));
    server2.invoke(() -> verifyKeyIsRemoved(REPLICATE_REGION_NAME, EMPTY_STRING));
  }

  private boolean regionMBeansAreInitialized() {
    Set<DistributedMember> members = getRegionAssociatedMembers(REPLICATE_REGION_NAME,
        (InternalCache) CacheFactory.getAnyInstance(), false);

    return CollectionUtils.isNotEmpty(members);
  }

  private void verifyAllKeysAreRemoved(String regionName) {
    Region region = getRegion(regionName);
    assertThat(region.size()).isEqualTo(0);
  }

  private void verifyKeyIsRemoved(String regionName, String key) {
    Region region = getRegion(regionName);
    assertThat(region.get(key)).isNull();
  }

  private void verifyKeyIsPresent(String regionName, String key) {
    Region region = getRegion(regionName);
    assertThat(region.get(key)).isNotNull();
  }

  private Region getRegion(String regionName) {
    return CacheFactory.getAnyInstance().getRegion(regionName);
  }
}
