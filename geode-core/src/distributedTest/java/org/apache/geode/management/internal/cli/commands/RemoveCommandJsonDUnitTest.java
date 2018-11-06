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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.dto.Key1;
import org.apache.geode.management.internal.cli.dto.Value2;
import org.apache.geode.test.dunit.SerializableCallableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class RemoveCommandJsonDUnitTest implements Serializable {
  private static final String JSON_REGION_NAME = "jsonReplicateRegion";

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private transient MemberVM locator;
  private transient MemberVM server1;
  private transient MemberVM server2;

  @Before
  public void setup() throws Exception {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.management.internal.cli.dto.*");

    locator = clusterStartupRule.startLocatorVM(0);

    server1 = clusterStartupRule.startServerVM(1, props, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, props, locator.getPort());

    server1.invoke(this::populateTestRegions);
    server2.invoke(this::populateTestRegions);

    gfsh.connectAndVerify(locator);

    await().until(() -> locator.getVM()
        .invoke((SerializableCallableIF<Boolean>) this::regionMBeansAreInitialized));
  }

  private void populateTestRegions() {
    Cache cache = CacheFactory.getAnyInstance();

    Region<Key1, Value2> complexRegion =
        cache.<Key1, Value2>createRegionFactory(RegionShortcut.REPLICATE).create(JSON_REGION_NAME);

    Value2 value1 = new Value2();
    value1.setStateName("State1");
    value1.setCapitalCity("capital1");
    value1.setPopulation(100);
    value1.setAreaInSqKm(100.4365);
    complexRegion.put(key(1), value1);

    Value2 value2 = new Value2();
    value1.setStateName("State2");
    value1.setCapitalCity("capital2");
    value1.setPopulation(200);
    value1.setAreaInSqKm(200.4365);
    complexRegion.put(key(2), value2);
  }

  @Test
  public void testRemoveJsonCommand() {
    String keyJson = "('id':'key1','name':'name1')";

    String command = removeCommand(keyJson);
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifyKeyIsRemoved(1));
    server2.invoke(() -> verifyKeyIsRemoved(1));

    server1.invoke(() -> verifyKeyIsPresent(2));
    server2.invoke(() -> verifyKeyIsPresent(2));
  }

  @Test
  public void testRemoveJsonCommandWithOnlyId() {
    String keyJson = "('id':'key1')";

    String command = removeCommand(keyJson);
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifyKeyIsRemoved(1));
    server2.invoke(() -> verifyKeyIsRemoved(1));

    server1.invoke(() -> verifyKeyIsPresent(2));
    server2.invoke(() -> verifyKeyIsPresent(2));
  }

  @Test
  public void testRemoveJsonCommandWithInvalidJson() {
    String keyJson = "('foo':'bar')";

    String command = removeCommand(keyJson);
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1.invoke(() -> verifyKeyIsPresent(1));
    server2.invoke(() -> verifyKeyIsPresent(1));

    server1.invoke(() -> verifyKeyIsPresent(2));
    server2.invoke(() -> verifyKeyIsPresent(2));
  }

  private String removeCommand(String keyJson) {
    return "remove --key=" + keyJson + " --region=" + JSON_REGION_NAME + " --key-class="
        + Key1.class.getCanonicalName();
  }

  private Key1 key(int n) {
    Key1 key = new Key1();
    key.setId("key" + n);
    key.setName("name" + n);

    return key;
  }

  private boolean regionMBeansAreInitialized() {
    Set<DistributedMember> members = CliUtil.getRegionAssociatedMembers(JSON_REGION_NAME,
        (InternalCache) CacheFactory.getAnyInstance(), false);

    return CollectionUtils.isNotEmpty(members);
  }

  private void verifyKeyIsRemoved(int n) {
    Region region = getJsonRegion();
    assertThat(region.get(key(n))).isNull();
  }

  private void verifyKeyIsPresent(int n) {
    Region region = getJsonRegion();
    assertThat(region.get(key(n))).isNotNull();
  }

  private Region getJsonRegion() {
    return CacheFactory.getAnyInstance().getRegion(JSON_REGION_NAME);
  }
}
