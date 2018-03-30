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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


@Category({IntegrationTest.class, GfshTest.class})
public class GetCommandIntegrationTest {

  private static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withAutoStart();

  private static GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @ClassRule
  public static RuleChain chain = RuleChain.outerRule(server).around(gfsh);

  @BeforeClass
  public static void beforeClass() throws Exception {
    Cache cache = server.getCache();
    RegionFactory<String, User> regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);

    regionFactory.setCacheLoader(new UserDataStoreCacheLoader());
    regionFactory.setInitialCapacity(51);
    regionFactory.setKeyConstraint(String.class);
    regionFactory.setLoadFactor(0.75f);
    regionFactory.setStatisticsEnabled(false);
    regionFactory.setValueConstraint(User.class);

    Region<String, User> users = regionFactory.create("Users");
    assertThat(users.getName()).isEqualTo("Users");
    assertThat(users.getFullPath()).isEqualTo("/Users");

    users.put("jonbloom", new User("jonbloom"));
    assertFalse(users.isEmpty());
    assertEquals(1, users.size());
  }

  @Test
  public void get() throws Exception {
    gfsh.executeAndAssertThat("get --region=Users --key=jonbloom").statusIsSuccess();
  }

  @Test
  public void getWithSlashedRegionName() throws Exception {
    gfsh.executeAndAssertThat("get --region=/Users --key=jonbloom").statusIsSuccess();
  }

  @Test
  public void getOnCacheMiss() throws Exception {
    gfsh.executeAndAssertThat("get --region=Users --key=jonbloom").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Result      : true");
    gfsh.executeAndAssertThat("get --region=Users --key=jondoe --load-on-cache-miss=false")
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Result      : false");

    gfsh.executeAndAssertThat("get --region=Users --key=jondoe").statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Result      : true");

    // get something that does not exist
    gfsh.executeAndAssertThat("get --region=Users --key=missingUser --load-on-cache-miss")
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains("Result      : false");
  }

  private static class User implements Serializable {
    private final String username;

    public User(final String username) {
      assert username != null : "The username cannot be null!";
      this.username = username;
    }

    public String getUsername() {
      return username;
    }

    @Override
    public boolean equals(final Object obj) {
      if (obj == this) {
        return true;
      }

      if (!(obj instanceof User)) {
        return false;
      }

      User that = (User) obj;

      return this.getUsername().equals(that.getUsername());
    }
  }

  private static class UserDataStoreCacheLoader implements CacheLoader<String, User>, Serializable {

    private static final Map<String, User> userDataStore = new HashMap<String, User>(5);

    static {
      userDataStore.put("jackhandy", createUser("jackhandy"));
      userDataStore.put("janedoe", createUser("janedoe"));
      userDataStore.put("jondoe", createUser("jondoe"));
      userDataStore.put("piedoe", createUser("piedoe"));
      userDataStore.put("supertool", createUser("supertool"));
    }

    protected static User createUser(final String username) {
      return new User(username);
    }

    @Override
    public User load(final LoaderHelper<String, User> helper) throws CacheLoaderException {
      return userDataStore.get(helper.getKey());
    }

    @Override
    public void close() {
      userDataStore.clear();
    }
  }
}
