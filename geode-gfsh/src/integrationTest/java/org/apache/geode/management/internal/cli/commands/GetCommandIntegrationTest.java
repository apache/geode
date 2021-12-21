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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.Serializable;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;
import org.apache.geode.pdx.internal.PdxInstanceFactoryImpl;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


public class GetCommandIntegrationTest {

  private static final Map<String, User> userDataStore = new HashMap<String, User>(5);

  static {
    userDataStore.put("jackhandy", new User("jackhandy"));
    userDataStore.put("janedoe", new User("janedoe"));
    userDataStore.put("jondoe", new User("jondoe"));
    userDataStore.put("piedoe", new User("piedoe"));
    userDataStore.put("supertool", new User("supertool"));
  }

  private static final ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withAutoStart();

  private static final GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @ClassRule
  public static RuleChain chain = RuleChain.outerRule(server).around(gfsh);

  @BeforeClass
  public static void beforeClass() throws Exception {
    InternalCache cache = server.getCache();

    // Region containing POJOs
    RegionFactory<String, User> userRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    userRegionFactory.setCacheLoader(new UserDataStoreCacheLoader());
    Region<String, User> users = userRegionFactory.create("Users");

    users.put("jonbloom", new User("jonbloom"));
    assertFalse(users.isEmpty());
    assertEquals(1, users.size());

    // Region containing PdxInstances
    RegionFactory<String, PdxInstance> userPdxRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    userPdxRegionFactory.setCacheLoader(new UserPdxDataStoreCacheLoader(cache));
    Region<String, PdxInstance> pdxRegion = userPdxRegionFactory.create("UsersPdx");
    pdxRegion.put("jonbloom", makePdxInstance(new User("jonbloom"), cache));

    // Region containing primitives
    RegionFactory<String, String> userStringRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    userStringRegionFactory.setCacheLoader(new UserStringDataStoreCacheLoader());
    Region<String, String> usersString = userStringRegionFactory.create("UsersString");

    usersString.put("jonbloom", "6a6f6e626c6f6f6d");
    assertFalse(usersString.isEmpty());
    assertEquals(1, usersString.size());
  }

  @Test
  public void get() throws Exception {
    gfsh.executeAndAssertThat("get --region=Users --key=jonbloom").statusIsSuccess();
  }

  @Test
  public void getWithSlashedRegionName() throws Exception {
    gfsh.executeAndAssertThat("get --region=" + SEPARATOR + "Users --key=jonbloom")
        .statusIsSuccess();
  }

  @Test
  public void getOnCacheMissForRegularRegion() throws Exception {
    gfsh.executeAndAssertThat("get --region=Users --key=jonbloom")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "true");

    gfsh.executeAndAssertThat("get --region=Users --key=jondoe --load-on-cache-miss=false")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "false")
        .containsEntry("Message", "Key is not present in the region");

    gfsh.executeAndAssertThat("get --region=Users --key=jondoe")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "true")
        .containsEntry("Value Class", User.class.getCanonicalName());

    // get something that does not exist
    gfsh.executeAndAssertThat("get --region=Users --key=missingUser --load-on-cache-miss")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION).hasContent()
        .containsEntry("Result", "false")
        .containsEntry("Value", "null");
  }

  @Test
  public void getOnCacheMissForPdxRegion() {
    gfsh.executeAndAssertThat("get --region=UsersPdx --key=jonbloom")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "true");

    gfsh.executeAndAssertThat("get --region=UsersPdx --key=jondoe --load-on-cache-miss=false")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "false")
        .containsEntry("Message", "Key is not present in the region");

    gfsh.executeAndAssertThat("get --region=UsersPdx --key=jondoe")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "true")
        .containsEntry("Value Class", PdxInstanceImpl.class.getCanonicalName());

    // get something that does not exist
    gfsh.executeAndAssertThat("get --region=UsersPdx --key=missingUser --load-on-cache-miss")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "false")
        .containsEntry("Value", "null");
  }

  @Test
  public void getOnCacheMissForStringRegion() throws Exception {
    gfsh.executeAndAssertThat("get --region=UsersString --key=jonbloom")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "true")
        .containsEntry("Value", "\"6a6f6e626c6f6f6d\"");

    gfsh.executeAndAssertThat("get --region=UsersString --key=jondoe --load-on-cache-miss=false")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "false")
        .containsEntry("Message", "Key is not present in the region")
        .containsEntry("Value", "null");

    gfsh.executeAndAssertThat("get --region=UsersString --key=jondoe")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "true")
        .containsEntry("Value Class", String.class.getName())
        .containsEntry("Value", "\"6a6f6e646f65\"");

    // get something that does not exist
    gfsh.executeAndAssertThat("get --region=UsersString --key=missingUser --load-on-cache-miss")
        .statusIsSuccess()
        .hasDataSection(DataCommandResult.DATA_INFO_SECTION)
        .hasContent()
        .containsEntry("Result", "false")
        .containsEntry("Value", "null");
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

    public String getHashcode() {
      StringBuilder sb = new StringBuilder(username.getBytes().length * 2);

      Formatter formatter = new Formatter(sb);
      for (byte b : username.getBytes()) {
        formatter.format("%02x", b);
      }

      return sb.toString();
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

      return getUsername().equals(that.getUsername());
    }
  }

  private static class UserDataStoreCacheLoader implements CacheLoader<String, User>, Serializable {
    @Override
    public User load(final LoaderHelper<String, User> helper) throws CacheLoaderException {
      return userDataStore.get(helper.getKey());
    }

    @Override
    public void close() {}
  }

  private static class UserPdxDataStoreCacheLoader
      implements CacheLoader<String, PdxInstance>, Serializable {

    private final InternalCache cache;

    UserPdxDataStoreCacheLoader(InternalCache cache) {
      this.cache = cache;
    }

    @Override
    public PdxInstance load(final LoaderHelper<String, PdxInstance> helper)
        throws CacheLoaderException {
      User user = userDataStore.get(helper.getKey());
      if (user == null) {
        return null;
      }

      return makePdxInstance(user, cache);
    }

    @Override
    public void close() {}
  }

  private static class UserStringDataStoreCacheLoader
      implements CacheLoader<String, String>, Serializable {

    @Override
    public String load(final LoaderHelper<String, String> helper) throws CacheLoaderException {
      User user = userDataStore.get(helper.getKey());
      if (user == null) {
        return null;
      }

      return user.getHashcode();
    }

    @Override
    public void close() {}
  }

  private static PdxInstance makePdxInstance(User user, InternalCache cache) {
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("User", false, cache);
    pf.writeString("username", user.getUsername());
    pf.writeString("hashcode", user.getHashcode());
    return pf.create();
  }
}
