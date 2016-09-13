/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.cache.*;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.ManagerMXBean;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultData;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

/**
 * The GetCommandOnRegionWithCacheLoaderDuringCacheMissDUnitTest class is test suite of test cases testing the Gfsh
 * 'get' data command when a cache miss occurs on data in a Region with a CacheLoader defined.
 *
 * @see org.apache.geode.management.internal.cli.commands.CliCommandTestBase
 * @see org.apache.geode.management.internal.cli.commands.DataCommands
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
@Category(DistributedTest.class)
public class GetCommandOnRegionWithCacheLoaderDuringCacheMissDUnitTest extends CliCommandTestBase {

  private static final String GEMFIRE_MANAGER_NAME = "GemManagerNode";
  private static final String GEMFIRE_SERVER_NAME = "GemServerDataNode";
  private static final String GEMFIRE_LOG_LEVEL = System.getProperty("logLevel", "config");
  private static final String USERS_REGION_NAME = "Users";

  @Override
  public final void postSetUpCliCommandTestBase() throws Exception {
    Properties managerDistributedSystemProperties = createDistributedSystemProperties(GEMFIRE_MANAGER_NAME);
    HeadlessGfsh gfsh = setUpJmxManagerOnVm0ThenConnect(managerDistributedSystemProperties); // vm 0 -- locator/manager

    assertNotNull(gfsh); // controller vm -- gfsh
    assertTrue(gfsh.isConnectedAndReady());

    setupGemFire(); // vm 1 -- server
    verifyGemFireSetup(createPeer(getHost(0).getVM(0), managerDistributedSystemProperties));
  }

  @Test
  public void testGetOnCacheMiss() {
    doHousekeeping();

    CommandStringBuilder command = new CommandStringBuilder(CliStrings.GET);
    command.addOption(CliStrings.GET__REGIONNAME, USERS_REGION_NAME);
    command.addOption(CliStrings.GET__KEY, "jonbloom");

    assertResult(true, runCommand(command.toString()));

    command = new CommandStringBuilder(CliStrings.GET);
    command.addOption(CliStrings.GET__REGIONNAME, USERS_REGION_NAME);
    command.addOption(CliStrings.GET__KEY, "jondoe");
    command.addOption(CliStrings.GET__LOAD, "false");

    assertResult(false, runCommand(command.toString()));

    command = new CommandStringBuilder(CliStrings.GET);
    command.addOption(CliStrings.GET__REGIONNAME, USERS_REGION_NAME);
    command.addOption(CliStrings.GET__KEY, "jondoe");
    command.addOption(CliStrings.GET__LOAD, "true");

    assertResult(true, runCommand(command.toString()));

    // NOTE test the unspecified default value for the --load-on-cache-miss
    command = new CommandStringBuilder(CliStrings.GET);
    command.addOption(CliStrings.GET__REGIONNAME, USERS_REGION_NAME);
    command.addOption(CliStrings.GET__KEY, "janedoe");

    assertResult(true, runCommand(command.toString()));

    // NOTE now test an absolute cache miss both for in the Region as well as the CacheLoader
    command = new CommandStringBuilder(CliStrings.GET);
    command.addOption(CliStrings.GET__REGIONNAME, USERS_REGION_NAME);
    command.addOption(CliStrings.GET__KEY, "nonexistinguser");
    command.addOption(CliStrings.GET__LOAD, "true");

    assertResult(false, runCommand(command.toString()));
  }

  private static String getRegionPath(final String regionName) {
    return (regionName.startsWith(Region.SEPARATOR) ? regionName : String.format("%1$s%2$s", Region.SEPARATOR, regionName));
  }

  private static String toString(final Result result) {
    assert result != null : "The Result object from the command execution was null!";

    StringBuilder buffer = new StringBuilder(System.getProperty("line.separator"));

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(System.getProperty("line.separator"));
    }

    return buffer.toString();
  }

  private void setupGemFire() throws Exception {
    initializePeer(createPeer(getHost(0).getVM(1), createDistributedSystemProperties(GEMFIRE_SERVER_NAME)));
  }

  private Properties createDistributedSystemProperties(final String gemfireName) {
    Properties distributedSystemProperties = new Properties();

    distributedSystemProperties.setProperty(LOG_LEVEL, GEMFIRE_LOG_LEVEL);
    distributedSystemProperties.setProperty(NAME, gemfireName);

    return distributedSystemProperties;
  }

  private Peer createPeer(final VM vm, final Properties distributedSystemProperties) {
    return new Peer(vm, distributedSystemProperties);
  }

  private void initializePeer(final Peer peer) throws Exception {
    peer.run(new SerializableRunnable(String.format("Initializes the '%1$s' with the '%2$s' Region having a CacheLoader.", GEMFIRE_SERVER_NAME, USERS_REGION_NAME)) {
      @Override
      public void run() {
        // create the GemFire Distributed System with custom distribution configuration properties and settings
        getSystem(peer.getConfiguration());

        Cache cache = getCache();
        RegionFactory<String, User> regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);

        regionFactory.setCacheLoader(new UserDataStoreCacheLoader());
        regionFactory.setInitialCapacity(51);
        regionFactory.setKeyConstraint(String.class);
        regionFactory.setLoadFactor(0.75f);
        regionFactory.setStatisticsEnabled(false);
        regionFactory.setValueConstraint(User.class);

        Region<String, User> users = regionFactory.create(USERS_REGION_NAME);

        assertNotNull(users);
        assertEquals("Users", users.getName());
        assertEquals("/Users", users.getFullPath());
        assertTrue(users.isEmpty());
        assertNull(users.put("jonbloom", new User("jonbloom")));
        assertFalse(users.isEmpty());
        assertEquals(1, users.size());
        assertEquals(new User("jonbloom"), users.get("jonbloom"));
      }
    });
  }

  private void verifyGemFireSetup(final Peer manager) throws Exception {
    manager.run(new SerializableRunnable("Verifies the GemFire Cluster was properly configured and initialized!") {
      @Override
      public void run() {
        final ManagementService managementService = ManagementService.getExistingManagementService(getCache());

        WaitCriterion waitOnManagerCriterion = new WaitCriterion() {
          @Override
          public boolean done() {
            ManagerMXBean managerBean = managementService.getManagerMXBean();
            DistributedRegionMXBean usersRegionBean = managementService.getDistributedRegionMXBean(getRegionPath(USERS_REGION_NAME));

            return !(managerBean == null || usersRegionBean == null);
          }

          @Override
          public String description() {
            return String.format("Probing for the GemFire Manager '%1$s' and '%2$s' Region MXBeans...", manager.getName(), USERS_REGION_NAME);
          }
        };

        waitForCriterion(waitOnManagerCriterion, 30000, 2000, true);
      }
    });
  }

  private void doHousekeeping() {
    runCommand(CliStrings.LIST_MEMBER);

    runCommand(new CommandStringBuilder(CliStrings.DESCRIBE_MEMBER).addOption(CliStrings.DESCRIBE_MEMBER__IDENTIFIER,
        GEMFIRE_SERVER_NAME).toString());

    runCommand(CliStrings.LIST_REGION);

    runCommand(new CommandStringBuilder(CliStrings.DESCRIBE_REGION).addOption(CliStrings.DESCRIBE_REGION__NAME,
        USERS_REGION_NAME).toString());
  }

  private void log(final Result result) {
    log("Result", toString(result));
  }

  private void log(final String tag, final String message) {
    //System.out.printf("%1$s (%2$s)%n", tag, message);
    getLogWriter().info(String.format("%1$s (%2$s)%n", tag, message));
  }

  private CommandResult runCommand(final String command) {
    CommandResult result = executeCommand(command);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    log(result);

    return result;
  }

  private void assertResult(final boolean expectedResult, final CommandResult commandResult) {
    if (ResultData.TYPE_COMPOSITE.equals(commandResult.getType())) {
      boolean actualResult = (Boolean) ((CompositeResultData) commandResult.getResultData()).retrieveSectionByIndex(0).retrieveObject("Result");
      assertEquals(expectedResult, actualResult);
    } else {
      fail(String.format("Expected composite result data; but was '%1$s'!%n", commandResult.getType()));
    }
  }

  private static final class Peer implements Serializable {

    private final Properties distributedSystemProperties;
    private final VM vm;

    public Peer(final VM vm, final Properties distributedSystemProperties) {
      assert distributedSystemProperties != null : "The GemFire Distributed System configuration properties and settings cannot be null!";
      this.vm = vm;
      this.distributedSystemProperties = distributedSystemProperties;
    }

    public Properties getConfiguration() {
      return this.distributedSystemProperties;
    }

    public String getName() {
      return getConfiguration().getProperty(NAME);
    }

    public VM getVm() {
      return vm;
    }

    public void run(final SerializableRunnableIF runnable) throws Exception {
      if (getVm() == null) {
        runnable.run();
      } else {
        getVm().invoke(runnable);
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder(getClass().getSimpleName());

      buffer.append(" {configuration = ").append(getConfiguration());
      buffer.append(", name = ").append(getName());
      buffer.append(", pid = ").append(getVm().getPid());
      buffer.append("}");

      return buffer.toString();
    }
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

    @Override
    public int hashCode() {
      int hashValue = 17;
      hashValue = 37 * hashValue + getUsername().hashCode();
      return hashValue;
    }

    @Override
    public String toString() {
      return getUsername();
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
