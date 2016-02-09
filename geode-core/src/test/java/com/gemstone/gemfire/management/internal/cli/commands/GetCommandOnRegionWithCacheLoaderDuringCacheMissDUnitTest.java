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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.DistributedRegionMXBean;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.ManagerMXBean;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultData;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.SerializableRunnableIF;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

/**
 * The GetCommandOnRegionWithCacheLoaderDuringCacheMissDUnitTest class is test suite of test cases testing the Gfsh
 * 'get' data command when a cache miss occurs on data in a Region with a CacheLoader defined.
 * <p>
 *
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase
 * @see com.gemstone.gemfire.management.internal.cli.commands.DataCommands
 * @since 8.0
 */
@SuppressWarnings("unused")
public class GetCommandOnRegionWithCacheLoaderDuringCacheMissDUnitTest extends CliCommandTestBase {

  private static final String GEMFIRE_MANAGER_NAME = "GemManagerNode";
  private static final String GEMFIRE_SERVER_NAME = "GemServerDataNode";
  private static final String GEMFIRE_LOG_LEVEL = System.getProperty("logLevel", "config");
  private static final String USERS_REGION_NAME = "Users";

  protected static String getRegionPath(final String regionName) {
    return (regionName.startsWith(Region.SEPARATOR) ? regionName : String.format("%1$s%2$s", Region.SEPARATOR,
        regionName));
  }

  protected static String toString(final Result result) {
    assert result != null : "The Result object from the command execution was null!";

    StringBuilder buffer = new StringBuilder(System.getProperty("line.separator"));

    while (result.hasNextLine()) {
      buffer.append(result.nextLine());
      buffer.append(System.getProperty("line.separator"));
    }

    return buffer.toString();
  }

  public GetCommandOnRegionWithCacheLoaderDuringCacheMissDUnitTest(final String testName) {
    super(testName);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    Properties managerDistributedSystemProperties = createDistributedSystemProperties(GEMFIRE_MANAGER_NAME);
    HeadlessGfsh gfsh = createDefaultSetup(managerDistributedSystemProperties);

    assertNotNull(gfsh);
    assertTrue(gfsh.isConnectedAndReady());

    setupGemFire();
    verifyGemFireSetup(createPeer(Host.getHost(0).getVM(0), managerDistributedSystemProperties));
  }

  private void setupGemFire() {
    initializePeer(createPeer(Host.getHost(0).getVM(1), createDistributedSystemProperties(GEMFIRE_SERVER_NAME)));
  }

  protected Properties createDistributedSystemProperties(final String gemfireName) {
    Properties distributedSystemProperties = new Properties();

    distributedSystemProperties.setProperty(DistributionConfig.LOG_LEVEL_NAME, GEMFIRE_LOG_LEVEL);
    distributedSystemProperties.setProperty(DistributionConfig.NAME_NAME, gemfireName);

    return distributedSystemProperties;
  }

  protected Peer createPeer(final VM vm, final Properties distributedSystemProperties) {
    return new Peer(vm, distributedSystemProperties);
  }

  protected void initializePeer(final Peer peer) {
    peer.run(new SerializableRunnable(
        String.format("Initializes the '%1$s' with the '%2$s' Region having a CacheLoader.", GEMFIRE_SERVER_NAME,
            USERS_REGION_NAME)) {
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

  private void verifyGemFireSetup(final Peer manager) {
    manager.run(new SerializableRunnable("Verifies the GemFire Cluster was properly configured and initialized!") {
      @Override
      public void run() {
        final ManagementService managementService = ManagementService.getExistingManagementService(getCache());

        WaitCriterion waitOnManagerCriterion = new WaitCriterion() {
          @Override
          public boolean done() {
            ManagerMXBean managerBean = managementService.getManagerMXBean();
            DistributedRegionMXBean usersRegionBean = managementService.getDistributedRegionMXBean(
                getRegionPath(USERS_REGION_NAME));

            return !(managerBean == null || usersRegionBean == null);
          }

          @Override
          public String description() {
            return String.format("Probing for the GemFire Manager '%1$s' and '%2$s' Region MXBeans...",
                manager.getName(), USERS_REGION_NAME);
          }
        };

        Wait.waitForCriterion(waitOnManagerCriterion, 30000, 2000, true);
      }
    });
  }

  protected void doHousekeeping() {
    runCommand(CliStrings.LIST_MEMBER);

    runCommand(new CommandStringBuilder(CliStrings.DESCRIBE_MEMBER).addOption(CliStrings.DESCRIBE_MEMBER__IDENTIFIER,
        GEMFIRE_SERVER_NAME).toString());

    runCommand(CliStrings.LIST_REGION);

    runCommand(new CommandStringBuilder(CliStrings.DESCRIBE_REGION).addOption(CliStrings.DESCRIBE_REGION__NAME,
        USERS_REGION_NAME).toString());
  }

  protected void log(final Result result) {
    log("Result", toString(result));
  }

  protected void log(final String tag, final String message) {
    //System.out.printf("%1$s (%2$s)%n", tag, message);
    LogWriterUtils.getLogWriter().info(String.format("%1$s (%2$s)%n", tag, message));
  }

  protected CommandResult runCommand(final String command) {
    CommandResult result = executeCommand(command);

    assertNotNull(result);
    assertEquals(Result.Status.OK, result.getStatus());

    log(result);

    return result;
  }

  protected void assertResult(final boolean expectedResult, final CommandResult commandResult) {
    if (ResultData.TYPE_COMPOSITE.equals(commandResult.getType())) {
      boolean actualResult = (Boolean) ((CompositeResultData) commandResult.getResultData()).retrieveSectionByIndex(
          0).retrieveObject("Result");
      assertEquals(expectedResult, actualResult);
    } else {
      fail(String.format("Expected composite result data; but was '%1$s'!%n", commandResult.getType()));
    }
  }

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

  protected static final class Peer implements Serializable {

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
      return getConfiguration().getProperty(DistributionConfig.NAME_NAME);
    }

    public VM getVm() {
      return vm;
    }

    public void run(final SerializableRunnableIF runnable) {
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

  protected static class User implements Serializable {

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

  protected static class UserDataStoreCacheLoader implements CacheLoader<String, User>, Serializable {

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
