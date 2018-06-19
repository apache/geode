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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MAX_WAIT_TIME_RECONNECT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang.ArrayUtils;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * the abstract class that's used by LocatorStarterRule and ServerStarterRule to avoid code
 * duplication.
 */
public abstract class MemberStarterRule<T> extends SerializableExternalResource implements Member {

  protected String oldUserDir;

  protected transient TemporaryFolder temporaryFolder;
  protected File workingDir;
  protected int memberPort = 0;
  protected int jmxPort = -1;
  protected int httpPort = -1;

  protected String name;
  protected boolean logFile = false;
  protected Properties properties = new Properties();

  protected boolean autoStart = false;

  public MemberStarterRule() {
    oldUserDir = System.getProperty("user.dir");

    // initial values
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    // set the reconnect wait time to 5 seconds in case some tests needs to reconnect in a timely
    // manner.
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "5000");
  }

  @Override
  public void after() {
    // invoke stopMember() first and then ds.disconnect
    stopMember();

    // this will clean up the SocketCreators created in this VM so that it won't contaminate
    // future tests
    SocketCreatorFactory.close();
    disconnectDSIfAny();

    if (temporaryFolder != null) {
      temporaryFolder.delete();
    }

    if (oldUserDir == null) {
      System.clearProperty("user.dir");
    } else {
      System.setProperty("user.dir", oldUserDir);
    }
  }

  public T withPort(int memberPort) {
    this.memberPort = memberPort;
    return (T) this;
  }

  public T withWorkingDir(File workingDir) {
    this.workingDir = workingDir;
    if (workingDir != null) {
      System.setProperty("user.dir", workingDir.toString());
    }
    return (T) this;
  }

  /**
   * create a working dir using temporaryFolder. Use with caution, this sets "user.dir" system
   * property that not approved by JDK
   */
  public T withWorkingDir() {
    temporaryFolder = new TemporaryFolder();
    try {
      temporaryFolder.create();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    withWorkingDir(temporaryFolder.getRoot().getAbsoluteFile());
    return (T) this;
  }

  /**
   * All the logs are written in the logfile instead on the console. this is usually used with
   * withWorkingDir so that logs are accessible and will be cleaned up afterwards.
   */
  public T withLogFile() {
    this.logFile = true;
    return (T) this;
  }

  public static void disconnectDSIfAny() {
    DistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
    if (ds != null) {
      ds.disconnect();
    }
  }

  public T withProperty(String key, String value) {
    properties.setProperty(key, value);
    return (T) this;
  }

  public T withProperties(Properties props) {
    if (props != null) {
      this.properties.putAll(props);
    }
    return (T) this;
  }

  public T withSecurityManager(Class<? extends SecurityManager> securityManager) {
    properties.setProperty(SECURITY_MANAGER, securityManager.getName());
    return (T) this;
  }

  public T withAutoStart() {
    this.autoStart = true;
    return (T) this;
  }

  public T withName(String name) {
    this.name = name;
    properties.putIfAbsent(NAME, name);
    return (T) this;
  }

  public T withConnectionToLocator(int... locatorPorts) {
    if (locatorPorts.length == 0) {
      return (T) this;
    }
    String locators = Arrays.stream(locatorPorts).mapToObj(i -> "localhost[" + i + "]")
        .collect(Collectors.joining(","));
    properties.setProperty(LOCATORS, locators);
    return (T) this;
  }

  /**
   * be able to start JMX manager and admin rest on default ports
   */
  public T withJMXManager(boolean useProductDefaultPorts) {
    if (!useProductDefaultPorts) {
      // do no override these properties if already exists
      properties.putIfAbsent(JMX_MANAGER_PORT,
          AvailablePortHelper.getRandomAvailableTCPPort() + "");
      properties.putIfAbsent(HTTP_SERVICE_PORT,
          AvailablePortHelper.getRandomAvailableTCPPort() + "");
      this.jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      this.httpPort = Integer.parseInt(properties.getProperty(HTTP_SERVICE_PORT));
    } else {
      // the real port numbers will be set after we started the server/locator.
      this.jmxPort = 0;
      this.httpPort = 0;
    }
    properties.putIfAbsent(JMX_MANAGER, "true");
    properties.putIfAbsent(JMX_MANAGER_START, "true");
    properties.putIfAbsent(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    return (T) this;
  }

  /**
   * start the jmx manager and admin rest on a random ports
   */
  public T withJMXManager() {
    return withJMXManager(false);
  }

  protected void normalizeProperties() {
    // if name is set via property, not with API
    if (name == null) {
      if (properties.containsKey(NAME)) {
        name = properties.getProperty(NAME);
      } else {
        if (this instanceof ServerStarterRule) {
          name = "server";
        } else {
          name = "locator";
        }
      }
      withName(name);
    }

    // if jmxPort is set via property, not with API
    if (jmxPort < 0 && properties.containsKey(JMX_MANAGER_PORT)) {
      // this will make sure we have all the missing properties, but it won't override
      // the existing properties
      withJMXManager(false);
    }

    // if caller wants the logs being put into a file instead of in console output
    // do it here since only here, we can guarantee the name is present
    if (logFile) {
      properties.putIfAbsent(LOG_FILE, new File(name + ".log").getAbsolutePath());
    }
  }

  public DistributedRegionMXBean getRegionMBean(String regionName) {
    return getManagementService().getDistributedRegionMXBean(regionName);
  }

  public ManagementService getManagementService() {
    ManagementService managementService =
        ManagementService.getExistingManagementService(getCache());
    if (managementService == null) {
      throw new IllegalStateException("Management service is not available on this member");
    }
    return managementService;
  }

  public abstract InternalCache getCache();

  public void waitTillRegionIsReadyOnServers(String regionName, int serverCount) {
    await().atMost(30, TimeUnit.SECONDS).until(() -> getRegionMBean(regionName) != null);
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> getRegionMBean(regionName).getMembers().length == serverCount);
  }

  private long getDiskStoreCount(String diskStoreName) {
    DistributedSystemMXBean dsMXBean = getManagementService().getDistributedSystemMXBean();
    Map<String, String[]> diskstores = dsMXBean.listMemberDiskstore();
    long count =
        diskstores.values().stream().filter(x -> ArrayUtils.contains(x, diskStoreName)).count();

    return count;
  }

  public void waitTilGatewaySendersAreReady(int expectedGatewayObjectCount) throws Exception {
    DistributedSystemMXBean dsMXBean = getManagementService().getDistributedSystemMXBean();
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> assertThat(dsMXBean.listGatewaySenderObjectNames().length,
            is(expectedGatewayObjectCount)));
  }

  public void waitTillDiskStoreIsReady(String diskstoreName, int serverCount) {
    await().atMost(30, TimeUnit.SECONDS)
        .until(() -> getDiskStoreCount(diskstoreName) == serverCount);
  }

  public void waitTillAsyncEventQueuesAreReadyOnServers(String queueId, int serverCount) {
    await().atMost(30, TimeUnit.SECONDS).until(
        () -> CliUtil.getMembersWithAsyncEventQueue(getCache(), queueId).size() == serverCount);
  }

  abstract void stopMember();

  public void forceDisconnectMember() {
    MembershipManagerHelper
        .crashDistributedSystem(InternalDistributedSystem.getConnectedInstance());
  }

  @Override
  public File getWorkingDir() {
    return workingDir;
  }

  @Override
  public int getPort() {
    return memberPort;
  }

  @Override
  public int getJmxPort() {
    return jmxPort;
  }

  @Override
  public int getHttpPort() {
    return httpPort;
  }

  @Override
  public String getName() {
    return name;
  }
}
