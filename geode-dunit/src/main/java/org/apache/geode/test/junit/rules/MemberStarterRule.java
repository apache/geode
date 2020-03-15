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
import static org.apache.geode.management.internal.ManagementConstants.OBJECTNAME__CLIENTSERVICE_MXBEAN;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionTimeoutException;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper;
import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.management.CacheServerMXBean;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.security.SecurityManager;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.accessible.AccessibleRestoreSystemProperties;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

/**
 * the abstract class that's used by LocatorStarterRule and ServerStarterRule to avoid code
 * duplication.
 *
 * The rule will try to clean up the working dir as best as it can. Any first level children
 * created in the test will be cleaned up after the test.
 */
public abstract class MemberStarterRule<T> extends SerializableExternalResource implements Member {
  protected int memberPort = 0;
  protected int jmxPort = -1;
  protected int httpPort = -1;

  protected String name;
  protected boolean logFile = false;
  protected Properties properties = new Properties();
  protected Properties systemProperties = new Properties();

  protected boolean autoStart = false;
  private final transient UniquePortSupplier portSupplier;

  private List<File> firstLevelChildrenFile = new ArrayList<>();
  private boolean cleanWorkingDir = true;

  public static void setWaitUntilTimeout(int waitUntilTimeout) {
    WAIT_UNTIL_TIMEOUT = waitUntilTimeout;
  }

  private static int WAIT_UNTIL_TIMEOUT = 30;

  private AccessibleRestoreSystemProperties restore = new AccessibleRestoreSystemProperties();

  public MemberStarterRule() {
    this(new UniquePortSupplier());
  }

  public MemberStarterRule(UniquePortSupplier portSupplier) {
    this.portSupplier = portSupplier;

    // initial values
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(MAX_WAIT_TIME_RECONNECT, "5000");
  }

  @Override
  public void before() {
    try {
      restore.before();
    } catch (Throwable throwable) {
      throw new RuntimeException(throwable.getMessage(), throwable);
    }
    normalizeProperties();
    if (httpPort < 0) {
      // at this point, httpPort is not being configured by api, we assume they do not
      // want to start the http service.
      // use putIfAbsent if it was configured using withProperty
      properties.putIfAbsent(HTTP_SERVICE_PORT, "0");
    }
    firstLevelChildrenFile = Arrays.asList(getWorkingDir().listFiles());

    for (String key : systemProperties.stringPropertyNames()) {
      System.setProperty(key, systemProperties.getProperty(key));
    }
  }

  @Override
  public void after() {
    restore.after();
    // invoke stop() first and then ds.disconnect
    stopMember();

    disconnectDSIfAny();
    // this will clean up the SocketCreators created in this VM so that it won't contaminate
    // future tests
    SocketCreatorFactory.close();

    // This is required if PDX is in use and tests are run repeatedly.
    TypeRegistry.init();

    // delete the first-level children files that are created in the tests
    if (cleanWorkingDir)
      Arrays.stream(getWorkingDir().listFiles())
          // do not delete the pre-existing files
          .filter(f -> !firstLevelChildrenFile.contains(f))
          // do not delete the dunit folder that might have been created by dunit launcher
          .filter(f -> !(f.isDirectory() && f.getName().equals("dunit")))
          .forEach(FileUtils::deleteQuietly);
  }

  public T withPort(int memberPort) {
    this.memberPort = memberPort;
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

  public T withSystemProperty(String key, String value) {
    systemProperties.put(key, value);
    return (T) this;
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

  public T withSSL(String components, boolean requireAuth,
      boolean endPointIdentification) {
    Properties sslProps = getSSLProperties(components, requireAuth, endPointIdentification);
    properties.putAll(sslProps);
    return (T) this;
  }

  public static Properties getSSLProperties(String components, boolean requireAuth,
      boolean endPointIdentification) {
    CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertificateMaterial memberMaterial = new CertificateBuilder()
        .commonName("member")
        .issuedBy(ca)
        .generate();

    CertStores memberStore = new CertStores("member");
    memberStore.withCertificate("member", memberMaterial);
    memberStore.trust("ca", ca);

    try {
      return memberStore.propertiesWith(components, requireAuth, endPointIdentification);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public T withSecurityManager(Class<? extends SecurityManager> securityManager) {
    properties.setProperty(SECURITY_MANAGER, securityManager.getName());
    return (T) this;
  }

  public T withCredential(String username, String password) {
    properties.setProperty(UserPasswordAuthInit.USER_NAME, username);
    properties.setProperty(UserPasswordAuthInit.PASSWORD, password);
    return (T) this;
  }

  public T withAutoStart() {
    this.autoStart = true;
    return (T) this;
  }

  public T withName(String name) {
    // only if name is not defined yet
    if (!properties.containsKey(NAME)) {
      this.name = name;
      properties.putIfAbsent(NAME, name);
    }
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
          portSupplier.getAvailablePort() + "");
      this.jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
    } else {
      // the real port numbers will be set after we started the server/locator.
      this.jmxPort = 0;
    }
    properties.putIfAbsent(JMX_MANAGER, "true");
    properties.putIfAbsent(JMX_MANAGER_START, "true");
    return (T) this;
  }

  public T withHttpService(boolean useDefaultPort) {
    properties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");
    if (!useDefaultPort) {
      properties.put(HTTP_SERVICE_PORT,
          portSupplier.getAvailablePort() + "");
      this.httpPort = Integer.parseInt(properties.getProperty(HTTP_SERVICE_PORT));
    } else {
      // indicate start http service but with default port
      // (different from Gemfire properties, 0 means do not start http service)
      httpPort = 0;
    }
    return (T) this;
  }

  public void setCleanWorkingDir(boolean cleanWorkingDir) {
    this.cleanWorkingDir = cleanWorkingDir;
  }

  /**
   * start the jmx manager and admin rest on a random ports
   */
  public T withJMXManager() {
    return withJMXManager(false);
  }

  public T withHttpService() {
    return withHttpService(false);
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

  public void waitUntilRegionIsReadyOnExactlyThisManyServers(String regionName,
      int exactServerCount) throws Exception {
    if (exactServerCount == 0) {
      waitUntilEqual(
          () -> getRegionMBean(regionName),
          Objects::isNull,
          true,
          String.format("Expecting to not find an mbean for region '%s'", regionName),
          WAIT_UNTIL_TIMEOUT, TimeUnit.SECONDS);
      return;
    }
    // First wait until the region mbean is not null...
    waitUntilEqual(
        () -> getRegionMBean(regionName),
        Objects::nonNull,
        true,
        String.format("Expecting to find an mbean for region '%s'", regionName),
        WAIT_UNTIL_TIMEOUT, TimeUnit.SECONDS);

    // Now actually wait for the members to receive the region
    String assertionConditionDescription = String.format(
        "Expecting region '%s' to be found on exactly %d servers", regionName, exactServerCount);
    waitUntilSatisfied(
        () -> Arrays.asList(getRegionMBean(regionName).getMembers()),
        Function.identity(),
        members -> Assertions.assertThat(members).isNotNull().hasSize(exactServerCount),
        assertionConditionDescription,
        WAIT_UNTIL_TIMEOUT, TimeUnit.SECONDS);
  }

  public void waitTillClientsAreReadyOnServer(String serverName, int serverPort, int clientCount) {
    waitTillCacheServerIsReady(serverName, serverPort);
    CacheServerMXBean bean = getCacheServerMXBean(serverName, serverPort);
    await().until(() -> bean.getClientIds().length == clientCount);
  }

  /**
   * Invoked in serverVM
   */

  /**
   * convenience method to create a region with customized regionFactory
   *
   * @param regionFactoryConsumer a lamda that allows you to customize the regionFactory
   */
  public <K, V> Region<K, V> createRegion(RegionShortcut type, String name,
      Consumer<RegionFactory<K, V>> regionFactoryConsumer) {
    RegionFactory<K, V> regionFactory = getCache().createRegionFactory(type);
    regionFactoryConsumer.accept(regionFactory);
    return regionFactory.create(name);
  }

  public <K, V> Region<K, V> createRegion(RegionShortcut type, String name) {
    final RegionFactory<K, V> regionFactory = getCache().createRegionFactory(type);
    return regionFactory.create(name);
  }

  /**
   * convenience method to create a partition region with customized regionFactory and a customized
   * PartitionAttributeFactory
   *
   * @param regionFactoryConsumer a lamda that allows you to customize the regionFactory
   * @param attributesFactoryConsumer a lamda that allows you to customize the
   *        partitionAttributeFactory
   */
  public Region createPartitionRegion(String name, Consumer<RegionFactory> regionFactoryConsumer,
      Consumer<PartitionAttributesFactory> attributesFactoryConsumer) {
    return createRegion(RegionShortcut.PARTITION, name, rf -> {
      regionFactoryConsumer.accept(rf);
      PartitionAttributesFactory attributeFactory = new PartitionAttributesFactory();
      attributesFactoryConsumer.accept(attributeFactory);
      rf.setPartitionAttributes(attributeFactory.create());
    });
  }

  public void waitTillCacheClientProxyHasBeenPaused() {
    await().until(() -> {
      CacheClientNotifier clientNotifier = CacheClientNotifier.getInstance();
      Collection<CacheClientProxy> clientProxies = clientNotifier.getClientProxies();

      for (CacheClientProxy clientProxy : clientProxies) {
        if (clientProxy.isPaused()) {
          return true;
        }
      }
      return false;
    });
  }

  public void waitTillCacheServerIsReady(String serverName, int serverPort) {
    await()
        .until(() -> getCacheServerMXBean(serverName, serverPort) != null);
  }

  public CacheServerMXBean getCacheServerMXBean(String serverName, int serverPort) {
    SystemManagementService managementService = (SystemManagementService) getManagementService();
    String objectName = MessageFormat.format(OBJECTNAME__CLIENTSERVICE_MXBEAN,
        String.valueOf(serverPort), serverName);
    ObjectName cacheServerMBeanName = MBeanJMXAdapter.getObjectName(objectName);
    return managementService.getMBeanProxy(cacheServerMBeanName, CacheServerMXBean.class);
  }

  public void waitUntilDiskStoreIsReadyOnExactlyThisManyServers(String diskStoreName,
      int exactServerCount) throws Exception {
    final Supplier<DistributedSystemMXBean> distributedSystemMXBeanSupplier =
        () -> getManagementService().getDistributedSystemMXBean();

    waitUntilSatisfied(distributedSystemMXBeanSupplier,
        Function.identity(),
        bean -> assertThat(bean, notNullValue()),
        "Distributed System MXBean should not be null",
        WAIT_UNTIL_TIMEOUT, TimeUnit.SECONDS);

    DistributedSystemMXBean dsMXBean = distributedSystemMXBeanSupplier.get();

    String predicateDescription = String.format(
        "Expecting exactly %d servers to present mbeans for a disk store with name %s.",
        exactServerCount, diskStoreName);
    Supplier<List<String[]>> diskStoreSupplier = () -> dsMXBean.listMemberDiskstore()
        .values().stream().filter(x1 -> ArrayUtils.contains(x1, diskStoreName))
        .collect(Collectors.toList());

    waitUntilEqual(diskStoreSupplier,
        x -> x.size(),
        exactServerCount,
        predicateDescription,
        WAIT_UNTIL_TIMEOUT, TimeUnit.SECONDS);
  }

  public void waitUntilAsyncEventQueuesAreReadyOnExactlyThisManyServers(String queueId,
      int exactServerCount)
      throws Exception {
    String examinerDescription = String.format(
        "Expecting exactly %d servers to have an AEQ with id '%s'.", exactServerCount, queueId);
    waitUntilEqual(
        () -> CliUtil.getMembersWithAsyncEventQueue(getCache(), queueId),
        membersWithAEQ -> membersWithAEQ.size(),
        exactServerCount,
        examinerDescription,
        WAIT_UNTIL_TIMEOUT, TimeUnit.SECONDS);
  }

  /**
   * This method wraps an {@link GeodeAwaitility#await()} call for more meaningful error
   * reporting.
   *
   * @param supplier Method to retrieve the result to be tested, e.g.,
   *        get a list of visible region mbeans
   * @param examiner Method to evaluate the result provided by {@code provider}, e.g.,
   *        get the length of the provided list.
   *        Use {@link java.util.function.Function#identity()} if {@code assertionConsumer}
   *        directly tests the value provided by {@code supplier}.
   * @param assertionConsumer assertThat styled condition on the output of {@code examiner} against
   *        which
   *        the {@code await().untilAsserted(...)} will be called. E.g.,
   *        {@code beanCount -> assertThat(beanCount, is(5))}
   * @param assertionConsumerDescription A description of the {@code assertionConsumer} method,
   *        for additional failure information should this call time out.
   *        E.g., "Visible region mbean count should be 5"
   * @param timeout With {@code unit}, the maximum time to wait before raising an exception.
   * @param unit With {@code timeout}, the maximum time to wait before raising an exception.
   * @throws org.awaitility.core.ConditionTimeoutException The timeout has been reached
   * @throws Exception Any exception produced by {@code provider.call()}
   */
  public <K, J> void waitUntilSatisfied(Supplier<K> supplier, Function<K, J> examiner,
      Consumer<J> assertionConsumer, String assertionConsumerDescription, long timeout,
      TimeUnit unit)
      throws Exception {
    try {
      await(assertionConsumerDescription)
          .atMost(timeout, unit)
          .untilAsserted(() -> assertionConsumer.accept(examiner.apply(supplier.get())));
    } catch (ConditionTimeoutException e) {
      // There is a very slight race condition here, where the above could conceivably time out,
      // and become satisfied before the next supplier.get()
      throw new ConditionTimeoutException(
          "The observed result '" + String.valueOf(supplier.get())
              + "' does not satisfy the provided assertionConsumer. \n" + e.getMessage());
    }
  }

  /**
   * Convenience alias for {@link #waitUntilSatisfied},
   * requiring equality rather than a generic assertion.
   */
  public <K, J> void waitUntilEqual(Supplier<K> provider,
      Function<K, J> examiner,
      J expectation,
      String expectationDesription,
      long timeout, TimeUnit unit)
      throws Exception {
    Consumer<J> assertionConsumer = examined -> assertThat(examined, is(expectation));
    waitUntilSatisfied(provider, examiner, assertionConsumer, expectationDesription, timeout, unit);
  }

  abstract void stopMember();

  public void forceDisconnectMember() {
    MembershipManagerHelper
        .crashDistributedSystem(InternalDistributedSystem.getConnectedInstance());
  }

  public abstract void waitTilFullyReconnected();

  @Override
  public File getWorkingDir() {
    return new File(System.getProperty("user.dir"));
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
