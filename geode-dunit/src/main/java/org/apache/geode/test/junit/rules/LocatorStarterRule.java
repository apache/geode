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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.awaitility.Awaitility;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;

/**
 * This is a rule to start up a locator in your current VM. It's useful for your Integration Tests.
 *
 * <p>
 * This rules allows you to create/start a locator using any @ConfigurationProperties, you can chain
 * the configuration of the rule like this: LocatorStarterRule locator = new LocatorStarterRule()
 * .withProperty(key, value) .withName(name) .withProperties(properties) .withSecurityManager(class)
 * .withJmxManager() etc, etc. If your rule calls withAutoStart(), the locator will be started
 * before your test code.
 *
 * <p>
 * In your test code, you can use the rule to access the locator's attributes, like the port
 * information, working dir, name, and the InternalLocator it creates.
 *
 * <p>
 * by default the rule starts a locator with jmx and cluster configuration service
 * you can turn off cluster configuration service to have your test
 * run faster if your test does not need them.
 *
 * http service can be started when needed in your test.
 * </p>
 *
 * <p>
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@code LocatorServerStartupRule}.
 */
public class LocatorStarterRule extends MemberStarterRule<LocatorStarterRule> implements Locator {
  private transient InternalLocator locator;
  private transient LocatorLauncher locatorLauncher;

  @Override
  public InternalCache getCache() {
    return locator.getCache();
  }

  @Override
  public InternalLocator getLocator() {
    return locator;
  }

  public LocatorStarterRule withoutClusterConfigurationService() {
    properties.put(ENABLE_CLUSTER_CONFIGURATION, "false");
    return this;
  }

  @Override
  public void before() {
    super.before();

    // MemberStarterRule deletes the member's workingDirectory (TemporaryFolder) within the after()
    // method but doesn't recreate it during before(), breaking the purpose of the TemporaryFolder
    // rule all together (it gets created only from the withWorkingDir() method, which is generally
    // called only once).
    try {
      if (temporaryFolder != null) {
        temporaryFolder.create();
      }
    } catch (IOException ioException) {
      // Should never happen.
      throw new RuntimeException(ioException);
    }

    // Always use a random jmxPort/httpPort when using the rule to start the locator
    if (jmxPort < 0) {
      withJMXManager(false);
    }

    if (autoStart) {
      startLocator();
    }
  }

  public void startLocator() {
    LocatorLauncher.Builder locatorLauncherBuilder = new LocatorLauncher.Builder();
    properties.forEach((key, value) -> locatorLauncherBuilder.set((String) key, (String) value));
    locatorLauncherBuilder.setForce(true);
    locatorLauncherBuilder.setPort(memberPort);
    locatorLauncherBuilder.setDeletePidFileOnStop(true);

    // Set the name configured, or the default is none is configured.
    if (StringUtils.isNotBlank(this.name)) {
      locatorLauncherBuilder.setMemberName(this.name);
    } else {
      this.name = locatorLauncherBuilder.getMemberName();
    }

    // The LocatorLauncher class doesn't provide a public way of logging only to console.
    locatorLauncher = spy(locatorLauncherBuilder.build());
    doAnswer((invocation) -> (!logFile) ? null : invocation.callRealMethod()).when(locatorLauncher)
        .getLogFile();

    // Start locator and wait until it comes online (max 60 seconds).
    locatorLauncher.start();
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(locatorLauncher.isRunning()).isTrue());
    locator = InternalLocator.getLocator();

    // Initially zero by default ("select random available port"), that's why it needs to be updated
    memberPort = locator.getPort();
    DistributionConfig config = locator.getConfig();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();

    // Wait at most 60 seconds for the cluster configuration service to come online.
    if (config.getEnableClusterConfiguration()) {
      Awaitility.await().atMost(60, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(locator.isSharedConfigurationRunning()).isTrue());
    }
  }

  @Override
  protected void stopMember() {
    if (locatorLauncher != null) {
      try {
        locatorLauncher.stop();
      } catch (Exception exception) {
        exception.printStackTrace(System.out);
      }
    }
  }

  @Override
  public void after() {
    super.after();

    // Files are generated in the current dir by default if workingDir is not set, clean them up.
    if (getWorkingDir() == null) {
      String baseName = this.name + this.memberPort;
      // GMS and view persistent files.
      Path locatorDatFile = Paths.get(baseName + "view.dat");
      Path locatorViewsFile = Paths.get(baseName + "views.log");
      FileUtils.deleteQuietly(locatorDatFile.toFile());
      FileUtils.deleteQuietly(locatorViewsFile.toFile());

      // InternalConfigurationPersistenceService
      Path clusterConfigDirectory = Paths
          .get(InternalConfigurationPersistenceService.CLUSTER_CONFIG_DISK_DIR_PREFIX + this.name);
      FileUtils.deleteQuietly(clusterConfigDirectory.toFile());
    }
  }
}
