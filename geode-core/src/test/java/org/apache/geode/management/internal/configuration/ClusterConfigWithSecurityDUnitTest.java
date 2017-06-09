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

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.io.FileUtils;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Properties;

@Category({DistributedTest.class, SecurityTest.class})
public class ClusterConfigWithSecurityDUnitTest {

  private String clusterConfigZipPath;
  private MemberVM locator0;
  private Properties locatorProps;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule connector = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    clusterConfigZipPath = buildSecureClusterConfigZip();

    locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    locator0 = lsRule.startLocatorVM(0, locatorProps);
  }

  @Test
  @Ignore("Fails until GEODE-2315 is implemented")
  public void testSecurityPropsInheritanceOnLocator() throws Exception {
    locatorProps.clear();
    locatorProps.setProperty(LOCATORS, "localhost[" + locator0.getPort() + "]");
    locatorProps.setProperty("security-username", "cluster");
    locatorProps.setProperty("security-password", "cluster");
    MemberVM locator1 = lsRule.startLocatorVM(1, locatorProps);

    // the second locator should inherit the first locator's security props
    locator1.invoke(() -> {
      InternalLocator locator = LocatorServerStartupRule.locatorStarter.getLocator();
      ClusterConfigurationService sc = locator.getSharedConfiguration();
      Properties clusterConfigProps = sc.getConfiguration("cluster").getGemfireProperties();
      assertThat(clusterConfigProps.getProperty(SECURITY_MANAGER))
          .isEqualTo(SimpleTestSecurityManager.class.getName());
      assertThat(locator.getConfig().getSecurityManager()).isNotEmpty();
    });
  }

  @Test
  public void testImportNotOverwriteSecurity() throws Exception {
    connector.connect(locator0, CliStrings.CONNECT__USERNAME, "cluster",
        CliStrings.CONNECT__PASSWORD, "cluster");

    connector.executeAndVerifyCommand(
        "import cluster-configuration --zip-file-name=" + clusterConfigZipPath);

    locator0.invoke(() -> {
      InternalLocator locator = LocatorServerStartupRule.locatorStarter.getLocator();
      ClusterConfigurationService sc = locator.getSharedConfiguration();
      Properties properties = sc.getConfiguration("cluster").getGemfireProperties();
      assertThat(properties.getProperty(MCAST_PORT)).isEqualTo("0");
      assertThat(properties.getProperty(LOG_FILE_SIZE_LIMIT)).isEqualTo("8000");

      // the security manager is still the locator's security manager, not the imported one.
      assertThat(properties.getProperty(SECURITY_MANAGER))
          .isEqualTo(SimpleTestSecurityManager.class.getName());
    });
  }

  @Test // fails due to GEODE-3062
  public void testSecurityPropsInheritanceOnServer() throws Exception {
    Properties serverProps = new Properties();
    serverProps.setProperty(LOCATORS, "localhost[" + locator0.getPort() + "]");
    serverProps.setProperty("security-username", "cluster");
    serverProps.setProperty("security-password", "cluster");
    MemberVM server = lsRule.startServerVM(1, serverProps);

    // cluster config specifies a security-manager so integrated security should be enabled
    server.invoke(() -> {
      InternalCache cache = LocatorServerStartupRule.serverStarter.getCache();
      Properties properties = cache.getDistributedSystem().getSecurityProperties();
      assertThat(properties.getProperty(SECURITY_MANAGER))
          .isEqualTo(SimpleTestSecurityManager.class.getName());
      assertThat(cache.getSecurityService().isIntegratedSecurity()).isTrue();
    });
  }

  private String buildSecureClusterConfigZip() throws Exception {
    File clusterDir = lsRule.getTempFolder().newFolder("cluster");
    File clusterSubDir = new File(clusterDir, "cluster");

    String clusterProperties = "mcast-port=0\n" + "log-file-size-limit=8000\n"
        + "security-manager=org.apache.geode.example.security.ExampleSecurityManager";
    FileUtils.writeStringToFile(new File(clusterSubDir, "cluster.properties"), clusterProperties);
    File clusterZip = new File(lsRule.getTempFolder().getRoot(), "cluster_config_security.zip");
    ZipUtils.zipDirectory(clusterDir.getCanonicalPath(), clusterZip.getCanonicalPath());
    FileUtils.deleteDirectory(clusterDir);
    return clusterZip.getCanonicalPath();
  }
}
