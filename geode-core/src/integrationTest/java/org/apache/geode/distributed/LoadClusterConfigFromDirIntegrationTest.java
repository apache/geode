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

package org.apache.geode.distributed;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class LoadClusterConfigFromDirIntegrationTest {
  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule();

  private File clusterConfigDir;

  @Before
  public void before() throws IOException {
    clusterConfigDir = new File(locator.getWorkingDir(), "cluster_config");
    File groupDir = new File(clusterConfigDir, "cluster");
    groupDir.mkdirs();
    File jarFile = new File(groupDir, "test.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(jarFile, "TestFunction");
  }

  @Test
  public void canStartWithDeployedJarInClusterConfig() {
    locator.withSecurityManager(SimpleSecurityManager.class)
        .withProperty("load-cluster-configuration-from-dir", "true")
        .startLocator();

    InternalConfigurationPersistenceService ccService =
        locator.getLocator().getConfigurationPersistenceService();
    Configuration config = ccService.getConfiguration("cluster");
    Collection<Deployment> deployments = config.getDeployments();
    assertThat(deployments).hasSize(1);
    Deployment deployment = deployments.iterator().next();
    assertThat(deployment.getJarFileName()).isEqualTo("test.jar");
    assertThat(deployment.getDeployedBy()).isNull();
  }

  @After
  public void after() {
    FileUtils.deleteQuietly(clusterConfigDir);
  }
}
