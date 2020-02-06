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

package org.apache.geode.management;

import static org.apache.geode.test.junit.assertions.ClusterManagementListResultAssert.assertManagementListResult;

import java.io.File;
import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.api.BaseConnectionConfig;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class DeploymentManagementUpgradeTest {
  @Rule
  public GfshRule oldGfsh = new GfshRule("1.10.0");

  @Rule
  public GfshRule gfsh = new GfshRule();

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();
  private static File stagingDir, clusterJar;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // prepare the jars to be deployed
    stagingDir = tempFolder.newFolder("staging");
    clusterJar = new File(stagingDir, "cluster.jar");
    JarBuilder jarBuilder = new JarBuilder();
    jarBuilder.buildJarFromClassNames(clusterJar, "Class1");
  }

  @Test
  public void newLocatorCanReadOldConfigurationData() throws IOException {
    File workingDir = tempFolder.newFolder();
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    oldGfsh.execute("start locator --name=test --port=" + ports[0] + " --http-service-port="
        + ports[1] + " --dir=" + workingDir.getAbsolutePath() + " --J=-Dgemfire.jmx-manager-port="
        + ports[2],
        "deploy --jar=" + clusterJar.getAbsolutePath(),
        "shutdown --include-locators");

    gfsh.execute("start locator --name=test --port=" + ports[0] + " --http-service-port=" + ports[1]
        + " --dir=" + workingDir.getAbsolutePath() + " --J=-Dgemfire.jmx-manager-port=" + ports[2]);

    ConnectionConfig connectionConfig =
        new BaseConnectionConfig("localhost", ports[1]);
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setConnectionConfig(connectionConfig).build();
    // ClusterManagementServiceBuilder.buildWithHostAddress().setHostAddress("localhost", ports[1])
    // .build();
    assertManagementListResult(cms.list(new Deployment())).isSuccessful()
        .hasConfigurations().hasSize(1);
  }
}
