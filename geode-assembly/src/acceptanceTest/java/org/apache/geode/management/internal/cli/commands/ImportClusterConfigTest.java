/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.management.internal.configuration.ClusterConfigTestBase;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;

public class ImportClusterConfigTest extends ClusterConfigTestBase {
  private static final String locatorName = "locator";
  private static final String serverNotShutDownName = "serverNotShutDown";

  @Rule
  public GfshRule gfsh = new GfshRule();

  @Test
  public void importWouldNotShutDownServer() {
    GfshExecution startCluster = GfshScript
        .of("start locator --name=" + locatorName,
            "start server --name=" + serverNotShutDownName + " --server-port=0")
        .withName("startCluster").execute(gfsh);
    assertThat(startCluster.getOutputText()).contains(locatorName + " is currently online")
        .contains(serverNotShutDownName + " is currently online");

    GfshExecution importConfiguration = GfshScript
        .of("connect", "import cluster-configuration --zip-file-name=" + clusterConfigZipPath)
        .withName("importConfiguration").execute(gfsh);
    assertThat(importConfiguration.getOutputText())
        .contains("Cluster configuration successfully imported")
        .contains("Configure the servers in 'cluster' group: ");

    GfshExecution listMembers =
        GfshScript.of("connect", "list members").withName("listMembers").execute(gfsh);
    assertThat(listMembers.getOutputText()).contains("serverNotShutDown");
  }

}
