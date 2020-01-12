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

package org.apache.geode.management.internal.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.management.internal.cli.domain.MyCacheListener;
import org.apache.geode.management.internal.cli.domain.MyCacheWriter;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class ClusterConfigWithCallbacksDUnitTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private MemberVM locator, server;
  private File clusterConfigZip;

  @Before
  public void buildClusterConfigZip() throws Exception {
    File clusterConfigDir = tempFolder.newFolder("cluster_config");
    File clusterDir = new File(clusterConfigDir, "cluster");
    clusterDir.mkdir();

    String clusterXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
        + "<region name=\"regionForCluster\">\n"
        + "  <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\">\n"
        + "    <cache-writer><class-name>org.apache.geode.management.internal.cli.domain.MyCacheWriter</class-name><parameter name=\"key\"><string>value</string></parameter></cache-writer>\n"
        + "    <cache-listener><class-name>org.apache.geode.management.internal.cli.domain.MyCacheListener</class-name><parameter name=\"key\"><string>value</string></parameter></cache-listener>\n"
        + "  </region-attributes>\n" + "</region></cache>";
    File xmlFile = new File(clusterDir, "cluster.xml");
    FileUtils.writeStringToFile(xmlFile, clusterXml, Charset.defaultCharset());

    clusterConfigZip = new File(tempFolder.getRoot(), "cluster_config.zip");
    ZipUtils.zipDirectory(clusterConfigDir.getAbsolutePath(), clusterConfigZip.getAbsolutePath());
  }

  @Test
  public void importCCWithCallbacks() throws Exception {
    locator = cluster.startLocatorVM(0);
    server = cluster.startServerVM(1, locator.getPort());

    gfsh.connectAndVerify(locator);

    // import cluster configuration with a running server
    gfsh.executeAndAssertThat(
        "import cluster-configuration --zip-file-name=" + clusterConfigZip.getAbsolutePath())
        .statusIsSuccess().containsOutput("Configure the servers in 'cluster' group");

    // assert that the callbacks are properly hooked up with the region
    server.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion("/regionForCluster");
      MyCacheWriter writer = (MyCacheWriter) region.getAttributes().getCacheWriter();
      MyCacheListener listener = (MyCacheListener) region.getAttributes().getCacheListeners()[0];
      assertThat(writer.getProperties().getProperty("key")).isEqualTo("value");
      assertThat(listener.getProperties().getProperty("key")).isEqualTo("value");
    });
  }
}
