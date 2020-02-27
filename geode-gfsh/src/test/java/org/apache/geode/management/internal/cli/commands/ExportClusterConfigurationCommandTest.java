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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.i18n.CliStrings.EXPORT_SHARED_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class ExportClusterConfigurationCommandTest {
  private static String CLUSTER_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
          + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
          + "<region name=\"regionForCluster\">\n"
          + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
          + "  </region>\n" + "</cache>\n";

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ExportClusterConfigurationCommand command;
  private InternalConfigurationPersistenceService ccService;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    ccService = mock(InternalConfigurationPersistenceService.class);
    command = spy(ExportClusterConfigurationCommand.class);
    doReturn(true).when(command).isSharedConfigurationRunning();
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    configuration = new Configuration("cluster");
  }

  @Test
  public void checkDefaultValue() {
    GfshParseResult parseResult = gfsh.parse(EXPORT_SHARED_CONFIG + " --xml-file=my.xml");
    assertThat(parseResult.getParamValue("group")).isEqualTo("cluster");
    assertThat(parseResult.getParamValue("xml-file")).isEqualTo("my.xml");

    parseResult = gfsh.parse(EXPORT_SHARED_CONFIG + " --group=''");
    assertThat(parseResult.getParamValue("group")).isEqualTo("cluster");
  }

  @Test
  public void preValidation() {
    gfsh.executeAndAssertThat(command, EXPORT_SHARED_CONFIG + " --group='group1,group2'")
        .statusIsError().containsOutput("Only a single group name is supported");

    gfsh.executeAndAssertThat(command,
        EXPORT_SHARED_CONFIG + " --zip-file-name=b.zip --xml-file=ab.xml").statusIsError()
        .containsOutput("Zip file and xml File can't both be specified");

    gfsh.executeAndAssertThat(command,
        EXPORT_SHARED_CONFIG + " --zip-file-name=b.zip --group=group1").statusIsError()
        .containsOutput("zip file can not be exported with a specific group");

    gfsh.executeAndAssertThat(command, EXPORT_SHARED_CONFIG + " --zip-file-name=b.zip")
        .statusIsSuccess();
  }

  @Test
  public void clusterConfigurationNotRunning() {
    doReturn(false).when(command).isSharedConfigurationRunning();

    gfsh.executeAndAssertThat(command, EXPORT_SHARED_CONFIG).statusIsError()
        .containsOutput("Cluster configuration service is not running");
  }

  @Test
  public void groupNotExist() {
    when(ccService.getConfiguration("groupA")).thenReturn(null);
    gfsh.executeAndAssertThat(command, EXPORT_SHARED_CONFIG + " --group=groupA").statusIsError()
        .containsOutput("No cluster configuration for 'groupA'.");
  }

  @Test
  public void get() {
    when(ccService.getConfiguration(any())).thenReturn(configuration);
    configuration.setCacheXmlContent(CLUSTER_XML);
    Properties properties = new Properties();
    properties.put("key1", "value1");
    properties.put("key2", "value2");
    configuration.setGemfireProperties(properties);
    configuration.putDeployment(new Deployment("jar1.jar", null, null));
    configuration.putDeployment(new Deployment("jar2.jar", null, null));
    gfsh.executeAndAssertThat(command, EXPORT_SHARED_CONFIG)
        .statusIsSuccess()
        .containsOutput("cluster.xml:")
        .containsOutput("Properties:")
        .containsOutput("Jars:")
        .containsOutput("jar1.jar, jar2.jar")
        .containsOutput("<?xml version=\"1.0\"")
        .containsOutput("</cache>");
  }
}
