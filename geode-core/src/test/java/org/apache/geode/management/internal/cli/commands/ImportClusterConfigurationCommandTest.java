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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.IMPORT_SHARED_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class ImportClusterConfigurationCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private ImportClusterConfigurationCommand command;
  private File xmlFile;
  private InternalConfigurationPersistenceService ccService;
  private String commandWithFile;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    command = spy(ImportClusterConfigurationCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    xmlFile = tempFolder.newFile("my.xml");
    commandWithFile = IMPORT_SHARED_CONFIG + " --xml-file=" + xmlFile.getAbsolutePath() + " ";
    doReturn(true).when(command).isSharedConfigurationRunning();
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    doNothing().when(command).backupTheOldConfig(ccService);
    doReturn(Collections.emptySet()).when(command).findMembers(any());
    doReturn(xmlFile).when(command).getUploadedFile();
    configuration = new Configuration("group");
  }

  @Test
  public void autoComplete() {
    GfshParserRule.CommandCandidate commandCandidate =
        gfsh.complete(IMPORT_SHARED_CONFIG + " --action=");
    assertThat(commandCandidate.getCandidates()).hasSize(2);
    assertThat(commandCandidate.getFirstCandidate())
        .isEqualTo(IMPORT_SHARED_CONFIG + " --action=APPLY");
  }

  @Test
  public void mandatory() {
    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG).statusIsError()
        .containsOutput("Either a zip file or a xml file is required");
    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG + " --xml-file=''").statusIsError()
        .containsOutput("Either a zip file or a xml file is required");
  }

  @Test
  public void defaultValue() {
    GfshParseResult parseResult = gfsh.parse(IMPORT_SHARED_CONFIG + " --xml-file=my.xml");
    assertThat(parseResult.getParamValue("group")).isEqualTo("cluster");
    assertThat(parseResult.getParamValue("xml-file")).isEqualTo("my.xml");
    assertThat(parseResult.getParamValue("action").toString()).isEqualTo("APPLY");


    parseResult = gfsh.parse(IMPORT_SHARED_CONFIG + " --group=''");
    assertThat(parseResult.getParamValue("group")).isEqualTo("cluster");
  }

  @Test
  public void preValidation() {
    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG + " --xml-file=abc").statusIsError()
        .containsOutput("Invalid file type");


    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG + " --zip-file-name=b.zip")
        .statusIsError().containsOutput("'b.zip' not found");


    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG + " --xml-file=a.xml").statusIsError()
        .containsOutput("'a.xml' not found");

    gfsh.executeAndAssertThat(command, commandWithFile + " --group='group1,group2'").statusIsError()
        .containsOutput("Only a single group name is supported");

    gfsh.executeAndAssertThat(command,
        IMPORT_SHARED_CONFIG + " --zip-file-name=b.zip --group=group1").statusIsError()
        .containsOutput("zip file can not be imported with a specific group");


    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG + " --zip-file-name=b.zip")
        .statusIsError().containsOutput("'b.zip' not found");


    gfsh.executeAndAssertThat(command, IMPORT_SHARED_CONFIG + " --xml-file=a.xml").statusIsError()
        .containsOutput("'a.xml' not found");

    gfsh.executeAndAssertThat(command, commandWithFile + " --zip-file-name=b.zip").statusIsError()
        .containsOutput("Zip file and xml File can't both be specified");

    gfsh.executeAndAssertThat(command,
        IMPORT_SHARED_CONFIG + " --zip-file-name=b.zip --group=group1").statusIsError()
        .containsOutput("zip file can not be imported with a specific group");

  }

  @Test
  public void clusterConfigurationNotRunning() {
    doReturn(false).when(command).isSharedConfigurationRunning();
    gfsh.executeAndAssertThat(command, commandWithFile).statusIsError()
        .containsOutput("Cluster configuration service is not running");
  }

  @Test
  public void noMemberFound() throws IOException {
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><cache/>";
    FileUtils.write(xmlFile, xmlContent, Charset.defaultCharset());
    when(ccService.getConfiguration(any())).thenReturn(configuration);

    gfsh.executeAndAssertThat(command, commandWithFile).statusIsSuccess()
        .containsOutput("Successfully set the 'cluster' configuration to the content of");
    assertThat(configuration.getCacheXmlContent()).isEqualTo(xmlContent);
  }

  @Test
  public void invalidXml() throws IOException {
    String xmlContent =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><cache><region></cache>";
    FileUtils.write(xmlFile, xmlContent, Charset.defaultCharset());
    when(ccService.getConfiguration(any())).thenReturn(configuration);

    gfsh.executeAndAssertThat(command, commandWithFile).statusIsError()
        .containsOutput("Error while processing command ");
  }

  @Test
  public void existingMembersWithoutStaging() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any());
    when(ccService.hasXmlConfiguration()).thenReturn(true);
    gfsh.executeAndAssertThat(command, commandWithFile).statusIsError()
        .containsOutput("Can not configure servers that are already configured");
  }

  @Test
  public void existingMembersWithBounce() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any());
    when(ccService.hasXmlConfiguration()).thenReturn(true);
    gfsh.executeAndAssertThat(command, commandWithFile + "--action=APPLY").statusIsError()
        .containsOutput("Can not configure servers that are already configured");
  }

  @Test
  public void existingMembersWithIgnore() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any());
    when(ccService.getConfiguration(any())).thenReturn(configuration);

    gfsh.executeAndAssertThat(command, commandWithFile + "--action=STAGE").statusIsSuccess()
        .containsOutput("Successfully set the 'cluster' configuration to the content of")
        .containsOutput("Existing servers are not affected with this configuration change");
  }
}
