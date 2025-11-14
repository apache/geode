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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

/**
 * Integration tests for the 'configure pdx' gfsh command.
 *
 * <p>
 * <b>IMPORTANT - Spring Shell 3.x Parameter Quoting:</b>
 * </p>
 * <p>
 * Parameter values containing '=' signs MUST be quoted to prevent incorrect parsing.
 * Spring Shell 3.x's {@link org.apache.geode.management.internal.cli.GfshParser#splitUserInput}
 * splits unquoted tokens on '=' by default, treating them as separate arguments.
 * </p>
 *
 * <p>
 * <b>Why Quotes Are Required:</b>
 * </p>
 * <ul>
 * <li><b>Without quotes:</b>
 * {@code --auto-serializable-classes=com.company.DomainObject.*#identity=id}
 * <br>
 * Parser sees: {@code ["com.company.DomainObject.*#identity", "id"]} (2 separate values)</li>
 * <li><b>With quotes:</b>
 * {@code --auto-serializable-classes="com.company.DomainObject.*#identity=id"}
 * <br>
 * Parser sees: {@code ["com.company.DomainObject.*#identity=id"]} (single value)</li>
 * </ul>
 *
 * <p>
 * <b>Impact:</b> PDX auto-serialization patterns use the format {@code pattern#param=value}.
 * Without quotes, the {@code =value} portion is lost, causing
 * {@link org.apache.geode.pdx.internal.AutoSerializableManager} to fail with:
 *
 * <pre>
 * "Unable to correctly process auto serialization init value: pattern#param"
 * </pre>
 *
 * because it expects {@code param=value} but receives only {@code param}.
 * </p>
 *
 * <p>
 * <b>GfshParser Behavior:</b> The parser explicitly checks for quoted strings and bypasses
 * the '=' splitting logic when quotes are detected (see {@code GfshParser.splitUserInput()}).
 * This is the intended mechanism for passing parameter values containing delimiter characters.
 * </p>
 *
 * @see org.apache.geode.management.internal.cli.GfshParser#splitUserInput
 * @see org.apache.geode.pdx.ReflectionBasedAutoSerializer
 * @see org.apache.geode.pdx.internal.AutoSerializableManager
 */
@Category({ClientServerTest.class})
public class ConfigurePDXCommandIntegrationTest {
  private static final String BASE_COMMAND_STRING = "configure pdx ";

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule().withTimeout(1);

  @Rule
  public LocatorStarterRule locator =
      new LocatorStarterRule().withAutoStart().withJMXManager();

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void commandShouldFailWhenNotConnected() throws Exception {
    gfsh.disconnect();
    gfsh.executeAndAssertThat(BASE_COMMAND_STRING).statusIsError().containsOutput("Command",
        "was found but is not currently available");
  }

  @Test
  public void commandShouldSucceedWhenUsingDefaults() {
    gfsh.executeAndAssertThat(BASE_COMMAND_STRING).statusIsSuccess();

    String sharedConfigXml = locator.getLocator().getConfigurationPersistenceService()
        .getConfiguration("cluster").getCacheXmlContent();
    assertThat(sharedConfigXml).contains(
        "<pdx read-serialized=\"false\" ignore-unread-fields=\"false\" persistent=\"false\"/>");
  }

  @Test
  public void commandShouldSucceedWhenConfiguringAutoSerializableClassesWithPersistence() {
    gfsh.executeAndAssertThat(BASE_COMMAND_STRING
        + "--read-serialized=true --disk-store=myDiskStore --ignore-unread-fields=true --auto-serializable-classes=\"com.company.DomainObject.*#identity=id\"")
        .statusIsSuccess();

    String sharedConfigXml = locator.getLocator().getConfigurationPersistenceService()
        .getConfiguration("cluster").getCacheXmlContent();
    assertThat(sharedConfigXml).contains(
        "<pdx read-serialized=\"true\" ignore-unread-fields=\"true\" persistent=\"true\" disk-store-name=\"myDiskStore\">");
    assertThat(sharedConfigXml).contains("<pdx-serializer>",
        "<class-name>org.apache.geode.pdx.ReflectionBasedAutoSerializer</class-name>",
        "<parameter name=\"classes\">",
        "<string>com.company.DomainObject.*, com.company.DomainObject.*#identity=id</string>",
        "</parameter>", "</pdx-serializer>");
    assertThat(sharedConfigXml).contains("</pdx>");
  }

  @Test
  public void commandShouldSucceedWhenConfiguringAutoSerializableClassesWithoutPersistence() {
    gfsh.executeAndAssertThat(BASE_COMMAND_STRING
        + "--read-serialized=false --ignore-unread-fields=false --auto-serializable-classes=\"com.company.DomainObject.*#identity=id\"")
        .statusIsSuccess();

    String sharedConfigXml = locator.getLocator().getConfigurationPersistenceService()
        .getConfiguration("cluster").getCacheXmlContent();
    assertThat(sharedConfigXml).contains(
        "<pdx read-serialized=\"false\" ignore-unread-fields=\"false\" persistent=\"false\">");
    assertThat(sharedConfigXml).contains("<pdx-serializer>",
        "<class-name>org.apache.geode.pdx.ReflectionBasedAutoSerializer</class-name>",
        "<parameter name=\"classes\">",
        "<string>com.company.DomainObject.*, com.company.DomainObject.*#identity=id</string>",
        "</parameter>", "</pdx-serializer>");
    assertThat(sharedConfigXml).contains("</pdx>");
  }

  @Test
  public void commandShouldSucceedWhenConfiguringPortableAutoSerializableClassesWithPersistence() {
    gfsh.executeAndAssertThat(BASE_COMMAND_STRING
        + "--read-serialized=true --disk-store=myDiskStore --ignore-unread-fields=true --portable-auto-serializable-classes=\"com.company.DomainObject.*#identity=id\"")
        .statusIsSuccess();

    String sharedConfigXml = locator.getLocator().getConfigurationPersistenceService()
        .getConfiguration("cluster").getCacheXmlContent();
    assertThat(sharedConfigXml).contains(
        "<pdx read-serialized=\"true\" ignore-unread-fields=\"true\" persistent=\"true\" disk-store-name=\"myDiskStore\">");
    assertThat(sharedConfigXml).contains("<parameter name=\"check-portability\">")
        .contains("<string>true</string>").contains("</parameter>");
    assertThat(sharedConfigXml).contains("<pdx-serializer>",
        "<class-name>org.apache.geode.pdx.ReflectionBasedAutoSerializer</class-name>",
        "<parameter name=\"classes\">",
        "<string>com.company.DomainObject.*, com.company.DomainObject.*#identity=id</string>",
        "</parameter>", "</pdx-serializer>");
    assertThat(sharedConfigXml).contains("</pdx>");
  }

  @Test
  public void commandShouldSucceedWhenConfiguringPortableAutoSerializableClassesWithoutPersistence() {
    gfsh.executeAndAssertThat(BASE_COMMAND_STRING
        + "--read-serialized=false --ignore-unread-fields=false --portable-auto-serializable-classes=\"com.company.DomainObject.*#identity=id\"")
        .statusIsSuccess();

    String sharedConfigXml = locator.getLocator().getConfigurationPersistenceService()
        .getConfiguration("cluster").getCacheXmlContent();
    assertThat(sharedConfigXml).contains(
        "<pdx read-serialized=\"false\" ignore-unread-fields=\"false\" persistent=\"false\">");
    assertThat(sharedConfigXml).contains("<parameter name=\"check-portability\">")
        .contains("<string>true</string>").contains("</parameter>");
    assertThat(sharedConfigXml).contains("<pdx-serializer>",
        "<class-name>org.apache.geode.pdx.ReflectionBasedAutoSerializer</class-name>",
        "<parameter name=\"classes\">",
        "<string>com.company.DomainObject.*, com.company.DomainObject.*#identity=id</string>",
        "</parameter>", "</pdx-serializer>");
    assertThat(sharedConfigXml).contains("</pdx>");
  }
}
