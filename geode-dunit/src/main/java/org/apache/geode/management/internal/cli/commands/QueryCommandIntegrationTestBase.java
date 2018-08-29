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

import java.io.File;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.Server;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class})
public class QueryCommandIntegrationTestBase {

  private final String DEFAULT_FETCH_SIZE = String.valueOf(Gfsh.DEFAULT_APP_FETCH_SIZE);

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager()
          .withHttpService()
          .withRegion(RegionShortcut.REPLICATE, "simpleRegion")
          .withRegion(RegionShortcut.REPLICATE, "complexRegion");

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void populateRegions() {
    Cache cache = server.getCache();
    Region<String, String> simpleRegion = cache.getRegion("simpleRegion");
    Region<String, Customer> complexRegion = cache.getRegion("complexRegion");

    for (int i = 0; i < Gfsh.DEFAULT_APP_FETCH_SIZE + 1; i++) {
      String key = "key" + i;

      simpleRegion.put(key, "value" + i);
      complexRegion.put(key, new Customer("name" + i, "Main Street " + i, "Hometown"));
    }
  }

  @Before
  public void before() throws Exception {
    connect(server);
  }

  protected void connect(Server server) throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
  }

  @Test
  public void doesShowLimitIfLimitNotInQuery() throws Exception {
    gfsh.executeAndAssertThat("query --query='select * from /simpleRegion'")
        .containsKeyValuePair("Rows", DEFAULT_FETCH_SIZE)
        .containsKeyValuePair("Limit", DEFAULT_FETCH_SIZE).hasResult();
  }

  @Test
  public void doesNotShowLimitIfLimitInQuery() throws Exception {
    gfsh.executeAndAssertThat("query --query='select * from /simpleRegion limit 50'")
        .containsKeyValuePair("Rows", "50").doesNotContainOutput("Limit").hasResult();
  }

  @Test
  public void invalidQueryShouldNotCreateFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    gfsh.executeAndAssertThat(
        "query --query='invalid query' --file=" + outputFile.getAbsolutePath()).hasNoResult()
        .doesNotContainOutput("Query results output to");

    assertThat(outputFile).doesNotExist();
  }

  @Test
  public void queryWithInvalidRegionNameDoesNotCreateFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    gfsh.executeAndAssertThat(
        "query --query='select * from /nonExistentRegion' --file=" + outputFile.getAbsolutePath())
        .hasNoResult().doesNotContainOutput("Query results output to");

    assertThat(outputFile).doesNotExist();
  }

  @Test
  public void outputToFileStillDisplaysResultMetaData() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    gfsh.executeAndAssertThat(
        "query --query='select * from /simpleRegion' --file=" + outputFile.getAbsolutePath())
        .hasResult().containsOutput("Rows").containsOutput("Limit")
        .containsOutput("Query results output to");
  }

  @Test
  public void doesNotOverwriteExistingFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    assertThat(outputFile).exists();

    gfsh.executeAndAssertThat(
        "query --query='select * from /simpleRegion' --file=" + outputFile.getAbsolutePath())
        .statusIsError().containsOutput("The specified output file already exists.");
  }

  @Test
  public void canOutputSimpleRegionToFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    CommandResult result = gfsh.executeCommand(
        "query --query='select * from /simpleRegion' --file=" + outputFile.getAbsolutePath());
    assertThat(result.getStatus()).isEqualTo(Result.Status.OK);
    // .statusIsSuccess().containsOutput(outputFile.getAbsolutePath());

    assertThat(outputFile).exists();

    List<String> lines = Files.readLines(outputFile, StandardCharsets.UTF_8);

    assertThat(lines.get(7)).isEqualTo("Result");
    assertThat(lines.get(8)).isEqualTo("--------");
    lines.subList(9, lines.size()).forEach(line -> assertThat(line).matches("value\\d+"));
  }

  @Test
  public void canOutputComplexRegionToFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    gfsh.executeAndAssertThat(
        "query --query='select c.name, c.address from /complexRegion c' --file="
            + outputFile.getAbsolutePath())
        .statusIsSuccess().containsOutput(outputFile.getAbsolutePath());

    assertThat(outputFile).exists();
    List<String> lines = Files.readLines(outputFile, StandardCharsets.UTF_8);

    assertThat(lines.get(7)).containsPattern("name\\s+\\|\\s+address");
    lines.subList(9, lines.size())
        .forEach(line -> assertThat(line).matches("name\\d+.*\"city\":\"Hometown\".*"));
  }

  @Test
  public void outputDisplaysResultsFromComplexRegion() throws Exception {
    String result = gfsh.execute("query --query='select c.name, c.address from /complexRegion c'");

    String[] resultLines = splitOnLineBreaks(result);

    assertThat(resultLines[0]).containsPattern("Result\\s+:\\s+true");
    assertThat(resultLines[1]).containsPattern("Limit\\s+:\\s+100");
    assertThat(resultLines[2]).containsPattern("Rows\\s+:\\s+100");
    assertThat(resultLines[3]).containsPattern("name\\s+\\|\\s+address");
    Arrays.asList(resultLines).subList(5, resultLines.length)
        .forEach(line -> assertThat(line).matches("name\\d+.*\"city\":\"Hometown\".*"));
  }

  @Test
  public void queryWithGfshEnvVariables() {
    gfsh.executeAndAssertThat("set variable --name=DATA_REGION --value=/complexRegion")
        .statusIsSuccess();
    gfsh.executeAndAssertThat("set variable --name=QUERY_LIMIT --value=10").statusIsSuccess();
    CommandResult result = gfsh.executeCommand(
        "query --query='select c.name, c.address from ${DATA_REGION} c limit ${QUERY_LIMIT}'");
    Map<String, String> data = result.getMapFromSection(DataCommandResult.DATA_INFO_SECTION);

    assertThat(data.get("Rows")).isEqualTo("10");
  }

  @Test
  public void queryWithInvalidRegionNameGivesDescriptiveErrorMessage() throws Exception {
    gfsh.executeAndAssertThat("query --query='select * from /nonExistentRegion'")
        .containsKeyValuePair("Result", "false")
        .containsOutput("Cannot find regions <[/nonExistentRegion]> in any of the members");
  }

  @Test
  public void invalidQueryGivesDescriptiveErrorMessage() {
    CommandResult result = gfsh.executeCommand("query --query='this is not a valid query'");

    Map<String, String> data = result.getMapFromSection(DataCommandResult.DATA_INFO_SECTION);
    assertThat(data.get("Result")).isEqualTo("false");
    assertThat(data.get("Message"))
        .startsWith("Query is invalid due to error : <Syntax error in query:");
  }

  @Test
  public void queryGivesDescriptiveErrorMessageIfNoQueryIsSpecified() {
    gfsh.executeAndAssertThat("query").containsOutput(
        "You should specify option (--query, --file, --interactive) for this command");
  }

  @Test
  public void queryReturnsUndefinedQueryResult() {
    CommandResult result =
        gfsh.executeCommand("query --query='select c.unknown from /complexRegion c limit 10'");

    Map<String, List<String>> table =
        result.getMapFromTableContent(DataCommandResult.QUERY_SECTION);
    assertThat(table.get("Value").size()).isEqualTo(10);
    assertThat(table.get("Value").get(0)).isEqualTo("UNDEFINED");
  }

  @Test
  public void queryReturnsNonSelectResult() {
    CommandResult result = gfsh.executeCommand(
        "query --query=\"(select c.address from /complexRegion c where c.name = 'name1' limit 1).size\"");

    Map<String, String> data = result.getMapFromSection(DataCommandResult.DATA_INFO_SECTION);
    assertThat(data.get("Rows")).isEqualTo("1");

    Map<String, List<String>> table =
        result.getMapFromTableContent(DataCommandResult.QUERY_SECTION);
    assertThat(table.get("Result")).contains("1");
  }

  private String[] splitOnLineBreaks(String multilineString) {
    return multilineString.split("[\\r\\n]+");
  }

  public static class Customer implements Serializable {
    public String name;
    public Address address;

    public Customer(String name, String street, String city) {
      this.name = name;
      this.address = new Address(street, city);
    }

    public String toString() {
      return name + address;
    }
  }

  public static class Address implements Serializable {
    public String street;
    public String city;

    public Address(String street, String city) {
      this.street = street;
      this.city = city;
    }
  }

}
