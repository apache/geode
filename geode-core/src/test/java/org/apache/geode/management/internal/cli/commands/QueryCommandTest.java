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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class QueryCommandTest {
  @Parameterized.Parameters(name = "Connect via http: {0}")
  public static Object[] data() {
    return new Object[] {true, false};
  }

  @Parameterized.Parameter
  public boolean useHttp;

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "simpleRegion")
          .withRegion(RegionShortcut.REPLICATE, "complexRegion");

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

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
      complexRegion.put(key, new Customer("name" + i, "address" + i));
    }
  }

  @Before
  public void connect() throws Exception {
    if (useHttp) {
      gfsh.connectAndVerify(server.getHttpPort(), GfshShellConnectionRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    }
  }

  @Test
  public void doesShowLimitIfLimitNotInQuery() throws Exception {
    String output = gfsh.execute("query --query='select * from /simpleRegion'");
    assertThat(output).contains("Rows   : " + Gfsh.DEFAULT_APP_FETCH_SIZE);
    assertThat(output).contains("Limit  : " + Gfsh.DEFAULT_APP_FETCH_SIZE);
    assertThatOutputHasResult(output);
  }

  @Test
  public void doesNotShowLimitIfLimitInQuery() throws Exception {
    String output = gfsh.execute("query --query='select * from /simpleRegion limit 50'");
    assertThat(output).contains("Rows   : 50");
    assertThat(output).doesNotContain("Limit");
    assertThatOutputHasResult(output);
  }

  @Test
  public void invalidQueryShouldNotCreateFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    String output =
        gfsh.execute("query --query='invalid query' --file=" + outputFile.getAbsolutePath());
    assertThat(outputFile).doesNotExist();

    assertThatOutputHasNoResult(output);
    assertThat(output).doesNotContain("Query results output to");
  }

  @Test
  public void queryWithInvalidRegionNameDoesNotCreateFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    String output = gfsh.execute(
        "query --query='select * from /nonExistentRegion' --file=" + outputFile.getAbsolutePath());
    assertThat(outputFile).doesNotExist();

    assertThatOutputHasNoResult(output);
    assertThat(output).doesNotContain("Query results output to");
  }

  @Test
  public void outputToFileStillDisplaysResultMetaData() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    String output = gfsh.execute(
        "query --query='select * from /simpleRegion' --file=" + outputFile.getAbsolutePath());

    assertThat(output).contains("Rows");
    assertThat(output).contains("Limit");
    assertThat(output).contains("Query results output to");
    assertThatOutputHasResult(output);
  }

  @Test
  public void doesNotOverwriteExistingFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    assertThat(outputFile).exists();

    CommandResult result = gfsh.executeCommand(
        "query --query='select * from /simpleRegion' --file=" + outputFile.getAbsolutePath());

    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().getString("message"))
        .contains("The specified output file already exists.");
  }

  @Test
  public void canOutputSimpleRegionToFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    CommandResult result = gfsh.executeAndVerifyCommand(
        "query --query='select * from /simpleRegion' --file=" + outputFile.getAbsolutePath());
    assertThat(outputFile).exists();
    assertThat(result.getContent().toString()).contains(outputFile.getAbsolutePath());

    List<String> lines = Files.readLines(outputFile, StandardCharsets.UTF_8);

    assertThat(lines.get(0)).isEqualTo("Result");
    assertThat(lines.get(1)).isEqualTo("--------");
    lines.subList(2, lines.size()).forEach(line -> assertThat(line).matches("value\\d+"));
  }

  @Test
  public void canOutputComplexRegionToFile() throws Exception {
    File outputFile = temporaryFolder.newFile("queryOutput.txt");
    FileUtils.deleteQuietly(outputFile);

    CommandResult result = gfsh.executeAndVerifyCommand(
        "query --query='select c.name, c.address from /complexRegion c' --file="
            + outputFile.getAbsolutePath());
    assertThat(outputFile).exists();
    assertThat(result.getContent().toString()).contains(outputFile.getAbsolutePath());

    List<String> lines = Files.readLines(outputFile, StandardCharsets.UTF_8);

    assertThat(lines.get(0)).containsPattern("name\\s+\\|\\s+address");
    lines.subList(2, lines.size())
        .forEach(line -> assertThat(line).matches("name\\d+\\s+\\|\\s+address\\d+"));
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
        .forEach(line -> assertThat(line).matches("name\\d+\\s+\\|\\s+address\\d+"));
  }

  @Test
  public void queryWithInvalidRegionNameGivesDescriptiveErrorMessage() throws Exception {
    String output = gfsh.execute("query --query='select * from /nonExistentRegion'");
    assertThatOutputHasNoResult(output);
    assertThatOutputHasMessage(output,
        "Cannot find regions <[/nonExistentRegion]> in any of the members");
  }

  @Test
  public void invalidQueryGivesDescriptiveErrorMessage() throws Exception {
    String output = gfsh.execute("query --query='this is not a valid query'");

    assertThatOutputHasNoResult(output);
    assertThatOutputHasMessage(output, "Query is invalid due for error : <Syntax error in query:");
  }

  @Test
  public void queryGivesDescriptiveErrorMessageIfNoQueryIsSpecified() throws Exception {
    String output = gfsh.execute("query");

    assertThat(output)
        .contains("You should specify option (--query, --file, --interactive) for this command");
  }

  private void assertThatOutputHasResult(String output) {
    String resultPattern = "Result\\s+:\\s+" + "true";
    assertThat(output).containsPattern(resultPattern);
  }

  private void assertThatOutputHasNoResult(String output) {
    String resultPattern = "Result\\s+:\\s+" + "false";
    assertThat(output).containsPattern(resultPattern);
  }

  private void assertThatOutputHasMessage(String output, String message) {
    String patternToMatch = "Message\\s+:\\s+" + Pattern.quote(message);
    assertThat(output).containsPattern(patternToMatch);
  }

  private String[] splitOnLineBreaks(String multilineString) {
    return multilineString.split("[\\r\\n]+");
  }

  public static class Customer {
    public String name;
    public String address;

    public Customer(String name, String address) {
      this.name = name;
      this.address = address;
    }

    public String toString() {
      return name + address;
    }
  }
}
