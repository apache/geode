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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class GfshCommandRedactionAcceptanceTest {

  private static final String LOCATOR_NAME = "locator";

  private int locatorPort;
  private int unusedPort;
  private Path locatorFolder;
  private LocatorLauncher locatorLauncher;

  @Rule
  public GfshCommandRule gfshCommandRule = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    locatorPort = ports[0];
    unusedPort = ports[1];
    locatorFolder = temporaryFolder.newFolder(LOCATOR_NAME).toPath().toAbsolutePath();

    locatorLauncher = new LocatorLauncher.Builder()
        .setMemberName(LOCATOR_NAME)
        .setWorkingDirectory(locatorFolder.toString())
        .setPort(locatorPort)
        .build();
    locatorLauncher.start();
  }

  @After
  public void tearDown() {
    locatorLauncher.stop();
  }

  @Test
  public void commandsAreLoggedAndRedacted() throws Exception {
    Path logFile = locatorFolder.resolve(LOCATOR_NAME + ".log");

    gfshCommandRule.connectAndVerify(locatorPort, GfshCommandRule.PortType.locator);
    gfshCommandRule.executeAndAssertThat(
        "start locator --properties-file=unknown --J=-Dgemfire.security-password=bob")
        .statusIsError();
    gfshCommandRule.executeAndAssertThat("disconnect")
        .statusIsSuccess();
    gfshCommandRule.executeAndAssertThat(
        "connect --jmx-manager=localhost[" + unusedPort + "] --password=secret")
        .statusIsError();

    Pattern startLocatorPattern = Pattern.compile(
        "Executing command: start locator --properties-file=unknown --J=-Dgemfire.security-password=\\*\\*\\*\\*\\*\\*\\*\\*");
    Pattern connectPattern = Pattern.compile(
        "Executing command: connect --jmx-manager=localhost\\[" + unusedPort
            + "] --password=\\*\\*\\*\\*\\*\\*\\*\\*");

    Predicate<String> isRelevantLine = startLocatorPattern.asPredicate()
        .or(connectPattern.asPredicate());

    await().untilAsserted(() -> {
      List<String> foundPatterns = Files
          .lines(logFile)
          .filter(isRelevantLine)
          .collect(Collectors.toList());

      assertThat(foundPatterns)
          .as("Log file " + logFile + " includes one line matching each of "
              + startLocatorPattern + " and " + connectPattern)
          .hasSize(2);

      assertThat(foundPatterns)
          .as("lines in the log file")
          .withFailMessage("%n Expect line matching %s %n but was %s",
              startLocatorPattern.pattern(), foundPatterns)
          .anyMatch(startLocatorPattern.asPredicate())
          .withFailMessage("%n Expect line matching %s %n but was %s",
              connectPattern.pattern(), foundPatterns)
          .anyMatch(connectPattern.asPredicate());
    });
  }
}
