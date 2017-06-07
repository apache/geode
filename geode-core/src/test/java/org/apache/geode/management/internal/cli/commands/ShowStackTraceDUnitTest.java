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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.json.GfJsonException;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/***
 * DUnit test for 'show stack-trace' command
 */
@Category(DistributedTest.class)
public class ShowStackTraceDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  @Rule
  public TemporaryFolder workDirectory = new SerializableTemporaryFolder();

  private void createCache(Properties props) {
    getSystem(props);
    getCache();
  }

  private Properties createProperties(Host host, String name, String groups) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(NAME, name);
    props.setProperty(GROUPS, groups);
    return props;
  }

  /***
   * Sets up a system of 3 peers
   */
  private void setupSystem() {
    disconnectAllFromDS();
    final Host host = Host.getHost(0);
    final VM[] servers = {host.getVM(0), host.getVM(1)};

    final Properties propsManager = createProperties(host, "Manager", "G1");
    final Properties propsServer2 = createProperties(host, "Server", "G2");

    setUpJmxManagerOnVm0ThenConnect(propsManager);

    servers[1].invoke(new SerializableRunnable("Create cache for server1") {
      public void run() {
        createCache(propsServer2);
      }
    });
  }

  /***
   * Tests the default behavior of the show stack-trace command
   *
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @Test
  public void testExportStacktrace() throws ClassNotFoundException, IOException {
    setupSystem();

    File allStacktracesFile = workDirectory.newFile("allStackTraces.txt");
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, allStacktracesFile.getCanonicalPath());
    String commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File mgrStacktraceFile = workDirectory.newFile("managerStacktrace.txt");
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, mgrStacktraceFile.getCanonicalPath());
    csb.addOption(CliStrings.MEMBER, "Manager");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File serverStacktraceFile = workDirectory.newFile("serverStacktrace.txt");
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, serverStacktraceFile.getCanonicalPath());
    csb.addOption(CliStrings.MEMBER, "Server");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File groupStacktraceFile = workDirectory.newFile("groupstacktrace.txt");
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, groupStacktraceFile.getCanonicalPath());
    csb.addOption(CliStrings.GROUP, "G2");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File wrongStackTraceFile = workDirectory.newFile("wrongStackTrace.txt");
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, wrongStackTraceFile.getCanonicalPath());
    csb.addOption(CliStrings.MEMBER, "WrongMember");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertFalse(commandResult.getStatus().equals(Status.OK));
  }

  /***
   * Tests the behavior of the show stack-trace command to verify that files with any extension are
   * allowed Refer: GEODE-734
   *
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @Test
  public void testExportStacktraceWithNonTXTFile() throws ClassNotFoundException, IOException {
    setupSystem();

    // Test non txt extension file is allowed
    File stacktracesFile = workDirectory.newFile("allStackTraces.log");
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    commandStringBuilder.addOption(CliStrings.EXPORT_STACKTRACE__FILE,
        stacktracesFile.getCanonicalPath());
    String exportCommandString = commandStringBuilder.toString();
    getLogWriter().info("CommandString : " + exportCommandString);
    CommandResult exportCommandResult = executeCommand(exportCommandString);
    getLogWriter().info("Output : \n" + commandResultToString(exportCommandResult));
    assertTrue(exportCommandResult.getStatus().equals(Status.OK));

    // test file with-out any extension
    File allStacktracesFile = workDirectory.newFile("allStackTraces");
    commandStringBuilder = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    commandStringBuilder.addOption(CliStrings.EXPORT_STACKTRACE__FILE,
        allStacktracesFile.getCanonicalPath());
    exportCommandString = commandStringBuilder.toString();
    getLogWriter().info("CommandString : " + exportCommandString);
    exportCommandResult = executeCommand(exportCommandString);
    getLogWriter().info("Output : \n" + commandResultToString(exportCommandResult));
    assertTrue(exportCommandResult.getStatus().equals(Status.OK));
  }

  /***
   * Tests the behavior of the show stack-trace command when file is already present and
   * abort-if-file-exists option is set to false(which is default). As a result it should overwrite
   * the file and return OK status
   *
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @Test
  public void testExportStacktraceWhenFilePresent() throws ClassNotFoundException, IOException {
    setupSystem();

    // test pass although file present
    File stacktracesFile = workDirectory.newFile("allStackTraces.log");
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    commandStringBuilder.addOption(CliStrings.EXPORT_STACKTRACE__FILE,
        stacktracesFile.getCanonicalPath());
    String exportCommandString = commandStringBuilder.toString();
    getLogWriter().info("CommandString : " + exportCommandString);
    CommandResult exportCommandResult = executeCommand(exportCommandString);
    getLogWriter().info("Output : \n" + commandResultToString(exportCommandResult));
    assertTrue(exportCommandResult.getStatus().equals(Status.OK));

  }

  /***
   * Tests the behavior of the show stack-trace command when file is already present and when
   * abort-if-file-exists option is set to true. As a result it should fail with ERROR status
   *
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @Test
  public void testExportStacktraceFilePresentWithAbort()
      throws ClassNotFoundException, IOException, GfJsonException {
    setupSystem();

    File stacktracesFile = workDirectory.newFile("allStackTraces.log");
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    commandStringBuilder.addOption(CliStrings.EXPORT_STACKTRACE__FILE,
        stacktracesFile.getCanonicalPath());
    commandStringBuilder.addOption(CliStrings.EXPORT_STACKTRACE__FAIL__IF__FILE__PRESENT,
        Boolean.TRUE.toString());
    String exportCommandString = commandStringBuilder.toString();
    getLogWriter().info("CommandString : " + exportCommandString);
    CommandResult exportCommandResult = executeCommand(exportCommandString);
    getLogWriter().info("Output : \n" + commandResultToString(exportCommandResult));
    assertTrue(exportCommandResult.getStatus().equals(Status.ERROR));
    assertTrue(((String) exportCommandResult.getResultData().getGfJsonObject()
        .getJSONObject("content").getJSONArray("message").get(0))
            .contains("file " + stacktracesFile.getCanonicalPath() + " already present"));
  }

  /***
   * Tests the behavior of the show stack-trace command when file option is not provided File should
   * get auto-generated
   *
   * @throws ClassNotFoundException
   * @throws IOException
   */
  @Test
  public void testExportStacktraceAutoGenerateFile()
      throws ClassNotFoundException, IOException, GfJsonException {
    setupSystem();

    // test auto generated file when file name is not provided
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    String exportCommandString = commandStringBuilder.toString();
    getLogWriter().info("CommandString : " + exportCommandString);
    CommandResult exportCommandResult = executeCommand(exportCommandString);
    getLogWriter().info("Output : \n" + commandResultToString(exportCommandResult));
    assertTrue(exportCommandResult.getStatus().equals(Status.OK));
    assertTrue(
        ((String) exportCommandResult.getResultData().getGfJsonObject().getJSONObject("content")
            .getJSONArray("message").get(0)).contains("stack-trace(s) exported to file:"));

  }
}
