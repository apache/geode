/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.commands;

import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/***
 * DUnit test for 'show stack-trace' command
 */
@Category(DistributedTest.class)
@RunWith(Parameterized.class)
public class ShowStackTraceDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  public ShowStackTraceDUnitTest(boolean useHttpOnConnect) {
    super(useHttpOnConnect);
  }

  private void createCache(Properties props) {
    getSystem(props);
    getCache();
  }

  private Properties createProperties(Host host, String name, String groups) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
    props.setProperty(DistributionConfig.NAME_NAME, name);
    props.setProperty(DistributionConfig.GROUPS_NAME, groups);
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

    File allStacktracesFile = new File("allStackTraces.txt");
    allStacktracesFile.createNewFile();
    allStacktracesFile.deleteOnExit();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, allStacktracesFile.getCanonicalPath());
    String commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    CommandResult commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File mgrStacktraceFile = new File("managerStacktrace.txt");
    mgrStacktraceFile.createNewFile();
    mgrStacktraceFile.deleteOnExit();
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, mgrStacktraceFile.getCanonicalPath());
    csb.addOption(CliStrings.EXPORT_STACKTRACE__MEMBER, "Manager");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File serverStacktraceFile = new File("serverStacktrace.txt");
    serverStacktraceFile.createNewFile();
    serverStacktraceFile.deleteOnExit();
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, serverStacktraceFile.getCanonicalPath());
    csb.addOption(CliStrings.EXPORT_STACKTRACE__MEMBER, "Server");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File groupStacktraceFile = new File("groupstacktrace.txt");
    groupStacktraceFile.createNewFile();
    groupStacktraceFile.deleteOnExit();
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, groupStacktraceFile.getCanonicalPath());
    csb.addOption(CliStrings.EXPORT_STACKTRACE__GROUP, "G2");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertTrue(commandResult.getStatus().equals(Status.OK));

    File wrongStackTraceFile = new File("wrongStackTrace.txt");
    wrongStackTraceFile.createNewFile();
    wrongStackTraceFile.deleteOnExit();
    csb = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);
    csb.addOption(CliStrings.EXPORT_STACKTRACE__FILE, wrongStackTraceFile.getCanonicalPath());
    csb.addOption(CliStrings.EXPORT_STACKTRACE__MEMBER, "WrongMember");
    commandString = csb.toString();
    getLogWriter().info("CommandString : " + commandString);
    commandResult = executeCommand(commandString);
    getLogWriter().info("Output : \n" + commandResultToString(commandResult));
    assertFalse(commandResult.getStatus().equals(Status.OK));
  }
}
