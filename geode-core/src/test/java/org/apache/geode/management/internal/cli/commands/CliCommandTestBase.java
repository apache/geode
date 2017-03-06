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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.parser.CommandTarget;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for all the CLI/gfsh command dunit tests.
 * 
 * @deprecated use LocatorServerStartupRule and GfshShellConnectorRule instead.
 */
public abstract class CliCommandTestBase extends JUnit4CacheTestCase {

  public static final String USE_HTTP_SYSTEM_PROPERTY = "useHTTP";

  private boolean useHttpOnConnect = Boolean.getBoolean(USE_HTTP_SYSTEM_PROPERTY);

  private ManagementService managementService;

  private transient HeadlessGfsh shell;

  protected transient int httpPort;
  protected transient int jmxPort;
  protected transient String jmxHost;
  protected transient String gfshDir;

  @Rule
  public transient DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  public final void postSetUp() throws Exception {
    setUpCliCommandTestBase();
    postSetUpCliCommandTestBase();
  }

  private void setUpCliCommandTestBase() throws Exception {
    this.gfshDir = this.temporaryFolder.newFolder("gfsh_files").getCanonicalPath();
  }

  protected void postSetUpCliCommandTestBase() throws Exception {}

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    preTearDownCliCommandTestBase();
    destroyDefaultSetup();
  }

  protected void preTearDownCliCommandTestBase() throws Exception {}

  /**
   * Create all of the components necessary for the default setup. The provided properties will be
   * used when creating the default cache. This will create GFSH in the controller VM (VM[4]) (no
   * cache) and the manager in VM[0] (with cache). When adding regions, functions, keys, whatever to
   * your cache for tests, you'll need to use Host.getHost(0).getVM(0).invoke(new
   * SerializableRunnable() { public void run() { ... } } in order to have this setup run in the
   * same VM as the manager.
   *
   * @param props the Properties used when creating the cache for this default setup.
   * @return the default testable GemFire shell.
   */
  @SuppressWarnings("serial")
  protected HeadlessGfsh setUpJmxManagerOnVm0ThenConnect(final Properties props) {
    Object[] result = setUpJMXManagerOnVM(0, props);
    this.jmxHost = (String) result[0];
    this.jmxPort = (Integer) result[1];
    this.httpPort = (Integer) result[2];
    connect(this.jmxHost, this.jmxPort, this.httpPort, getDefaultShell());
    return shell;
  }

  protected Object[] setUpJMXManagerOnVM(int vm, final Properties props) {
    return setUpJMXManagerOnVM(vm, props, null);
  }

  /**
   * @return an object array, result[0] is jmxHost(String), result[1] is jmxPort, result[2] is
   *         httpPort
   */
  protected Object[] setUpJMXManagerOnVM(int vm, final Properties props, String jsonFile) {
    Object[] result = Host.getHost(0).getVM(vm).invoke("setUpJmxManagerOnVm" + vm, () -> {
      final Object[] results = new Object[3];
      final Properties localProps = (props != null ? props : new Properties());

      try {
        jmxHost = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException ignore) {
        jmxHost = "localhost";
      }

      if (!localProps.containsKey(NAME)) {
        localProps.setProperty(NAME, "Manager");
      }

      if (jsonFile != null) {
        localProps.setProperty(TestSecurityManager.SECURITY_JSON, jsonFile);
      }

      final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

      jmxPort = ports[0];
      httpPort = ports[1];

      localProps.setProperty(JMX_MANAGER, "true");
      localProps.setProperty(JMX_MANAGER_START, "true");
      localProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
      localProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
      localProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

      getSystem(localProps);
      verifyManagementServiceStarted(getCache());

      IgnoredException.addIgnoredException("org.eclipse.jetty.io.EofException");
      IgnoredException.addIgnoredException("java.nio.channels.ClosedChannelException");

      results[0] = jmxHost;
      results[1] = jmxPort;
      results[2] = httpPort;
      return results;
    });

    return result;
  }

  /**
   * Destroy all of the components created for the default setup.
   */
  protected final void destroyDefaultSetup() {
    if (this.shell != null) {
      executeCommand(shell, "exit");
      this.shell.terminate();
      this.shell = null;
    }

    disconnectAllFromDS();

    Host.getHost(0).getVM(0).invoke("verify service stopped",
        () -> verifyManagementServiceStopped());
  }

  /**
   * Start the default management service using the provided Cache.
   *
   * @param cache Cache to use when creating the management service
   */
  private void verifyManagementServiceStarted(Cache cache) {
    assertTrue(cache != null);

    this.managementService = ManagementService.getExistingManagementService(cache);
    assertNotNull(this.managementService);
    assertTrue(this.managementService.isManager());
    assertTrue(checkIfCommandsAreLoadedOrNot());
  }

  public static boolean checkIfCommandsAreLoadedOrNot() {
    CommandManager manager;
    try {
      manager = CommandManager.getInstance();
      Map<String, CommandTarget> commands = manager.getCommands();
      if (commands.size() < 1) {
        return false;
      }
      return true;
    } catch (ClassNotFoundException | IOException e) {
      throw new RuntimeException("Could not load commands", e);
    }
  }

  /**
   * Stop the default management service.
   */
  private void verifyManagementServiceStopped() {
    if (this.managementService != null) {
      assertFalse(this.managementService.isManager());
      this.managementService = null;
    }
  }

  protected void connect(final String host, final int jmxPort, final int httpPort,
      HeadlessGfsh shell) {
    connect(host, jmxPort, httpPort, shell, null, null);
  }

  protected void connect(final String host, final int jmxPort, final int httpPort,
      HeadlessGfsh shell, String username, String password) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CONNECT);

    String endpoint;
    if (useHttpOnConnect) {
      endpoint = "http://" + host + ":" + httpPort + "/gemfire/v1";
      command.addOption(CliStrings.CONNECT__USE_HTTP, Boolean.TRUE.toString());
      command.addOption(CliStrings.CONNECT__URL, endpoint);
    } else {
      endpoint = host + "[" + jmxPort + "]";
      command.addOption(CliStrings.CONNECT__JMX_MANAGER, endpoint);
    }
    if (username != null) {
      command.addOption(CliStrings.CONNECT__USERNAME, username);
    }
    if (password != null) {
      command.addOption(CliStrings.CONNECT__PASSWORD, password);
    }
    System.out.println(getClass().getSimpleName() + " using endpoint: " + endpoint);

    CommandResult result = executeCommand(shell, command.toString());

    if (!shell.isConnectedAndReady()) {
      throw new AssertionError("Connect command failed to connect to manager " + endpoint
          + " result=" + commandResultToString(result));
    }

    info("Successfully connected to managing node using " + (useHttpOnConnect ? "HTTP" : "JMX"));
    assertEquals(true, shell.isConnectedAndReady());
  }

  /**
   * Get the default shell (will create one if it doesn't already exist).
   *
   * @return The default shell
   */
  protected synchronized final HeadlessGfsh getDefaultShell() {
    if (this.shell == null) {
      this.shell = createShell();
    }

    return this.shell;
  }

  /**
   * Create a HeadlessGfsh object.
   *
   * @return The created shell.
   */
  protected HeadlessGfsh createShell() {
    try {
      Gfsh.SUPPORT_MUTLIPLESHELL = true;
      String shellId = getClass().getSimpleName() + "_" + getName();
      HeadlessGfsh shell = new HeadlessGfsh(shellId, 30, this.gfshDir);
      // Added to avoid trimming of the columns
      info("Started testable shell: " + shell);
      return shell;
    } catch (ClassNotFoundException e) {
      throw new AssertionError(e);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Execute a command using the default shell and clear the shell events before returning.
   *
   * @param command Command to execute
   * @return The result of the command execution
   */
  protected CommandResult executeCommand(String command) {
    assert (command != null);

    return executeCommand(getDefaultShell(), command);
  }

  /**
   * Execute a command in the provided shell and clear the shell events before returning.
   *
   * @param shell Shell in which to execute the command.
   * @param command Command to execute
   * @return The result of the command execution
   */
  protected CommandResult executeCommand(HeadlessGfsh shell, String command) {
    assert (shell != null);
    assert (command != null);

    CommandResult commandResult = executeCommandWithoutClear(shell, command);
    shell.clearEvents();
    return commandResult;
  }

  /**
   * Execute a command using the default shell. Useful for getting additional information from the
   * shell after the command has been executed (using getDefaultShell().???). Caller is responsible
   * for calling getDefaultShell().clearEvents() when done.
   *
   * @param command Command to execute
   * @return The result of the command execution
   */
  @SuppressWarnings("unused")
  protected CommandResult executeCommandWithoutClear(String command) {
    assert (command != null);

    return executeCommandWithoutClear(getDefaultShell(), command);
  }

  /**
   * Execute a command in the provided shell. Useful for getting additional information from the
   * shell after the command has been executed (using getDefaultShell().???). Caller is responsible
   * for calling getDefaultShell().clearEvents() when done.
   *
   * @param shell Shell in which to execute the command.
   * @param command Command to execute
   * @return The result of the command execution
   */
  protected CommandResult executeCommandWithoutClear(HeadlessGfsh shell, String command) {
    assert (shell != null);
    assert (command != null);

    try {
      info("Executing command " + command + " with command Mgr " + CommandManager.getInstance());
    } catch (ClassNotFoundException cnfex) {
      throw new AssertionError(cnfex);
    } catch (IOException ioex) {
      throw new AssertionError(ioex);
    }

    shell.executeCommand(command);
    if (shell.hasError()) {
      error("executeCommand completed with error : " + shell.getError());
    }

    CommandResult result = null;
    try {
      result = (CommandResult) shell.getResult(); // TODO: this can result in ClassCastException if
                                                  // command resulted in error
    } catch (InterruptedException ex) {
      error("shell received InterruptedException");
    }

    if (result != null) {
      result.resetToFirstLine();
    }

    return result;
  }

  /**
   * Utility method for viewing the results of a command.
   *
   * @param commandResult Results to dump
   * @param printStream Stream to dump the results to
   */
  protected void printResult(final CommandResult commandResult, PrintStream printStream) {
    assert (commandResult != null);
    assert (printStream != null);

    commandResult.resetToFirstLine();
    printStream.print(commandResultToString(commandResult));
  }

  protected static String commandResultToString(final CommandResult commandResult) {
    assertNotNull(commandResult);

    commandResult.resetToFirstLine();

    StringBuilder buffer = new StringBuilder(commandResult.getHeader());

    while (commandResult.hasNextLine()) {
      buffer.append(commandResult.nextLine());
    }

    buffer.append(commandResult.getFooter());

    return buffer.toString();
  }

  /**
   * Utility method for finding the CommandResult object in the Map of CommandOutput objects.
   *
   * @param commandOutput CommandOutput Map to search
   * @return The CommandResult object or null if not found.
   */
  protected CommandResult extractCommandResult(Map<String, Object> commandOutput) {
    assert (commandOutput != null);

    for (Object resultObject : commandOutput.values()) {
      if (resultObject instanceof CommandResult) {
        CommandResult result = (CommandResult) resultObject;
        result.resetToFirstLine();
        return result;
      }
    }
    return null;
  }

  /**
   * Utility method to determine how many times a string occurs in another string. Note that when
   * looking for matches substrings of other matches will be counted as a match. For example,
   * looking for "AA" in the string "AAAA" will result in a return value of 3.
   *
   * @param stringToSearch String to search
   * @param stringToCount String to look for and count
   * @return The number of matches.
   */
  protected int countMatchesInString(final String stringToSearch, final String stringToCount) {
    assert (stringToSearch != null);
    assert (stringToCount != null);

    int length = stringToSearch.length();
    int count = 0;
    for (int i = 0; i < length; i++) {
      if (stringToSearch.substring(i).startsWith(stringToCount)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Determines if a string contains a trimmed line that matches the pattern. So, any single line
   * whose leading and trailing spaces have been removed which contains a string that exactly
   * matches the given pattern will be considered a match.
   *
   * @param stringToSearch String to search
   * @param stringPattern Pattern to search for
   * @return True if a match is found, false otherwise
   */
  protected boolean stringContainsLine(final String stringToSearch, final String stringPattern) {
    assert (stringToSearch != null);
    assert (stringPattern != null);

    Pattern pattern = Pattern.compile("^\\s*" + stringPattern + "\\s*$", Pattern.MULTILINE);
    Matcher matcher = pattern.matcher(stringToSearch);
    return matcher.find();
  }

  /**
   * Counts the number of distinct lines in a String.
   *
   * @param stringToSearch String to search for lines.
   * @param countBlankLines Whether to count blank lines (true to count)
   * @return The number of lines found.
   */
  protected int countLinesInString(final String stringToSearch, final boolean countBlankLines) {
    assert (stringToSearch != null);

    int length = stringToSearch.length();
    int count = 0;
    char character = 0;
    boolean foundNonSpaceChar = false;

    for (int i = 0; i < length; i++) {
      character = stringToSearch.charAt(i);
      if (character == '\r' && (i + 1) < length && stringToSearch.charAt(i + 1) == '\n') {
        i++;
      }
      if (character == '\n' || character == '\r') {
        if (countBlankLines) {
          count++;
        } else {
          if (foundNonSpaceChar) {
            count++;
          }
        }
        foundNonSpaceChar = false;
      } else if (character != ' ' && character != '\t') {
        foundNonSpaceChar = true;
      }
    }

    // Even if the last line isn't terminated, it still counts as a line
    if (character != '\n' && character != '\r') {
      count++;
    }

    return count;
  }

  /**
   * Get a specific line from the string (using \n or \r as a line separator).
   *
   * @param stringToSearch String to get the line from
   * @param lineNumber Line number to get
   * @return The line
   */
  protected String getLineFromString(final String stringToSearch, final int lineNumber) {
    assert (stringToSearch != null);
    assert (lineNumber > 0);

    int length = stringToSearch.length();
    int count = 0;
    int startIndex = 0;
    char character;
    int endIndex = length;

    for (int i = 0; i < length; i++) {
      character = stringToSearch.charAt(i);
      if (character == '\r' && (i + 1) < length && stringToSearch.charAt(i + 1) == '\n') {
        i++;
      }
      if (character == '\n' || character == '\r') {
        if (lineNumber == 1) {
          endIndex = i;
          break;
        }
        if (++count == lineNumber - 1) {
          startIndex = i + 1;
        } else if (count >= lineNumber) {
          endIndex = i;
          break;
        }
      }
    }

    return stringToSearch.substring(startIndex, endIndex);
  }

  protected void info(String string) {
    getLogWriter().info(string);
  }

  protected void debug(String string) {
    getLogWriter().fine(string);
  }

  protected void error(String string) {
    getLogWriter().error(string);
  }

  protected void error(String string, Throwable e) {
    getLogWriter().error(string, e);
  }
}
