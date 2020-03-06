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
package org.apache.geode.management.internal.cli;

import java.io.PrintStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.lang3.StringUtils;
import org.springframework.shell.core.ExitShellRequest;

import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * Launcher class for :
 * <ul>
 * <li>gfsh 7.0
 * <li>server
 * <li>locator
 * <li>Tools (Pulse, VSD, JConsole, JVisualVM)
 * <li>Running Command Line Interface (CLI) Commands from OS prompt like
 * <ol>
 * <li>
 * <ul>
 * <li>compact offline-disk-store - Compact an offline disk store. If the disk store is large,
 * additional memory may need to be allocated to the process using the --J=-Xmx??? parameter.
 * <li>describe offline-disk-store - Display information about an offline disk store.
 * <li>encrypt password - Encrypt a password for use in data source configuration.
 * <li>run - Execute a set of GFSH commands. Commands that normally prompt for additional input will
 * instead use default values.
 * <li>start jconsole - Start the JDK's JConsole tool in a separate process. JConsole will be
 * launched, but connecting to GemFire must be done manually.
 * <li>start jvisualvm - Start the JDK's Java VisualVM (jvisualvm) tool in a separate process. Java
 * VisualVM will be launched, but connecting to GemFire must be done manually.
 * <li>start locator - Start a Locator.
 * <li>start pulse - Open a new window in the default Web browser with the URL for the Pulse
 * application.
 * <li>start server - Start a GemFire Cache Server.
 * <li>start vsd - Start VSD in a separate process.
 * <li>status locator - Display the status of a Locator. Possible statuses are: started, online,
 * offline or not responding.
 * <li>status server - Display the status of a GemFire Cache Server.
 * <li>stop locator - Stop a Locator.
 * <li>stop server - Stop a GemFire Cache Server.
 * <li>validate offline-disk-store - Scan the contents of a disk store to verify that it has no
 * errors.
 * <li>version - Display product version information.
 * </ul>
 * </li>
 * <li>multiple commands specified using an option "-e"
 * </ol>
 * </ul>
 *
 * @since GemFire 7.0
 */
public class Launcher {
  private static final String EXECUTE_OPTION = "execute";
  private static final String HELP_OPTION = "help";
  private static final String HELP = CliStrings.HELP;

  private static final String MSG_INVALID_COMMAND_OR_OPTION = "Invalid command or option : {0}."
      + GfshParser.LINE_SEPARATOR + "Use 'gfsh help' to display additional information.";
  public static final String SEPARATOR = ", ";

  private final Set<String> allowedCommandLineCommands;
  private final OptionParser commandLineParser;
  private StartupTimeLogHelper startupTimeLogHelper;

  protected Launcher() {
    this.startupTimeLogHelper = new StartupTimeLogHelper();
    this.allowedCommandLineCommands = new HashSet<>();
    this.allowedCommandLineCommands.add(CliStrings.RUN);
    this.allowedCommandLineCommands.add(CliStrings.START_PULSE);
    this.allowedCommandLineCommands.add(CliStrings.START_JCONSOLE);
    this.allowedCommandLineCommands.add(CliStrings.START_JVISUALVM);
    this.allowedCommandLineCommands.add(CliStrings.START_LOCATOR);
    this.allowedCommandLineCommands.add(CliStrings.START_MANAGER);
    this.allowedCommandLineCommands.add(CliStrings.START_SERVER);
    this.allowedCommandLineCommands.add(CliStrings.START_VSD);
    this.allowedCommandLineCommands.add(CliStrings.STATUS_LOCATOR);
    this.allowedCommandLineCommands.add(CliStrings.STATUS_SERVER);
    this.allowedCommandLineCommands.add(CliStrings.STOP_LOCATOR);
    this.allowedCommandLineCommands.add(CliStrings.STOP_SERVER);
    this.allowedCommandLineCommands.add(CliStrings.VERSION);
    this.allowedCommandLineCommands.add(CliStrings.COMPACT_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.DESCRIBE_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.EXPORT_OFFLINE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.VALIDATE_DISK_STORE);
    this.allowedCommandLineCommands.add(CliStrings.PDX_DELETE_FIELD);
    this.allowedCommandLineCommands.add(CliStrings.PDX_RENAME);

    this.commandLineParser = new OptionParser();
    this.commandLineParser.accepts(EXECUTE_OPTION).withOptionalArg().ofType(String.class);
    this.commandLineParser.accepts(HELP_OPTION).withOptionalArg().ofType(Boolean.class);
    this.commandLineParser.posixlyCorrect(false);
  }

  public static void main(final String[] args) {
    // first check whether required dependencies exist in the classpath
    // should we start without tomcat/servlet jars?
    String nonExistingDependency = CliUtil.cliDependenciesExist(true);
    if (nonExistingDependency != null) {
      System.err.println("Required (" + nonExistingDependency
          + ") libraries not found in the classpath. gfsh can't start.");
      return;
    }

    Launcher launcher = new Launcher();
    int exitValue = launcher.parseCommandLine(args);
    ExitCode.fromValue(exitValue).doSystemExit();
  }

  private int parseCommandLineCommand(final String... args) {
    Gfsh gfsh = null;
    try {
      gfsh = Gfsh.getInstance(false, args, new GfshConfig());
      this.startupTimeLogHelper.logStartupTime();
    } catch (IllegalStateException isex) {
      System.err.println("ERROR : " + isex.getMessage());
    }

    ExitShellRequest exitRequest = ExitShellRequest.NORMAL_EXIT;

    if (gfsh != null) {
      final String commandLineCommand = combineStrings(args);

      if (commandLineCommand.startsWith(HELP)) {
        if (commandLineCommand.equals(HELP)) {
          printUsage(gfsh, System.out);
        } else {
          // help is also available for commands which are not available under
          // allowedCommandLineCommands
          gfsh.executeCommand(commandLineCommand);
        }
      } else {
        boolean commandIsAllowed = false;
        for (String allowedCommandLineCommand : this.allowedCommandLineCommands) {
          if (commandLineCommand.startsWith(allowedCommandLineCommand)) {
            commandIsAllowed = true;
            break;
          }
        }

        if (!commandIsAllowed) {
          System.err.println(
              CliStrings.format(MSG_INVALID_COMMAND_OR_OPTION, StringUtils.join(args, SEPARATOR)));
          exitRequest = ExitShellRequest.FATAL_EXIT;
        } else {
          if (!gfsh.executeScriptLine(commandLineCommand)) {
            if (gfsh.getLastExecutionStatus() != 0)
              exitRequest = ExitShellRequest.FATAL_EXIT;
          } else if (gfsh.getLastExecutionStatus() != 0) {
            exitRequest = ExitShellRequest.FATAL_EXIT;
          }
        }
      }
    }

    return exitRequest.getExitCode();
  }

  private int parseOptions(final String... args) {
    OptionSet parsedOptions;
    try {
      parsedOptions = this.commandLineParser.parse(args);
    } catch (OptionException e) {
      System.err.println(
          CliStrings.format(MSG_INVALID_COMMAND_OR_OPTION, StringUtils.join(args, SEPARATOR)));
      return ExitShellRequest.FATAL_EXIT.getExitCode();
    }
    boolean launchShell = true;
    boolean onlyPrintUsage = parsedOptions.has(HELP_OPTION);
    if (parsedOptions.has(EXECUTE_OPTION) || onlyPrintUsage) {
      launchShell = false;
    }

    Gfsh gfsh = null;
    try {
      gfsh = Gfsh.getInstance(launchShell, args, new GfshConfig());
      this.startupTimeLogHelper.logStartupTime();
    } catch (IllegalStateException isex) {
      System.err.println("ERROR : " + isex.getMessage());
    }

    ExitShellRequest exitRequest = ExitShellRequest.NORMAL_EXIT;

    if (gfsh != null) {
      try {
        if (launchShell) {
          gfsh.start();
          gfsh.waitForComplete();
          exitRequest = gfsh.getExitShellRequest();
        } else if (onlyPrintUsage) {
          printUsage(gfsh, System.out);
        } else {
          @SuppressWarnings("unchecked")
          List<String> commandsToExecute = (List<String>) parsedOptions.valuesOf(EXECUTE_OPTION);

          // Execute all of the commands in the list, one at a time.
          for (int i = 0; i < commandsToExecute.size()
              && exitRequest == ExitShellRequest.NORMAL_EXIT; i++) {
            String command = commandsToExecute.get(i);
            // sanitize the output string to not show the password
            System.out.println(GfshParser.LINE_SEPARATOR + "(" + (i + 1) + ") Executing - "
                + ArgumentRedactor.redact(command) + GfshParser.LINE_SEPARATOR);
            if (!gfsh.executeScriptLine(command) || gfsh.getLastExecutionStatus() != 0) {
              exitRequest = ExitShellRequest.FATAL_EXIT;
            }
          }
        }
      } catch (InterruptedException iex) {
        log(iex, gfsh);
      }
    }

    return exitRequest.getExitCode();
  }

  private int parseCommandLine(final String... args) {
    if (args.length > 0 && !args[0].startsWith(GfshParser.SHORT_OPTION_SPECIFIER)) {
      return parseCommandLineCommand(args);
    }

    return parseOptions(args);
  }

  private void log(Throwable t, Gfsh gfsh) {
    if (!(gfsh != null && gfsh.logToFile(t.getMessage(), t))) {
      t.printStackTrace();
    }
  }

  private String combineStrings(String... strings) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String string : strings) {
      stringBuilder.append(string).append(" ");
    }

    return stringBuilder.toString().trim();
  }

  private void printUsage(final Gfsh gfsh, final PrintStream stream) {
    int terminalWidth = gfsh.getTerminalWidth();
    stream.print("Pivotal GemFire(R) v");
    stream.print(GemFireVersion.getGemFireVersion());
    stream.println(" Command Line Shell" + GfshParser.LINE_SEPARATOR);
    stream.println("USAGE");
    stream.println(
        "gfsh [ <command> [option]* | <help> [command] | [--help | -h] | [-e \"<command> [option]*\"]* ]"
            + GfshParser.LINE_SEPARATOR);
    stream.println("OPTIONS");
    stream.println("-e  Execute a command");
    stream.println(Gfsh.wrapText(
        "Commands may be any that are available from the interactive gfsh prompt.  "
            + "For commands that require a Manager to complete, the first command in the list must be \"connect\".",
        1, terminalWidth));
    stream.println("EXAMPLES");
    stream.println("gfsh");
    stream.println(Gfsh.wrapText("Start GFSH in interactive mode.", 1, terminalWidth));
    stream.println("gfsh -h");
    stream.println(Gfsh.wrapText(
        "Displays 'this' help. ('gfsh --help' or 'gfsh help' is equivalent)", 1, terminalWidth));
    stream.println("gfsh help start locator");
    stream.println(
        Gfsh.wrapText("Display help for the \"start locator\" command.", 1, terminalWidth));
    stream.println("gfsh start locator --name=locator1");
    stream.println(Gfsh.wrapText("Start a Locator with the name \"locator1\".", 1, terminalWidth));
    stream.println("gfsh -e \"connect\" -e \"list members\"");
    stream.println(Gfsh.wrapText(
        "Connect to a running Locator using the default connection information and run the \"list members\" command.",
        1, terminalWidth));
    stream.println();
  }

  private static class StartupTimeLogHelper {
    private final long start = System.currentTimeMillis();
    private long done;

    public void logStartupTime() {
      done = System.currentTimeMillis();
      LogWrapper.getInstance(null)
          .info("Startup done in " + ((done - start) / 1000.0) + " seconds.");
    }

    @Override
    public String toString() {
      return StartupTimeLogHelper.class.getName() + " [start=" + start + ", done=" + done + "]";
    }
  }
}
