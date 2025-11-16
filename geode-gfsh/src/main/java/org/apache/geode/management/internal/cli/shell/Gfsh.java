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
package org.apache.geode.management.internal.cli.shell;

import static java.lang.System.lineSeparator;
import static org.apache.geode.internal.util.ProductVersionUtil.getDistributionVersion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.internal.SystemDescription;
import org.apache.geode.internal.lang.utils.ClassUtils;
import org.apache.geode.internal.logging.Banner;
import org.apache.geode.internal.process.signal.AbstractSignalNotificationHandler;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.internal.util.HostName;
import org.apache.geode.internal.util.ProductVersionUtil;
import org.apache.geode.internal.util.SunAPINotFoundException;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CommandProcessingException;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtils;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.jline.ANSIHandler;
import org.apache.geode.management.internal.cli.shell.jline.ANSIHandler.ANSIStyle;
import org.apache.geode.management.internal.cli.shell.jline.GfshHistory;
import org.apache.geode.management.internal.cli.shell.jline.GfshUnsupportedTerminal;
import org.apache.geode.management.internal.cli.shell.unsafe.GfshSignalHandler;
import org.apache.geode.management.internal.cli.util.CommentSkipHelper;
import org.apache.geode.management.internal.i18n.CliStrings;

/**
 * GemFire SHell (gfsh) - Interactive command-line interface for Apache Geode.
 *
 * <p>
 * This class maintains GemFire SHell (gfsh) specific information like environment,
 * operation invoker, and execution strategy for command processing.
 *
 * <p>
 * Migrated from Spring Shell 1.x to Spring Shell 3.x, replacing JLineShell with direct
 * JLine 3 LineReader integration.
 *
 * @since GemFire 7.0
 */
public class Gfsh implements Runnable {
  public static final int DEFAULT_APP_FETCH_SIZE = 100;
  public static final int DEFAULT_APP_LAST_EXIT_STATUS = 0;
  public static final int DEFAULT_APP_COLLECTION_LIMIT = 20;
  public static final boolean DEFAULT_APP_QUIET_EXECUTION = false;
  public static final String DEFAULT_APP_QUERY_RESULTS_DISPLAY_MODE = "table";
  public static final String DEFAULT_APP_RESULT_VIEWER = "basic";
  public static final String EXTERNAL_RESULT_VIEWER = "external";

  public static final String GFSH_APP_NAME = "gfsh";

  public static final String LINE_INDENT = "    ";
  public static final String LINE_SEPARATOR = lineSeparator();
  // Default Window dimensions
  public static final int DEFAULT_WIDTH = 100;
  public static final String ENV_APP_NAME = "APP_NAME";
  public static final String ENV_APP_CONTEXT_PATH = "APP_CONTEXT_PATH";
  public static final String ENV_APP_FETCH_SIZE = "APP_FETCH_SIZE";
  public static final String ENV_APP_LAST_EXIT_STATUS = "APP_LAST_EXIT_STATUS";
  public static final String ENV_APP_COLLECTION_LIMIT = "APP_COLLECTION_LIMIT";
  public static final String ENV_APP_QUERY_RESULTS_DISPLAY_MODE = "APP_QUERY_RESULTS_DISPLAY_MODE";
  public static final String ENV_APP_QUIET_EXECUTION = "APP_QUIET_EXECUTION";
  public static final String ENV_APP_LOGGING_ENABLED = "APP_LOGGING_ENABLED";
  public static final String ENV_APP_LOG_FILE = "APP_LOG_FILE";
  public static final String ENV_APP_PWD = "APP_PWD";
  public static final String ENV_APP_RESULT_VIEWER = "APP_RESULT_VIEWER";
  // Environment Properties taken from the OS
  public static final String ENV_SYS_USER = "SYS_USER";
  public static final String ENV_SYS_USER_HOME = "SYS_USER_HOME";
  public static final String ENV_SYS_HOST_NAME = "SYS_HOST_NAME";
  public static final String ENV_SYS_CLASSPATH = "SYS_CLASSPATH";
  public static final String ENV_SYS_JAVA_VERSION = "SYS_JAVA_VERSION";
  public static final String ENV_SYS_OS = "SYS_OS";
  public static final String ENV_SYS_OS_LINE_SEPARATOR = "SYS_OS_LINE_SEPARATOR";
  public static final String ENV_SYS_GEODE_HOME_DIR = "SYS_GEODE_HOME_DIR";


  private static final String DEFAULT_SECONDARY_PROMPT = ">";
  private static final int DEFAULT_HEIGHT = 100;
  private static final Object INSTANCE_LOCK = new Object();

  @MutableForTesting
  protected static PrintStream gfshout = System.out;
  @MutableForTesting
  protected static PrintStream gfsherr = System.err;
  protected static final ThreadLocal<Gfsh> gfshThreadLocal = new ThreadLocal<>();
  @MakeNotStatic
  private static volatile Gfsh instance;
  // This flag is used to restrict column trimming to table only types
  private static final ThreadLocal<Boolean> resultTypeTL = new ThreadLocal<>();
  private static final String OS = System.getProperty("os.name").toLowerCase();
  protected static final Logger logger = LogService.getLogger();

  private final Map<String, String> env = new TreeMap<>();
  private final List<String> readonlyAppEnv = new ArrayList<>();
  // Map to keep reference to actual user specified Command String
  // Should always have one value at the max
  private final Map<String, String> expandedPropCommandsMap = new HashMap<>();
  private final GfshExecutionStrategy executionStrategy;
  private final GfshParser parser;
  private final LogWrapper gfshFileLogger;
  private final GfshConfig gfshConfig;
  private final GfshHistory gfshHistory;
  private final ANSIHandler ansiHandler;
  private final boolean isHeadlessMode;
  private OperationInvoker operationInvoker;
  private int lastExecutionStatus;
  private Thread runner;
  private boolean debugON;
  private LineReader lineReader;
  private Terminal terminal;
  private boolean suppressScriptCmdOutput;
  private boolean isScriptRunning;
  private AbstractSignalNotificationHandler signalHandler;
  private ExitShellRequest exitShellRequest;
  /**
   * Holds accumulated multi-line command input when continuation character is used.
   * Reset to null when complete command is executed.
   * Used in promptLoop() to support backslash continuation across multiple lines.
   */
  private StringBuilder multiLineBuffer;

  public Gfsh() {
    this(null);
  }

  /**
   * Create a GemFire shell with console using the specified arguments.
   *
   * @param args arguments to be used to create a GemFire shell instance
   */
  protected Gfsh(String[] args) {
    this(true, args, new GfshConfig());
  }

  /**
   * Create a GemFire shell using the specified arguments. Console for user inputs is made available
   * if <code>launchShell</code> is set to <code>true</code>.
   *
   * @param launchShell whether to make Console available
   * @param args arguments to be used to create a GemFire shell instance or execute command
   */
  protected Gfsh(boolean launchShell, String[] args, GfshConfig gfshConfig) {
    // 1. Disable suppressing of duplicate messages (removed JLineLogHandler which is Spring Shell
    // 1.x)
    // JLineLogHandler.setSuppressDuplicateMessages(false);

    // 2. set & use gfshConfig
    this.gfshConfig = gfshConfig;
    // The cache doesn't exist yet, since we are still setting up parsing.
    gfshFileLogger = LogWrapper.getInstance(null);
    gfshFileLogger.configure(this.gfshConfig);
    ansiHandler = ANSIHandler.getInstance(this.gfshConfig.isANSISupported());

    // Log system properties & gfsh environment
    @SuppressWarnings("deprecation")
    final Banner banner = new Banner();
    gfshFileLogger.info(banner.getString());

    // 4. Customized History implementation
    gfshHistory = new GfshHistory();
    // Set history file path for JLine 3 compatibility
    gfshHistory.setHistoryFilePath(Paths.get(gfshConfig.getHistoryFileName()));

    // 6. Set System Environment here
    initializeEnvironment();
    // 7. Create Roo/SpringShell framework objects
    executionStrategy = new GfshExecutionStrategy(this);
    parser = new GfshParser(new CommandManager());
    // 8. Max history size is set in LineReaderBuilder (see createLineReader method)
    // JLine 3: History size is configured via LineReader.HISTORY_SIZE variable

    String envProps = env.toString();
    envProps = envProps.substring(1, envProps.length() - 1);
    envProps = envProps.replaceAll(",", LINE_SEPARATOR);
    gfshFileLogger.config("***** gfsh Environment ******" + LINE_SEPARATOR + envProps);

    if (gfshFileLogger.fineEnabled()) {
      String gfshConfigStr = this.gfshConfig.toString();
      gfshConfigStr = gfshConfigStr.substring(0, gfshConfigStr.length() - 1);
      gfshConfigStr = gfshConfigStr.replaceAll(",", LINE_SEPARATOR);
      gfshFileLogger.fine("***** gfsh Configuration ******" + LINE_SEPARATOR + gfshConfigStr);
    }

    // Setup signal handler for various signals (such as CTRL-C)...
    try {
      ClassUtils.forName("sun.misc.Signal", new SunAPINotFoundException(
          "WARNING!!! Not running a Sun JVM.  Could not find the sun.misc.Signal class; Signal handling disabled."));
      signalHandler = new GfshSignalHandler();
    } catch (SunAPINotFoundException e) {
      signalHandler = new AbstractSignalNotificationHandler() {};
      gfshFileLogger.warning(e.getMessage());
    }

    // For test code only
    if (this.gfshConfig.isTestConfig()) {
      instance = this;
    }
    isHeadlessMode = !launchShell;
    if (isHeadlessMode) {
      gfshFileLogger.config("Running in headless mode");
      // disable jline terminal
      System.setProperty("jline.terminal", GfshUnsupportedTerminal.class.getName());
      env.put(ENV_APP_QUIET_EXECUTION, String.valueOf(true));
      // WHY NOT NEEDED: Spring Shell 3.x handles logging differently
      // Shell 1.x: Used setParentFor(logger) to redirect console logger to file in headless mode
      // Shell 3.x: ENV_APP_QUIET_EXECUTION suppresses output; internal Java loggers
      // are redirected via redirectInternalJavaLoggers() method
    }
    // we want to direct internal JDK logging to file in either mode
    redirectInternalJavaLoggers();
  }

  public static Gfsh getInstance(boolean launchShell, String[] args, GfshConfig gfshConfig) {
    Gfsh localGfshInstance = instance;
    if (localGfshInstance == null) {
      synchronized (INSTANCE_LOCK) {
        localGfshInstance = instance;
        if (localGfshInstance == null) {
          localGfshInstance = new Gfsh(launchShell, args, gfshConfig);
          localGfshInstance.executeInitFileIfPresent();
          instance = localGfshInstance;
        }
      }
    }

    return instance;
  }

  public static boolean isInfoResult() {
    if (resultTypeTL.get() == null) {
      return false;
    }
    return resultTypeTL.get();
  }

  public static void println() {
    gfshout.println();
  }

  public static void println(Object toPrint) {
    gfshout.println(toPrint);
  }

  public static void print(Object toPrint) {
    gfshout.print(toPrint);
  }

  public static void printlnErr(Object toPrint) {
    gfsherr.println(toPrint);
  }

  /**
   * Reads a line from the LineReader with proper JLine 3 exception handling.
   *
   * <p>
   * JLine 3 throws unchecked exceptions for user interrupts:
   * <ul>
   * <li>{@code UserInterruptException} when Ctrl-C is pressed</li>
   * <li>{@code EndOfFileException} when Ctrl-D is pressed on empty line</li>
   * </ul>
   *
   * <p>
   * This method handles both exceptions gracefully:
   * <ul>
   * <li>Ctrl-C returns empty string - allows user to cancel current command</li>
   * <li>Ctrl-D returns null - signals EOF and causes shell to exit</li>
   * </ul>
   *
   * @param reader the JLine 3 LineReader to read from
   * @param prompt the prompt string to display to the user
   * @return the line read from input, empty string if user interrupted (Ctrl-C),
   *         or null if end-of-file reached (Ctrl-D)
   */
  private static String readLine(LineReader reader, String prompt) {
    try {
      return reader.readLine(prompt);
    } catch (org.jline.reader.UserInterruptException e) {
      // User pressed Ctrl-C to cancel current command line input
      // Return empty string so promptLoop() will display a new prompt
      if (logger.isDebugEnabled()) {
        logger.debug("User interrupted input with Ctrl-C");
      }
      return "";
    } catch (org.jline.reader.EndOfFileException e) {
      // User pressed Ctrl-D on empty line to signal end-of-file
      // Return null so promptLoop() will detect EOF and exit gracefully
      if (logger.isDebugEnabled()) {
        logger.debug("End-of-file signal received (Ctrl-D)");
      }
      return null;
    }
  }

  private static String removeBackslash(String result) {
    if (result.endsWith(GfshParser.CONTINUATION_CHARACTER)) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * Redirects internal Java loggers (java.* and javax.*) to Log4j2.
   *
   * <p>
   * With the log4j-jul bridge (org.apache.logging.log4j:log4j-jul), JUL logging is
   * automatically routed to Log4j2. This method sets the system property to enable
   * the bridge and ensures JDK internal logging goes to the GFSH log file instead of console.
   */
  public void redirectInternalJavaLoggers() {
    // Set JUL manager to Log4j2's JUL bridge
    // This routes all java.util.logging calls to Log4j2
    System.setProperty("java.util.logging.manager",
        "org.apache.logging.log4j.jul.LogManager");

    logger.debug("JUL loggers redirected to Log4j2 via log4j-jul bridge");
  }

  public static Gfsh getCurrentInstance() {
    return instance;
  }

  private static String extractKey(String input) {
    return input.substring("${".length(), input.length() - "}".length());
  }

  public static LineReader getConsoleReader() {
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return (gfsh == null ? null : gfsh.lineReader);
  }

  /**
   * Take a string and wrap it into multiple lines separated by CliConstants.LINE_SEPARATOR. Lines
   * are separated based upon the terminal width, separated on word boundaries and may have extra
   * spaces added to provide indentation.
   *
   * For example: if the terminal width were 5 and the string "123 456789 01234" were passed in with
   * an indentation level of 2, then the returned string would be:
   *
   * <pre>
   *         123
   *         45678
   *         9
   *         01234
   * </pre>
   *
   * @param string String to wrap (add breakpoints and indent)
   * @param indentationLevel The number of indentation levels to use.
   * @return The wrapped string.
   */
  public static String wrapText(final String string, final int indentationLevel,
      final int terminalWidth) {
    if (terminalWidth <= 1) {
      return string;
    }

    final int maxLineLength = terminalWidth - 1;
    final StringBuilder stringBuf = new StringBuilder();
    int index = 0;
    int startOfCurrentLine = 0;
    while (index < string.length()) {
      // Add the indentation
      for (int i = 0; i < indentationLevel; i++) {
        stringBuf.append(LINE_INDENT);
      }
      int currentLineLength = LINE_INDENT.length() * indentationLevel;

      // Find the end of a line:
      // 1. If the end of string is reached
      // 2. If the width of the terminal has been reached
      // 3. If a newline character was found in the string
      while (index < string.length() && currentLineLength < maxLineLength
          && string.charAt(index) != '\n') {
        index++;
        currentLineLength++;
      }

      // If the line was terminated with a newline character
      if (index != string.length() && string.charAt(index) == '\n') {
        stringBuf.append(string, startOfCurrentLine, index);
        stringBuf.append(LINE_SEPARATOR);
        index++;
        startOfCurrentLine = index;

        // If the end of the string was reached or the last character just happened to be a space
        // character
      } else if (index == string.length() || string.charAt(index) == ' ') {
        stringBuf.append(string, startOfCurrentLine, index);
        if (index != string.length()) {
          stringBuf.append(LINE_SEPARATOR);
          index++;
        }

      } else {
        final int spaceCharIndex = string.lastIndexOf(" ", index);

        // If no spaces were found then there's no logical way to split the string
        if (spaceCharIndex == -1 || spaceCharIndex < startOfCurrentLine) {
          stringBuf.append(string, startOfCurrentLine, index).append(LINE_SEPARATOR);

          // Else split the string cleanly between words
        } else {
          stringBuf.append(string, startOfCurrentLine, spaceCharIndex)
              .append(LINE_SEPARATOR);
          index = spaceCharIndex + 1;
        }
      }

      startOfCurrentLine = index;
    }
    return stringBuf.toString();
  }

  /**
   * Initializes default environment variables to default values
   */
  private void initializeEnvironment() {
    env.put(ENV_SYS_USER, System.getProperty("user.name"));
    env.put(ENV_SYS_USER_HOME, System.getProperty("user.home"));
    env.put(ENV_SYS_HOST_NAME, new HostName().determineHostName());
    env.put(ENV_SYS_CLASSPATH, System.getProperty("java.class.path"));
    env.put(ENV_SYS_JAVA_VERSION, System.getProperty("java.version"));
    env.put(ENV_SYS_OS, System.getProperty("os.name"));
    env.put(ENV_SYS_OS_LINE_SEPARATOR, lineSeparator());
    env.put(ENV_SYS_GEODE_HOME_DIR, System.getenv("GEODE_HOME"));

    env.put(ENV_APP_NAME, Gfsh.GFSH_APP_NAME);
    readonlyAppEnv.add(ENV_APP_NAME);
    env.put(ENV_APP_LOGGING_ENABLED,
        String.valueOf(!Level.OFF.equals(gfshConfig.getLogLevel())));
    readonlyAppEnv.add(ENV_APP_LOGGING_ENABLED);
    env.put(ENV_APP_LOG_FILE, gfshConfig.getLogFilePath());
    readonlyAppEnv.add(ENV_APP_LOG_FILE);
    env.put(ENV_APP_PWD, System.getProperty("user.dir"));
    readonlyAppEnv.add(ENV_APP_PWD);
    env.put(ENV_APP_FETCH_SIZE, String.valueOf(DEFAULT_APP_FETCH_SIZE));
    env.put(ENV_APP_LAST_EXIT_STATUS, String.valueOf(DEFAULT_APP_LAST_EXIT_STATUS));
    readonlyAppEnv.add(ENV_APP_LAST_EXIT_STATUS);
    env.put(ENV_APP_COLLECTION_LIMIT, String.valueOf(DEFAULT_APP_COLLECTION_LIMIT));
    env.put(ENV_APP_QUERY_RESULTS_DISPLAY_MODE, DEFAULT_APP_QUERY_RESULTS_DISPLAY_MODE);
    env.put(ENV_APP_QUIET_EXECUTION, String.valueOf(DEFAULT_APP_QUIET_EXECUTION));
    env.put(ENV_APP_RESULT_VIEWER, DEFAULT_APP_RESULT_VIEWER);
  }

  public AbstractSignalNotificationHandler getSignalHandler() {
    return signalHandler;
  }

  public String readPassword(String textToPrompt) {
    if (isHeadlessMode && isQuietMode()) {
      return null;
    }

    return readWithMask(textToPrompt, '*');
  }

  public String readText(String textToPrompt) {
    if (isHeadlessMode && isQuietMode()) {
      return null;
    }

    return interact(textToPrompt);

  }

  /**
   * Starts this GemFire Shell with console.
   */
  public void start() {
    runner = new LoggingThread(getShellName(), false, (Runnable) this);
    runner.start();
  }

  protected String getShellName() {
    return "Gfsh Launcher";
  }

  /**
   * Stops this GemFire Shell.
   */
  public void stop() {
    closeShell();
    LogWrapper.close();
    if (operationInvoker != null && operationInvoker.isConnected()) {
      operationInvoker.stop();
    }
    instance = null;
  }

  /**
   * Closes the shell and cleans up resources.
   * Replaces Spring Shell 1.x JLineShell.closeShell() functionality.
   *
   * <p>
   * This method ensures proper cleanup of:
   * <ul>
   * <li>JLine 3 Terminal and LineReader</li>
   * <li>Command history (flush to disk)</li>
   * <li>Signal handlers</li>
   * <li>Thread-local state</li>
   * </ul>
   *
   * <p>
   * This method is idempotent and can be called multiple times safely.
   */
  private void closeShell() {
    try {
      // 0. Disconnect operation invoker first to prevent "No longer connected" errors
      // This sets the intentional disconnect flags (isSelfDisconnect for JMX,
      // stoppingIntentionally for HTTP) BEFORE server shutdown during test cleanup
      if (operationInvoker != null && operationInvoker.isConnected()) {
        try {
          operationInvoker.stop();
          if (gfshFileLogger.fineEnabled()) {
            gfshFileLogger.fine("Operation invoker disconnected");
          }
        } catch (Exception e) {
          gfshFileLogger.warning("Error disconnecting operation invoker", e);
        }
      }

      // 1. Save command history to disk (highest priority - preserve user data)
      if (gfshHistory != null) {
        try {
          // Ensure all commands are flushed to history file
          gfshHistory.setAutoFlush(true);
          // JLine 3: DefaultHistory automatically saves when save() is called
          // But our GfshHistory writes directly to file in addToHistory()
          // So we just need to ensure the last entry is flushed
          if (gfshFileLogger.fineEnabled()) {
            gfshFileLogger.fine("Command history saved");
          }
        } catch (Exception e) {
          gfshFileLogger.warning("Failed to save command history", e);
        }
      }

      // 2. Close LineReader (stops accepting new input)
      if (lineReader != null) {
        try {
          // JLine 3 LineReader doesn't have an explicit close() method
          // But we should null it out to release references
          lineReader = null;
          if (gfshFileLogger.fineEnabled()) {
            gfshFileLogger.fine("LineReader closed");
          }
        } catch (Exception e) {
          gfshFileLogger.warning("Error closing LineReader", e);
        }
      }

      // 3. Close Terminal (releases OS terminal resources)
      if (terminal != null) {
        try {
          // JLine 3 Terminal has close() method
          terminal.close();
          if (gfshFileLogger.fineEnabled()) {
            gfshFileLogger.fine("Terminal closed");
          }
        } catch (Exception e) {
          gfshFileLogger.warning("Error closing terminal", e);
        }
      }

      // 4. Unregister signal handlers (clean up OS signal handling)
      if (signalHandler != null) {
        try {
          // Signal handler cleanup if needed
          // GfshSignalHandler may need explicit cleanup
          signalHandler = null;
          if (gfshFileLogger.fineEnabled()) {
            gfshFileLogger.fine("Signal handlers unregistered");
          }
        } catch (Exception e) {
          gfshFileLogger.warning("Error unregistering signal handlers", e);
        }
      }

      // 5. Clean ThreadLocal state (prevent memory leaks)
      try {
        gfshThreadLocal.remove();
        resultTypeTL.remove();
        if (gfshFileLogger.fineEnabled()) {
          gfshFileLogger.fine("ThreadLocal state cleaned");
        }
      } catch (Exception e) {
        gfshFileLogger.warning("Error cleaning ThreadLocal state", e);
      }

      // 6. Set exit request (signal shutdown is complete)
      if (exitShellRequest == null) {
        exitShellRequest = ExitShellRequest.NORMAL_EXIT;
      }

      if (gfshFileLogger.fineEnabled()) {
        gfshFileLogger.fine("Shell closed successfully");
      }
    } catch (Exception e) {
      // Log but don't throw - we're shutting down anyway
      gfshFileLogger.severe("Error during shell shutdown", e);
    }
  }

  public ExitShellRequest getExitShellRequest() {
    return exitShellRequest;
  }

  public void waitForComplete() throws InterruptedException {
    runner.join();
  }

  /*
   * If an init file is provided, as a system property or in the default location, run it as a
   * command script.
   */
  private void executeInitFileIfPresent() {

    String initFileName = gfshConfig.getInitFileName();
    if (initFileName != null) {
      gfshFileLogger.info("Using " + initFileName);
      try {
        File gfshInitFile = new File(initFileName);
        boolean continueOnError = false;
        executeScript(gfshInitFile, isQuietMode(), continueOnError);
      } catch (Exception exception) {
        gfshFileLogger.severe(initFileName, exception);
        setLastExecutionStatus(-1);
      }
    }

  }

  /**
   * See findResources in {@link AbstractShell}
   */
  protected Collection<URL> findResources(String resourceName) {
    return null;
  }

  /**
   * Returns the {@link GfshExecutionStrategy} used by Gfsh.
   *
   * @return ExecutionStrategy used by Gfsh
   */
  protected GfshExecutionStrategy getExecutionStrategy() {
    return executionStrategy;
  }

  /**
   * Returns the {@link GfshParser} used by Gfsh.
   *
   * @return Parser used by Gfsh
   */
  public GfshParser getParser() {
    return parser;
  }

  public LogWrapper getGfshFileLogger() {
    return gfshFileLogger;
  }

  /**
   * Executes a single command string.
   * It substitutes the variables defined within the command, if any, and then executes it.
   *
   * <p>
   * This method is used for programmatic command execution (e.g., from tests, APIs).
   * Unlike {@link #executeScriptLine(String)}, this method:
   * <ul>
   * <li>Returns a CommandResult object instead of boolean</li>
   * <li>Does not automatically print output</li>
   * <li>Does not add commands to history</li>
   * </ul>
   *
   * <p>
   * Migrated from Spring Shell 1.x to work with direct JLine 3 integration.
   *
   * @param line command string to be executed
   * @return command execution result, or null if input is null/empty
   */
  public CommandResult executeCommand(String line) {
    // 1. Validate input
    if (line == null || line.trim().isEmpty()) {
      return null; // Match Spring Shell 1.x behavior for empty input
    }

    try {
      // 2. Expand properties (${VAR} → value)
      String expandedLine = !line.contains("$") ? line : expandProperties(line);

      // 3. Store mapping for logging (if expansion occurred)
      if (!line.equals(expandedLine)) {
        expandedPropCommandsMap.put(expandedLine, line);
      }

      // 4. Parse the command
      GfshParseResult parseResult;
      try {
        parseResult = parser.parse(expandedLine);
      } catch (IllegalArgumentException e) {
        // Parameter validation error from parser
        String errorMessage = e.getClass().getName() + ": " + e.getMessage();
        ResultModel errorResult = ResultModel.createError(errorMessage);
        setLastExecutionStatus(-1);
        return new CommandResult(errorResult);
      } catch (Exception e) {
        // Unexpected parse error
        logWarning("Error parsing command: " + expandedLine, e);
        ResultModel errorResult = ResultModel.createError("Parse error: " + e.getMessage());
        setLastExecutionStatus(-1);
        return new CommandResult(errorResult);
      }

      // 5. Handle unrecognized command
      if (parseResult == null) {
        String commandName = extractCommandNameForError(expandedLine);
        ResultModel errorResult =
            ResultModel.createError("Command '" + commandName + "' not found");
        setLastExecutionStatus(-1);
        return new CommandResult(errorResult);
      }

      // 6. Execute the command
      Object result;
      try {
        result = executionStrategy.execute(parseResult);
      } catch (Exception e) {
        logWarning("Error executing command: " + expandedLine, e);
        ResultModel errorResult = ResultModel.createError("Execution error: " + e.getMessage());
        setLastExecutionStatus(-1);
        return new CommandResult(errorResult);
      }

      // 7. Convert result to CommandResult
      CommandResult commandResult = convertToCommandResult(result);

      // 8. Update execution status based on result
      if (commandResult != null && Result.Status.ERROR.equals(commandResult.getStatus())) {
        setLastExecutionStatus(-2);
      } else {
        setLastExecutionStatus(0);
      }

      // 9. Log command execution (fine level)
      if (gfshFileLogger.fineEnabled()) {
        logCommandToOutput(expandedLine);
      }

      return commandResult;

    } finally {
      // 10. Clean up mapping
      expandedPropCommandsMap.clear();
    }
  }

  /**
   * Converts execution result to CommandResult.
   * Handles multiple result types that can be returned from command execution.
   *
   * @param result The execution result (can be ResultModel, CommandResult, String, etc.)
   * @return CommandResult representation, never null
   */
  private CommandResult convertToCommandResult(Object result) {
    if (result == null) {
      return new CommandResult(ResultModel.createError("Command returned no result"));
    }

    if (result instanceof CommandResult) {
      return (CommandResult) result;
    }

    if (result instanceof ResultModel) {
      return new CommandResult((ResultModel) result);
    }

    if (result instanceof String) {
      return new CommandResult(ResultModel.createInfo((String) result));
    }

    // Fallback: convert toString() to info result
    return new CommandResult(ResultModel.createInfo(result.toString()));
  }

  /**
   * Executes the given command string. We have over-ridden the behavior to extend the original
   * implementation to store the 'last command execution status'.
   *
   * @param line command string to be executed
   * @return true if execution is successful; false otherwise
   */
  public boolean executeScriptLine(final String line) {
    boolean success = false;
    String withPropsExpanded = line;

    try {
      // expand env property if the string contains $
      if (line.contains("$")) {
        withPropsExpanded = expandProperties(line);
      }
      String logMessage = "Command String to execute .. ";
      if (!line.equals(withPropsExpanded)) {
        if (!isQuietMode()) {
          Gfsh.println("Post substitution: " + withPropsExpanded);
        }
        logMessage = "Command String after substitution : ";
        expandedPropCommandsMap.put(withPropsExpanded, line);
      }
      if (gfshFileLogger.fineEnabled()) {
        gfshFileLogger.fine(logMessage + ArgumentRedactor.redact(withPropsExpanded));
      }
      success = executeScriptLineInternal(withPropsExpanded);
    } catch (Exception e) {
      setLastExecutionStatus(-1);
    } finally { // Add all commands to in-memory GfshHistory
      gfshHistory.setAutoFlush(true);
      gfshHistory.addToHistory(line);
      gfshHistory.setAutoFlush(false);

      // clear the map
      expandedPropCommandsMap.clear();
    }
    return success;
  }

  private boolean executeScriptLineInternal(String line) {
    try {
      // Parse the command line
      GfshParseResult parseResult;
      try {
        parseResult = parser.parse(line);
      } catch (IllegalArgumentException e) {
        // Parameter validation error from parser - format and display it
        // Include the exception class name to match Spring Shell 1.x/2.x behavior
        String errorMessage = e.getClass().getName() + ": " + e.getMessage();
        ResultModel errorResult = ResultModel.createError(errorMessage);
        handleExecutionResult(errorResult);
        setLastExecutionStatus(-1);
        return false;
      }

      if (parseResult == null) {
        // Command not recognized or parse failed
        // For multi-word commands, we need to find the longest matching command name
        // Try to extract a meaningful command name for the error message
        String commandName = extractCommandNameForError(line);
        ResultModel errorResult =
            ResultModel.createError("Command '" + commandName + "' not found");
        handleExecutionResult(errorResult);
        setLastExecutionStatus(-1);
        return false;
      }

      // Execute using the execution strategy
      Object result = executionStrategy.execute(parseResult);

      // Handle the execution result
      // This will print output and set execution status
      handleExecutionResult(result);

      // Check if the command was successful based on the execution status
      // getLastExecutionStatus() will have been set by handleExecutionResult
      return getLastExecutionStatus() == 0;
    } catch (Exception e) {
      logWarning("Error executing command: " + line, e);
      setLastExecutionStatus(-1);
      return false;
    }
  }

  /**
   * Extracts a command name from the input line for error reporting.
   * This method attempts to find the longest matching command name by trying
   * progressively longer token combinations (up to 5 words).
   *
   * @param line the command line input
   * @return the best-guess command name for error messages
   */
  private String extractCommandNameForError(String line) {
    if (line == null || line.trim().isEmpty()) {
      return "";
    }

    // Tokenize the line (split on whitespace, but respect quotes)
    String[] tokens = line.trim().split("\\s+");

    // Try to match progressively longer command names (up to 5 words, which covers most cases)
    // Start from the longest possible and work down
    for (int wordCount = Math.min(5, tokens.length); wordCount >= 1; wordCount--) {
      StringBuilder candidateCommand = new StringBuilder();
      for (int i = 0; i < wordCount; i++) {
        if (i > 0) {
          candidateCommand.append(" ");
        }
        // Remove any option prefixes (--) to avoid including them in the command name
        String token = tokens[i];
        if (token.startsWith("--")) {
          break; // Stop at first option
        }
        candidateCommand.append(token);
      }

      String candidate = candidateCommand.toString();
      // For now, just return multi-word candidates if they look reasonable
      // This helps with commands like "alter region", "create region", etc.
      if (wordCount > 1 && candidate.split("\\s+").length == wordCount) {
        return candidate;
      }
    }

    // If no multi-word match found, return just the first token as fallback
    return tokens[0];
  }

  public String interact(String textToPrompt) {
    try {
      return lineReader.readLine(textToPrompt);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String readWithMask(String textToPrompt, Character mask) {
    try {
      return lineReader.readLine(textToPrompt, mask);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void printBannerAndWelcome() {
    printAsInfo(getBanner());
    printAsInfo(getWelcomeMessage());
  }

  public String getBanner() {
    final String sb = "    _________________________     __" + LINE_SEPARATOR
        + "   / _____/ ______/ ______/ /____/ /" + LINE_SEPARATOR
        + "  / /  __/ /___  /_____  / _____  / " + LINE_SEPARATOR
        + " / /__/ / ____/  _____/ / /    / /  " + LINE_SEPARATOR
        + "/______/_/      /______/_/    /_/   " + " " + getVersion()
        + LINE_SEPARATOR;
    return ansiHandler.decorateString(sb, ANSIStyle.BLUE);
  }

  protected String getProductName() {
    return "gfsh";
  }

  public String getVersion() {
    return getVersion(false);
  }

  public String getVersion(boolean full) {
    return full ? getFullVersion() : getShortVersion();
  }

  private String getShortVersion() {
    return getDistributionVersion().getVersion();
  }

  private String getFullVersion() {
    try {
      return ProductVersionUtil.appendFullVersion(new StringBuilder())
          .append(lineSeparator())
          .append(SystemDescription.getRunningOnInfo())
          .append(lineSeparator()).toString();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public String getGeodeSerializationVersion() {
    return KnownVersion.CURRENT.getName();
  }

  public String getWelcomeMessage() {
    return ansiHandler.decorateString("Monitor and Manage " + getDistributionVersion().getName(),
        ANSIStyle.CYAN);
  }

  // Over-ridden to avoid default behavior which is:
  // For Iterable: go through all elements & call toString
  // For others: call toString
  protected void handleExecutionResult(Object result) {
    try {
      if (result instanceof Result) {
        Result commandResult = (Result) result;
        boolean isError = Result.Status.ERROR.equals(commandResult.getStatus());

        if (isError) {
          setLastExecutionStatus(-2);
        } else {
          setLastExecutionStatus(0);
        }

        if (useExternalViewer(commandResult)) {
          // - Save file and pass to less so that viewer can scroll through
          // results
          CliUtils.runLessCommandAsExternalViewer(commandResult);
        } else {
          if (!isScriptRunning) {
            // Normal Command
            while (commandResult.hasNextLine()) {
              String nextLine = commandResult.nextLine();
              write(nextLine, isError);
            }
          } else if (!suppressScriptCmdOutput) {
            // Command is part of script. Show output only when quite=false
            while (commandResult.hasNextLine()) {
              write(commandResult.nextLine(), isError);
            }
          }
          commandResult.resetToFirstLine();
        }

        resultTypeTL.set(null);
      }
      if (result != null && !(result instanceof Result)) {
        printAsInfo(result.toString());
      }
    } catch (Exception e) {
      printAsWarning(e.getMessage());
      logToFile(e.getMessage(), e);
    }
  }

  private boolean useExternalViewer(Result result) {
    boolean flag =
        EXTERNAL_RESULT_VIEWER.equals(getEnvProperty(Gfsh.ENV_APP_RESULT_VIEWER)) && isUnix();
    if (result instanceof CommandResult) {
      CommandResult commandResult = (CommandResult) result;
      resultTypeTL.set(commandResult.getType().equals("info"));
      return flag && !commandResult.getType().equals("info");
    } else {
      return false;
    }
  }

  private boolean isUnix() {
    return !(OS.contains("win"));
  }

  private void write(String message, boolean isError) {
    if (isError) {
      printAsWarning(message);
    } else {
      Gfsh.println(message);
    }
  }

  protected LineReader createConsoleReader() {
    // Check and migrate old history file format before creating LineReader
    migrateHistoryFileIfNeeded();

    // Create GfshCompleter with our parser to enable TAB completion
    GfshCompleter completer = new GfshCompleter(this.parser);

    // Build LineReader with JLine 3, registering our completer for TAB completion
    LineReader lineReader = LineReaderBuilder.builder()
        .terminal(terminal)
        .history(gfshHistory)
        .variable(LineReader.HISTORY_FILE, Paths.get(getHistoryFileName()))
        .variable(LineReader.HISTORY_SIZE, gfshConfig.getHistorySize()) // Set max history size
        .completer(completer) // Enable TAB completion
        .build();

    this.lineReader = lineReader;
    return lineReader;
  }

  /**
   * Checks if the history file exists and is in old JLine 2 format.
   * If so, backs it up and creates a new empty history file.
   */
  private void migrateHistoryFileIfNeeded() {
    try {
      Path historyPath = Paths.get(getHistoryFileName());
      if (!Files.exists(historyPath)) {
        return;
      }

      // Check if file contains old format markers (lines starting with // or complex format)
      java.util.List<String> lines = Files.readAllLines(historyPath);
      boolean hasOldFormat = false;
      for (String line : lines) {
        String trimmed = line.trim();
        // Old format had // comments and complex history format
        if (trimmed.startsWith("//") || trimmed.startsWith("#")) {
          hasOldFormat = true;
          break;
        }
        // JLine 3 format should be simple: just command lines or :time:command format
        // If we see anything complex, assume old format
        if (trimmed.contains(":") && !trimmed.matches("^\\d+:\\d+:.*")) {
          // Might be old format - be conservative and migrate
          hasOldFormat = true;
          break;
        }
      }

      if (hasOldFormat) {
        // Backup old history file
        Path backupPath = historyPath.getParent()
            .resolve(historyPath.getFileName().toString() + ".old");
        Files.move(historyPath, backupPath,
            java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        gfshFileLogger.info("Migrated old history file format. Backup saved to: " + backupPath);
      }
    } catch (IOException e) {
      // Ignore - history migration is not critical
      gfshFileLogger.warning("Could not migrate history file", e);
    }
  }

  protected void logCommandToOutput(String processedLine) {
    String originalString = expandedPropCommandsMap.get(processedLine);
    if (originalString != null) {
      // In history log the original command string & expanded line as a comment
      gfshFileLogger.info(ArgumentRedactor.redact(originalString));
      gfshFileLogger.info(ArgumentRedactor.redact("// Post substitution"));
      gfshFileLogger.info(ArgumentRedactor.redact("//" + processedLine));
    } else {
      gfshFileLogger.info(ArgumentRedactor.redact(processedLine));
    }
  }

  public String versionInfo() {
    return getVersion();
  }

  public int getTerminalHeight() {
    return terminal != null ? terminal.getHeight() : DEFAULT_HEIGHT;
  }

  public int getTerminalWidth() {
    if (terminal != null) {
      return terminal.getWidth();
    }

    Map<String, String> env = System.getenv();
    String columnsFromEnv = env.get("COLUMNS");
    if (columnsFromEnv != null) {
      return Integer.parseInt(columnsFromEnv);
    }

    return DEFAULT_WIDTH;
  }

  /**
   * @return the lastExecutionStatus
   */
  public int getLastExecutionStatus() {
    // APP_LAST_EXIT_STATUS
    return lastExecutionStatus;
  }

  /**
   * Set the last command execution status
   *
   * @param lastExecutionStatus last command execution status
   */
  public void setLastExecutionStatus(int lastExecutionStatus) {
    this.lastExecutionStatus = lastExecutionStatus;
    env.put(ENV_APP_LAST_EXIT_STATUS, String.valueOf(lastExecutionStatus));
  }

  public void printAsInfo(String message) {
    // Always print to stdout for user-facing messages like banner
    println(message);
    // Also log to file if not in headless mode
    if (!isHeadlessMode) {
      logger.info(message);
    }
  }

  public void printAsWarning(String message) {
    if (isHeadlessMode) {
      printlnErr(message);
    } else {
      logger.warn(message);
    }
  }

  public void printAsSevere(String message) {
    if (isHeadlessMode) {
      printlnErr(message);
    } else {
      logger.error(message);
    }
  }

  public void logInfo(String message, Throwable t) {
    // No level enabled check for logger - it prints on console in colors as per level
    if (debugON) {
      logger.info(message, t);
    } else {
      logger.info(message);
    }
    if (gfshFileLogger.infoEnabled()) {
      gfshFileLogger.info(message, t);
    }
  }

  public void logWarning(String message, Throwable t) {
    // No level enabled check for logger - it prints on console in colors as per level
    if (debugON) {
      logger.warn(message, t);
    } else {
      logger.warn(message);
    }
    if (gfshFileLogger.warningEnabled()) {
      gfshFileLogger.warning(message, t);
    }
  }

  public void logSevere(String message, Throwable t) {
    // No level enabled check for logger - it prints on console in colors as per level
    if (debugON) {
      logger.error(message, t);
    } else {
      logger.error(message);
    }
    if (gfshFileLogger.severeEnabled()) {
      gfshFileLogger.severe(message, t);
    }
  }

  public boolean logToFile(String message, Throwable t) {
    boolean loggedMessage = false;
    if (gfshFileLogger != null) {
      gfshFileLogger.info(message, t);
      loggedMessage = true;
    }
    return loggedMessage;
  }

  public ResultModel executeScript(File scriptFile, boolean quiet, boolean continueOnError) {
    ResultModel result = null;
    String initialIsQuiet = getEnvProperty(ENV_APP_QUIET_EXECUTION);
    try {
      isScriptRunning = true;
      if (scriptFile == null) {
        throw new IllegalArgumentException("Given script file is null.");
      } else if (!scriptFile.exists()) {
        throw new IllegalArgumentException("Given script file does not exist.");
      } else if (scriptFile.exists() && scriptFile.isDirectory()) {
        throw new IllegalArgumentException(scriptFile.getPath() + " is a directory.");
      }

      ScriptExecutionDetails scriptInfo = new ScriptExecutionDetails(scriptFile.getPath());
      if (scriptFile.exists()) {
        setEnvProperty(ENV_APP_QUIET_EXECUTION, String.valueOf(quiet));
        suppressScriptCmdOutput = quiet;
        BufferedReader reader = new BufferedReader(new FileReader(scriptFile));
        String lineRead = "";
        StringBuilder linesBuffer = new StringBuilder();
        String linesBufferString = "";
        int commandSrNum = 0;
        CommentSkipHelper commentSkipper = new CommentSkipHelper();

        LINEREAD_LOOP: while (exitShellRequest == null && (lineRead = reader.readLine()) != null) {
          if (linesBuffer == null) {
            linesBuffer = new StringBuilder();
          }
          String lineWithoutComments = commentSkipper.skipComments(lineRead);
          if (lineWithoutComments == null || lineWithoutComments.isEmpty()) {
            continue;
          }

          if (linesBuffer.length() != 0) {// add " " between lines
            linesBuffer.append(" ");
          }
          linesBuffer.append(lineWithoutComments);
          linesBufferString = linesBuffer.toString();
          // NOTE: Similar code is in promptLoop()
          if (!linesBufferString.endsWith(GfshParser.CONTINUATION_CHARACTER)) { // see 45893

            List<String> commandList = MultiCommandHelper.getMultipleCommands(linesBufferString);
            for (String cmdLet : commandList) {
              if (!cmdLet.isEmpty()) {
                String redactedCmdLet = ArgumentRedactor.redact(cmdLet);
                ++commandSrNum;
                Gfsh.println(commandSrNum + ". Executing - " + redactedCmdLet);
                Gfsh.println();
                boolean executeSuccess = executeScriptLine(cmdLet);
                if (!executeSuccess) {
                  setLastExecutionStatus(-1);
                }
                scriptInfo.addCommandAndStatus(cmdLet,
                    getLastExecutionStatus() == -1 || getLastExecutionStatus() == -2 ? "FAILED"
                        : "PASSED");
                if ((getLastExecutionStatus() == -1 || getLastExecutionStatus() == -2)
                    && !continueOnError) {
                  break LINEREAD_LOOP;
                }
              }
            }

            // reset buffer
            linesBuffer = null;
            linesBufferString = null;
          } else {
            linesBuffer.deleteCharAt(linesBuffer.length() - 1);
          }
        }
        reader.close();
      } else {
        throw new CommandProcessingException(scriptFile.getPath() + " doesn't exist.",
            CommandProcessingException.ARGUMENT_INVALID, scriptFile);
      }
      result = scriptInfo.getResult();
      scriptInfo.logScriptExecutionInfo(gfshFileLogger, result);
      if (quiet) {
        // Create empty result when in quiet mode
        result = ResultModel.createInfo("");
      }
    } catch (IOException e) {
      throw new CommandProcessingException("Error while reading file " + scriptFile,
          CommandProcessingException.RESOURCE_ACCESS_ERROR, e);
    } finally {
      // reset to original Quiet Execution value
      setEnvProperty(ENV_APP_QUIET_EXECUTION, initialIsQuiet);
      isScriptRunning = false;
    }

    return result;
  }


  public String setEnvProperty(String propertyName, String propertyValue) {
    if (propertyName == null || propertyValue == null) {
      throw new IllegalArgumentException(
          "Environment Property name and/or value can not be set to null.");
    }
    if (propertyName.startsWith("SYS") || readonlyAppEnv.contains(propertyName)) {
      throw new IllegalArgumentException("The Property " + propertyName + " can not be modified.");
    }
    return env.put(propertyName, propertyValue);
  }

  public String getEnvProperty(String propertyName) {
    return env.get(propertyName);
  }

  public String getEnvAppContextPath() {
    String path = getEnvProperty(Gfsh.ENV_APP_CONTEXT_PATH);
    if (path == null) {
      return "";
    }
    return path;
  }

  public Map<String, String> getEnv() {
    return new TreeMap<>(env);
  }

  public boolean isQuietMode() {
    return Boolean.parseBoolean(env.get(ENV_APP_QUIET_EXECUTION));
  }

  /**
   * Main interactive prompt loop for the shell.
   *
   * <p>
   * Reads commands from the user and executes them until exit is requested.
   * Supports multi-line commands using the continuation character (backslash).
   *
   * <p>
   * <b>Multi-line Command Support:</b>
   * <ul>
   * <li>Lines ending with '\' are accumulated into a buffer</li>
   * <li>Secondary prompt ('>') indicates continuation mode</li>
   * <li>Ctrl-C cancels multi-line input and resets buffer</li>
   * <li>Empty Enter skips to next prompt (resets if in continuation mode)</li>
   * <li>Complete command executes when line doesn't end with '\'</li>
   * </ul>
   *
   * <p>
   * <b>JLine 3 Migration Notes:</b>
   * <ul>
   * <li>Replaced JLine 2's CursorBuffer with StringBuilder accumulation</li>
   * <li>Matches pattern used in executeScript() for consistency</li>
   * <li>User cannot edit previous lines after pressing Enter</li>
   * </ul>
   *
   * @see #executeScript(File, boolean, boolean) for similar multi-line handling
   * @see GfshParser#CONTINUATION_CHARACTER
   */
  public void promptLoop() {
    String line = null;
    String prompt = getPromptText();
    gfshHistory.setAutoFlush(false);
    multiLineBuffer = null; // Initialize multi-line buffer

    // NOTE: Similar code is in executeScript()
    while (exitShellRequest == null && (line = readLine(lineReader, prompt)) != null) {
      // Skip empty input (from Ctrl-C or Enter on empty line)
      if (line.trim().isEmpty()) {
        // Reset multi-line buffer if user cancels with Ctrl-C
        if (multiLineBuffer != null) {
          multiLineBuffer = null;
          prompt = getPromptText(); // Back to primary prompt
        } else {
          prompt = getPromptText();
        }
        continue;
      }

      // Accumulate multi-line input
      if (multiLineBuffer == null) {
        multiLineBuffer = new StringBuilder();
      } else {
        // Add space between continued lines
        multiLineBuffer.append(" ");
      }
      multiLineBuffer.append(line);

      String accumulatedCommand = multiLineBuffer.toString();

      if (!accumulatedCommand.endsWith(GfshParser.CONTINUATION_CHARACTER)) {
        // Complete command - execute it
        List<String> commandList = MultiCommandHelper.getMultipleCommands(accumulatedCommand);
        for (String cmdLet : commandList) {
          String trimmedCommand = cmdLet.trim();
          if (!trimmedCommand.isEmpty()) {
            CommandResult result = executeCommand(cmdLet);
            // Display result in interactive mode
            if (result != null) {
              handleExecutionResult(result);
            }
          }
        }
        // Reset buffer and prompt after execution
        multiLineBuffer = null;
        prompt = getPromptText();
      } else {
        // Incomplete command - remove trailing backslash and continue
        multiLineBuffer.deleteCharAt(multiLineBuffer.length() - 1);
        prompt = getDefaultSecondaryPrompt();
      }
    }
    if (line == null) {
      // Possibly Ctrl-D was pressed on empty prompt. ConsoleReader.readLine
      // returns null on Ctrl-D
      exitShellRequest = ExitShellRequest.NORMAL_EXIT;
      gfshFileLogger.info("Exiting gfsh, it seems Ctrl-D was pressed.");
    }
    println((line == null ? LINE_SEPARATOR : "") + "Exiting... ");
  }

  String getDefaultSecondaryPrompt() {
    return ansiHandler.decorateString(DEFAULT_SECONDARY_PROMPT, ANSIStyle.YELLOW);
  }

  public boolean isConnectedAndReady() {
    return operationInvoker != null && operationInvoker.isConnected() && operationInvoker.isReady();
  }

  public static boolean isCurrentInstanceConnectedAndReady() {
    return (getCurrentInstance() != null && getCurrentInstance().isConnectedAndReady());
  }

  /**
   * @return the operationInvoker
   */
  public OperationInvoker getOperationInvoker() {
    return operationInvoker;
  }

  /**
   * @param operationInvoker the operationInvoker to set
   */
  public void setOperationInvoker(final OperationInvoker operationInvoker) {
    this.operationInvoker = operationInvoker;
  }

  public GfshConfig getGfshConfig() {
    return gfshConfig;
  }

  protected String getHistoryFileName() {
    return gfshConfig.getHistoryFileName();
  }

  public void clearHistory() {
    // Clear in-memory history (JLine 3 DefaultHistory has purge method)
    try {
      gfshHistory.purge();
    } catch (IOException e) {
      printAsWarning("Failed to clear history: " + e.getMessage());
    }
    if (!gfshConfig.deleteHistoryFile()) {
      printAsWarning("Gfsh history file is not deleted");
    }
  }

  public String getLogFilePath() {
    return gfshConfig.getLogFilePath();
  }

  public boolean isLoggingEnabled() {
    return gfshConfig.isLoggingEnabled();
  }

  protected String getPromptText() {
    String defaultPrompt = gfshConfig.getDefaultPrompt();
    String contextPath = "";
    String clusterString = "";

    if (getOperationInvoker() != null && isConnectedAndReady()) {
      int clusterId = getOperationInvoker().getClusterId();
      if (clusterId != OperationInvoker.CLUSTER_ID_WHEN_NOT_CONNECTED) {
        clusterString = "Cluster-" + clusterId + " ";
      }
    }

    defaultPrompt = MessageFormat.format(defaultPrompt, clusterString, contextPath);

    return ansiHandler.decorateString(defaultPrompt, ANSIStyle.YELLOW);
  }

  public void notifyDisconnect(String endPoints) {
    String message =
        CliStrings.format(CliStrings.GFSH__MSG__NO_LONGER_CONNECTED_TO_0, new Object[] {endPoints});

    // Connection disconnections during normal operations:
    // 1. exitShellRequest != null: Intentional GFSH shutdown - log at INFO
    // 2. exitShellRequest == null: Server shutdown or network issue
    // - In production: User needs to know (log at SEVERE for console visibility)
    // - In tests: Expected during cleanup (but we can't distinguish here)
    //
    // GEODE-10466: During test cleanup, servers shut down before GFSH disconnects,
    // triggering background heartbeat threads to detect "connection lost" and call
    // this method with exitShellRequest=null, causing SEVERE logs that fail tests.
    //
    // Solution: Always log at INFO level to file logger (for debugging), but only
    // print to console as SEVERE in interactive mode (not headless/test mode).
    if (exitShellRequest != null) {
      // Normal shutdown - connection close is expected
      printAsInfo(LINE_SEPARATOR + message);
      if (gfshFileLogger.infoEnabled()) {
        gfshFileLogger.info(message);
      }
    } else {
      // Connection lost while still connected
      // In interactive mode: Show as error to user
      // In headless/test mode: Just log at INFO to avoid polluting test logs
      if (!isHeadlessMode) {
        printAsSevere(LINE_SEPARATOR + message);
      }
      // Always log to file at INFO level for debugging
      if (gfshFileLogger.infoEnabled()) {
        gfshFileLogger.info(message);
      }
    }

    // Reset prompt path to default after disconnect (Shell 3.x uses env property)
    // Shell 1.x: setPromptPath() method updated prompt directly
    // Shell 3.x: Set ENV_APP_CONTEXT_PATH; getPromptText() reads it dynamically
    setEnvProperty(ENV_APP_CONTEXT_PATH, getEnvAppContextPath());
  }

  public boolean getDebug() {
    return debugON;
  }

  public void setDebug(boolean flag) {
    debugON = flag;
  }

  public boolean isHeadlessMode() {
    return isHeadlessMode;
  }

  public GfshHistory getGfshHistory() {
    return gfshHistory;
  }

  protected String expandProperties(final String input) {
    String output = input;
    try (Scanner s = new Scanner(output)) {
      String foundInLine;
      while ((foundInLine = s.findInLine("(\\$[\\{]\\w+[\\}])")) != null) {
        String envProperty = getEnvProperty(extractKey(foundInLine));
        envProperty = envProperty != null ? envProperty : "";
        output = output.replace(foundInLine, envProperty);
      }
    }
    return output;
  }

  @Override
  public void run() {
    try {
      // Print banner before initializing terminal to ensure it's visible
      printBannerAndWelcome();
      // Initialize terminal and line reader before starting prompt loop
      if (!isHeadlessMode) {
        initializeTerminal();
        createConsoleReader();
      }
      promptLoop();
    } catch (Exception e) {
      gfshFileLogger.severe("Error in shell main loop", e);
    }
  }

  /**
   * Initializes the JLine 3 Terminal for interactive shell use.
   * This must be called before creating the LineReader.
   */
  private void initializeTerminal() throws IOException {
    if (terminal == null) {
      terminal = TerminalBuilder.builder()
          .system(true)
          .build();
    }
  }
}
