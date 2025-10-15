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
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;

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
  protected static final Logger logger = Logger.getLogger(Gfsh.class.getName());

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

    // 3. log system properties & gfsh environment TODO: change GFSH to use Geode logging
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

  // See 46369
  // TODO: Adapt this for JLine 3 LineReader when needed
  private static String readLine(LineReader reader, String prompt) throws IOException {
    // Simplified for now - JLine 3 has different API
    String readLine = reader.readLine(prompt);
    return readLine;
  }

  private static String removeBackslash(String result) {
    if (result.endsWith(GfshParser.CONTINUATION_CHARACTER)) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * This method sets the parent of all loggers whose name starts with "java" or "javax" to
   * LogWrapper.
   *
   * logWrapper disables any parents's log handler, and only logs to the file if specified. This
   * would prevent JDK's logging show up in the console
   */
  public void redirectInternalJavaLoggers() {
    // Do we need to this on re-connect?
    LogManager logManager = LogManager.getLogManager();

    try {
      Enumeration<String> loggerNames = logManager.getLoggerNames();

      while (loggerNames.hasMoreElements()) {
        String loggerName = loggerNames.nextElement();
        if (loggerName.startsWith("java.") || loggerName.startsWith("javax.")) {
          Logger javaLogger = logManager.getLogger(loggerName);
          /*
           * From Java Docs: It is also important to note that the Logger associated with the String
           * name may be garbage collected at any time if there is no strong reference to the
           * Logger. The caller of this method must check the return value for null in order to
           * properly handle the case where the Logger has been garbage collected.
           */
          if (javaLogger != null) {
            gfshFileLogger.setParentFor(javaLogger);
          }
        }
      }
    } catch (SecurityException e) {
      gfshFileLogger.warning(e.getMessage(), e);
    }
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
    // TODO: Implement closeShell() for Spring Shell 3.x
    // closeShell();
    LogWrapper.close();
    if (operationInvoker != null && operationInvoker.isConnected()) {
      operationInvoker.stop();
    }
    instance = null;
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
   * @param line command string to be executed
   * @return command execution result.
   */
  public CommandResult executeCommand(String line) {
    String expandedLine = !line.contains("$") ? line : expandProperties(line);
    // TODO: Implement direct command execution logic for Spring Shell 3.x
    return null;
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
      // TODO: Implement executeScriptLine logic for Spring Shell 3.x
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
    if (isHeadlessMode) {
      println(message);
    } else {
      logger.info(message);
    }
  }

  public void printAsWarning(String message) {
    if (isHeadlessMode) {
      printlnErr(message);
    } else {
      logger.warning(message);
    }
  }

  public void printAsSevere(String message) {
    if (isHeadlessMode) {
      printlnErr(message);
    } else {
      logger.severe(message);
    }
  }

  public void logInfo(String message, Throwable t) {
    // No level enabled check for logger - it prints on console in colors as per level
    if (debugON) {
      logger.log(Level.INFO, message, t);
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
      logger.log(Level.WARNING, message, t);
    } else {
      logger.warning(message);
    }
    if (gfshFileLogger.warningEnabled()) {
      gfshFileLogger.warning(message, t);
    }
  }

  public void logSevere(String message, Throwable t) {
    // No level enabled check for logger - it prints on console in colors as per level
    if (debugON) {
      logger.log(Level.SEVERE, message, t);
    } else {
      logger.severe(message);
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

  public void promptLoop() {
    String line = null;
    String prompt = getPromptText();
    try {
      gfshHistory.setAutoFlush(false);
      // NOTE: Similar code is in executeScript()
      // TODO: Adapt for JLine 3 LineReader
      while (exitShellRequest == null && (line = readLine(lineReader, prompt)) != null) {
        if (!line.endsWith(GfshParser.CONTINUATION_CHARACTER)) { // see 45893
          List<String> commandList = MultiCommandHelper.getMultipleCommands(line);
          for (String cmdLet : commandList) {
            String trimmedCommand = cmdLet.trim();
            if (!trimmedCommand.isEmpty()) {
              executeCommand(cmdLet);
            }
          }
          prompt = getPromptText();
        } else {
          prompt = getDefaultSecondaryPrompt();
          // TODO: Adapt cursor buffer handling for JLine 3
          // reader.getCursorBuffer().cursor = 0;
          // reader.getCursorBuffer().write(removeBackslash(line) + LINE_SEPARATOR);
        }
      }
      if (line == null) {
        // Possibly Ctrl-D was pressed on empty prompt. ConsoleReader.readLine
        // returns null on Ctrl-D
        exitShellRequest = ExitShellRequest.NORMAL_EXIT;
        gfshFileLogger.info("Exiting gfsh, it seems Ctrl-D was pressed.");
      }
    } catch (IOException e) {
      logSevere(e.getMessage(), e);
    }
    println((line == null ? LINE_SEPARATOR : "") + "Exiting... ");
    // TODO: Implement shell status tracking for Spring Shell 3.x
    // setShellStatus(Status.SHUTTING_DOWN);
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
    printAsSevere(LINE_SEPARATOR + message);
    if (gfshFileLogger.severeEnabled()) {
      gfshFileLogger.severe(message);
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
    // TODO: Implement run() method for Spring Shell 3.x
    // This method should start the prompt loop
    try {
      printBannerAndWelcome();
      promptLoop();
    } catch (Exception e) {
      gfshFileLogger.severe("Error in shell main loop", e);
    }
  }
}
