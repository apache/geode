/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import jline.ConsoleReader;
import jline.Terminal;

import org.springframework.shell.core.AbstractShell;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.ExecutionStrategy;
import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.core.JLineLogHandler;
import org.springframework.shell.core.JLineShell;
import org.springframework.shell.core.Parser;
import org.springframework.shell.event.ShellStatus.Status;

import com.gemstone.gemfire.internal.Banner;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.lang.ClassUtils;
import com.gemstone.gemfire.internal.process.signal.AbstractSignalNotificationHandler;
import com.gemstone.gemfire.internal.util.SunAPINotFoundException;
import com.gemstone.gemfire.management.cli.CommandProcessingException;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.parser.SyntaxConstants;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData.SectionResultData;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.shell.jline.ANSIHandler;
import com.gemstone.gemfire.management.internal.cli.shell.jline.ANSIHandler.ANSIStyle;
import com.gemstone.gemfire.management.internal.cli.shell.jline.GfshHistory;
import com.gemstone.gemfire.management.internal.cli.shell.jline.GfshUnsupportedTerminal;
import com.gemstone.gemfire.management.internal.cli.shell.unsafe.GfshSignalHandler;
import com.gemstone.gemfire.management.internal.cli.util.CommentSkipHelper;

/**
 * Extends an interactive shell provided by <a
 * href="https://github.com/SpringSource/spring-shell">Spring Shell</a> library.
 * <p />
 * This class is used to plug-in implementations of the following Spring (Roo)
 * Shell components customized to suite GemFire Command Line Interface (CLI)
 * requirements:<ul>
 * <li> <code>org.springframework.roo.shell.ExecutionStrategy</code>
 * <li> <code>org.springframework.roo.shell.Parser</code>
 * </ul>
 * <p />
 * Additionally, this class is used to maintain GemFire SHell (gfsh) specific
 * information like: environment TODO
 *
 * @author Abhishek Chaudhari
 * @author Nikhil Jadhav
 *
 * @since 7.0
 */
public class Gfsh extends JLineShell {
  public static final int     DEFAULT_APP_FETCH_SIZE                 = 1000;
  public static final int     DEFAULT_APP_LAST_EXIT_STATUS           = 0;
  public static final int     DEFAULT_APP_COLLECTION_LIMIT           = 20;
  public static final boolean DEFAULT_APP_QUIET_EXECUTION            = false;
  public static final String  DEFAULT_APP_QUERY_RESULTS_DISPLAY_MODE = "table";
  public static final String  DEFAULT_APP_RESULT_VIEWER = "basic";
  public static final String  EXTERNAL_RESULT_VIEWER = "external";

  public static final String GFSH_APP_NAME = "gfsh";

  public static final String LINE_INDENT = "    ";
  public static final String LINE_SEPARATOR = System.getProperty("line.separator");

  private static final String DEFAULT_SECONDARY_PROMPT = ">";

  // Default Window dimensions
  public static final int DEFAULT_WIDTH  = 100;
  private static final int DEFAULT_HEIGHT = 100;

  public static final String ENV_APP_NAME                       = "APP_NAME";
  public static final String ENV_APP_CONTEXT_PATH               = "APP_CONTEXT_PATH";
  public static final String ENV_APP_FETCH_SIZE                 = "APP_FETCH_SIZE";
  public static final String ENV_APP_LAST_EXIT_STATUS           = "APP_LAST_EXIT_STATUS";
  public static final String ENV_APP_COLLECTION_LIMIT           = "APP_COLLECTION_LIMIT";
  public static final String ENV_APP_QUERY_RESULTS_DISPLAY_MODE = "APP_QUERY_RESULTS_DISPLAY_MODE";
  public static final String ENV_APP_QUIET_EXECUTION            = "APP_QUIET_EXECUTION";
  public static final String ENV_APP_LOGGING_ENABLED            = "APP_LOGGING_ENABLED";
  public static final String ENV_APP_LOG_FILE                   = "APP_LOG_FILE";
  public static final String ENV_APP_PWD                        = "APP_PWD";
  public static final String ENV_APP_RESULT_VIEWER              = "APP_RESULT_VIEWER";

  // Environment Properties taken from the OS
  public static final String ENV_SYS_USER                       = "SYS_USER";
  public static final String ENV_SYS_USER_HOME                  = "SYS_USER_HOME";
  public static final String ENV_SYS_HOST_NAME                  = "SYS_HOST_NAME";
  public static final String ENV_SYS_CLASSPATH                  = "SYS_CLASSPATH";
  public static final String ENV_SYS_JAVA_VERSION               = "SYS_JAVA_VERSION";
  public static final String ENV_SYS_OS                         = "SYS_OS";
  public static final String ENV_SYS_OS_LINE_SEPARATOR          = "SYS_OS_LINE_SEPARATOR";
  public static final String ENV_SYS_GEMFIRE_DIR                = "SYS_GEMFIRE_DIR";

  // TODO merge find a better place for these
  // SSL Configuration properties. keystore/truststore type is not include
  public static final String SSL_KEYSTORE                       = "javax.net.ssl.keyStore";
  public static final String SSL_KEYSTORE_PASSWORD   = "javax.net.ssl.keyStorePassword";
  public static final String SSL_TRUSTSTORE          = "javax.net.ssl.trustStore";
  public static final String SSL_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  public static final String SSL_ENABLED_CIPHERS     = "javax.rmi.ssl.client.enabledCipherSuites";
  public static final String SSL_ENABLED_PROTOCOLS   = "javax.rmi.ssl.client.enabledProtocols";

  protected static PrintStream gfshout = System.out;
  protected static PrintStream gfsherr = System.err;

//  private static final String ANIMATION_SLOT = "A"; //see 46072

  private static       Gfsh   instance;
  private static final Object INSTANCE_LOCK = new Object();

  private final Map<String, String> env     = new TreeMap<String, String>();
  private final List<String> readonlyAppEnv = new ArrayList<String>();

  // Map to keep reference to actual user specified Command String
  // Should always have one value at the max
  private final Map<String, String> expandedPropCommandsMap = new HashMap<String, String>();

  private final CommandManager    commandManager;
  private final ExecutionStrategy executionStrategy;
  private final GfshParser        parser;
  private       OperationInvoker  operationInvoker;
  private       int               lastExecutionStatus;
  private       Thread            runner;
  private       boolean           debugON;
  private final LogWrapper        gfshFileLogger;
  private final GfshConfig        gfshConfig;
  private final GfshHistory       gfshHistory;
  private final ANSIHandler       ansiHandler;
  private       Terminal          terminal;
  private final boolean           isHeadlessMode;

  private AbstractSignalNotificationHandler signalHandler;
  //This flag is used to restrict column trimming to table only types
  private static ThreadLocal<Boolean> resultTypeTL = new ThreadLocal<Boolean>();

  protected Gfsh() throws ClassNotFoundException, IOException {
    this(null);
  }

  /**
   * Create a GemFire shell with console using the specified arguments.
   *
   * @param args
   *          arguments to be used to create a GemFire shell instance
   * @throws IOException
   * @throws ClassNotFoundException
   */
  protected Gfsh(String[] args) throws ClassNotFoundException, IOException {
    this(true, args, new GfshConfig());
  }

  /**
   * Create a GemFire shell using the specified arguments. Console for user
   * inputs is made available if <code>launchShell</code> is set to
   * <code>true</code>.
   *
   * @param launchShell
   *          whether to make Console available
   * @param args
   *          arguments to be used to create a GemFire shell instance or execute
   *          command
   * @throws IOException
   * @throws ClassNotFoundException
   */
  protected Gfsh(boolean launchShell, String[] args, GfshConfig gfshConfig) throws ClassNotFoundException, IOException {
    //1. Disable suppressing of duplicate messages
    JLineLogHandler.setSuppressDuplicateMessages(false);

    //2. set & use gfshConfig
    this.gfshConfig = gfshConfig;
    this.gfshFileLogger = LogWrapper.getInstance();
    this.gfshFileLogger.configure(this.gfshConfig);
    this.ansiHandler = ANSIHandler.getInstance(this.gfshConfig.isANSISupported()); //TODO - Abhishek : should take it from ConsoleReader.terminal??

    /*3. log system properties & gfsh environment */
    this.gfshFileLogger.info(Banner.getString(args));

    //4. Customized History implementation
    this.gfshHistory = new GfshHistory();

    //5. Create CommandManager & load Commands & Converters
    this.commandManager = CommandManager.getInstance();

    //6. Set System Environment here
    initializeEnvironment();
    //7. Create Roo/SpringShell framework objects
    this.executionStrategy = new GfshExecutionStrategy(this);
    this.parser = new GfshParser(commandManager);
    //8. Set max History file size
    setHistorySize(gfshConfig.getHistorySize());

    String envProps = env.toString();
    envProps = envProps.substring(1, envProps.length() - 1);
    envProps = envProps.replaceAll(",", LINE_SEPARATOR);
    this.gfshFileLogger.config("***** gfsh Environment ******" + LINE_SEPARATOR + envProps);

    if (this.gfshFileLogger.fineEnabled()) {
      String gfshConfigStr = this.gfshConfig.toString();
      gfshConfigStr = gfshConfigStr.substring(0, gfshConfigStr.length() - 1);
      gfshConfigStr = gfshConfigStr.replaceAll(",", LINE_SEPARATOR);
      this.gfshFileLogger.fine("***** gfsh Configuration ******" + LINE_SEPARATOR + gfshConfigStr);
    }

    // Setup signal handler for various signals (such as CTRL-C)...
    try {
      ClassUtils.forName("sun.misc.Signal", new SunAPINotFoundException(
        "WARNING!!! Not running a Sun JVM.  Could not find the sun.misc.Signal class; Signal handling disabled."));
      signalHandler = (CliUtil.isGfshVM() ? new GfshSignalHandler() : new AbstractSignalNotificationHandler() { });
    }
    catch (SunAPINotFoundException e) {
      signalHandler = new AbstractSignalNotificationHandler() { };
      this.gfshFileLogger.warning(e.getMessage());
    }

    // For test code only
    if (this.gfshConfig.isTestConfig()) {
      instance = this;
    }
    this.isHeadlessMode = !launchShell;
    if (this.isHeadlessMode) {
      this.gfshFileLogger.config("Running in headless mode");
      // disable jline terminal
      System.setProperty("jline.terminal", GfshUnsupportedTerminal.class.getName());
      env.put(ENV_APP_QUIET_EXECUTION, String.valueOf(true));
      // redirect Internal Java Loggers Now
      // When run with shell, this is done on connection. See 'connect' command.
      redirectInternalJavaLoggers();
      // AbstractShell.logger - we don't want it to log on screen
      LogWrapper.getInstance().setParentFor(logger);
    }
  }

  /**
   * Initializes default environment variables to default values
   */
  private void initializeEnvironment() {
    env.put(ENV_SYS_USER,              System.getProperty("user.name"));
    env.put(ENV_SYS_USER_HOME,         System.getProperty("user.home"));
    env.put(ENV_SYS_HOST_NAME,         System.getProperty("user.name"));
    env.put(ENV_SYS_CLASSPATH,         System.getProperty("java.class.path"));
    env.put(ENV_SYS_JAVA_VERSION,      System.getProperty("java.version"));
    env.put(ENV_SYS_OS,                System.getProperty("os.name"));
    env.put(ENV_SYS_OS_LINE_SEPARATOR, System.getProperty("line.separator"));
    env.put(ENV_SYS_GEMFIRE_DIR,       System.getenv("GEMFIRE"));

    env.put(ENV_APP_NAME,                       com.gemstone.gemfire.management.internal.cli.shell.Gfsh.GFSH_APP_NAME);
    readonlyAppEnv.add(ENV_APP_NAME);
    env.put(ENV_APP_LOGGING_ENABLED,                String.valueOf(!Level.OFF.equals(this.gfshConfig.getLogLevel())) );
    readonlyAppEnv.add(ENV_APP_LOGGING_ENABLED);
    env.put(ENV_APP_LOG_FILE,                   this.gfshConfig.getLogFilePath());
    readonlyAppEnv.add(ENV_APP_LOG_FILE);
    env.put(ENV_APP_PWD,                        System.getProperty("user.dir"));
    readonlyAppEnv.add(ENV_APP_PWD);
// Enable when "use region" command is required. See #46110
//    env.put(CliConstants.ENV_APP_CONTEXT_PATH,               CliConstants.DEFAULT_APP_CONTEXT_PATH);
//    readonlyAppEnv.add(CliConstants.ENV_APP_CONTEXT_PATH);
    env.put(ENV_APP_FETCH_SIZE,                 String.valueOf(DEFAULT_APP_FETCH_SIZE));
    env.put(ENV_APP_LAST_EXIT_STATUS,           String.valueOf(DEFAULT_APP_LAST_EXIT_STATUS));
    readonlyAppEnv.add(ENV_APP_LAST_EXIT_STATUS);
    env.put(ENV_APP_COLLECTION_LIMIT,           String.valueOf(DEFAULT_APP_COLLECTION_LIMIT));
    env.put(ENV_APP_QUERY_RESULTS_DISPLAY_MODE, DEFAULT_APP_QUERY_RESULTS_DISPLAY_MODE);
    env.put(ENV_APP_QUIET_EXECUTION,            String.valueOf(DEFAULT_APP_QUIET_EXECUTION));
    env.put(ENV_APP_RESULT_VIEWER,            String.valueOf(DEFAULT_APP_RESULT_VIEWER));
  }

  public static Gfsh getInstance(boolean launchShell, String[] args, GfshConfig gfshConfig)
    throws ClassNotFoundException, IOException
  {
    if (instance == null) {
      synchronized (INSTANCE_LOCK) {
        if (instance == null) {
          instance = new Gfsh(launchShell, args, gfshConfig);
          instance.executeInitFileIfPresent();
        }
      }
    }

    return instance;
  }

  public AbstractSignalNotificationHandler getSignalHandler() {
    return signalHandler;
  }

  /**
   * Starts this GemFire Shell with console.
   */
  public void start() {
    runner = new Thread(this, getShellName());
    runner.start();
  }

  protected String getShellName() {
    return "Gfsh Launcher";
  }

  /**
   * Stops this GemFire Shell.
   */
  public void stop() {
//    flashMessage("\b"); // see 46072
    closeShell();
    LogWrapper.close();
    if (operationInvoker != null && operationInvoker.isConnected()) {
      operationInvoker.stop();
    }
  }

  public void waitForComplete() throws InterruptedException {
    runner.join();
  }

  /* If an init file is provided, as a system property or in the default
   * location, run it as a command script.
   */
  private void executeInitFileIfPresent() {

    String initFileName = this.gfshConfig.getInitFileName();
    if (initFileName != null) {
      this.gfshFileLogger.info("Using " + initFileName);
      try {
        File gfshInitFile = new File(initFileName);
        boolean continueOnError = false;
        this.executeScript(gfshInitFile, isQuietMode(), continueOnError);
      } catch (Exception exception) {
        this.gfshFileLogger.severe(initFileName, exception);
        setLastExecutionStatus(-1);
      }
    }

  }

  //////////////////// JLineShell Class Methods Start //////////////////////////
  //////////////////////// Implemented Methods ////////////////////////////////
  /**
   * See findResources in {@link AbstractShell}
   */
  @Override
  protected Collection<URL> findResources(String resourceName) {
//    return Collections.singleton(ClassPathLoader.getLatest().getResource(resourceName));
    return null;
  }

  /**
   * Returns the {@link ExecutionStrategy} implementation used by this
   * implementation of {@link AbstractShell}. {@link Gfsh} uses
   * {@link GfshExecutionStrategy}.
   *
   * @return ExecutionStrategy used by Gfsh
   */
  @Override
  protected ExecutionStrategy getExecutionStrategy() {
    return executionStrategy;
  }

  /**
   * Returns the {@link Parser} implementation used by this implementation of
   * {@link AbstractShell}.{@link Gfsh} uses {@link GfshParser}.
   *
   * @return Parser used by Gfsh
   */
  @Override
  protected Parser getParser() {
    return parser;
  }

  //////////////////////// Overridden Behavior /////////////////////////////////
  /**
   * Executes the given command string. We have over-ridden the behavior to
   * extend the original implementation to store the 'last command execution
   * status'.
   *
   * @param line
   *          command string to be executed
   * @return true if execution is successful; false otherwise
   */
  @Override
  public boolean executeCommand(final String line) {
    boolean success = false;
    String withPropsExpanded = line;

    try {
      //expand env property if the string contains $
      if (line.contains("$")) {
        withPropsExpanded = expandProperties(line);
      }
      String logMessage = "Command String to execute .. ";
      if (!line.equals(withPropsExpanded)) {
        if (!isQuietMode()) {
          Gfsh.println("Post substitution: "+withPropsExpanded);
        }
        logMessage = "Command String after substitution : ";
        expandedPropCommandsMap.put(withPropsExpanded, line);
      }
      if (gfshFileLogger.fineEnabled()) {
        gfshFileLogger.fine(logMessage + withPropsExpanded);
      }
      success = super.executeCommand(withPropsExpanded);
    } catch (Exception e) {
      //TODO: should there be a way to differentiate error in shell & error on
      //server. May be by exception type.
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

  public String interact(String textToPrompt) throws IOException {
    return reader.readLine(textToPrompt);
  }

  public String readWithMask(String textToPrompt, Character mask) throws IOException {
    return reader.readLine(textToPrompt, mask);
  }

  public void add(final CommandMarker command) {
    commandManager.add(command);
  }

  public void add(final Converter<?> converter) {
    commandManager.add(converter);
  }

  @Override
  public void printBannerAndWelcome() {
    printAsInfo(getBanner());
    printAsInfo(getWelcomeMessage());
  }

  public String getBanner() {
    StringBuilder sb = new StringBuilder();
    sb.append("    _________________________     __").append(LINE_SEPARATOR);
    sb.append("   / _____/ ______/ ______/ /____/ /").append(LINE_SEPARATOR);
    sb.append("  / /  __/ /___  /_____  / _____  / ").append(LINE_SEPARATOR);
    sb.append(" / /__/ / ____/  _____/ / /    / /  ").append(LINE_SEPARATOR);
    sb.append("/______/_/      /______/_/    /_/   ").append(" ").append(this.getVersion()).append(LINE_SEPARATOR);
    return ansiHandler.decorateString(sb.toString(), ANSIStyle.BLUE);
  }

  @Override
  protected String getProductName() {
    return "gfsh";
  }

  @Override
  public String getVersion() {
    return getVersion(false);
  }

  public String getVersion(boolean full) {
    if (full) {
      return GemFireVersion.asString();
    } else {
      return "v" + GemFireVersion.getGemFireVersion();
    }
  }

  public String getWelcomeMessage() {
    return ansiHandler.decorateString("Monitor and Manage GemFire", ANSIStyle.CYAN);
  }

  //Over-ridden to avoid default behavior which is:
  //For Iterable: go through all elements & call toString
  //For others: call toString
  @Override
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
          CliUtil.runLessCommandAsExternalViewer(commandResult, isError);
        } else {
          while (commandResult.hasNextLine()) {
            write(commandResult.nextLine(), isError);
          }
        } 
        
        resultTypeTL.set(null);

        if (result instanceof CommandResult) {
          CommandResult cmdResult = (CommandResult) result;
          if (cmdResult.hasIncomingFiles()) {
            boolean isAlreadySaved = cmdResult.getNumTimesSaved() > 0;
            if (!isAlreadySaved) {
              cmdResult.saveIncomingFiles(null);
            }
            Gfsh.println();// Empty line
          }
        }
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
    boolean flag = EXTERNAL_RESULT_VIEWER.equals(getEnvProperty(Gfsh.ENV_APP_RESULT_VIEWER)) && isUnix();
    if(result instanceof CommandResult) {
      CommandResult commandResult = (CommandResult) result;
      resultTypeTL.set(commandResult.getType().equals("info"));
      return flag && !commandResult.getType().equals("info");      
    }else
      return false;
  }
  
  public static boolean isInfoResult(){
    if(resultTypeTL.get()==null)
      return false;
    return resultTypeTL.get();
  }

  private static String OS = System.getProperty("os.name").toLowerCase();

  private boolean isUnix() {
    return !(OS.indexOf("win") >= 0);
  }

  private void write(String message, boolean isError) {
    if (isError) {
      printAsWarning(message);
    } else {
      Gfsh.println(message);
    }
  }

  /////// Save multiple-line commands as single line in history - starts ///////
  @Override
  protected ConsoleReader createConsoleReader() {
    ConsoleReader consoleReader = super.createConsoleReader();
    consoleReader.setHistory(gfshHistory);
    terminal = consoleReader.getTerminal();
    return consoleReader;
  }

  @Override
  protected void logCommandToOutput(String processedLine) {
    String originalString = expandedPropCommandsMap.get(processedLine);
    if (originalString != null) {
      // In history log the original command string & expanded line as a comment
      super.logCommandToOutput(GfshHistory.toHistoryLoggable(originalString));
      super.logCommandToOutput(GfshHistory.toHistoryLoggable("// Post substitution"));
      super.logCommandToOutput(GfshHistory.toHistoryLoggable("//" + processedLine));
    } else {
      super.logCommandToOutput(GfshHistory.toHistoryLoggable(processedLine));
    }
  }

  @Override
  public String versionInfo() {
    return getVersion();
  }

// causes instability on MacOS See #46072
//  public void flashMessage(String message) {
//    if (reader != null) {
//      flash(Level.FINE, message, ANIMATION_SLOT);
//    }
//  }
  /////// Save multiple-line commands as single line in history - ends /////////
  ///////////////////// JLineShell Class Methods End  //////////////////////////

  public int getTerminalHeight() {
    return terminal != null ? terminal.getTerminalHeight() : DEFAULT_HEIGHT;
  }

  public int getTerminalWidth() {
    if (terminal != null) {
      return terminal.getTerminalWidth();
    }

    Map<String, String> env = System.getenv();
    String columnsFromEnv = env.get("COLUMNS");
    if (columnsFromEnv != null) {
      return Integer.parseInt(columnsFromEnv);
    }

    return DEFAULT_WIDTH;
  }

  public static void println() {
    gfshout.println();
  }

  public static <T>void println(T toPrint) {
    gfshout.println(toPrint);
  }

  public static <T>void print(T toPrint) {
    gfshout.print(toPrint);
  }

  public static <T>void printlnErr(T toPrint) {
    gfsherr.println(toPrint);
  }

  /**
   * @return the lastExecutionStatus
   */
  public int getLastExecutionStatus() {
    //APP_LAST_EXIT_STATUS
    return lastExecutionStatus;
  }

  /**
   * Set the last command execution status
   *
   * @param lastExecutionStatus
   *          last command execution status
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

  public Result executeScript(File scriptFile, boolean quiet, boolean continueOnError) {
    Result result = null;
    String initialIsQuiet = getEnvProperty(ENV_APP_QUIET_EXECUTION);
    try {
      if (scriptFile == null) {
        throw new IllegalArgumentException("Given script file is null.");
      } else if (scriptFile.exists() && scriptFile.isDirectory()) {
        throw new IllegalArgumentException(scriptFile.getPath() + " is a directory.");
      }

      ScriptExecutionDetails scriptInfo = new ScriptExecutionDetails(scriptFile.getPath());
      if (scriptFile.exists()) {
        setEnvProperty(ENV_APP_QUIET_EXECUTION, String.valueOf(quiet));
        BufferedReader reader = new BufferedReader(new FileReader(scriptFile));
        String lineRead = "";
        StringBuilder linesBuffer = new StringBuilder();
        String linesBufferString = ""; // used to check whether the string in a buffer contains a ";".
        int commandSrNum = 0;
        CommentSkipHelper commentSkipper = new CommentSkipHelper();

        LINEREAD_LOOP:
        while (exitShellRequest == null && (lineRead = reader.readLine()) != null) {
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
          if (!linesBufferString.endsWith(SyntaxConstants.CONTINUATION_CHARACTER)) { // see 45893
            //String command = null;
            
            
            List<String> commandList = MultiCommandHelper.getMultipleCommands(linesBufferString);
            for(String cmdLet : commandList) {
              String trimmedCommand = cmdLet.trim();
              if (!trimmedCommand.isEmpty()) {
                ++commandSrNum;
                Gfsh.println(commandSrNum+". Executing - " + cmdLet);                
                Gfsh.println();
                boolean executeSuccess = executeCommand(cmdLet);
                if (!executeSuccess) {
                  setLastExecutionStatus(-1);
                }
                scriptInfo.addCommandAndStatus(cmdLet, getLastExecutionStatus() == -1 || getLastExecutionStatus() == -2 ? "FAILED" : "PASSED");                
                if ((getLastExecutionStatus() == -1 || getLastExecutionStatus() == -2) && !continueOnError) {
                  break LINEREAD_LOOP;
                }
              }
            }
            
            // reset buffer
            linesBuffer       = null;
            linesBufferString = null;
          } else {
            linesBuffer.deleteCharAt(linesBuffer.length() - 1);
          }
        }
        reader.close();
      } else {
        throw new CommandProcessingException(scriptFile.getPath()+" doesn't exist.", CommandProcessingException.ARGUMENT_INVALID, scriptFile);
      }
      result = scriptInfo.getResult();
      scriptInfo.logScriptExecutionInfo(gfshFileLogger, result);
      if (quiet) {
        // Create empty result when in quiet mode
        result = ResultBuilder.createInfoResult("");
      }
    } catch (IOException e) {
      throw new CommandProcessingException("Error while reading file "+scriptFile, CommandProcessingException.RESOURCE_ACCESS_ERROR, e);
    } finally {
      // reset to original Quiet Execution value
      setEnvProperty(ENV_APP_QUIET_EXECUTION, initialIsQuiet);
    }

    return result;
  }

  /////////////// For setting shell environment properties START ///////////////
  public String setEnvProperty(String propertyName, String propertyValue) {
    if (propertyName == null || propertyValue == null) {
      throw new IllegalArgumentException("Environment Property name and/or value can not be set to null.");
    }
    if (propertyName.startsWith("SYS") || readonlyAppEnv.contains(propertyName)) {
      throw new IllegalArgumentException("The Property " + propertyName + " can not be modified.");
    }
    return env.put(propertyName, propertyValue);
  }

  public String getEnvProperty(String propertyName) {
    return env.get(propertyName);
  }

  public Map<String, String> getEnv() {
    Map<String, String> map = new TreeMap<String, String>();
    map.putAll(env);
    return map;
  }

  @Override
  public void setPromptPath(String currentContext) {
    super.setPromptPath(currentContext);
// Enable when "use region" command is required. See #46110
//    env.put(CliConstants.ENV_APP_CONTEXT_PATH, currentContext);
  }

  public boolean isQuietMode() {
//    System.out.println(env.get(CliConstants.ENV_APP_QUIET_EXECUTION));
    return Boolean.parseBoolean(env.get(ENV_APP_QUIET_EXECUTION));
  }

  ////////////////// Providing Multiple-line support starts ///////////////////
  @Override
  public void promptLoop() {
    String line   = null;
    String prompt = getPromptText();
    try {
      gfshHistory.setAutoFlush(false);
      // NOTE: Similar code is in executeScript()
      while (exitShellRequest == null && (line = readLine(reader, prompt)) != null) {
        if (!line.endsWith(SyntaxConstants.CONTINUATION_CHARACTER)) { // see 45893                   
          List<String> commandList = MultiCommandHelper.getMultipleCommands(line);
          for(String cmdLet : commandList) {
            String trimmedCommand = cmdLet.trim();
            if (!trimmedCommand.isEmpty()) {            
              executeCommand(cmdLet);
            }
          }
          prompt = getPromptText();          
        } else {
          prompt = getDefaultSecondaryPrompt();
          reader.getCursorBuffer().cursor = 0;
          reader.getCursorBuffer().write(removeBackslash(line) + LINE_SEPARATOR);
        }
      }
      if (line == null) {
        // Possibly Ctrl-D was pressed on empty prompt. ConsoleReader.readLine
        // returns null on Ctrl-D
        this.exitShellRequest = ExitShellRequest.NORMAL_EXIT;
        gfshFileLogger.info("Exiting gfsh, it seems Ctrl-D was pressed.");
      }
    } catch (IOException e) {
      logSevere(e.getMessage(), e);
    }
    println((line == null ? LINE_SEPARATOR : "") + "Exiting... ");
    setShellStatus(Status.SHUTTING_DOWN);
  }

  // See 46369
  private static String readLine(ConsoleReader reader, String prompt) throws IOException {
    String earlierLine = reader.getCursorBuffer().toString();
    String readLine    = null;
    try {
      readLine = reader.readLine(prompt);
    } catch (IndexOutOfBoundsException e) {
      if (earlierLine.length() == 0) {
        reader.printNewline();
        readLine = LINE_SEPARATOR;
        reader.getCursorBuffer().cursor = 0;
      } else {
        readLine = readLine(reader, prompt);
      }
    }
    return readLine;
  }

  private static String removeBackslash(String result) {
    if (result.endsWith(SyntaxConstants.CONTINUATION_CHARACTER)) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  String getDefaultSecondaryPrompt() {
    return ansiHandler.decorateString(DEFAULT_SECONDARY_PROMPT, ANSIStyle.YELLOW);
  }
  ///////////////// Providing Multiple-line support ends //////////////////////

  /////////////// For setting shell environment properties END /////////////////

  /////////////////////// OperationInvoker code START //////////////////////////
  public boolean isConnectedAndReady() {
    return operationInvoker != null
            && operationInvoker.isConnected()
              && operationInvoker.isReady();
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
  //////////////////////// OperationInvoker code END //////////////////////////

  //////////////////////// Fields for TestableShell Start //////////////////////
  public static boolean SUPPORT_MUTLIPLESHELL = false;
  protected static ThreadLocal<Gfsh> gfshThreadLocal = new ThreadLocal<Gfsh>();
  ////////////////////////Fields for TestableShell End  ////////////////////////

  public static void redirectInternalJavaLoggers() {
    // Do we need to this on re-connect?
    LogManager logManager = LogManager.getLogManager();

    try {
      Enumeration<String> loggerNames = logManager.getLoggerNames();

      while (loggerNames.hasMoreElements()) {
        String loggerName = loggerNames.nextElement();
        if (loggerName.startsWith("java.") || loggerName.startsWith("javax.")) {
//          System.out.println(loggerName);
          Logger javaLogger = logManager.getLogger(loggerName);
          /*
           * From Java Docs: It is also important to note that the Logger
           * associated with the String name may be garbage collected at any
           * time if there is no strong reference to the Logger. The caller of
           * this method must check the return value for null in order to
           * properly handle the case where the Logger has been garbage
           * collected.
           */
          if (javaLogger != null) {
            LogWrapper.getInstance().setParentFor(javaLogger);
          }
        }
      }
    } catch (SecurityException e) {
//      e.printStackTrace();
      LogWrapper.getInstance().warning(e.getMessage(), e);
    }
  }

  public static Gfsh getCurrentInstance() {
    if (!SUPPORT_MUTLIPLESHELL) {
      return instance;
    } else {
      return gfshThreadLocal.get();
    }
  }

  public String obtainHelp(String userInput, Set<String> requiredCommandNames) {
      return parser.obtainHelp(userInput, requiredCommandNames);
  }

  public List<String> obtainHelpCommandNames(String userInput) {
    return parser.obtainHelpCommandNames(userInput);
  }

  public GfshConfig getGfshConfig() {
    return this.gfshConfig;
  }

  public List<String> getCommandNames(String matchingWith) {
    return parser.getCommandNames(matchingWith);
  }

  @Override
  protected String getHistoryFileName() {
    return gfshConfig.getHistoryFileName();
  }

  public String getLogFilePath() {
    return gfshConfig.getLogFilePath();
  }

  public boolean isLoggingEnabled() {
    return gfshConfig.isLoggingEnabled();
  }

  @Override
  protected String getPromptText() {
    String defaultPrompt = gfshConfig.getDefaultPrompt();
    String contextPath = /*getEnvProperty(CliConstants.ENV_APP_CONTEXT_PATH)*/ "";
    String clusterString = "";

    if (getOperationInvoker() != null && isConnectedAndReady()) {
      int clusterId = getOperationInvoker().getClusterId();
      if (clusterId != OperationInvoker.CLUSTER_ID_WHEN_NOT_CONNECTED) {
        clusterString = "Cluster-" + clusterId+" ";
      }
//  //As "use region" is not in scope for 7.0, see 46110.
//      if (contextPath == null) {
//        contextPath = "." + CliConstants.DEFAULT_APP_CONTEXT_PATH;
//      } else {
//        contextPath = "." + contextPath;
//      }
//    } else {
//      contextPath = "." + CliConstants.DEFAULT_APP_CONTEXT_PATH;
    }

    defaultPrompt = MessageFormat.format(defaultPrompt, new Object[] {clusterString, contextPath});

    return ansiHandler.decorateString(defaultPrompt, ANSIStyle.YELLOW);
  }

  public void notifyDisconnect(String endPoints) {
    String message = CliStrings.format(CliStrings.GFSH__MSG__NO_LONGER_CONNECTED_TO_0, new Object[] {endPoints});
    printAsSevere(LINE_SEPARATOR + message);
    if (gfshFileLogger.severeEnabled()) {
      gfshFileLogger.severe(message);
    }
    setPromptPath(com.gemstone.gemfire.management.internal.cli.converters.RegionPathConverter.DEFAULT_APP_CONTEXT_PATH);
  }

  public boolean getDebug(){
    return debugON;
  }

  public void setDebug(boolean flag){
    debugON = flag;
  }

  public boolean isHeadlessMode() {
    return isHeadlessMode;
  }

  public GfshHistory getGfshHistory(){
    return gfshHistory;
  }

  private String expandProperties(final String input) {
    String output = input;
    Scanner s = new Scanner(output);
    String foundInLine = null;
    while ( (foundInLine = s.findInLine("(\\$[\\{]\\w+[\\}])"))  != null) {
      String envProperty = getEnvProperty(extractKey(foundInLine));
      envProperty = envProperty != null ? envProperty : "";
      output = output.replace(foundInLine, envProperty);
    }
    return output;
  }

  private static String extractKey(String input) {
    return input.substring("${".length(), input.length() - "}".length());
  }

  public static ConsoleReader getConsoleReader(){
    Gfsh gfsh = Gfsh.getCurrentInstance();
    return (gfsh == null ? null : gfsh.reader);
  }

  /**
   * Take a string and wrap it into multiple lines separated by CliConstants.LINE_SEPARATOR.
   * Lines are separated based upon the terminal width, separated on word boundaries and may have
   * extra spaces added to provide indentation.
   *
   * For example: if the terminal width were 5 and the string "123 456789 01234" were passed in with
   * an indentation level of 2, then the returned string would be:
   * <pre>
   *         123
   *         45678
   *         9
   *         01234
   * </pre>
   * @param string String to wrap (add breakpoints and indent)
   * @param indentationLevel The number of indentation levels to use.
   * @return The wrapped string.
   */
  public static String wrapText(final String string, final int indentationLevel) {
    Gfsh gfsh = getCurrentInstance();
    if (gfsh == null) {
      return string;
    }

    final int maxLineLength = gfsh.getTerminalWidth() - 1;
    final StringBuffer stringBuf = new StringBuffer();
    int index = 0;
    int startOfCurrentLine = 0;
    while (index < string.length()) {
      // Add the indentation
      for (int i = 0; i < indentationLevel; i++) {
        stringBuf.append(LINE_INDENT);
      }
      int currentLineLength = LINE_INDENT.length() * indentationLevel;

      // Find the end of a line:
      //   1. If the end of string is reached
      //   2. If the width of the terminal has been reached
      //   3. If a newline character was found in the string
      while (index < string.length() && currentLineLength < maxLineLength && string.charAt(index) != '\n') {
        index++;
        currentLineLength++;
      }

      // If the line was terminated with a newline character
      if (index != string.length() && string.charAt(index) == '\n') {
        stringBuf.append(string.substring(startOfCurrentLine, index));
        stringBuf.append(LINE_SEPARATOR);
        index++;
        startOfCurrentLine = index;

      // If the end of the string was reached or the last character just happened to be a space character
      } else if (index == string.length() || string.charAt(index) == ' ') {
        stringBuf.append(string.substring(startOfCurrentLine, index));
        if (index != string.length()) {
          stringBuf.append(LINE_SEPARATOR);
          index++;
        }

      } else {
        final int spaceCharIndex = string.lastIndexOf(" ", index);

        // If no spaces were found then there's no logical wayto split the string
        if (spaceCharIndex == -1) {
          stringBuf.append(string.substring(startOfCurrentLine, index)).append(LINE_SEPARATOR);

          // Else split the string cleanly between words
        } else {
          stringBuf.append(string.substring(startOfCurrentLine, spaceCharIndex)).append(LINE_SEPARATOR);
          index = spaceCharIndex+1;
        }
      }

      startOfCurrentLine = index;
    }
    return stringBuf.toString();
  }

//  // for testing only
//  public static void main(String[] args) {
//    try {
//      Gfsh gfsh = new Gfsh();
//      String expandProperties = gfsh.expandProperties("execute function --id=group-with-arguments-with-result-collector --result-collector=management.operations.ops.FunctionOperations$CustomResultCollector --arguments=group-with-arguments-with-result-collector --group=managed1");
////      String expandProperties = gfsh.expandProperties("My name is ${NAME}");
//      System.out.println(expandProperties);
//    } catch (ClassNotFoundException e) {
//      e.printStackTrace();
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//  }
}

class ScriptExecutionDetails {
  private final String filePath;
  private final List<CommandAndStatus> commandAndStatusList;

  ScriptExecutionDetails(String filePath) {
    this.filePath = filePath;
    this.commandAndStatusList = new ArrayList<CommandAndStatus>();
  }

  void addCommandAndStatus(String command, String status) {
    this.commandAndStatusList.add(new CommandAndStatus(command, status));
  }

  Result getResult() {
    CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
    compositeResultData.setHeader("************************* Execution Summary ***********************\nScript file: " + filePath);

    for (int i = 0; i < this.commandAndStatusList.size(); i++) {
      int commandSrNo = i + 1;
      SectionResultData section = compositeResultData.addSection(""+(i+1));
      CommandAndStatus commandAndStatus = commandAndStatusList.get(i);
      section.addData("Command-"+String.valueOf(commandSrNo), commandAndStatus.command);
      section.addData("Status", commandAndStatus.status);
      if (i != this.commandAndStatusList.size()) {
        section.setFooter(Gfsh.LINE_SEPARATOR);
      }
    }

    return ResultBuilder.buildResult(compositeResultData);
  }

  void logScriptExecutionInfo(LogWrapper logWrapper, Result result) {
    logWrapper.info(ResultBuilder.resultAsString(result));
  }

  static class CommandAndStatus {
    private final String command;
    private final String status;

    public CommandAndStatus(String command, String status) {
      this.command = command;
      this.status = status;
    }

    @Override
    public String toString() {
      return command + "     " +status;
    }
  }
}
