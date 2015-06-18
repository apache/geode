package com.gemstone.gemfire.management.internal.cli.shell;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.junit.UnitTest;

/**
 * <P>
 * Unit tests for supplying an init file to Gfsh.
 * </P>
 * <P>
 * Makes use of reflection to reset private static variables on some classes to
 * replace loggers that would otherwise clutter the console.
 * </P>
 */
@Category(UnitTest.class)
public class GfshInitFileJUnitTest {
  private static final String INIT_FILE_NAME = GfshConfig.DEFAULT_INIT_FILE_NAME;
  private static final boolean APPEND = true;
  private static final int BANNER_LINES = 1;
  private static final int INIT_FILE_CITATION_LINES = 1;

  private static String saveLog4j2Config;
  private static String saveUserDir;
  private static String saveUserHome;
  private static java.util.logging.Logger julLogger;
  private static Handler[] saveHandlers;

  @ClassRule
  public static TemporaryFolder temporaryFolder_Config = new TemporaryFolder();
  @Rule
  public TemporaryFolder temporaryFolder_CurrentDirectory = new TemporaryFolder();

  private ByteArrayOutputStream sysout = new ByteArrayOutputStream();
  private String gfshHistoryFileName;
  private LogWrapper gfshFileLogger;
  private JUnitLoggerHandler junitLoggerHandler;

  /*
   * Turn off console logging from JUL and Log4j2 for the duration of this
   * class's tests, to reduce noise level of output in automated build.
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    saveLog4j2Config = System
        .getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    saveUserDir = System.getProperty("user.dir");
    saveUserHome = System.getProperty("user.home");

    julLogger = java.util.logging.Logger.getLogger("");
    saveHandlers = julLogger.getHandlers();
    for (Handler handler : saveHandlers) {
      julLogger.removeHandler(handler);
    }

    File log4j2XML = temporaryFolder_Config.newFile("log4j2.xml");
    FileUtils.writeStringToFile(log4j2XML, "<Configuration/>", APPEND);
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
        log4j2XML.toURI().toString());
  }

  /*
   * Restore logging to state prior to the execution of this class
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    for (Handler handler : saveHandlers) {
      julLogger.addHandler(handler);
    }

    if (saveLog4j2Config == null) {
      System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    } else {
      System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY,
          saveLog4j2Config);
      ((LoggerContext) LogManager.getContext(false)).reconfigure();
    }

    if (saveUserDir == null) {
      System.clearProperty("user.dir");
    } else {
      System.setProperty("user.dir", saveUserDir);
    }
    if (saveUserHome == null) {
      System.clearProperty("user.home");
    } else {
      System.setProperty("user.home", saveUserHome);
    }
  }

  /*
   * Reset $HOME and current directory for tests, and confirm this has been
   * accepted.
   */
  @Before
  public void setUp() throws Exception {
    // Fake running from home directory
    String userDir = temporaryFolder_CurrentDirectory.getRoot()
        .getAbsolutePath();
    String userHome = userDir;

    System.setProperty("user.dir", userDir);
    System.setProperty("user.home", userHome);

    // Abort all tests if system properties cannot be overridden
    assertEquals("user.dir", System.getProperty("user.dir"), userDir);
    assertEquals("user.home", System.getProperty("user.home"), userHome);
  }

  @Before
  public void setUp2() throws Exception {

    // Null out static instance so can reinitialise
    Field gfsh_instance = Gfsh.class.getDeclaredField("instance");
    gfsh_instance.setAccessible(true);
    gfsh_instance.set(null, null);

    this.junitLoggerHandler = new JUnitLoggerHandler();

    this.gfshFileLogger = LogWrapper.getInstance();
    Field logWrapper_INSTANCE = LogWrapper.class.getDeclaredField("INSTANCE");
    logWrapper_INSTANCE.setAccessible(true);
    logWrapper_INSTANCE.set(null, this.gfshFileLogger);

    Field logWrapper_logger = LogWrapper.class.getDeclaredField("logger");
    logWrapper_logger.setAccessible(true);
    julLogger.addHandler(this.junitLoggerHandler);
    logWrapper_logger.set(this.gfshFileLogger, julLogger);

    Gfsh.gfshout = new PrintStream(sysout);
    this.gfshHistoryFileName = temporaryFolder_CurrentDirectory.newFile(
        "historyFile").getAbsolutePath();
  }

  @After
  public void tearDown() throws Exception {
    julLogger.removeHandler(this.junitLoggerHandler);
  }

  @Test
  public void testInitFile_NotProvided() throws Exception {
    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, null);
    assertNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertEquals("Status 0==success", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  @Test
  public void testInitFile_NotFound() throws Exception {
    // Construct the file name but not the file
    String initFileName = temporaryFolder_CurrentDirectory.getRoot()
        .getAbsolutePath() + File.separator + INIT_FILE_NAME;

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFileName);
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertNotEquals("Status <0==failure", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    Throwable exception = null;
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      if (logRecord.getThrown() != null) {
        exception = logRecord.getThrown();
        break;
      }
    }
    assertNotNull("Exceptions in log", exception);
  }

  @Test
  public void testInitFile_Empty() throws Exception {
    File initFile = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFile.getAbsolutePath());
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertEquals("Status 0==success", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  @Test
  public void testInitFile_OneGoodCommand() throws Exception {
    File initFile = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    FileUtils.writeStringToFile(initFile, "echo --string=hello"
        + Gfsh.LINE_SEPARATOR, APPEND);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFile.getAbsolutePath());
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertEquals("Status 0==success", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  @Test
  public void testInitFile_TwoGoodCommands() throws Exception {
    File initFile = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    FileUtils.writeStringToFile(initFile, "echo --string=hello"
        + Gfsh.LINE_SEPARATOR, APPEND);
    FileUtils.writeStringToFile(initFile, "echo --string=goodbye"
        + Gfsh.LINE_SEPARATOR, APPEND);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFile.getAbsolutePath());
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertEquals("Status 0==success", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  @Test
  public void testInitFile_OneBadCommand() throws Exception {
    File initFile = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    FileUtils.writeStringToFile(initFile, "fail" + Gfsh.LINE_SEPARATOR, APPEND);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFile.getAbsolutePath());
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertNotEquals("Status <0==failure", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  @Test
  public void testInitFile_TwoBadCommands() throws Exception {
    File initFile = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    FileUtils.writeStringToFile(initFile, "fail" + Gfsh.LINE_SEPARATOR, APPEND);
    FileUtils.writeStringToFile(initFile, "fail" + Gfsh.LINE_SEPARATOR, APPEND);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFile.getAbsolutePath());
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertNotEquals("Status <0==failure", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  @Test
  public void testInitFile_BadAndGoodCommands() throws Exception {
    File initFile = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    FileUtils.writeStringToFile(initFile, "fail" + Gfsh.LINE_SEPARATOR, APPEND);
    FileUtils.writeStringToFile(initFile, "echo --string=goodbye"
        + Gfsh.LINE_SEPARATOR, APPEND);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig(this.gfshHistoryFileName, "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, initFile.getAbsolutePath());
    assertNotNull(INIT_FILE_NAME, gfshConfig.getInitFileName());

    /*
     * boolean launchShell, String[] args, GfshConfig gfshConfig
     */
    Gfsh gfsh = Gfsh.getInstance(false, new String[] {}, gfshConfig);

    int actualStatus = gfsh.getLastExecutionStatus();
    int expectedStatus = 0;
    assertNotEquals("Status <0==failure", expectedStatus, actualStatus);

    int expectedLogCount = BANNER_LINES + INIT_FILE_CITATION_LINES + 1;
    assertEquals("Log records written", expectedLogCount,
        this.junitLoggerHandler.getLog().size());
    for (LogRecord logRecord : this.junitLoggerHandler.getLog()) {
      assertNull("No exceptions in log", logRecord.getThrown());
    }
  }

  /**
   * <P>
   * Log handler for testing. Capture logged messages for later inspection.
   * </P>
   * 
   * @see java.util.logging.Handler#publish(java.util.logging.LogRecord)
   */
  private class JUnitLoggerHandler extends Handler {
    private List<LogRecord> log;

    public JUnitLoggerHandler() {
      log = new ArrayList<>();
    }

    public List<LogRecord> getLog() {
      return log;
    }

    @Override
    public void publish(LogRecord record) {
      log.add(record);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws SecurityException {
    }
  }
}
