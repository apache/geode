package com.gemstone.gemfire.management.internal.cli.shell;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.junit.UnitTest;

@Category(UnitTest.class)
public class GfshConfigInitFileJUnitTest {
  private static final String INIT_FILE_NAME = GfshConfig.DEFAULT_INIT_FILE_NAME;
  private static final String INIT_FILE_PROPERTY = GfshConfig.INIT_FILE_PROPERTY;

  private static String saveUserDir;
  private static String saveUserHome;

  @Rule
  public TemporaryFolder temporaryFolder_HomeDirectory = new TemporaryFolder();
  @Rule
  public TemporaryFolder temporaryFolder_CurrentDirectory = new TemporaryFolder();
  @Rule
  public TemporaryFolder temporaryFolder_AnotherDirectory = new TemporaryFolder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    saveUserDir = System.getProperty("user.dir");
    saveUserHome = System.getProperty("user.home");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
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

  @Before
  public void setUp() throws Exception {
    String userDir = temporaryFolder_CurrentDirectory.getRoot()
        .getAbsolutePath();
    String userHome = temporaryFolder_HomeDirectory.getRoot().getAbsolutePath();

    System.setProperty("user.dir", userDir);
    System.setProperty("user.home", userHome);

    // Abort all tests if system properties cannot be overridden
    assertEquals("user.dir", System.getProperty("user.dir"), userDir);
    assertEquals("user.home", System.getProperty("user.home"), userHome);
  }

  // If a file is given to the constructor, all other options should be ignored
  @Test
  public void constructorArgumentUsed() throws Exception {
    temporaryFolder_HomeDirectory.newFile(INIT_FILE_NAME);
    temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    System.setProperty(INIT_FILE_PROPERTY, temporaryFolder_AnotherDirectory
        .newFile(INIT_FILE_NAME).getAbsolutePath());

    String argument = temporaryFolder_AnotherDirectory.newFile("junit")
        .getAbsolutePath();

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig("historyFileName", "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, argument);

    String result = gfshConfig.getInitFileName();

    assertEquals(argument, result);
  }

  // System property should be chosen ahead of current and home directories
  @Test
  public void systemPropertySelectedFirst() throws Exception {
    temporaryFolder_HomeDirectory.newFile(INIT_FILE_NAME);
    temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME);
    String fileName = temporaryFolder_AnotherDirectory.newFile(INIT_FILE_NAME)
        .getAbsolutePath();
    System.setProperty(INIT_FILE_PROPERTY, fileName);

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig("historyFileName", "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, null);

    String result = gfshConfig.getInitFileName();

    assertEquals(fileName, result);
  }

  // Current directory file selected ahead of home directory file
  @Test
  public void currentDirectorySelectedSecond() throws Exception {
    temporaryFolder_HomeDirectory.newFile(INIT_FILE_NAME);
    String fileName = temporaryFolder_CurrentDirectory.newFile(INIT_FILE_NAME)
        .getAbsolutePath();

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig("historyFileName", "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, null);

    String result = gfshConfig.getInitFileName();

    assertEquals(fileName, result);
  }

  // Home directory file selected if only one present
  @Test
  public void homeDirectorySelectedThird() throws Exception {
    String fileName = temporaryFolder_HomeDirectory.newFile(INIT_FILE_NAME)
        .getAbsolutePath();

    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig("historyFileName", "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, null);

    String result = gfshConfig.getInitFileName();

    assertEquals(fileName, result);
  }

  // No files need match
  @Test
  public void noMatches() throws Exception {
    /*
     * String historyFileName, String defaultPrompt, int historySize, String
     * logDir, Level logLevel, Integer logLimit, Integer logCount, String
     * initFileName
     */
    GfshConfig gfshConfig = new GfshConfig("historyFileName", "", 0,
        temporaryFolder_CurrentDirectory.getRoot().getAbsolutePath(), null,
        null, null, null);

    String result = gfshConfig.getInitFileName();

    assertNull(result);
  }

}
