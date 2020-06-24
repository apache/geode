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
package org.apache.geode.management.internal.cli.shell;

import static java.lang.System.lineSeparator;
import static java.nio.charset.Charset.defaultCharset;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createFile;
import static org.apache.commons.io.FileUtils.writeStringToFile;
import static org.apache.geode.management.internal.cli.shell.GfshConfig.DEFAULT_INIT_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

/**
 * Unit tests for supplying an init file to Gfsh.
 *
 * <p>
 * Makes use of reflection to reset private static variables on some classes to replace loggers that
 * would otherwise clutter the console.
 */
@Ignore("Rewrite GfshInitFileIntegrationTest without using reflection to change Gfsh statics")
public class GfshInitFileIntegrationTest {

  private static final int BANNER_LINE_COUNT = 1;
  private static final int INIT_FILE_CITATION_LINE_COUNT = 1;

  private static Logger logger;
  private static Handler[] savedHandlers;

  private CollectingHandler collectingHandler;

  private Path gfshHistoryFilePath;
  private Path gfshLogDirPath;
  private Path initFilePath;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  /**
   * Turn off console logging from JUL for the duration of this class's tests, to reduce
   * noise level of output in automated build.
   */
  @BeforeClass
  public static void setUpBeforeClass() {
    logger = Logger.getLogger("");
    savedHandlers = logger.getHandlers();
    for (Handler handler : savedHandlers) {
      logger.removeHandler(handler);
    }
  }

  /**
   * Restore logging to state prior to the execution of this class
   */
  @AfterClass
  public static void tearDownAfterClass() {
    for (Handler handler : savedHandlers) {
      logger.addHandler(handler);
    }

    Gfsh.getCurrentInstance().stop();
  }

  @Before
  public void setUp() throws Exception {
    gfshHistoryFilePath = temporaryFolder.getRoot().toPath().resolve("history").toAbsolutePath();
    gfshLogDirPath = temporaryFolder.getRoot().toPath().resolve("logs").toAbsolutePath();
    initFilePath =
        temporaryFolder.getRoot().toPath().resolve(DEFAULT_INIT_FILE_NAME).toAbsolutePath();

    createFile(gfshHistoryFilePath);
    createDirectory(gfshLogDirPath);

    collectingHandler = new CollectingHandler();
    initializeGfshStatics(logger, collectingHandler);

    Gfsh.gfshout = new PrintStream(new ByteArrayOutputStream());
  }

  @After
  public void tearDown() {
    logger.removeHandler(collectingHandler);
  }

  @Test
  public void initFile_isNull() {
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, null);
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (success is zero)")
        .isZero();

    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT);

    assertThat(collectingHandler.getLogThrowables())
        .as("log throwables")
        .isEmpty();
  }

  @Test
  public void initFile_doesNotExist() {
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (failure is non-zero)")
        .isNotZero();

    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 1);

    assertThat(collectingHandler.getLogThrowables())
        .as("log throwables")
        .isNotEmpty();
  }

  @Test
  public void initFile_existsButIsEmpty() throws Exception {
    createFile(initFilePath);
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (success is zero)")
        .isZero();

    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 1);

    assertThat(collectingHandler.getLogThrowables())
        .as("log throwables")
        .isEmpty();
  }

  @Test
  public void initFile_existsAndContains_oneGoodCommand() throws Exception {
    writeStringToFile(initFilePath.toFile(), "echo --string=hello" + lineSeparator(),
        defaultCharset());
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (success is zero)")
        .isZero();

    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 1);

    assertThat(collectingHandler.getLogThrowables())
        .as("log throwables")
        .isEmpty();
  }

  @Test
  public void initFile_existsAndContains_twoGoodCommands() throws Exception {
    writeStringToFile(initFilePath.toFile(), "echo --string=hello" + lineSeparator(),
        defaultCharset());
    writeStringToFile(initFilePath.toFile(), "echo --string=goodbye" + lineSeparator(),
        defaultCharset(), true);
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (success is zero)")
        .isZero();

    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 1);

    assertThat(collectingHandler.getLogThrowables())
        .as("log throwables")
        .isEmpty();
  }

  @Test
  public void initFile_existsAndContains_oneBadCommand() throws Exception {
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset());
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (failure is non-zero)")
        .isNotZero();

    // after upgrading to Spring-shell 1.2, the bad command exception is logged as well
    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 2);
  }

  @Test
  public void initFile_existsAndContains_twoBadCommands() throws Exception {
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset());
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset(), true);
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (failure is non-zero)")
        .isNotZero();

    // after upgrading to Spring-shell 1.2, the bad command exception is logged as well
    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 2);
  }

  @Test
  public void initFile_existsAndContains_badAndGoodCommands() throws Exception {
    writeStringToFile(initFilePath.toFile(), "fail" + lineSeparator(), defaultCharset());
    writeStringToFile(initFilePath.toFile(), "echo --string=goodbye" + lineSeparator(),
        defaultCharset(), true);
    GfshConfig gfshConfig = new GfshConfig(gfshHistoryFilePath.toString(), "", 0,
        gfshLogDirPath.toString(), null, null, null, initFilePath.toString());
    Gfsh gfsh = Gfsh.getInstance(false, null, gfshConfig, new ServiceLoaderModuleService(
        LogService.getLogger()));

    assertThat(gfsh.getLastExecutionStatus())
        .as("gfsh last execution status (failure is non-zero)")
        .isNotZero();

    // after upgrading to Spring-shell 1.2, the bad command exception is logged as well
    assertThat(collectingHandler.getLogMessages())
        .as("log messages")
        .hasSize(BANNER_LINE_COUNT + INIT_FILE_CITATION_LINE_COUNT + 2);
  }

  private static void initializeGfshStatics(Logger logger, Handler handler)
      throws NoSuchFieldException, IllegalAccessException {
    // Null out static instance so can reinitialise
    Field gfsh_instance = Gfsh.class.getDeclaredField("instance");
    gfsh_instance.setAccessible(true);
    gfsh_instance.set(null, null);

    LogWrapper gfshFileLogger = LogWrapper.getInstance(null);
    Field logWrapper_INSTANCE = LogWrapper.class.getDeclaredField("INSTANCE");
    logWrapper_INSTANCE.setAccessible(true);
    logWrapper_INSTANCE.set(null, gfshFileLogger);

    Field logWrapper_logger = LogWrapper.class.getDeclaredField("logger");
    logWrapper_logger.setAccessible(true);
    logger.addHandler(handler);
    logWrapper_logger.set(gfshFileLogger, logger);
  }

  /**
   * Log handler for testing. Capture logged messages for later inspection.
   *
   * @see Handler#publish(LogRecord)
   */
  private static class CollectingHandler extends Handler {

    private final List<LogRecord> log;

    CollectingHandler() {
      log = new CopyOnWriteArrayList<>();
    }

    Collection<String> getLogMessages() {
      return log.stream()
          .map(LogRecord::getMessage)
          .collect(Collectors.toList());
    }

    Collection<Throwable> getLogThrowables() {
      return log.stream()
          .map(LogRecord::getThrown)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }

    @Override
    public void publish(LogRecord record) {
      log.add(record);
    }

    @Override
    public void flush() {
      // nothing
    }

    @Override
    public void close() throws SecurityException {
      // nothing
    }
  }
}
