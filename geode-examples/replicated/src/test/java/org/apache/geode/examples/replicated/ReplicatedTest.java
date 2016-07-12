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
package org.apache.geode.examples.replicated;

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.environment.EnvironmentUtils;
import org.apache.geode.example.utils.ShellUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the shell scripts of the replicated example
 */
public class ReplicatedTest {

  //TODO: parameterize
  public static final String GEODE_LOCATOR_PORT = "GEODE_LOCATOR_PORT=";
  private static final String startScriptFileName = "startAll.sh";
  private static final String stopScriptFileName = "stopAll.sh";
  private static final String pidkillerScriptFileName = "pidkiller.sh";
  private boolean processRunning = false;
  private ShellUtil shell = new ShellUtil();
  private final long scriptTimeout = TimeUnit.SECONDS.toMillis(60);
  private static final Logger logger = Logger.getAnonymousLogger();

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  private int locatorPort;
  private Map environment;

  @Before
  public void setup() throws IOException {
    // ignores test if running on windows
    assumeThat(System.getProperty("os.name").startsWith("Windows"), is(false));

    locatorPort = getAvailablePort();
    environment = EnvironmentUtils.getProcEnvironment();
    EnvironmentUtils.addVariableToEnvironment(environment, GEODE_LOCATOR_PORT + locatorPort);
    logger.fine("Locator port: " + locatorPort);
  }

  @Test
  public void checkIfScriptsExistsAndAreExecutable() throws IOException {
    assertTrue(shell.getFileFromClassLoader(startScriptFileName).map(x -> x.isFile()).orElse(false));
    assertTrue(shell.getFileFromClassLoader(stopScriptFileName).map(x -> x.isFile()).orElse(false));
  }

  @Test
  public void executeStartThenStopScript() throws InterruptedException, IOException {
    final int exitCodeStart = executeScript(startScriptFileName);
    assertEquals(0, exitCodeStart);

    final int exitCodeStop = executeScript(stopScriptFileName);
    assertEquals(0, exitCodeStop);
  }

  @Test
  public void failToStopWhenNoServersAreRunning() throws InterruptedException, IOException {
    final int exitCode;

    exitCode = executeScript(stopScriptFileName);
    assertEquals(1, exitCode);
  }

  /**
   * Execute the kill script that looks for pid files
   * @throws IOException
   * @throws InterruptedException
   */
  private void runKillScript() throws IOException, InterruptedException {
    CommandLine cmdLine = CommandLine.parse(shell.getFileFromClassLoader(pidkillerScriptFileName)
                                                 .map(x -> x.getAbsolutePath())
                                                 .orElseThrow(IllegalArgumentException::new));
    cmdLine.addArgument(testFolder.getRoot().getAbsolutePath());

    DefaultExecuteResultHandler resultHandler = shell.execute(cmdLine, scriptTimeout, environment, testFolder
      .getRoot());
    resultHandler.waitFor(scriptTimeout);
  }

  /**
   * Given a script file name, runs the script and return the exit code.
   * If exitCode != 0 extract and prints exception.
   * @param scriptName
   * @return <code>int</code> with exitCode
   * @throws IOException
   * @throws InterruptedException
   */
  private int executeScript(String scriptName) throws IOException, InterruptedException {
    final int exitCode;
    DefaultExecuteResultHandler resultHandler = shell.execute(scriptName, scriptTimeout, environment, testFolder
      .getRoot());
    processRunning = true;
    resultHandler.waitFor();

    logger.finest(String.format("Executing %s...", scriptName));
    exitCode = resultHandler.getExitValue();

    // extract and log exception if any happened
    if (exitCode != 0) {
      ExecuteException executeException = resultHandler.getException();
      logger.log(Level.SEVERE, executeException.getMessage(), executeException);
    }
    return exitCode;
  }

  @After
  public void tearDown() {
    if (processRunning) {
      try {
        runKillScript();
      } catch (IOException | InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Get a random available port
   * @return <code>int</code>  port number
   */
  private static int getAvailablePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ioex) {
      logger.log(Level.SEVERE, ioex.getMessage(), ioex);
    }
    throw new IllegalStateException("No TCP/IP ports available.");
  }

}
