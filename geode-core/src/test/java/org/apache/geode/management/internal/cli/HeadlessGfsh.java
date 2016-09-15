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
package org.apache.geode.management.internal.cli;

import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.shell.GfshConfig;
import org.apache.geode.management.internal.cli.shell.jline.GfshUnsupportedTerminal;
import jline.console.ConsoleReader;
import org.springframework.shell.core.ExitShellRequest;
import org.springframework.shell.event.ShellStatus.Status;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


/**
 * This is headless shell which can be used to submit random commands and get command-result It is used for commands
 * testing but can be used as for anything like programmatically sending commands to operate on GemFire Distributed
 * systems. TODO : Merge HeadlessGfsh and HeadlessGfshShell TODO : Provide constructor for optionally specifying
 * GfshConfig to provide logDirectory and logLevel
 *
 */
@SuppressWarnings("rawtypes")
public class HeadlessGfsh implements ResultHandler {

  public static final String ERROR_RESULT = "_$_ERROR_RESULT";

  private HeadlessGfshShell shell = null;
  private LinkedBlockingQueue queue = new LinkedBlockingQueue<>();
  private long timeout = 20;
  public String outputString = null;

  public HeadlessGfsh(String name, int timeout, String parentDir) throws ClassNotFoundException, IOException {
    this(name, timeout, null, parentDir);
  }

  public HeadlessGfsh(String name, int timeout, Properties envProps, String parentDir) throws ClassNotFoundException, IOException {
    this.timeout = timeout;
    System.setProperty("jline.terminal", GfshUnsupportedTerminal.class.getName());
    this.shell = new HeadlessGfshShell(name, this, parentDir);
    this.shell.setEnvProperty(Gfsh.ENV_APP_RESULT_VIEWER, "non-basic");

    if (envProps != null) {
      for (String key : envProps.stringPropertyNames()) {
        this.shell.setEnvProperty(key, envProps.getProperty(key));
      }
    }

    // This allows us to avoid race conditions during startup - in particular a NPE on the ConsoleReader which is
    // created in a separate thread during start()
    CountDownLatch shellStarted = new CountDownLatch(1);
    this.shell.addShellStatusListener((oldStatus, newStatus) -> {
      if (newStatus.getStatus() == Status.STARTED) {
        shellStarted.countDown();
      }
    });

    this.shell.start();
    this.setThreadLocalInstance();

    try {
      shellStarted.await();
    } catch (InterruptedException e) {
      e.printStackTrace(System.out);
    }
  }

  public void setThreadLocalInstance() {
    shell.setThreadLocalInstance();
  }

  //TODO : Have non-blocking method also where we move executeCommand call to separate thread-pool
  public boolean executeCommand(String command) {
    boolean status = false;
    try {
      outputString = null;
      status = shell.executeScriptLine(command);
    } catch (Exception e) {
      outputString = e.getMessage();
    }
    return status;
  }

  public int getCommandExecutionStatus() {
    return shell.getCommandExecutionStatus();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleExecutionResult(Object result, String sysout) {
    queue.add(result);
    outputString = sysout;
  }

  public Object getResult() throws InterruptedException {
    //Dont wait for when some command calls gfsh.stop();
    if (shell.stopCalledThroughAPI) return null;
    try {
      Object result = queue.poll(timeout, TimeUnit.SECONDS);
      queue.clear();
      return result;
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public void clear() {
    queue.clear();
    outputString = null;
  }

  public void clearEvents() {
    queue.clear();
    outputString = null;
  }

  public void terminate() {
    shell.terminate();
  }

  public boolean isConnectedAndReady() {
    return shell.isConnectedAndReady();
  }

  public String getErrorString() {
    return shell.errorString;
  }

  public boolean hasError() {
    return shell.hasError();
  }

  public String getError() {
    return shell.errorString;
  }

  public static class HeadlessGfshShell extends Gfsh {

    private ResultHandler handler = null;
    private final Lock lock = new ReentrantLock();
    private final Condition endOfShell = lock.newCondition();
    private ByteArrayOutputStream output = null;
    private String errorString = null;
    private boolean hasError = false;
    boolean stopCalledThroughAPI = false;

    protected HeadlessGfshShell(String testName, ResultHandler handler, String parentDir) throws ClassNotFoundException, IOException {
      super(false, new String[]{}, new HeadlessGfshConfig(testName, parentDir));
      this.handler = handler;
    }

    public void setThreadLocalInstance() {
      gfshThreadLocal.set(this);
    }

    protected void handleExecutionResult(Object result) {
      if (!result.equals(ERROR_RESULT)) {
        super.handleExecutionResult(result);
        handler.handleExecutionResult(result, output.toString());
        output.reset();
      } else {
        //signal waiting queue with error condition with empty output
        output.reset();
        handler.handleExecutionResult(result, output.toString());
      }
    }

    int getCommandExecutionStatus() {
      return getLastExecutionStatus();
    }

    public void terminate() {
      closeShell();
      stopPromptLoop();
      stop();
    }

    public void stop() {
      stopCalledThroughAPI = true;
    }

    private void stopPromptLoop() {
      lock.lock();
      try {
        endOfShell.signalAll();
      } finally {
        lock.unlock();
      }
    }

    public String getErrorString() {
      return errorString;
    }

    public boolean hasError() {
      return hasError;
    }

    /**
     * We override this method just to fool runner thread in reading from nothing. It waits for Condition endOfShell
     * which is signalled when terminate is called. This achieves clean shutdown of runner thread.
     */
    @Override
    public void promptLoop() {
      lock.lock();
      try {
        while (true) {
          try {
            endOfShell.await();
          } catch (InterruptedException e) {
            //e.printStackTrace();
          }
          this.exitShellRequest = ExitShellRequest.NORMAL_EXIT;
          setShellStatus(Status.SHUTTING_DOWN);
          break;
        }
      } finally {
        lock.unlock();
      }
    }

    private static void setGfshOutErr(PrintStream outToUse) {
      Gfsh.gfshout = outToUse;
      Gfsh.gfsherr = outToUse;
    }

    /**
     * This prints out error messages when Exceptions occur in shell. Capture it and set error flag=true and send
     * ERROR_RESULT on the queue to signal thread waiting for CommandResult
     */
    @Override
    public void logWarning(String message, Throwable t) {
      super.logWarning(message, t);
      errorString = message;
      hasError = true;
      //signal waiting queue with error condition
      handleExecutionResult(ERROR_RESULT);
    }

    /**
     * This prints out error messages when Exceptions occur in shell. Capture it and set error flag=true and send
     * ERROR_RESULT on the queue to signal thread waiting for CommandResult
     */
    @Override
    public void logSevere(String message, Throwable t) {
      super.logSevere(message, t);
      errorString = message;
      hasError = true;
      //signal waiting queue with error condition
      handleExecutionResult(ERROR_RESULT);
    }

    /**
     * Setup console-reader to capture Shell output
     */
    @Override
    protected ConsoleReader createConsoleReader() {
      try {
        output = new ByteArrayOutputStream(1024 * 10);
        PrintStream sysout = new PrintStream(output);
        setGfshOutErr(sysout);
        return new ConsoleReader(new FileInputStream(FileDescriptor.in), sysout);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * HeadlessGfshConfig for tests. Taken from TestableGfsh
   */
  static class HeadlessGfshConfig extends GfshConfig {
    {
      // set vm as a gfsh vm
      CliUtil.isGfshVM = true;
    }

    private File parentDir;
    private String fileNamePrefix;
    private String name;
    private String generatedHistoryFileName = null;

    public HeadlessGfshConfig(String name, String parentDir) {
      this.name = name;

      if (isDUnitTest(this.name)) {
        fileNamePrefix = this.name;
      } else {
        fileNamePrefix = "non-hydra-client";
      }

      this.parentDir = new File(parentDir);
      this.parentDir.mkdirs();
    }

    private static boolean isDUnitTest(String name) {
      boolean isDUnitTest = false;
      if (name != null) {
        String[] split = name.split("_");
        if (split.length != 0 && split[0].endsWith("DUnitTest")) {
          isDUnitTest = true;
        }
      }
      return isDUnitTest;
    }

    @Override
    public String getLogFilePath() {
      return new File(parentDir, getFileNamePrefix() + "-gfsh.log").getAbsolutePath();
    }

    private String getFileNamePrefix() {
      String timeStamp = new java.sql.Time(System.currentTimeMillis()).toString();
      timeStamp = timeStamp.replace(':', '_');
      return fileNamePrefix + "-" + timeStamp;
    }

    @Override
    public String getHistoryFileName() {
      if (generatedHistoryFileName == null) {
        String fileName = new File(parentDir, (getFileNamePrefix() + "-gfsh.history")).getAbsolutePath();
        generatedHistoryFileName = fileName;
        return fileName;
      } else {
        return generatedHistoryFileName;
      }
    }

    @Override
    public boolean isTestConfig() {
      return true;
    }

    @Override
    public Level getLogLevel() {
      // Keep log level fine for tests
      return Level.FINE;
    }
  }

}
