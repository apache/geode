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

package org.apache.geode.example.utils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.ShutdownHookProcessDestroyer;

/**
 * Utility class for executing shell commands using Apache commons-exec
 */
public class ShellUtil {

  private final ClassLoader classLoader = getClass().getClassLoader();

  public Optional<File> getFileFromClassLoader(String fileName) {
    URL resourceURL = classLoader.getResource(fileName);
    return (resourceURL == null) ? Optional.empty() : Optional.of(new File(resourceURL.getFile()));
  }

  public CommandLine parseCommandLine(String fileName) {
    return getFileFromClassLoader(fileName).map(file -> CommandLine.parse(file.getAbsolutePath()))
                                           .orElseThrow(IllegalArgumentException::new);
  }

  public DefaultExecuteResultHandler execute(CommandLine cmdLine, long timeout, Map environment, File dir) throws IOException {
    ExecutorTemplate exampleTestExecutor = new ExecutorTemplate(timeout, dir).invoke();
    DefaultExecutor executor = exampleTestExecutor.getExecutor();
    DefaultExecuteResultHandler resultHandler = exampleTestExecutor.getResultHandler();
    executor.execute(cmdLine, environment, resultHandler);

    return resultHandler;

  }

  public DefaultExecuteResultHandler execute(String fileName, long timeout, Map environment, File dir) throws IOException {
    ExecutorTemplate exampleTestExecutor = new ExecutorTemplate(timeout, dir).invoke();
    DefaultExecutor executor = exampleTestExecutor.getExecutor();
    DefaultExecuteResultHandler resultHandler = exampleTestExecutor.getResultHandler();
    executor.execute(parseCommandLine(fileName), environment, resultHandler);

    return resultHandler;
  }

  /**
   * Executor template for common scenarios
   */
  private static class ExecutorTemplate {

    private final long timeout;
    private final File dir;
    private DefaultExecutor executor;
    private DefaultExecuteResultHandler resultHandler;

    public ExecutorTemplate(final long timeout, final File dir) {
      this.timeout = timeout;
      this.dir = dir;
    }

    public DefaultExecutor getExecutor() {
      return executor;
    }

    public DefaultExecuteResultHandler getResultHandler() {
      return resultHandler;
    }

    public ExecutorTemplate invoke() {
      executor = new DefaultExecutor();
      ExecuteWatchdog watchdog = new ExecuteWatchdog(timeout);
      executor.setWatchdog(watchdog);

      PumpStreamHandler psh = new PumpStreamHandler(System.out, System.err);
      executor.setProcessDestroyer(new ShutdownHookProcessDestroyer());
      executor.setStreamHandler(psh);
      executor.setWorkingDirectory(dir);

      resultHandler = new DefaultExecuteResultHandler();
      return this;
    }
  }
}