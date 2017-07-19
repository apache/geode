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
package org.apache.geode.test.dunit.rules.gfsh;

import java.io.File;
import java.util.List;

public class GfshExecution {
  private final Process process;
  private final File workingDir;
  private final ProcessLogger processLogger;

  protected GfshExecution(Process process, File workingDir) {
    this.process = process;
    this.workingDir = workingDir;
    this.processLogger = new ProcessLogger(process, workingDir.getName());
  }

  public List<String> getStdOutLines() {
    return processLogger.getStdOutLines();
  }

  public List<String> getStdErrLines() {
    return processLogger.getStdErrLines();
  }

  /**
   * Note that this is the working directory of gfsh itself. If your script started a server or
   * locator, this will be the parent directory of that member's working directory.
   */
  public File getWorkingDir() {
    return workingDir;
  }

  public Process getProcess() {
    return this.process;
  }
}
