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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;

public class ReplicatedTest {

  //TODO: parameterize
  private String startScriptFileName = "startAll.sh";
  private String stopScriptFileName = "stopAll.sh";
  private boolean processRunning = false;
  private ShellUtil shell = new ShellUtil();
  private Process process;

  private int waitTimeForScript=60;

  @Test
  public void checkIfScriptsExists() throws IOException {
    ClassLoader classLoader = getClass().getClassLoader();

    File file = new File(classLoader.getResource(startScriptFileName).getFile());
    assertTrue(file.exists());

    file = new File(classLoader.getResource(stopScriptFileName).getFile());
    assertTrue(file.exists());
  }


  private Process start() {
    Process p = shell.executeFile(startScriptFileName).get();
    processRunning = true;
    return p;
  }

  private Process stop() {
    Process p = shell.executeFile(stopScriptFileName).get();
    processRunning = false;
    return p;
  }

  @Test
  public void testStartAndStop() throws InterruptedException {
    boolean status = false;
    int exitCode = -1;

    process = start();
    status =  process.waitFor(waitTimeForScript, TimeUnit.SECONDS);
    exitCode = process.exitValue();
    verify(status, exitCode);

    process = stop();
    status = process.waitFor(waitTimeForScript, TimeUnit.SECONDS);
    exitCode = process.exitValue();
    verify(status, exitCode);
  }

  private void verify(boolean status, int exitCode) {
    assertEquals(exitCode, 0);
    assertEquals(status, true);
  }

  @After
  public void tearDown() {
    if (processRunning) {
      stop();
    }
  }

}
