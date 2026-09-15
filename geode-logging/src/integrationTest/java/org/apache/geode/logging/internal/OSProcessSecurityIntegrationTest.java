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
package org.apache.geode.logging.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.nio.file.Files;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OSProcessSecurityIntegrationTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void checksRequestedExecutableBeforeLaunchingShell() throws Exception {
    // Exercise the compatibility policy on the two JDKs targeted by this migration.
    assumeTrue(Runtime.version().feature() == 17 || Runtime.version().feature() == 21);
    runProbe("deny-target");
  }

  @Test
  public void launchesWithoutSecurityManager() throws Exception {
    runProbe("no-manager");
  }

  private void runProbe(String policy) throws Exception {
    File output = temporaryFolder.newFile("probe-output.txt");
    List<String> command = new ArrayList<>();
    command.add(javaExecutable());
    if (policy.equals("deny-target")) {
      command.add("-Djava.security.manager=allow");
    }
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(Probe.class.getName());
    command.add(policy);
    command.add(temporaryFolder.getRoot().getAbsolutePath());
    Process process = new ProcessBuilder(command)
        .redirectErrorStream(true).redirectOutput(output).start();
    try {
      assertTrue("Probe timed out", process.waitFor(30, TimeUnit.SECONDS));
      assertEquals(Files.readString(output.toPath()), 0, process.exitValue());
    } finally {
      process.destroyForcibly();
    }
  }

  private static String javaExecutable() {
    String executable = System.getProperty("os.name").startsWith("Windows") ? "java.exe" : "java";
    return new File(new File(System.getProperty("java.home"), "bin"), executable).getAbsolutePath();
  }

  // A separate JVM keeps the process-wide test policy out of other tests.
  public static class Probe {
    @SuppressWarnings("removal")
    public static void main(String[] args) throws Exception {
      String executable = javaExecutable();
      File directory = new File(args[1]);
      File log = new File(directory, "child.log");
      boolean denyTarget = args[0].equals("deny-target");
      if (denyTarget) {
        System.setSecurityManager(new SecurityManager() {
          @Override
          public void checkPermission(Permission permission) {
            // Permit unrelated operations, including loading classes and writing the log.
          }

          @Override
          public void checkExec(String command) {
            if (command.equals(executable)) {
              throw new SecurityException("requested executable denied");
            }
            // Fail before creating a child if the explicit target check is accidentally removed.
            throw new AssertionError("Shell checked before requested executable: " + command);
          }
        });
      }
      try {
        OSProcess.bgexec(new String[] {executable, "-version"}, directory, log, true, null);
        if (denyTarget) {
          throw new AssertionError("Requested executable was not checked");
        }
      } catch (SecurityException expected) {
        if (!denyTarget || !"requested executable denied".equals(expected.getMessage())) {
          throw expected;
        }
        return;
      }
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (System.nanoTime() < deadline) {
        if (log.exists() && Files.readString(log.toPath()).contains("version")) {
          return;
        }
        Thread.sleep(10);
      }
      throw new AssertionError("No java -version output in " + log);
    }
  }
}
