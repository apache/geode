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
package org.apache.geode.test.dunit.standalone;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_NETWORK_PARTITION_DETECTION;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;

import org.apache.commons.io.FileUtils;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.nio.file.Files;
import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class ProcessManager {
  private int namingPort;
  private Map<Integer, ProcessHolder> processes = new HashMap<>();
  private File log4jConfig;
  private int pendingVMs;
  private Registry registry;
  private int debugPort = Integer.getInteger("dunit.debug.basePort", 0);
  private int suspendVM = Integer.getInteger("dunit.debug.suspendVM", -100);
  private VersionManager versionManager;

  public ProcessManager(int namingPort, Registry registry) {
    this.versionManager = VersionManager.getInstance();
    this.namingPort = namingPort;
    this.registry = registry;
  }

  public synchronized void launchVM(int vmNum) throws IOException {
    launchVM(VersionManager.CURRENT_VERSION, vmNum, false);
  }

  public synchronized void launchVM(String version, int vmNum, boolean bouncedVM)
      throws IOException {
    if (processes.containsKey(vmNum)) {
      throw new IllegalStateException("VM " + vmNum + " is already running.");
    }

    String[] cmd = buildJavaCommand(vmNum, namingPort, version);
    System.out.println("Executing " + Arrays.toString(cmd));
    File workingDir = getVMDir(version, vmNum);
    if (!workingDir.exists()) {
      workingDir.mkdirs();
    } else if (!bouncedVM || DUnitLauncher.MAKE_NEW_WORKING_DIRS) {
      try {
        FileUtils.deleteDirectory(workingDir);
      } catch (IOException e) {
        // This delete is occasionally failing on some platforms, maybe due to a lingering
        // process. Allow the process to be launched anyway.
        System.err.println("Unable to delete " + workingDir + ". Currently contains "
            + Arrays.asList(workingDir.list()));
      }
      workingDir.mkdirs();
    }
    if (log4jConfig != null) {
      FileUtils.copyFileToDirectory(log4jConfig, workingDir);
    }

    // TODO - delete directory contents, preferably with commons io FileUtils
    try {
      Process process = Runtime.getRuntime().exec(cmd, null, workingDir);
      pendingVMs++;
      ProcessHolder holder = new ProcessHolder(process);
      processes.put(vmNum, holder);
      linkStreams(version, vmNum, holder, process.getErrorStream(), System.err);
      linkStreams(version, vmNum, holder, process.getInputStream(), System.out);
    } catch (RuntimeException | Error t) {
      t.printStackTrace();
      throw t;
    }
  }

  public void validateVersion(String version) {
    if (!versionManager.isValidVersion(version)) {
      throw new IllegalArgumentException("Version " + version + " is not configured for use");
    }
  }

  public static File getVMDir(String version, int vmNum) {
    return new File(DUnitLauncher.DUNIT_DIR, VM.getVMName(VersionManager.CURRENT_VERSION, vmNum));
  }

  public synchronized void killVMs() {
    for (ProcessHolder process : processes.values()) {
      if (process != null) {
        process.kill();
      }
    }
  }

  public synchronized boolean hasLiveVMs() {
    for (ProcessHolder process : processes.values()) {
      if (process != null && process.isAlive()) {
        return true;
      }
    }
    return false;
  }

  public synchronized void bounce(String version, int vmNum) {
    if (!processes.containsKey(vmNum)) {
      throw new IllegalStateException("No such process " + vmNum);
    }
    try {
      ProcessHolder holder = processes.remove(vmNum);
      holder.kill();
      holder.getProcess().waitFor();
      System.out.println("Old process for vm_" + vmNum + " has exited");
      launchVM(version, vmNum, true);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException("Unable to restart VM " + vmNum, e);
    }
  }

  private void linkStreams(final String version, final int vmNum, final ProcessHolder holder,
      final InputStream in, final PrintStream out) {
    final String vmName = "[" + VM.getVMName(version, vmNum) + "] ";
    System.out.println("linking IO streams for " + vmName);
    Thread ioTransport = new Thread() {
      public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        try {
          String line = reader.readLine();
          while (line != null) {
            if (line.length() == 0) {
              out.println();
            } else {
              out.print(vmName);
              out.println(line);
            }
            line = reader.readLine();
          }
        } catch (Exception e) {
          if (!holder.isKilled()) {
            out.println("Error transporting IO from child process");
            e.printStackTrace(out);
          }
        }
      }
    };

    ioTransport.setDaemon(true);
    ioTransport.start();
  }

  private String[] buildJavaCommand(int vmNum, int namingPort, String version) {
    String cmd = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    String dunitClasspath = System.getProperty("java.class.path");
    String separator = File.separator;
    String classPath;
    if (VersionManager.isCurrentVersion(version)) {
      classPath = dunitClasspath;
    } else {
      // remove current-version product classes and resources from the classpath
      String buildDir = separator + "geode-core" + separator + "build" + separator;

      String mainClasses = buildDir + "classes" + separator + "main";
      dunitClasspath = removeFromPath(dunitClasspath, mainClasses);

      String mainResources = buildDir + "resources" + separator + "main";
      dunitClasspath = removeFromPath(dunitClasspath, mainResources);

      String generatedResources = buildDir + "generated-resources" + separator + "main";
      dunitClasspath = removeFromPath(dunitClasspath, generatedResources);

      buildDir = separator + "geode-common" + separator + "build" + separator + "classes"
          + separator + "main";
      dunitClasspath = removeFromPath(dunitClasspath, buildDir);

      buildDir = separator + "geode-json" + separator + "build" + separator + "classes" + separator
          + "main";
      dunitClasspath = removeFromPath(dunitClasspath, buildDir);

      classPath = versionManager.getClasspath(version) + File.pathSeparator + dunitClasspath;
    }

    // String tmpDir = System.getProperty("java.io.tmpdir");
    String agent = getAgentString();

    String jdkDebug = "";
    if (debugPort > 0) {
      jdkDebug += ",address=" + debugPort;
      debugPort++;
    }

    String jdkSuspend = vmNum == suspendVM ? "y" : "n"; // ignore version
    ArrayList<String> cmds = new ArrayList<String>();
    cmds.add(cmd);
    cmds.add("-classpath");
    String jreLib = separator + "jre" + separator + "lib" + separator;
    classPath = removeFromPath(classPath, jreLib);
    cmds.add(classPath);
    cmds.add("-D" + DUnitLauncher.RMI_PORT_PARAM + "=" + namingPort);
    cmds.add("-D" + DUnitLauncher.VM_NUM_PARAM + "=" + vmNum);
    cmds.add("-D" + DUnitLauncher.VM_VERSION_PARAM + "=" + version);
    cmds.add("-D" + DUnitLauncher.WORKSPACE_DIR_PARAM + "=" + new File(".").getAbsolutePath());
    if (vmNum >= 0) { // let the locator print a banner
      if (version.equals(VersionManager.CURRENT_VERSION)) { // enable the banner for older versions
        cmds.add("-D" + InternalLocator.INHIBIT_DM_BANNER + "=true");
      }
    } else {
      // most distributed unit tests were written under the assumption that network partition
      // detection is disabled, so we turn it off in the locator. Tests for network partition
      // detection should create a separate locator that has it enabled
      cmds.add(
          "-D" + DistributionConfig.GEMFIRE_PREFIX + ENABLE_NETWORK_PARTITION_DETECTION + "=false");
      cmds.add(
          "-D" + DistributionConfig.GEMFIRE_PREFIX + "allow_old_members_to_join_for_testing=true");
    }
    cmds.add("-D" + LOG_LEVEL + "=" + DUnitLauncher.logLevel);
    if (DUnitLauncher.LOG4J != null) {
      cmds.add("-Dlog4j.configurationFile=" + DUnitLauncher.LOG4J);
    }
    cmds.add("-Djava.library.path=" + System.getProperty("java.library.path"));
    cmds.add("-Xrunjdwp:transport=dt_socket,server=y,suspend=" + jdkSuspend + jdkDebug);
    cmds.add("-XX:+HeapDumpOnOutOfMemoryError");
    cmds.add("-Xmx512m");
    cmds.add("-D" + DistributionConfig.GEMFIRE_PREFIX + "DEFAULT_MAX_OPLOG_SIZE=10");
    cmds.add("-D" + DistributionConfig.GEMFIRE_PREFIX + "disallowMcastDefaults=true");
    cmds.add("-D" + DistributionConfig.RESTRICT_MEMBERSHIP_PORT_RANGE + "=true");
    cmds.add("-ea");
    cmds.add("-XX:MetaspaceSize=512m");
    cmds.add("-XX:+PrintGC");
    cmds.add("-XX:+PrintGCDetails");
    cmds.add("-XX:+PrintGCTimeStamps");
    cmds.add(agent);
    cmds.add(ChildVM.class.getName());
    String[] rst = new String[cmds.size()];
    cmds.toArray(rst);

    return rst;
  }

  private String removeFromPath(String classpath, String partialPath) {
    String[] jars = classpath.split(File.pathSeparator);
    StringBuilder sb = new StringBuilder(classpath.length());
    Boolean firstjar = true;
    for (String jar : jars) {
      if (!jar.contains(partialPath)) {
        if (!firstjar) {
          sb.append(File.pathSeparator);
        }
        sb.append(jar);
        firstjar = false;
      }
    }
    return sb.toString();
  }

  /**
   * Get the java agent passed to this process and pass it to the child VMs. This was added to
   * support jacoco code coverage reports
   */
  private String getAgentString() {
    RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
    if (runtimeBean != null) {
      for (String arg : runtimeBean.getInputArguments()) {
        if (arg.contains("-javaagent:")) {
          // HACK for gradle bug GRADLE-2859. Jacoco is passing a relative path
          // That won't work when we pass this to dunit VMs in a different
          // directory
          arg = arg.replace("-javaagent:..",
              "-javaagent:" + System.getProperty("user.dir") + File.separator + "..");
          arg = arg.replace("destfile=..",
              "destfile=" + System.getProperty("user.dir") + File.separator + "..");
          return arg;
        }
      }
    }

    return "-DdummyArg=true";
  }

  synchronized void signalVMReady() {
    pendingVMs--;
    this.notifyAll();
  }

  public synchronized boolean waitForVMs(long timeout) throws InterruptedException {
    long end = System.currentTimeMillis() + timeout;
    while (pendingVMs > 0) {
      long remaining = end - System.currentTimeMillis();
      if (remaining <= 0) {
        return false;
      }
      this.wait(remaining);
    }

    return true;
  }

  public static class ProcessHolder {
    private final Process process;
    private volatile boolean killed = false;

    public ProcessHolder(Process process) {
      this.process = process;
    }

    public void kill() {
      this.killed = true;
      process.destroy();

    }

    public Process getProcess() {
      return process;
    }

    public boolean isKilled() {
      return killed;
    }

    public boolean isAlive() {
      return !killed && process.isAlive();
    }
  }

  public RemoteDUnitVMIF getStub(int i)
      throws AccessException, RemoteException, NotBoundException, InterruptedException {
    return getStub(VersionManager.CURRENT_VERSION, i);
  }

  public RemoteDUnitVMIF getStub(String version, int i)
      throws AccessException, RemoteException, NotBoundException, InterruptedException {
    waitForVMs(DUnitLauncher.STARTUP_TIMEOUT);
    return (RemoteDUnitVMIF) registry.lookup("vm" + i);
  }

  private static class VersionedVMNumber {
    String version;
    int number;

    VersionedVMNumber(String version, int number) {
      this.version = version;
      this.number = number;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      VersionedVMNumber that = (VersionedVMNumber) o;

      if (number != that.number) {
        return false;
      }
      return version.equals(that.version);
    }

    @Override
    public int hashCode() {
      int result = version.hashCode();
      result = 31 * result + number;
      return result;
    }
  }
}
